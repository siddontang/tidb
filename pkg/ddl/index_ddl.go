// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// ValidateRenameIndex checks if index name is ok to be renamed.
func ValidateRenameIndex(from, to ast.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, false)
	}

	renameIndexes(tblInfo, from, to)
	renameHiddenColumns(tblInfo, from, to)

	if ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(ctx sessionctx.Context, indexName ast.CIStr, invisible bool, tbl *model.TableInfo) (bool, error) {
	var idx *model.IndexInfo
	if idx = tbl.FindIndexByName(indexName.L); idx == nil || idx.State != model.StatePublic {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	}
	if ctx == nil || ctx.GetSessionVars() == nil || ctx.GetSessionVars().StmtCtx.MultiSchemaInfo == nil {
		// Early return.
		if idx.Invisible == invisible {
			return true, nil
		}
	}
	if invisible && idx.IsColumnarIndex() {
		return false, dbterror.ErrUnsupportedIndexType.FastGen("INVISIBLE can not be used in %s INDEX", idx.Tp)
	}
	return false, nil
}

func onAlterIndexVisibility(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	setIndexVisibility(tblInfo, from, invisible)
	if ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func setIndexVisibility(tblInfo *model.TableInfo, name ast.CIStr, invisible bool) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == name.L || idx.GetChangingOriginName() == name.O {
			idx.Invisible = invisible
		}
	}
}

func getNullColInfos(tblInfo *model.TableInfo, cols []*model.IndexColumn) []*model.ColumnInfo {
	nullCols := make([]*model.ColumnInfo, 0, len(cols))
	for _, colName := range cols {
		col := model.FindColumnInfo(tblInfo.Columns, colName.Name.L)
		if !mysql.HasNotNullFlag(col.GetFlag()) || mysql.HasPreventNullInsertFlag(col.GetFlag()) {
			nullCols = append(nullCols, col)
		}
	}
	return nullCols
}

func checkPrimaryKeyNotNull(jobCtx *jobContext, w *worker, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
	if err != nil {
		return nil, err
	}
	nullCols := getNullColInfos(tblInfo, indexInfo.Columns)
	if len(nullCols) == 0 {
		return nil, nil
	}

	err = modifyColsFromNull2NotNull(
		jobCtx.stepCtx,
		w,
		dbInfo,
		tblInfo,
		nullCols,
		&model.ColumnInfo{Name: ast.NewCIStr("")},
		false,
	)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(jobCtx, job, tblInfo, []*model.IndexInfo{indexInfo}, err)
	// TODO: Support non-strict mode.
	// warnings = append(warnings, ErrWarnDataTruncated.GenWithStackByArgs(oldCol.Name.L, 0).Error())
	return nil, err
}

// moveAndUpdateHiddenColumnsToPublic updates the hidden columns to public, and
// moves the hidden columns to proper offsets, so that Table.Columns' states meet the assumption of
// [public, public, ..., public, non-public, non-public, ..., non-public].
func moveAndUpdateHiddenColumnsToPublic(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffset := make(map[int]struct{}, 0)
	for _, col := range idxInfo.Columns {
		if tblInfo.Columns[col.Offset].Hidden {
			hiddenColOffset[col.Offset] = struct{}{}
		}
	}
	if len(hiddenColOffset) == 0 {
		return
	}
	// Find the first non-public column.
	firstNonPublicPos := len(tblInfo.Columns) - 1
	for i, c := range tblInfo.Columns {
		if c.State != model.StatePublic {
			firstNonPublicPos = i
			break
		}
	}
	for _, col := range idxInfo.Columns {
		tblInfo.Columns[col.Offset].State = model.StatePublic
		if _, needMove := hiddenColOffset[col.Offset]; needMove {
			tblInfo.MoveColumnInfo(col.Offset, firstNonPublicPos)
		}
	}
}

func checkAndBuildIndexInfo(
	job *model.Job, tblInfo *model.TableInfo,
	columnarIndexType model.ColumnarIndexType, isPK bool, args *model.IndexArg,
) (*model.IndexInfo, error) {
	var err error
	indexInfo := tblInfo.FindIndexByName(args.IndexName.L)
	if indexInfo != nil {
		if indexInfo.State == model.StatePublic {
			err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s", args.IndexName)
			if isPK {
				err = infoschema.ErrMultiplePriKey
			}
			return nil, err
		}
		return indexInfo, nil
	}

	for _, hiddenCol := range args.HiddenCols {
		columnInfo := model.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
		if columnInfo != nil && columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			// TODO: refine the error message
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
		}
	}

	if len(args.HiddenCols) > 0 {
		for _, hiddenCol := range args.HiddenCols {
			InitAndAddColumnToTable(tblInfo, hiddenCol)
		}
	}
	if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
		return nil, errors.Trace(err)
	}
	indexInfo, err = BuildIndexInfo(
		nil,
		tblInfo,
		args.IndexName,
		isPK,
		args.Unique,
		columnarIndexType,
		args.IndexPartSpecifications,
		args.IndexOption,
		model.StateNone,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if isPK {
		if _, err = CheckPKOnGeneratedColumn(tblInfo, args.IndexPartSpecifications); err != nil {
			return nil, err
		}
	}
	indexInfo.ID = AllocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	if err = checkTooManyIndexes(tblInfo.Indices); err != nil {
		return nil, errors.Trace(err)
	}
	// Here we need do this check before set state to `DeleteOnly`,
	// because if hidden columns has been set to `DeleteOnly`,
	// the `DeleteOnly` columns are missing when we do this check.
	if err := checkInvisibleIndexOnPK(tblInfo); err != nil {
		return nil, err
	}
	logutil.DDLLogger().Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	return indexInfo, nil
}

func (w *worker) onCreateColumnarIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if err := checkTableTypeForColumnarIndex(tblInfo); err != nil {
		return ver, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	a := args.IndexArgs[0]
	columnarIndexType := a.GetColumnarIndexType()
	if columnarIndexType == model.ColumnarIndexTypeVector {
		a.IndexPartSpecifications[0].Expr, err = generatedexpr.ParseExpression(a.FuncExpr)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		defer func() {
			a.IndexPartSpecifications[0].Expr = nil
		}()
	}

	indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, columnarIndexType, false, a)
	if err != nil {
		return ver, errors.Trace(err)
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if job.IsCancelling() {
			return convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, dbterror.ErrCancelledDDLJob)
		}

		// Send sync schema notification to TiFlash.
		if job.SnapshotVer == 0 {
			currVer, err := getValidCurrentVersion(jobCtx.store)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tbl.Meta().ID)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.SnapshotVer = currVer.Ver
			return ver, nil
		}

		// Check the progress of the TiFlash backfill index.
		var done bool
		done, ver, err = w.checkColumnarIndexProcessOnTiFlash(jobCtx, job, tbl, indexInfo)
		if err != nil || !done {
			return ver, err
		}

		indexInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		finishedArgs := &model.ModifyIndexArgs{
			IndexArgs:    []*model.IndexArg{{IndexID: indexInfo.ID}},
			PartitionIDs: getPartitionIDs(tblInfo),
			OpType:       model.OpAddIndex,
		}
		job.FillFinishedArgs(finishedArgs)

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		logutil.DDLLogger().Info("[ddl] run add columnar index job done",
			zap.Int64("ver", ver),
			zap.String("charset", job.Charset),
			zap.String("collation", job.Collate))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
	}

	return ver, errors.Trace(err)
}

func (w *worker) checkColumnarIndexProcessOnTiFlash(jobCtx *jobContext, job *model.Job, tbl table.Table, indexInfo *model.IndexInfo,
) (done bool, ver int64, err error) {
	err = w.checkColumnarIndexProcess(jobCtx, tbl, job, indexInfo)
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			return false, ver, nil
		}
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add columnar index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, err)
		}
		return false, ver, errors.Trace(err)
	}

	return true, ver, nil
}

func (w *worker) checkColumnarIndexProcess(jobCtx *jobContext, tbl table.Table, job *model.Job, index *model.IndexInfo) error {
	waitTimeout := 500 * time.Millisecond
	ticker := time.NewTicker(waitTimeout)
	defer ticker.Stop()
	notAddedRowCnt := int64(-1)
	for {
		select {
		case <-w.ddlCtx.ctx.Done():
			return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
		case <-ticker.C:
			logutil.DDLLogger().Info(
				"index backfill state running, check columnar index process",
				zap.Stringer("job", job),
				zap.Stringer("index name", index.Name),
				zap.Int64("index ID", index.ID),
				zap.Duration("wait time", waitTimeout),
				zap.Int64("total added row count", job.RowCount),
				zap.Int64("not added row count", notAddedRowCnt))
			return dbterror.ErrWaitReorgTimeout
		default:
		}

		if !w.ddlCtx.isOwner() {
			// If it's not the owner, we will try later, so here just returns an error.
			logutil.DDLLogger().Info("DDL is not the DDL owner", zap.String("ID", w.ddlCtx.uuid))
			return errors.Trace(dbterror.ErrNotOwner)
		}

		isDone, notAddedIndexCnt, addedIndexCnt, err := w.checkColumnarIndexProcessOnce(jobCtx, tbl, index.ID)
		if err != nil {
			return errors.Trace(err)
		}
		notAddedRowCnt = notAddedIndexCnt
		job.RowCount = addedIndexCnt

		if isDone {
			break
		}
	}
	return nil
}

// checkColumnarIndexProcessOnce checks the backfill process of a columnar index from TiFlash once.
func (w *worker) checkColumnarIndexProcessOnce(jobCtx *jobContext, tbl table.Table, indexID int64) (
	isDone bool, notAddedIndexCnt, addedIndexCnt int64, err error) {
	failpoint.Inject("MockCheckColumnarIndexProcess", func(val failpoint.Value) {
		if valInt, ok := val.(int); ok {
			logutil.DDLLogger().Info("MockCheckColumnarIndexProcess", zap.Int("val", valInt))
			if valInt < 0 {
				failpoint.Return(false, 0, 0, dbterror.ErrTiFlashBackfillIndex.FastGenByArgs("mock a check error"))
			} else if valInt == 0 {
				failpoint.Return(false, 0, 0, nil)
			} else {
				failpoint.Return(true, 0, int64(valInt), nil)
			}
		}
	})

	sql := fmt.Sprintf("select rows_stable_not_indexed, rows_stable_indexed, error_message from information_schema.tiflash_indexes where table_id = %d and index_id = %d;",
		tbl.Meta().ID, indexID)
	rows, err := w.sess.Execute(jobCtx.stepCtx, sql, "add_vector_index_check_result")
	if err != nil || len(rows) == 0 {
		return false, 0, 0, errors.Trace(err)
	}

	// Get and process info from multiple TiFlash nodes.
	errMsg := ""
	for _, row := range rows {
		notAddedIndexCnt += row.GetInt64(0)
		addedIndexCnt += row.GetInt64(1)
		errMsg = row.GetString(2)
		if len(errMsg) != 0 {
			err = dbterror.ErrTiFlashBackfillIndex.FastGenByArgs(errMsg)
			break
		}
	}
	if err != nil {
		return false, 0, 0, errors.Trace(err)
	}
	if notAddedIndexCnt != 0 {
		return false, 0, 0, nil
	}

	return true, notAddedIndexCnt, addedIndexCnt, nil
}

func (w *worker) onCreateIndex(jobCtx *jobContext, job *model.Job, isPK bool) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	allIndexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, arg := range args.IndexArgs {
		indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, model.ColumnarIndexTypeNA, job.Type == model.ActionAddPrimaryKey, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// The condition in the index option is not marshaled, so we need to set it here.
		if len(arg.ConditionString) > 0 {
			indexInfo.ConditionExprString = arg.ConditionString
			// As we've updated the `ConditionExprString`, we need to rebuild the AffectColumn.
			indexInfo.AffectColumn, err = buildAffectColumn(indexInfo, tblInfo)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		allIndexInfos = append(allIndexInfos, indexInfo)
	}

	originalState := allIndexInfos[0].State

SwitchIndexState:
	switch allIndexInfos[0].State {
	case model.StateNone:
		// none -> delete only
		err = initForReorgIndexes(w, job, allIndexInfos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		err = preSplitIndexRegions(jobCtx.stepCtx, w.sess.Context, jobCtx.store, tblInfo, allIndexInfos, job.ReorgMeta, args)
		if err != nil {
			if !isRetryableJobError(err, job.ErrorCount) {
				job.State = model.JobStateCancelled
			}
			return ver, err
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
			moveAndUpdateHiddenColumnsToPublic(tblInfo, indexInfo)
		}
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteReorganization
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteReorganization)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			// reorg the index data.
			var done bool
			done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, allIndexInfos)
			if !done {
				return ver, err
			}
			// For multi-schema change, analyze is done by parent job.
			if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
				job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
			} else {
				job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
				checkAndMarkNonRevertible(job)
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			// Set column index flag.
			for _, indexInfo := range allIndexInfos {
				AddIndexColumnFlag(tblInfo, indexInfo)
				if isPK {
					if err = UpdateColsNull2NotNull(tblInfo, indexInfo); err != nil {
						return ver, errors.Trace(err)
					}
				}
				indexInfo.State = model.StatePublic
			}

			// Inject the failpoint to prevent the progress of index creation.
			failpoint.Inject("create-index-stuck-before-public", func(v failpoint.Value) {
				if sigFile, ok := v.(string); ok {
					for {
						time.Sleep(1 * time.Second)
						if _, err := os.Stat(sigFile); err != nil {
							if os.IsNotExist(err) {
								continue
							}
							failpoint.Return(ver, errors.Trace(err))
						}
						break
					}
				}
			})

			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StatePublic)
			if err != nil {
				return ver, errors.Trace(err)
			}

			a := &model.ModifyIndexArgs{
				PartitionIDs: getPartitionIDs(tbl.Meta()),
				OpType:       model.OpAddIndex,
			}
			for _, indexInfo := range allIndexInfos {
				a.IndexArgs = append(a.IndexArgs, &model.IndexArg{
					IndexID:  indexInfo.ID,
					IfExist:  false,
					IsGlobal: indexInfo.Global,
				})
			}
			job.FillFinishedArgs(a)

			analyzed := job.ReorgMeta.AnalyzeState == model.AnalyzeStateDone
			addIndexEvent := notifier.NewAddIndexEvent(tblInfo, allIndexInfos, analyzed)
			err2 := asyncNotifyEvent(jobCtx, addIndexEvent, job, noSubJob, w.sess)
			if err2 != nil {
				return ver, errors.Trace(err2)
			}

			// Finish this job.
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
			logutil.DDLLogger().Info("run add index job done",
				zap.String("charset", job.Charset),
				zap.String("collation", job.Collate))
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func initForReorgIndexes(w *worker, job *model.Job, idxInfos []*model.IndexInfo) error {
	if len(idxInfos) == 0 {
		return nil
	}
	reorgTp, err := pickBackfillType(job)
	if err != nil {
		return err
	}
	// Partial Index is not supported without fast reorg.
	for _, indexInfo := range idxInfos {
		if (reorgTp == model.ReorgTypeTxn || reorgTp == model.ReorgTypeTxnMerge) && indexInfo.HasCondition() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("add partial index without fast reorg is not supported")
		}
	}
	loadCloudStorageURI(w, job)
	if reorgTp.NeedMergeProcess() {
		// Increase telemetryAddIndexIngestUsage
		telemetryAddIndexIngestUsage.Inc()
		for _, indexInfo := range idxInfos {
			indexInfo.BackfillState = model.BackfillStateRunning
		}
	}
	return nil
}

type analyzeStatus int

const (
	analyzeUnknown analyzeStatus = iota
	analyzeRunning
	analyzeFinished
	analyzeFailed
)

// queryAnalyzeStatusSince queries `analyze_jobs` for the specified table
// and start_time >= startTS. It returns one of analyzeStatus values. If the
// sessPool cannot provide a session or the query fails, it returns
// analyzeUnknown and a nil error so the caller may decide to proceed with
// starting ANALYZE.
func (w *worker) queryAnalyzeStatusSince(startTS uint64, dbName, tblName string) (analyzeStatus, error) {
	sessCtx, err := w.sessPool.Get()
	if err != nil {
		return analyzeUnknown, err
	}
	defer w.sessPool.Put(sessCtx)

	startTimeStr := time.Now().UTC().Format(time.DateTime)
	if startTS > 0 {
		startTimeStr = model.TSConvert2Time(startTS).UTC().Format(time.DateTime)
	}
	kctx := kv.WithInternalSourceType(w.ctx, kv.InternalTxnStats)

	// set session time zone to UTC to match the time format in `startTimeStr`
	originalTimeZone := sessCtx.GetSessionVars().TimeZone
	sessCtx.GetSessionVars().TimeZone = time.UTC
	defer func() {
		sessCtx.GetSessionVars().TimeZone = originalTimeZone
	}()
	exec := sessCtx.GetRestrictedSQLExecutor()
	rows, _, chkErr := exec.ExecRestrictedSQL(kctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SELECT state FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND start_time >= %?", dbName, tblName, startTimeStr)
	if chkErr != nil {
		return analyzeUnknown, chkErr
	}
	if len(rows) == 0 {
		return analyzeUnknown, nil
	}

	for _, r := range rows {
		var state string
		if !r.IsNull(0) {
			state = r.GetString(0)
		}
		switch {
		case strings.EqualFold(state, "running"):
			return analyzeRunning, nil
		case strings.EqualFold(state, "failed"):
			return analyzeFailed, nil
		case strings.EqualFold(state, "finished"):
			return analyzeFinished, nil
		default:
			// unknown state: continue checking other rows
		}
	}
	// If no recognizable state found, treat as unknown so caller may attempt to start ANALYZE.
	return analyzeUnknown, nil
}

// analyzeStatusDecision encapsulates the decision logic after observing
// the analyze status for a table. It returns three booleans:
//
//	done: whether the caller should consider analyze finished (true) and continue DDL;
//	timedOut: whether the analyze has exceeded cumulative timeout and caller should proceed with timeout handling;
//	failed: whether the analyze has failed;
//	proceed: whether the caller should proceed to start ANALYZE locally (true for unknown status).
func (w *worker) analyzeStatusDecision(job *model.Job, dbName, tblName string, status analyzeStatus, cumulativeTimeout time.Duration) (done, timedOut, failed, proceed bool) {
	switch status {
	case analyzeFinished:
		logutil.DDLLogger().Info("analyze already finished by other owner", zap.Int64("jobID", job.ID), zap.String("db", dbName), zap.String("table", tblName))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, false, false
	case analyzeFailed:
		logutil.DDLLogger().Warn("analyze previously failed on another owner, continue finishing DDL", zap.Int64("jobID", job.ID), zap.String("db", dbName), zap.String("table", tblName))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, true, false
	case analyzeRunning:
		// If analyze is running, respect cumulative timeout. If expired, return timeout to let caller proceed.
		if start, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); ok {
			if time.Since(start) > cumulativeTimeout {
				logutil.DDLLogger().Warn("analyze table is running but exceeded cumulative timeout, proceeding to finish DDL", zap.Int64("jobID", job.ID), zap.Duration("elapsed", time.Since(start)))
				w.ddlCtx.clearAnalyzeStartTime(job.ID)
				w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
				return false, true, false, false
			}
		} else {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}
		select {
		case <-w.ctx.Done():
			logutil.DDLLogger().Info("analyze table after create index context done", zap.Int64("jobID", job.ID), zap.Error(w.ctx.Err()))
			w.ddlCtx.clearAnalyzeStartTime(job.ID)
			w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
			return true, false, false, false
		case <-time.After(DefaultAnalyzeCheckInterval):
			return false, false, false, false
		}
	default:
		// analyzeUnknown: caller should proceed to start ANALYZE locally.
		return false, false, false, true
	}
}

// doAnalyzeWithoutReorg performs analyze for the table if needed without the logic of
// reorg and and returns whether the table info is updated.
func (w *worker) doAnalyzeWithoutReorg(job *model.Job, tblInfo *model.TableInfo) (finished bool) {
	switch job.ReorgMeta.AnalyzeState {
	case model.AnalyzeStateNone:
		if checkNeedAnalyze(job, tblInfo) {
			// Start analyze for the next time.
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
		} else {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
		}
		return false
	case model.AnalyzeStateRunning:
		w.startAnalyzeAndWait(job, tblInfo)
		return false
	default:
		logutil.DDLLogger().Info("analyze skipped or finished for multi-schema change",
			zap.Int64("job", job.ID), zap.Int8("state", job.ReorgMeta.AnalyzeState))
		return true
	}
}

func (w *worker) startAnalyzeAndWait(job *model.Job, tblInfo *model.TableInfo) {
	done, timedOut, failed := w.analyzeTableInner(job, tblInfo, job.SchemaName)
	failpoint.InjectCall("analyzeTableDone", job)
	if done || timedOut || failed {
		if done {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateDone
		}
		if timedOut {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateTimeout
		}
		if failed {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateFailed
		}
	}
}

// analyzeTableInner analyzes the table after creating index/modify column.
func (w *worker) analyzeTableInner(job *model.Job, tblInfo *model.TableInfo, dbName string) (done, timedOut, failed bool) {
	doneCh := w.ddlCtx.getAnalyzeDoneCh(job.ID)
	tblName := tblInfo.Name.L
	cumulativeTimeout, found := w.ddlCtx.getAnalyzeCumulativeTimeout(job.ID)
	if !found {
		cumulativeTimeout = DefaultCumulativeTimeout
		if job.RealStartTS != 0 {
			addStart := model.TSConvert2Time(job.RealStartTS)
			elapsed := time.Since(addStart)
			if elapsed*2 > cumulativeTimeout {
				cumulativeTimeout = elapsed * 2
			}
		}
		w.ddlCtx.setAnalyzeCumulativeTimeout(job.ID, cumulativeTimeout)
	}

	if doneCh == nil {
		status, err := w.queryAnalyzeStatusSince(job.StartTS, dbName, tblName)
		if err != nil {
			logutil.DDLLogger().Warn("query analyze status failed", zap.Int64("jobID", job.ID), zap.Error(err))
			status = analyzeUnknown
		}

		done, timedOut, failed, proceed := w.analyzeStatusDecision(job, dbName, tblName, status, cumulativeTimeout)
		if done || timedOut || failed {
			return done, timedOut, failed
		}
		if !proceed {
			// We decided not to proceed to start ANALYZE locally (i.e. it's running and we waited),
			// so simply return and retry later.
			return false, false, false
		}

		if _, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); !ok {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}

		doneCh = make(chan error)
		eg := util.NewErrorGroupWithRecover()
		eg.Go(func() error {
			sessCtx, err := w.sessPool.Get()
			if err != nil {
				return err
			}
			defer func() {
				w.sessPool.Put(sessCtx)
				close(doneCh)
			}()
			dbTable := fmt.Sprintf("`%s`.`%s`", dbName, tblName)

			exec, ok := sessCtx.(sqlexec.RestrictedSQLExecutor)
			if !ok {
				return errors.Errorf("not restricted SQL executor: %T", sessCtx)
			}
			// internal sql may not init the analysis related variable correctly.
			err = statsutil.UpdateSCtxVarsForStats(sessCtx)
			if err != nil {
				return err
			}
			failpoint.InjectCall("beforeAnalyzeTable")
			_, _, err = exec.ExecRestrictedSQL(w.ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession, sqlexec.ExecOptionEnableDDLAnalyze}, "ANALYZE TABLE "+dbTable+";", "ddl analyze table")
			failpoint.InjectCall("afterAnalyzeTable", &err)
			if err != nil {
				logutil.DDLLogger().Warn("analyze table failed",
					zap.Int64("jobID", job.ID),
					zap.String("db", dbName),
					zap.String("table", tblName),
					zap.Error(err),
					zap.Stack("stack"))
				// We can continue to finish the job even if analyze table failed.
				doneCh <- err
			}
			return nil
		})
		w.ddlCtx.setAnalyzeDoneCh(job.ID, doneCh)
	}
	select {
	case err := <-doneCh:
		logutil.DDLLogger().Info("analyze table after create index done", zap.Int64("jobID", job.ID))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, err != nil
	case <-w.ctx.Done():
		logutil.DDLLogger().Info("analyze table after create index context done",
			zap.Int64("jobID", job.ID), zap.Error(w.ctx.Err()))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, false
	case <-time.After(DefaultAnalyzeCheckInterval):
		failpoint.Inject("mockAnalyzeTimeout", func(val failpoint.Value) {
			if v, ok := val.(int); ok {
				cumulativeTimeout = time.Duration(v) * time.Millisecond
			}
		})
		if start, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); ok {
			if time.Since(start) > cumulativeTimeout {
				logutil.DDLLogger().Warn("analyze table after create index exceed cumulative timeout, proceeding to finish DDL",
					zap.Int64("jobID", job.ID), zap.Duration("elapsed", time.Since(start)))
				// Do not persist job here. Let the caller mark AnalyzeStateTimeout and persist.
				w.ddlCtx.clearAnalyzeStartTime(job.ID)
				w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
				return false, true, false
			}
		} else {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}
		return false, false, false
	}
}

func checkIfTableReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	job *model.Job,
) bool {
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.attachTopProfilingInfo(job.Query)
	if isEmpty, err := checkIfTableIsEmpty(ctx, store, tbl, startTS); err != nil || !isEmpty {
		return false
	}
	return true
}

// CheckImportIntoTableIsEmpty check import into table is empty or not.
func CheckImportIntoTableIsEmpty(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
) (bool, error) {
	failpoint.Inject("checkImportIntoTableIsEmpty", func(_val failpoint.Value) {
		if val, ok := _val.(string); ok {
			switch val {
			case "error":
				failpoint.Return(false, errors.New("check is empty get error"))
			case "notEmpty":
				failpoint.Return(false, nil)
			}
		}
	})
	txn, err := sessCtx.Txn(true)
	if err != nil {
		return false, err
	}
	validTxn := txn != nil && txn.Valid()
	if !validTxn {
		return false, errors.New("check if table is empty failed")
	}
	startTS := txn.StartTS()
	return checkIfTableIsEmpty(NewReorgContext(), store, tbl, startTS)
}

func checkIfTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	startTS uint64,
) (bool, error) {
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			pTbl := pTbl.GetPartition(pid)
			if isEmpty, err := checkIfPhysicalTableIsEmpty(ctx, store, pTbl, startTS); err != nil || !isEmpty {
				return false, err
			}
		}
		return true, nil
	}
	//nolint:forcetypeassert
	plainTbl := tbl.(table.PhysicalTable)
	return checkIfPhysicalTableIsEmpty(ctx, store, plainTbl, startTS)
}

func checkIfPhysicalTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.PhysicalTable,
	startTS uint64,
) (bool, error) {
	hasRecord, err := existsTableRow(ctx, store, tbl, startTS)
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false, err
	}
	return !hasRecord, nil
}

func checkIfTempIndexReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	job *model.Job,
) bool {
	failpoint.Inject("skipReorgWorkForTempIndex", func(val failpoint.Value) {
		if v, ok := val.(bool); ok {
			failpoint.Return(v)
		}
	})
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if temp index is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.attachTopProfilingInfo(job.Query)
	firstIdxID := allIndexInfos[0].ID
	lastIdxID := allIndexInfos[len(allIndexInfos)-1].ID
	var globalIdxIDs []int64
	for _, idxInfo := range allIndexInfos {
		if idxInfo.Global {
			globalIdxIDs = append(globalIdxIDs, idxInfo.ID)
		}
	}
	return checkIfTempIndexIsEmpty(ctx, store, tbl, firstIdxID, lastIdxID, globalIdxIDs, startTS)
}

func checkIfTempIndexIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	firstIdxID, lastIdxID int64,
	globalIdxIDs []int64,
	startTS uint64,
) bool {
	tblMetaID := tbl.Meta().ID
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, pid, firstIdxID, lastIdxID, startTS) {
				return false
			}
		}
		for _, globalIdxID := range globalIdxIDs {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, globalIdxID, globalIdxID, startTS) {
				return false
			}
		}
		return true
	}
	return checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, firstIdxID, lastIdxID, startTS)
}

func checkIfTempIndexIsEmptyForPhysicalTable(
	ctx *ReorgContext,
	store kv.Storage,
	pid int64,
	firstIdxID, lastIdxID int64,
	startTS uint64,
) bool {
	start, end := encodeTempIndexRange(pid, firstIdxID, lastIdxID)
	foundKey := false
	idxPrefix := tablecodec.GenTableIndexPrefix(pid)
	err := iterateSnapshotKeys(ctx, store, kv.PriorityLow, idxPrefix, startTS, start, end,
		func(_ kv.Handle, _ kv.Key, _ []byte) (more bool, err error) {
			foundKey = true
			return false, nil
		})
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Info("check if temp index is empty failed", zap.Error(err))
		return false
	}
	return !foundKey
}

// pickBackfillType determines which backfill process will be used. The result is
// both stored in job.ReorgMeta.ReorgTp and returned.
func pickBackfillType(job *model.Job) (model.ReorgType, error) {
	if job.ReorgMeta.ReorgTp != model.ReorgTypeNone {
		// The backfill task has been started.
		// Don't change the backfill type.
		return job.ReorgMeta.ReorgTp, nil
	}
	if !job.ReorgMeta.IsFastReorg {
		job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
		return model.ReorgTypeTxn, nil
	}
	if ingest.LitInitialized {
		if job.ReorgMeta.UseCloudStorage {
			job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
			return model.ReorgTypeIngest, nil
		}
		if err := ingest.LitDiskRoot.PreCheckUsage(); err != nil {
			logutil.DDLIngestLogger().Info("ingest backfill is not available", zap.Error(err))
			return model.ReorgTypeNone, err
		}
		job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
		return model.ReorgTypeIngest, nil
	}
	// The lightning environment is unavailable, but we can still use the txn-merge backfill.
	logutil.DDLLogger().Info("fallback to txn-merge backfill process",
		zap.Bool("lightning env initialized", ingest.LitInitialized))
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
	return model.ReorgTypeTxnMerge, nil
}

func loadCloudStorageURI(w *worker, job *model.Job) {
	jc := w.jobContext(job.ID, job.ReorgMeta)
	jc.cloudStorageURI = handle.GetCloudStorageURI(w.workCtx, w.store)
	job.ReorgMeta.UseCloudStorage = len(jc.cloudStorageURI) > 0 && job.ReorgMeta.IsDistReorg
	failpoint.InjectCall("afterLoadCloudStorageURI", job)
}

func doReorgWorkForCreateIndex(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
) (done bool, ver int64, err error) {
	var reorgTp model.ReorgType
	reorgTp, err = pickBackfillType(job)
	if err != nil {
		return false, ver, err
	}
	if !reorgTp.NeedMergeProcess() {
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if skipReorg {
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
			return true, ver, nil
		}
		return runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	}
	switch allIndexInfos[0].BackfillState {
	case model.BackfillStateRunning:
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if !skipReorg {
			logutil.DDLLogger().Info("index backfill state running",
				zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
				zap.Bool("ingest mode", reorgTp == model.ReorgTypeIngest),
				zap.String("index", allIndexInfos[0].Name.O))
			switch reorgTp {
			case model.ReorgTypeIngest:
				if job.ReorgMeta.IsDistReorg {
					done, ver, err = runIngestReorgJobDist(w, jobCtx, job, tbl, allIndexInfos)
				} else {
					done, ver, err = runIngestReorgJob(w, jobCtx, job, tbl, allIndexInfos)
				}
			case model.ReorgTypeTxnMerge:
				done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
			}
			if err != nil || !done {
				return false, ver, errors.Trace(err)
			}
		} else {
			failpoint.InjectCall("afterCheckTableReorgCanSkip")
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateReadyToMerge
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		failpoint.InjectCall("afterBackfillStateRunningDone", job)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		failpoint.InjectCall("beforeBackfillMerge")
		logutil.DDLLogger().Info("index backfill state ready to merge",
			zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O),
			zap.String("index", allIndexInfos[0].Name.O))
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateMerging
		}
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		skipReorg := checkIfTempIndexReorgWorkCanSkip(w.store, w.sess.Session(), tbl, allIndexInfos, job)
		if !skipReorg {
			done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, true)
			if !done {
				return false, ver, err
			}
		} else {
			failpoint.InjectCall("afterCheckTempIndexReorgCanSkip")
			logutil.DDLLogger().Info("temp index is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return true, ver, errors.Trace(err)
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", allIndexInfos[0].BackfillState)
	}
}

func runIngestReorgJobDist(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		return false, ver, errors.Trace(err)
	}

	if !done {
		return false, ver, nil
	}

	return true, ver, nil
}

func runIngestReorgJob(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		if kv.ErrKeyExists.Equal(err) {
			logutil.DDLLogger().Warn("import index duplicate key, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run reorg job failed, convert job to rollback",
				zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else {
			logutil.DDLLogger().Warn("run add index ingest job error", zap.Error(err))
		}
		return false, ver, errors.Trace(err)
	}
	failpoint.InjectCall("afterRunIngestReorgJob", job, done)
	return done, ver, nil
}

func isRetryableJobError(err error, jobErrCnt int64) bool {
	if jobErrCnt+1 >= vardef.GetDDLErrorCountLimit() {
		return false
	}
	return isRetryableError(err)
}

func isRetryableError(err error) bool {
	errMsg := err.Error()
	for _, m := range dbterror.ReorgRetryableErrMsgs {
		if strings.Contains(errMsg, m) {
			return true
		}
	}
	originErr := errors.Cause(err)
	if tErr, ok := originErr.(*terror.Error); ok {
		sqlErr := terror.ToSQLError(tErr)
		_, ok := dbterror.ReorgRetryableErrCodes[sqlErr.Code]
		return ok
	}
	// For the unknown errors, we should retry.
	return true
}

func runReorgJobAndHandleErr(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	mergingTmpIdx bool,
) (done bool, ver int64, err error) {
	elements := make([]*meta.Element, 0, len(allIndexInfos))
	for _, indexInfo := range allIndexInfos {
		elements = append(elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
	}

	failpoint.InjectCall("beforeRunReorgJobAndHandleErr", allIndexInfos)

	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = err1
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, tbl, elements, mergingTmpIdx)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}
	err = overwriteReorgInfoFromGlobalCheckpoint(w, rh.s, job, reorgInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, allIndexInfos[0].Name)
			}, false)
		return w.addTableIndex(jobCtx, tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		// TODO(tangenta): get duplicate column and match index.
		err = ingest.TryConvertToKeyExistsErr(err, allIndexInfos[0], tbl.Meta())
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
			if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.DDLLogger().Warn("run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed", zap.Stringer("job", job), zap.Error(err1))
			}
		}
		return false, ver, errors.Trace(err)
	}

	failpoint.InjectCall("afterRunReorgJobAndHandleErr")
	return true, ver, nil
}

func onDropIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, allIndexInfos, ifExists, err := checkDropIndex(jobCtx.infoCache, jobCtx.metaMut, job)
	if err != nil {
		if ifExists && dbterror.ErrCantDropFieldOrKey.Equal(err) {
			job.Warning = toTError(err)
			job.State = model.JobStateDone
			return ver, nil
		}
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if job.MultiSchemaInfo != nil && !job.IsRollingback() && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		job.SchemaState = allIndexInfos[0].State
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	originalState := allIndexInfos[0].State
	switch allIndexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteReorganization
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteReorganization)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		isColumnarIndex := false
		indexIDs := make([]int64, 0, len(allIndexInfos))
		for _, indexInfo := range allIndexInfos {
			if indexInfo.IsColumnarIndex() {
				isColumnarIndex = true
			}
			indexInfo.State = model.StateNone
			// Set column index flag.
			DropIndexColumnFlag(tblInfo, indexInfo)
			RemoveDependentHiddenColumns(tblInfo, indexInfo)
			removeIndexInfo(tblInfo, indexInfo)
			indexIDs = append(indexIDs, indexInfo.ID)
		}

		failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
			//nolint:forcetypeassert
			if val.(bool) {
				panic("panic test in cancelling add index")
			}
		})

		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if isColumnarIndex {
			// Send sync schema notification to TiFlash.
			if err := infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tblInfo.ID); err != nil {
				logutil.DDLLogger().Warn("run drop column index but syncing TiFlash schema failed", zap.Error(err))
			}
		}

		// Finish this job.
		if job.IsRollingback() {
			dropArgs, err := model.GetFinishedModifyIndexArgs(job)
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}

			// Convert drop index args to finished add index args again to finish add index job.
			// Only rolled back add index jobs will get here, since drop index jobs can only be cancelled, not rolled back.
			addIndexArgs := &model.ModifyIndexArgs{
				PartitionIDs: dropArgs.PartitionIDs,
				OpType:       model.OpAddIndex,
			}
			for i, indexID := range indexIDs {
				addIndexArgs.IndexArgs = append(addIndexArgs.IndexArgs,
					&model.IndexArg{
						IndexID: indexID,
						IfExist: dropArgs.IndexArgs[i].IfExist,
					})
			}
			job.FillFinishedArgs(addIndexArgs)
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			// Global index key has t{tableID}_ prefix.
			// Assign partitionIDs empty to guarantee correct prefix in insertJobIntoDeleteRangeTable.
			dropArgs, err := model.GetDropIndexArgs(job)
			dropArgs.OpType = model.OpDropIndex
			if err != nil {
				return ver, errors.Trace(err)
			}
			dropArgs.IndexArgs[0].IndexID = indexIDs[0]
			dropArgs.IndexArgs[0].IsColumnar = allIndexInfos[0].IsColumnarIndex()
			dropArgs.IndexArgs[0].ColumnarIndexType = allIndexInfos[0].GetColumnarIndexType()
			if !allIndexInfos[0].Global {
				dropArgs.PartitionIDs = getPartitionIDs(tblInfo)
			}
			job.FillFinishedArgs(dropArgs)
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State))
	}
	job.SchemaState = allIndexInfos[0].State
	return ver, errors.Trace(err)
}

// RemoveDependentHiddenColumns removes hidden columns by the indexInfo.
func RemoveDependentHiddenColumns(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffs := make([]int, 0)
	for _, indexColumn := range idxInfo.Columns {
		col := tblInfo.Columns[indexColumn.Offset]
		if col.Hidden {
			hiddenColOffs = append(hiddenColOffs, col.Offset)
		}
	}
	// Sort the offset in descending order.
	slices.SortFunc(hiddenColOffs, func(a, b int) int { return cmp.Compare(b, a) })
	// Move all the dependent hidden columns to the end.
	endOffset := len(tblInfo.Columns) - 1
	for _, offset := range hiddenColOffs {
		tblInfo.MoveColumnInfo(offset, endOffset)
	}
	tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(hiddenColOffs)]
}

func removeIndexInfo(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	indices := tblInfo.Indices
	offset := -1
	for i, idx := range indices {
		if idxInfo.ID == idx.ID {
			offset = i
			break
		}
	}
	if offset == -1 {
		// The target index has been removed.
		return
	}
	// Remove the target index.
	tblInfo.Indices = slices.Delete(tblInfo.Indices, offset, offset+1)
}

func checkDropIndex(infoCache *infoschema.InfoCache, t *meta.Mutator, job *model.Job) (*model.TableInfo, []*model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	args, err := model.GetDropIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, errors.Trace(err)
	}

	indexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, idxArg := range args.IndexArgs {
		indexInfo := tblInfo.FindIndexByName(idxArg.IndexName.L)
		if indexInfo == nil {
			job.State = model.JobStateCancelled
			return nil, nil, idxArg.IfExist, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", idxArg.IndexName)
		}

		// Check that drop primary index will not cause invisible implicit primary index.
		if err := checkInvisibleIndexesOnPK(tblInfo, []*model.IndexInfo{indexInfo}, job); err != nil {
			job.State = model.JobStateCancelled
			return nil, nil, false, errors.Trace(err)
		}

		// Double check for drop index needed in foreign key.
		if err := checkIndexNeededInForeignKeyInOwner(infoCache, job, job.SchemaName, tblInfo, indexInfo); err != nil {
			return nil, nil, false, errors.Trace(err)
		}
		indexInfos = append(indexInfos, indexInfo)
	}
	return tblInfo, indexInfos, false, nil
}

func checkInvisibleIndexesOnPK(tblInfo *model.TableInfo, indexInfos []*model.IndexInfo, job *model.Job) error {
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, oidx := range tblInfo.Indices {
		needAppend := true
		for _, idx := range indexInfos {
			if idx.Name.L == oidx.Name.L {
				needAppend = false
				break
			}
		}
		if needAppend {
			newIndices = append(newIndices, oidx)
		}
	}
	newTbl := tblInfo.Clone()
	newTbl.Indices = newIndices
	if err := checkInvisibleIndexOnPK(newTbl); err != nil {
		job.State = model.JobStateCancelled
		return err
	}

	return nil
}

func checkRenameIndex(t *meta.Mutator, job *model.Job) (tblInfo *model.TableInfo, from, to ast.CIStr, err error) {
	schemaID := job.SchemaID
	tblInfo, err = GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	from, to = args.GetRenameIndexes()

	// Double check. See function `RenameIndex` in executor.go
	duplicate, err := ValidateRenameIndex(from, to, tblInfo)
	if duplicate {
		return nil, from, to, nil
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	return tblInfo, from, to, errors.Trace(err)
}

func checkAlterIndexVisibility(t *meta.Mutator, job *model.Job) (*model.TableInfo, ast.CIStr, bool, error) {
	var (
		indexName ast.CIStr
		invisible bool
	)

	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, indexName, invisible, errors.Trace(err)
	}

	args, err := model.GetAlterIndexVisibilityArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	indexName, invisible = args.IndexName, args.Invisible

	skip, err := validateAlterIndexVisibility(nil, indexName, invisible, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	if skip {
		job.State = model.JobStateDone
		return nil, indexName, invisible, nil
	}
	return tblInfo, indexName, invisible, nil
}
