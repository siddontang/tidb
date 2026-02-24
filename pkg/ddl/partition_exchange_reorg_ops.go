// Copyright 2018 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// newStatsDDLEventForJob creates a util.SchemaChangeEvent for a job.
// It is used for reorganize partition, add partitioning and remove partitioning.
func newStatsDDLEventForJob(
	jobType model.ActionType,
	oldTblID int64,
	tblInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) (*notifier.SchemaChangeEvent, error) {
	var event *notifier.SchemaChangeEvent
	switch jobType {
	case model.ActionReorganizePartition:
		event = notifier.NewReorganizePartitionEvent(
			tblInfo,
			addedPartInfo,
			droppedPartInfo,
		)
	case model.ActionAlterTablePartitioning:
		event = notifier.NewAddPartitioningEvent(
			oldTblID,
			tblInfo,
			addedPartInfo,
		)
	case model.ActionRemovePartitioning:
		event = notifier.NewRemovePartitioningEvent(
			oldTblID,
			tblInfo,
			droppedPartInfo,
		)
	default:
		return nil, errors.Errorf("unknown job type: %s", jobType.String())
	}
	return event, nil
}

func doPartitionReorgWork(w *worker, jobCtx *jobContext, job *model.Job, tbl table.Table, physTblIDs []int64) (done bool, ver int64, err error) {
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		return done, ver, err1
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	reorgTblInfo := tbl.Meta().Clone()
	var elements []*meta.Element
	indices := make([]*model.IndexInfo, 0, len(tbl.Meta().Indices))
	for _, index := range tbl.Meta().Indices {
		if isNew, ok := tbl.Meta().GetPartitionInfo().DDLChangedIndex[index.ID]; ok && !isNew {
			// Skip old replaced indexes, but rebuild all other indexes
			continue
		}
		indices = append(indices, index)
	}
	elements = BuildElements(tbl.Meta().Columns[0], indices)
	reorgTbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, reorgTblInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	partTbl, ok := reorgTbl.(table.PartitionedTable)
	if !ok {
		return false, ver, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfoFromPartitions(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, partTbl, physTblIDs, elements)
	err = w.runReorgJob(jobCtx, reorgInfo, reorgTbl.Meta(), func() (reorgErr error) {
		defer tidbutil.Recover(metrics.LabelDDL, "doPartitionReorgWork",
			func() {
				reorgErr = dbterror.ErrCancelledDDLJob.GenWithStack("reorganize partition for table `%v` panic", tbl.Meta().Name)
			}, false)
		return w.reorgPartitionDataAndIndex(jobCtx, reorgTbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}

		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.DDLLogger().Warn("reorg partition job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.Stringer("job", job), zap.Error(err1))
		}
		logutil.DDLLogger().Warn("reorg partition job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
		// TODO: Test and verify that this returns an error on the ALTER TABLE session.
		ver, err = rollbackReorganizePartitionWithErr(jobCtx, job, err)
		return false, ver, errors.Trace(err)
	}
	return true, ver, err
}

type reorgPartitionWorker struct {
	*backfillCtx
	// Static allocated to limit memory allocations
	rowRecords        []*rowRecord
	rowDecoder        *decoder.RowDecoder
	rowMap            map[int64]types.Datum
	writeColOffsetMap map[int64]int
	maxOffset         int
	reorgedTbl        table.PartitionedTable
}

func newReorgPartitionWorker(i int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*reorgPartitionWorker, error) {
	bCtx, err := newBackfillCtx(i, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblReorgPartitionRate, false)
	if err != nil {
		return nil, err
	}
	reorgedTbl, err := tables.GetReorganizedPartitionedTable(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pt := t.GetPartitionedTable()
	if pt == nil {
		return nil, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	partColIDs := reorgedTbl.GetPartitionColumnIDs()
	writeColOffsetMap := make(map[int64]int, len(partColIDs))
	maxOffset := 0
	for _, id := range partColIDs {
		var offset int
		for _, col := range pt.Cols() {
			if col.ID == id {
				offset = col.Offset
				break
			}
		}
		writeColOffsetMap[id] = offset
		maxOffset = max(maxOffset, offset)
	}
	return &reorgPartitionWorker{
		backfillCtx:       bCtx,
		rowDecoder:        decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap),
		rowMap:            make(map[int64]types.Datum, len(decodeColMap)),
		writeColOffsetMap: writeColOffsetMap,
		maxOffset:         maxOffset,
		reorgedTbl:        reorgedTbl,
	}, nil
}

func (w *reorgPartitionWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		failpoint.InjectCall("PartitionBackfillData", len(w.rowRecords) > 0)
		// For non-clustered tables, we need to replace the _tidb_rowid handles since
		// there may be duplicates across different partitions, due to EXCHANGE PARTITION.
		// Meaning we need to check here if a record was double written to the new partition,
		// i.e. concurrently written by StateWriteOnly or StateWriteReorganization.
		// and if so, skip it.
		var found map[string][]byte
		lockKey := make([]byte, 0, tablecodec.RecordRowKeyLen)
		lockKey = append(lockKey, handleRange.startKey[:tablecodec.TableSplitKeyLen]...)
		if !w.table.Meta().HasClusteredIndex() && len(w.rowRecords) > 0 {
			failpoint.InjectCall("PartitionBackfillNonClustered", w.rowRecords[0].vals)
			// we must check if old IDs already been written,
			// i.e. double written by StateWriteOnly or StateWriteReorganization.

			// TODO: test how to use PresumeKeyNotExists/NeedConstraintCheckInPrewrite/DO_CONSTRAINT_CHECK
			// to delay the check until commit.
			// And handle commit errors and fall back to this method of checking all keys to see if we need to skip any.
			newKeys := make([]kv.Key, 0, len(w.rowRecords))
			for i := range w.rowRecords {
				newKeys = append(newKeys, w.rowRecords[i].key)
			}
			found, err = kv.BatchGetValue(ctx, txn, newKeys)
			if err != nil {
				return errors.Trace(err)
			}

			// TODO: Add test that kills (like `kill -9`) the currently running
			// ddl owner, to see how it handles re-running this backfill when some batches has
			// committed and reorgInfo has not been updated, so it needs to redo some batches.
		}
		tmpRow := make([]types.Datum, len(w.reorgedTbl.Cols()))

		for _, prr := range w.rowRecords {
			taskCtx.scanCount++
			key := prr.key
			lockKey = lockKey[:tablecodec.TableSplitKeyLen]
			lockKey = append(lockKey, key[tablecodec.TableSplitKeyLen:]...)
			// Lock the *old* key, since there can still be concurrent update happening on
			// the rows from fetchRowColVals(). If we cannot lock the keys in this
			// transaction and succeed when committing, then another transaction did update
			// the same key, and we will fail and retry. When retrying, this key would be found
			// through BatchGet and skipped.
			// TODO: would it help to accumulate the keys in a slice and then only call this once?
			err = txn.LockKeys(context.Background(), new(kv.LockCtx), lockKey)
			if err != nil {
				return errors.Trace(err)
			}

			if vals, ok := found[string(key)]; ok {
				if len(vals) == len(prr.vals) && bytes.Equal(vals, prr.vals) {
					// Already backfilled or double written earlier by concurrent DML
					continue
				}
				// Not same row, due to earlier EXCHANGE PARTITION.
				// Update the current read row by Remove it and Add it back (which will give it a new _tidb_rowid)
				// which then also will be used as unique id in the new partition.
				var h kv.Handle
				var currPartID int64
				currPartID, h, err = tablecodec.DecodeRecordKey(lockKey)
				if err != nil {
					return errors.Trace(err)
				}
				_, err = w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, h, prr.vals, w.loc, w.rowMap)
				if err != nil {
					return errors.Trace(err)
				}
				for _, col := range w.table.WritableCols() {
					d, ok := w.rowMap[col.ID]
					if !ok {
						return dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
					}
					tmpRow[col.Offset] = d
				}
				// Use RemoveRecord/AddRecord to keep the indexes in-sync!
				pt := w.table.GetPartitionedTable().GetPartition(currPartID)
				err = pt.RemoveRecord(w.tblCtx, txn, h, tmpRow)
				if err != nil {
					return errors.Trace(err)
				}
				h, err = pt.AddRecord(w.tblCtx, txn, tmpRow)
				if err != nil {
					return errors.Trace(err)
				}
				w.cleanRowMap()
				// tablecodec.prefixLen is not exported, but is just TableSplitKeyLen + 2 ("_r")
				key = tablecodec.EncodeRecordKey(key[:tablecodec.TableSplitKeyLen+2], h)
				// OK to only do txn.Set() for the new partition, and defer creating the indexes,
				// since any DML changes the record it will also update or create the indexes,
				// by doing RemoveRecord+UpdateRecord
			}
			err = txn.Set(key, prr.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillData", 3000)

	return
}

func (w *reorgPartitionWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) (kv.Key, bool, error) {
	w.rowRecords = w.rowRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	sysTZ := w.loc

	tmpRow := make([]types.Datum, len(w.reorgedTbl.Cols()))
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, w.table.RecordPrefix(), txn.StartTS(), taskRange.startKey, taskRange.endKey,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in reorgPartitionWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, handle, rawRow, sysTZ, w.rowMap)
			if err != nil {
				return false, errors.Trace(err)
			}

			// Set all partitioning columns and calculate which partition to write to
			for colID, offset := range w.writeColOffsetMap {
				d, ok := w.rowMap[colID]
				if !ok {
					return false, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
				}
				tmpRow[offset] = d
			}
			p, err := w.reorgedTbl.GetPartitionByRow(w.exprCtx.GetEvalCtx(), tmpRow)
			if err != nil {
				return false, errors.Trace(err)
			}
			newKey := tablecodec.EncodeTablePrefix(p.GetPhysicalID())
			newKey = append(newKey, recordKey[tablecodec.TableSplitKeyLen:]...)
			w.rowRecords = append(w.rowRecords, &rowRecord{key: newKey, vals: rawRow})

			w.cleanRowMap()
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	logutil.DDLLogger().Debug("txn fetches handle info",
		zap.Uint64("txnStartTS", txn.StartTS()),
		zap.Stringer("taskRange", &taskRange),
		zap.Duration("takeTime", time.Since(startTime)))
	return getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *reorgPartitionWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

func (w *reorgPartitionWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*reorgPartitionWorker) String() string {
	return typeReorgPartitionWorker.String()
}

func (w *reorgPartitionWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *worker) reorgPartitionDataAndIndex(
	jobCtx *jobContext,
	t table.Table,
	reorgInfo *reorgInfo,
) (err error) {
	// First copy all table data to the new AddingDefinitions partitions
	// from each of the DroppingDefinitions partitions.
	// Then create all indexes on the AddingDefinitions partitions,
	// both new local and new global indexes
	// And last update new global indexes from the non-touched partitions
	// Note it is hard to update global indexes in-place due to:
	//   - Transactions on different TiDB nodes/domains may see different states of the table/partitions
	//   - We cannot have multiple partition ids for a unique index entry.

	// Copy the data from the DroppingDefinitions to the AddingDefinitions
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		err = w.updatePhysicalTableRow(jobCtx.stepCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		if len(reorgInfo.elements) <= 1 {
			// No indexes to (re)create, all done!
			return nil
		}
	}

	failpoint.Inject("reorgPartitionAfterDataCopy", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test in reorgPartitionAfterDataCopy")
		}
	})

	if !bytes.Equal(reorgInfo.currElement.TypeKey, meta.IndexElementKey) {
		// row data has been copied, now proceed with creating the indexes
		// on the new AddingDefinitions partitions
		reorgInfo.PhysicalTableID = t.Meta().Partition.AddingDefinitions[0].ID
		reorgInfo.currElement = reorgInfo.elements[1]
		var physTbl table.PhysicalTable
		if tbl, ok := t.(table.PartitionedTable); ok {
			physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
		} else if tbl, ok := t.(table.PhysicalTable); ok {
			// This may be used when partitioning a non-partitioned table
			physTbl = tbl
		}
		// Get the original start handle and end handle.
		currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
		if err != nil {
			return errors.Trace(err)
		}
		startHandle, endHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
		if err != nil {
			return errors.Trace(err)
		}

		// Always (re)start with the full PhysicalTable range
		reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle

		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.Int64("partitionTableId", physTbl.GetPhysicalID()),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
	}

	pi := t.Meta().GetPartitionInfo()
	if _, err = findNextPartitionID(reorgInfo.PhysicalTableID, pi.AddingDefinitions); err == nil {
		// Now build all the indexes in the new partitions.
		err = w.addTableIndex(jobCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// All indexes are up-to-date for new partitions,
		// now we only need to add the existing non-touched partitions
		// to the global indexes
		reorgInfo.elements = reorgInfo.elements[:0]
		for _, indexInfo := range t.Meta().Indices {
			if indexInfo.Global && indexInfo.State == model.StateWriteReorganization {
				reorgInfo.elements = append(reorgInfo.elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
			}
		}
		if len(reorgInfo.elements) == 0 {
			reorgInfo.PhysicalTableID = 0
		}
		if reorgInfo.PhysicalTableID != 0 {
			reorgInfo.currElement = reorgInfo.elements[0]
			// Find the first non-touched partition: one that is NOT in
			// AddingDefinitions (newly created, already indexed above) and
			// NOT in DroppingDefinitions (being removed).
			pid := int64(0)
			for _, def := range pi.Definitions {
				if _, addErr := findNextPartitionID(def.ID, pi.AddingDefinitions); addErr == nil {
					continue
				}
				if _, dropErr := findNextPartitionID(def.ID, pi.DroppingDefinitions); dropErr == nil {
					continue
				}
				pid = def.ID
				break
			}

			// if pid == 0 => All partitions will be dropped/added, nothing more to add to global indexes.
			reorgInfo.PhysicalTableID = pid
		}
		if reorgInfo.PhysicalTableID != 0 {
			var physTbl table.PhysicalTable
			if tbl, ok := t.(table.PartitionedTable); ok {
				physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
			} else if tbl, ok := t.(table.PhysicalTable); ok {
				// This may be used when partitioning a non-partitioned table
				physTbl = tbl
			}
			// Get the original start handle and end handle.
			currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
			if err != nil {
				return errors.Trace(err)
			}
			startHandle, endHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
			if err != nil {
				return errors.Trace(err)
			}

			// Always (re)start with the full PhysicalTable range
			reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle
		}

		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
	}
	if _, err = findNextNonTouchedPartitionID(reorgInfo.PhysicalTableID, pi); err == nil {
		err = w.addTableIndex(jobCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		reorgInfo.PhysicalTableID = 0
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("Non touched partitions done",
			zap.Int64("jobID", reorgInfo.Job.ID), zap.Error(err))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func bundlesForExchangeTablePartition(t *meta.Mutator, pt *model.TableInfo, newPar *model.PartitionDefinition, nt *model.TableInfo) ([]*placement.Bundle, error) {
	bundles := make([]*placement.Bundle, 0, 3)

	ptBundle, err := placement.NewTableBundle(t, pt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ptBundle != nil {
		bundles = append(bundles, ptBundle)
	}

	parBundle, err := placement.NewPartitionBundle(t, *newPar)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if parBundle != nil {
		bundles = append(bundles, parBundle)
	}

	ntBundle, err := placement.NewTableBundle(t, nt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ntBundle != nil {
		bundles = append(bundles, ntBundle)
	}

	if parBundle == nil && ntBundle != nil {
		// newPar.ID is the ID of old table to exchange, so ntBundle != nil means it has some old placement settings.
		// We should remove it in this situation
		bundles = append(bundles, placement.NewBundle(newPar.ID))
	}

	if parBundle != nil && ntBundle == nil {
		// nt.ID is the ID of old partition to exchange, so parBundle != nil means it has some old placement settings.
		// We should remove it in this situation
		bundles = append(bundles, placement.NewBundle(nt.ID))
	}

	return bundles, nil
}

func checkExchangePartitionRecordValidation(
	ctx context.Context,
	w *worker,
	ptbl, ntbl table.Table,
	pschemaName, nschemaName, partitionName string,
) error {
	verifyFunc := func(sql string, params ...any) error {
		sctx, err := w.sessPool.Get()
		if err != nil {
			return errors.Trace(err)
		}
		defer w.sessPool.Put(sctx)

		rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
			ctx,
			nil,
			sql,
			params...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := len(rows)
		if rowCount != 0 {
			return errors.Trace(dbterror.ErrRowDoesNotMatchPartition)
		}
		// Check warnings!
		// Is it possible to check how many rows where checked as well?
		return nil
	}
	genConstraintCondition := func(constraints []*table.Constraint) string {
		var buf strings.Builder
		buf.WriteString("not (")
		for i, cons := range constraints {
			if i != 0 {
				buf.WriteString(" and ")
			}
			buf.WriteString(fmt.Sprintf("(%s)", cons.ExprString))
		}
		buf.WriteString(")")
		return buf.String()
	}
	type CheckConstraintTable interface {
		WritableConstraint() []*table.Constraint
	}

	pt := ptbl.Meta()
	index, _, err := getPartitionDef(pt, partitionName)
	if err != nil {
		return errors.Trace(err)
	}

	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramList := []any{nschemaName, ntbl.Meta().Name.L}
	checkNt := true

	pi := pt.Partition
	switch pi.Type {
	case ast.PartitionTypeHash:
		if pi.Num == 1 {
			checkNt = false
		} else {
			buf.WriteString("mod(")
			buf.WriteString(pi.Expr)
			buf.WriteString(", %?) != %?")
			paramList = append(paramList, pi.Num, index)
			if index != 0 {
				// TODO: if hash result can't be NULL, we can remove the check part.
				// For example hash(id), but id is defined not NULL.
				buf.WriteString(" or mod(")
				buf.WriteString(pi.Expr)
				buf.WriteString(", %?) is null")
				paramList = append(paramList, pi.Num, index)
			}
		}
	case ast.PartitionTypeRange:
		// Table has only one partition and has the maximum value
		if len(pi.Definitions) == 1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
			checkNt = false
		} else {
			// For range expression and range columns
			if len(pi.Columns) == 0 {
				conds, params := buildCheckSQLConditionForRangeExprPartition(pi, index)
				buf.WriteString(conds)
				paramList = append(paramList, params...)
			} else {
				conds, params := buildCheckSQLConditionForRangeColumnsPartition(pi, index)
				buf.WriteString(conds)
				paramList = append(paramList, params...)
			}
		}
	case ast.PartitionTypeList:
		if len(pi.Columns) == 0 {
			conds := buildCheckSQLConditionForListPartition(pi, index)
			buf.WriteString(conds)
		} else {
			conds := buildCheckSQLConditionForListColumnsPartition(pi, index)
			buf.WriteString(conds)
		}
	default:
		return dbterror.ErrUnsupportedPartitionType.GenWithStackByArgs(pt.Name.O)
	}

	if vardef.EnableCheckConstraint.Load() {
		pcc, ok := ptbl.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert table partition failed")
		}
		pCons := pcc.WritableConstraint()
		if len(pCons) > 0 {
			if !checkNt {
				checkNt = true
			} else {
				buf.WriteString(" or ")
			}
			buf.WriteString(genConstraintCondition(pCons))
		}
	}
	// Check non-partition table records.
	if checkNt {
		buf.WriteString(" limit 1")
		err = verifyFunc(buf.String(), paramList...)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Check partition table records.
	if vardef.EnableCheckConstraint.Load() {
		ncc, ok := ntbl.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert table partition failed")
		}
		nCons := ncc.WritableConstraint()
		if len(nCons) > 0 {
			buf.Reset()
			buf.WriteString("select 1 from %n.%n partition(%n) where ")
			buf.WriteString(genConstraintCondition(nCons))
			buf.WriteString(" limit 1")
			err = verifyFunc(buf.String(), pschemaName, pt.Name.L, partitionName)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func checkExchangePartitionPlacementPolicy(t *meta.Mutator, ntPPRef, ptPPRef, partPPRef *model.PolicyRefInfo) error {
	partitionPPRef := partPPRef
	if partitionPPRef == nil {
		partitionPPRef = ptPPRef
	}

	if ntPPRef == nil && partitionPPRef == nil {
		return nil
	}
	if ntPPRef == nil || partitionPPRef == nil {
		return dbterror.ErrTablesDifferentMetadata
	}

	ptPlacementPolicyInfo, _ := getPolicyInfo(t, partitionPPRef.ID)
	ntPlacementPolicyInfo, _ := getPolicyInfo(t, ntPPRef.ID)
	if ntPlacementPolicyInfo == nil && ptPlacementPolicyInfo == nil {
		return nil
	}
	if ntPlacementPolicyInfo == nil || ptPlacementPolicyInfo == nil {
		return dbterror.ErrTablesDifferentMetadata
	}
	if ntPlacementPolicyInfo.Name.L != ptPlacementPolicyInfo.Name.L {
		return dbterror.ErrTablesDifferentMetadata
	}

	return nil
}

func buildCheckSQLConditionForRangeExprPartition(pi *model.PartitionInfo, index int) (string, []any) {
	var buf strings.Builder
	paramList := make([]any, 0, 2)
	// Since the pi.Expr string may contain the identifier, which couldn't be escaped in our ParseWithParams(...)
	// So we write it to the origin sql string here.
	if index == 0 {
		// TODO: Handle MAXVALUE in first partition
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %?")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index].LessThan[0]))
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" is null")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index-1].LessThan[0]))
	} else {
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" is null")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index-1].LessThan[0]), driver.UnwrapFromSingleQuotes(pi.Definitions[index].LessThan[0]))
	}
	return buf.String(), paramList
}

func buildCheckSQLConditionForRangeColumnsPartition(pi *model.PartitionInfo, index int) (string, []any) {
	var buf strings.Builder
	paramList := make([]any, 0, len(pi.Columns)*2)

	hasLowerBound := index > 0
	needOR := false

	// Lower bound check (for all partitions except first)
	if hasLowerBound {
		currVals := pi.Definitions[index-1].LessThan
		for i := range pi.Columns {
			nextIsMax := false
			if i < (len(pi.Columns)-1) && strings.EqualFold(currVals[i+1], partitionMaxValue) {
				nextIsMax = true
			}
			if needOR {
				buf.WriteString(" OR ")
			}
			if i > 0 {
				buf.WriteString("(")
				// All previous columns must be equal and non-NULL
				for j := range i {
					if j > 0 {
						buf.WriteString(" AND ")
					}
					buf.WriteString("(%n = %?)")
					paramList = append(paramList, pi.Columns[j].L, driver.UnwrapFromSingleQuotes(currVals[j]))
				}
				buf.WriteString(" AND ")
			}
			paramList = append(paramList, pi.Columns[i].L, driver.UnwrapFromSingleQuotes(currVals[i]), pi.Columns[i].L)
			if nextIsMax {
				buf.WriteString("(%n <= %? OR %n IS NULL)")
			} else {
				buf.WriteString("(%n < %? OR %n IS NULL)")
			}
			if i > 0 {
				buf.WriteString(")")
			}
			needOR = true
			if nextIsMax {
				break
			}
		}
	}

	currVals := pi.Definitions[index].LessThan
	// Upper bound check (for all partitions)
	for i := range pi.Columns {
		if strings.EqualFold(currVals[i], partitionMaxValue) {
			break
		}
		if needOR {
			buf.WriteString(" OR ")
		}
		if i > 0 {
			buf.WriteString("(")
			// All previous columns must be equal
			for j := range i {
				if j > 0 {
					buf.WriteString(" AND ")
				}
				paramList = append(paramList, pi.Columns[j].L, driver.UnwrapFromSingleQuotes(currVals[j]))
				buf.WriteString("(%n = %?)")
			}
			buf.WriteString(" AND ")
		}
		isLast := i == len(pi.Columns)-1
		if isLast {
			buf.WriteString("(%n >= %?)")
		} else {
			buf.WriteString("(%n > %?)")
		}
		paramList = append(paramList, pi.Columns[i].L, driver.UnwrapFromSingleQuotes(currVals[i]))
		if i > 0 {
			buf.WriteString(")")
		}
		needOR = true
	}

	return buf.String(), paramList
}

func buildCheckSQLConditionForListPartition(pi *model.PartitionInfo, index int) string {
	var buf strings.Builder
	// TODO: Handle DEFAULT partition
	buf.WriteString("not (")
	for i, inValue := range pi.Definitions[index].InValues {
		if i != 0 {
			buf.WriteString(" OR ")
		}
		// AND has higher priority than OR, so no need for parentheses
		for j, val := range inValue {
			if j != 0 {
				// Should never happen, since there should be no multi-columns, only a single expression :)
				buf.WriteString(" AND ")
			}
			// null-safe compare '<=>'
			buf.WriteString(fmt.Sprintf("(%s) <=> %s", pi.Expr, val))
		}
	}
	buf.WriteString(")")
	return buf.String()
}

func buildCheckSQLConditionForListColumnsPartition(pi *model.PartitionInfo, index int) string {
	var buf strings.Builder
	// TODO: Verify if this is correct!!!
	// TODO: Handle DEFAULT partition!
	// TODO: use paramList with column names, instead of quoting.
	// How to find a match?
	// (row <=> vals1) OR (row <=> vals2)
	// How to find a non-matching row:
	// NOT ( (row <=> vals1) OR (row <=> vals2) ... )
	buf.WriteString("not (")
	colNames := make([]string, 0, len(pi.Columns))
	for i := range pi.Columns {
		// TODO: Add test for this!
		// TODO: check if there are no proper quoting function for this?
		// TODO: Maybe Sprintf("%#q", str) ?
		n := "`" + strings.ReplaceAll(pi.Columns[i].O, "`", "``") + "`"
		colNames = append(colNames, n)
	}
	for i, colValues := range pi.Definitions[index].InValues {
		if i != 0 {
			buf.WriteString(" OR ")
		}
		// AND has higher priority than OR, so no need for parentheses
		for j, val := range colValues {
			if j != 0 {
				buf.WriteString(" AND ")
			}
			// null-safe compare '<=>'
			buf.WriteString(fmt.Sprintf("%s <=> %s", colNames[j], val))
		}
	}
	buf.WriteString(")")
	return buf.String()
}
