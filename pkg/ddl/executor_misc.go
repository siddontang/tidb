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
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	rg "github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

func (e *executor) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	lockTables := make([]model.TableLockTpInfo, 0, len(stmt.TableLocks))
	sessionInfo := model.SessionInfo{
		ServerID:  e.uuid,
		SessionID: ctx.GetSessionVars().ConnectionID,
	}
	uniqueTableID := make(map[int64]struct{})
	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(stmt.TableLocks))
	// Check whether the table was already locked by another.
	for _, tl := range stmt.TableLocks {
		tb := tl.Table
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := e.getSchemaAndTableByIdent(ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return table.ErrUnsupportedOp.GenWithStackByArgs()
		}

		err = checkTableLocked(t.Meta(), tl.Type, sessionInfo)
		if err != nil {
			return err
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		lockTables = append(lockTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID, Tp: tl.Type})
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}

	unlockTables := ctx.GetAllTableLocks()
	args := &model.LockTablesArgs{
		LockTables:   lockTables,
		UnlockTables: unlockTables,
		SessionInfo:  sessionInfo,
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            lockTables[0].SchemaID,
		TableID:             lockTables[0].TableID,
		Type:                model.ActionLockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	// AddTableLock here is avoiding this job was executed successfully but the session was killed before return.
	ctx.AddTableLock(lockTables)
	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseTableLocks(unlockTables)
		ctx.AddTableLock(lockTables)
	}
	return errors.Trace(err)
}

// UnlockTables uses to execute unlock tables statement.
func (e *executor) UnlockTables(ctx sessionctx.Context, unlockTables []model.TableLockTpInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	args := &model.LockTablesArgs{
		UnlockTables: unlockTables,
		SessionInfo: model.SessionInfo{
			ServerID:  e.uuid,
			SessionID: ctx.GetSessionVars().ConnectionID,
		},
	}

	involveSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(unlockTables))
	is := e.infoCache.GetLatest()
	for _, t := range unlockTables {
		schema, ok := is.SchemaByID(t.SchemaID)
		if !ok {
			continue
		}
		tbl, ok := is.TableByID(e.ctx, t.TableID)
		if !ok {
			continue
		}
		involveSchemaInfo = append(involveSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    tbl.Meta().Name.L,
		})
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            unlockTables[0].SchemaID,
		TableID:             unlockTables[0].TableID,
		Type:                model.ActionUnlockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}

	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseAllTableLocks()
	}
	return errors.Trace(err)
}

func (e *executor) AlterTableMode(sctx sessionctx.Context, args *model.AlterTableModeArgs) error {
	is := e.infoCache.GetLatest()

	schema, ok := is.SchemaByID(args.SchemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(fmt.Sprintf("SchemaID: %v", args.SchemaID))
	}

	table, ok := is.TableByID(e.ctx, args.TableID)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(
			schema.Name, fmt.Sprintf("TableID: %d", args.TableID))
	}

	ok = validateTableMode(table.Meta().Mode, args.TableMode)
	if !ok {
		return infoschema.ErrInvalidTableModeSet.GenWithStackByArgs(table.Meta().Mode, args.TableMode, table.Meta().Name.O)
	}
	if table.Meta().Mode == args.TableMode {
		return nil
	}

	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       args.SchemaID,
		TableID:        args.TableID,
		SchemaName:     schema.Name.O,
		Type:           model.ActionAlterTableMode,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    table.Meta().Name.L,
			},
		},
	}
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := e.doDDLJob2(sctx, job, args)
	return errors.Trace(err)
}

func throwErrIfInMemOrSysDB(ctx sessionctx.Context, dbLowerName string) error {
	if metadef.IsMemOrSysDB(dbLowerName) {
		if ctx.GetSessionVars().User != nil {
			return infoschema.ErrAccessDenied.GenWithStackByArgs(ctx.GetSessionVars().User.Username, ctx.GetSessionVars().User.Hostname)
		}
		return infoschema.ErrAccessDenied.GenWithStackByArgs("", "")
	}
	return nil
}

func (e *executor) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	uniqueTableID := make(map[int64]struct{})
	cleanupTables := make([]model.TableLockTpInfo, 0, len(tables))
	unlockedTablesNum := 0
	involvingSchemaInfo := make([]model.InvolvingSchemaInfo, 0, len(tables))
	// Check whether the table was already locked by another.
	for _, tb := range tables {
		err := throwErrIfInMemOrSysDB(ctx, tb.Schema.L)
		if err != nil {
			return err
		}
		schema, t, err := e.getSchemaAndTableByIdent(ast.Ident{Schema: tb.Schema, Name: tb.Name})
		if err != nil {
			return errors.Trace(err)
		}
		if t.Meta().IsView() || t.Meta().IsSequence() {
			return table.ErrUnsupportedOp
		}
		// Maybe the table t was not locked, but still try to unlock this table.
		// If we skip unlock the table here, the job maybe not consistent with the job.Query.
		// eg: unlock tables t1,t2;  If t2 is not locked and skip here, then the job will only unlock table t1,
		// and this behaviour is not consistent with the sql query.
		if !t.Meta().IsLocked() {
			unlockedTablesNum++
		}
		if _, ok := uniqueTableID[t.Meta().ID]; ok {
			return infoschema.ErrNonuniqTable.GenWithStackByArgs(t.Meta().Name)
		}
		uniqueTableID[t.Meta().ID] = struct{}{}
		cleanupTables = append(cleanupTables, model.TableLockTpInfo{SchemaID: schema.ID, TableID: t.Meta().ID})
		involvingSchemaInfo = append(involvingSchemaInfo, model.InvolvingSchemaInfo{
			Database: schema.Name.L,
			Table:    t.Meta().Name.L,
		})
	}
	// If the num of cleanupTables is 0, or all cleanupTables is unlocked, just return here.
	if len(cleanupTables) == 0 || len(cleanupTables) == unlockedTablesNum {
		return nil
	}

	args := &model.LockTablesArgs{
		UnlockTables: cleanupTables,
		IsCleanup:    true,
	}
	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            cleanupTables[0].SchemaID,
		TableID:             cleanupTables[0].TableID,
		Type:                model.ActionUnlockTable,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involvingSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	err := e.doDDLJob2(ctx, job, args)
	if err == nil {
		ctx.ReleaseTableLocks(cleanupTables)
	}
	return errors.Trace(err)
}

func (e *executor) RepairTable(ctx sessionctx.Context, createStmt *ast.CreateTableStmt) error {
	// Existence of DB and table has been checked in the preprocessor.
	oldTableInfo, ok := (ctx.Value(domainutil.RepairedTable)).(*model.TableInfo)
	if !ok || oldTableInfo == nil {
		return dbterror.ErrRepairTableFail.GenWithStack("Failed to get the repaired table")
	}
	oldDBInfo, ok := (ctx.Value(domainutil.RepairedDatabase)).(*model.DBInfo)
	if !ok || oldDBInfo == nil {
		return dbterror.ErrRepairTableFail.GenWithStack("Failed to get the repaired database")
	}
	// By now only support same DB repair.
	if createStmt.Table.Schema.L != oldDBInfo.Name.L {
		return dbterror.ErrRepairTableFail.GenWithStack("Repaired table should in same database with the old one")
	}

	// It is necessary to specify the table.ID and partition.ID manually.
	newTableInfo, err := buildTableInfoWithCheck(NewMetaBuildContextWithSctx(ctx), ctx.GetStore(), createStmt,
		oldTableInfo.Charset, oldTableInfo.Collate, oldTableInfo.PlacementPolicyRef)
	if err != nil {
		return errors.Trace(err)
	}
	if err = rewritePartitionQueryString(ctx, createStmt.Partition, newTableInfo); err != nil {
		return errors.Trace(err)
	}
	// Override newTableInfo with oldTableInfo's element necessary.
	// TODO: There may be more element assignments here, and the new TableInfo should be verified with the actual data.
	newTableInfo.ID = oldTableInfo.ID
	if err = checkAndOverridePartitionID(newTableInfo, oldTableInfo); err != nil {
		return err
	}
	newTableInfo.AutoIncID = oldTableInfo.AutoIncID
	// If any old columnInfo has lost, that means the old column ID lost too, repair failed.
	for i, newOne := range newTableInfo.Columns {
		old := oldTableInfo.FindPublicColumnByName(newOne.Name.L)
		if old == nil {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " has lost")
		}
		if newOne.GetType() != old.GetType() {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Column " + newOne.Name.L + " type should be the same")
		}
		if newOne.GetFlen() != old.GetFlen() {
			logutil.DDLLogger().Warn("admin repair table : Column " + newOne.Name.L + " flen is not equal to the old one")
		}
		newTableInfo.Columns[i].ID = old.ID
	}
	// If any old indexInfo has lost, that means the index ID lost too, so did the data, repair failed.
	for i, newOne := range newTableInfo.Indices {
		old := getIndexInfoByNameAndColumn(oldTableInfo, newOne)
		if old == nil {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " has lost")
		}
		if newOne.Tp != old.Tp {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Index " + newOne.Name.L + " type should be the same")
		}
		newTableInfo.Indices[i].ID = old.ID
	}

	newTableInfo.State = model.StatePublic
	err = checkTableInfoValid(newTableInfo)
	if err != nil {
		return err
	}
	newTableInfo.State = model.StateNone

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       oldDBInfo.ID,
		TableID:        newTableInfo.ID,
		SchemaName:     oldDBInfo.Name.L,
		TableName:      newTableInfo.Name.L,
		Type:           model.ActionRepairTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.RepairTableArgs{TableInfo: newTableInfo}
	err = e.doDDLJob2(ctx, job, args)
	if err == nil {
		// Remove the old TableInfo from repairInfo before domain reload.
		domainutil.RepairInfo.RemoveFromRepairInfo(oldDBInfo.Name.L, oldTableInfo.Name.L)
	}
	return errors.Trace(err)
}

func (e *executor) OrderByColumns(ctx sessionctx.Context, ident ast.Ident) error {
	_, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	if tb.Meta().GetPkColInfo() != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("ORDER BY ignored as there is a user-defined clustered index in the table '%s'", ident.Name))
	}
	return nil
}

func (e *executor) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	sequenceInfo, err := buildSequenceInfo(stmt, ident)
	if err != nil {
		return err
	}
	// TiDB describe the sequence within a tableInfo, as a same-level object of a table and view.
	tbInfo, err := BuildTableInfo(NewMetaBuildContextWithSctx(ctx), ident.Name, nil, nil, "", "")
	if err != nil {
		return err
	}
	tbInfo.Sequence = sequenceInfo

	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}

	return e.CreateTableWithInfo(ctx, ident.Schema, tbInfo, nil, WithOnExist(onExist))
}

func (e *executor) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	ident := ast.Ident{Name: stmt.Name.Name, Schema: stmt.Name.Schema}
	is := e.infoCache.GetLatest()
	// Check schema existence.
	db, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}
	// Check table existence.
	tbl, err := is.TableByName(context.Background(), ident.Schema, ident.Name)
	if err != nil {
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	if !tbl.Meta().IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(ident.Schema, ident.Name, "SEQUENCE")
	}

	// Validate the new sequence option value in old sequenceInfo.
	oldSequenceInfo := tbl.Meta().Sequence
	copySequenceInfo := *oldSequenceInfo
	_, _, err = alterSequenceOptions(stmt.SeqOptions, ident, &copySequenceInfo)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       db.ID,
		TableID:        tbl.Meta().ID,
		SchemaName:     db.Name.L,
		TableName:      tbl.Meta().Name.L,
		Type:           model.ActionAlterSequence,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterSequenceArgs{
		Ident:      ident,
		SeqOptions: stmt.SeqOptions,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return e.dropTableObject(ctx, stmt.Sequences, stmt.IfExists, sequenceObject)
}

func (e *executor) AlterIndexVisibility(ctx sessionctx.Context, ident ast.Ident, indexName ast.CIStr, visibility ast.IndexVisibility) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return err
	}

	invisible := false
	if visibility == ast.IndexVisibilityInvisible {
		invisible = true
	}

	skip, err := validateAlterIndexVisibility(ctx, indexName, invisible, tb.Meta())
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		return nil
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionAlterIndexVisibility,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AlterIndexVisibilityArgs{
		IndexName: indexName,
		Invisible: invisible,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) error {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}
	meta := tb.Meta()

	rule := label.NewRule()
	err = rule.ApplyAttributesSpec(spec.AttributesSpec)
	if err != nil {
		return dbterror.ErrInvalidAttributesSpec.GenWithStackByArgs(err)
	}
	ids := getIDs([]*model.TableInfo{meta})
	rule.Reset(schema.Name.L, meta.Name.L, "", ids...)

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTableAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	pdLabelRule := (*pdhttp.LabelRule)(rule)
	args := &model.AlterTableAttributesArgs{LabelRule: pdLabelRule}
	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (e *executor) AlterTablePartitionAttributes(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	schema, tb, err := e.getSchemaAndTableByIdent(ident)
	if err != nil {
		return errors.Trace(err)
	}

	meta := tb.Meta()
	if meta.Partition == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(meta, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	rule := label.NewRule()
	err = rule.ApplyAttributesSpec(spec.AttributesSpec)
	if err != nil {
		return dbterror.ErrInvalidAttributesSpec.GenWithStackByArgs(err)
	}
	rule.Reset(schema.Name.L, meta.Name.L, spec.PartitionNames[0].L, partitionID)

	pdLabelRule := (*pdhttp.LabelRule)(rule)
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        meta.ID,
		SchemaName:     schema.Name.L,
		TableName:      meta.Name.L,
		Type:           model.ActionAlterTablePartitionAttributes,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePartitionArgs{
		PartitionID: partitionID,
		LabelRule:   pdLabelRule,
	}

	err = e.doDDLJob2(ctx, job, args)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(err)
}

func (e *executor) AlterTablePartitionOptions(ctx sessionctx.Context, ident ast.Ident, spec *ast.AlterTableSpec) (err error) {
	var policyRefInfo *model.PolicyRefInfo
	if spec.Options != nil {
		for _, op := range spec.Options {
			switch op.Tp {
			case ast.TableOptionPlacementPolicy:
				policyRefInfo = &model.PolicyRefInfo{
					Name: ast.NewCIStr(op.StrValue),
				}
			default:
				return errors.Trace(errors.New("unknown partition option"))
			}
		}
	}

	if policyRefInfo != nil {
		err = e.AlterTablePartitionPlacement(ctx, ident, spec, policyRefInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *executor) AlterTablePartitionPlacement(ctx sessionctx.Context, tableIdent ast.Ident, spec *ast.AlterTableSpec, policyRefInfo *model.PolicyRefInfo) (err error) {
	schema, tb, err := e.getSchemaAndTableByIdent(tableIdent)
	if err != nil {
		return errors.Trace(err)
	}

	tblInfo := tb.Meta()
	if tblInfo.Partition == nil {
		return errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	partitionID, err := tables.FindPartitionByName(tblInfo, spec.PartitionNames[0].L)
	if err != nil {
		return errors.Trace(err)
	}

	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	policyRefInfo, err = checkAndNormalizePlacementPolicy(ctx, policyRefInfo)
	if err != nil {
		return errors.Trace(err)
	}

	var involveSchemaInfo []model.InvolvingSchemaInfo
	if policyRefInfo != nil {
		involveSchemaInfo = []model.InvolvingSchemaInfo{
			{
				Database: schema.Name.L,
				Table:    tblInfo.Name.L,
			},
			{
				Policy: policyRefInfo.Name.L,
				Mode:   model.SharedInvolving,
			},
		}
	}

	job := &model.Job{
		Version:             model.GetJobVerInUse(),
		SchemaID:            schema.ID,
		TableID:             tblInfo.ID,
		SchemaName:          schema.Name.L,
		TableName:           tblInfo.Name.L,
		Type:                model.ActionAlterTablePartitionPlacement,
		BinlogInfo:          &model.HistoryInfo{},
		CDCWriteSource:      ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: involveSchemaInfo,
		SQLMode:             ctx.GetSessionVars().SQLMode,
	}
	args := &model.AlterTablePartitionArgs{
		PartitionID:   partitionID,
		PolicyRefInfo: policyRefInfo,
	}

	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// AddResourceGroup implements the DDL interface, creates a resource group.
func (e *executor) AddResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	groupInfo := &model.ResourceGroupInfo{Name: groupName, ResourceGroupSettings: model.NewResourceGroupSettings()}
	groupInfo, err = buildResourceGroup(groupInfo, stmt.ResourceGroupOptionList)
	if err != nil {
		return err
	}

	if _, ok := e.infoCache.GetLatest().ResourceGroupByName(groupName); ok {
		if stmt.IfNotExists {
			err = infoschema.ErrResourceGroupExists.FastGenByArgs(groupName)
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return infoschema.ErrResourceGroupExists.GenWithStackByArgs(groupName)
	}

	if err := checkResourceGroupValidation(groupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("create resource group", zap.String("name", groupName.O), zap.Stringer("resource group settings", groupInfo.ResourceGroupSettings))

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaName:     groupName.L,
		Type:           model.ActionCreateResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: groupInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: groupInfo}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

// DropResourceGroup implements the DDL interface.
func (e *executor) DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	if groupName.L == rg.DefaultResourceGroupName {
		return resourcegroup.ErrDroppingInternalResourceGroup
	}
	is := e.infoCache.GetLatest()
	// Check group existence.
	group, ok := is.ResourceGroupByName(groupName)
	if !ok {
		err = infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(groupName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	// check to see if some user has dependency on the group
	checker := privilege.GetPrivilegeManager(ctx)
	if checker == nil {
		return errors.New("miss privilege checker")
	}
	user, matched := checker.MatchUserResourceGroupName(ctx.GetRestrictedSQLExecutor(), groupName.L)
	if matched {
		err = errors.Errorf("user [%s] depends on the resource group to drop", user)
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       group.ID,
		SchemaName:     group.Name.L,
		Type:           model.ActionDropResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: groupName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: &model.ResourceGroupInfo{Name: groupName}}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

// AlterResourceGroup implements the DDL interface.
func (e *executor) AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) (err error) {
	groupName := stmt.ResourceGroupName
	is := e.infoCache.GetLatest()
	// Check group existence.
	group, ok := is.ResourceGroupByName(groupName)
	if !ok {
		err := infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(groupName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}
	newGroupInfo, err := buildResourceGroup(group, stmt.ResourceGroupOptionList)
	if err != nil {
		return errors.Trace(err)
	}

	if err := checkResourceGroupValidation(newGroupInfo); err != nil {
		return err
	}

	logutil.DDLLogger().Debug("alter resource group", zap.String("name", groupName.L), zap.Stringer("new resource group settings", newGroupInfo.ResourceGroupSettings))

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       newGroupInfo.ID,
		SchemaName:     newGroupInfo.Name.L,
		Type:           model.ActionAlterResourceGroup,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			ResourceGroup: newGroupInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.ResourceGroupArgs{RGInfo: newGroupInfo}
	err = e.doDDLJob2(ctx, job, args)
	return err
}

func (e *executor) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}

	if stmt.OrReplace && stmt.IfNotExists {
		return dbterror.ErrWrongUsage.GenWithStackByArgs("OR REPLACE", "IF NOT EXISTS")
	}

	policyInfo, err := buildPolicyInfo(stmt.PolicyName, stmt.PlacementOptions)
	if err != nil {
		return errors.Trace(err)
	}

	var onExists OnExist
	switch {
	case stmt.IfNotExists:
		onExists = OnExistIgnore
	case stmt.OrReplace:
		onExists = OnExistReplace
	default:
		onExists = OnExistError
	}

	return e.CreatePlacementPolicyWithInfo(ctx, policyInfo, onExists)
}

func (e *executor) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := e.infoCache.GetLatest()
	// Check policy existence.
	policy, ok := is.PolicyByName(policyName)
	if !ok {
		err = infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	if err = CheckPlacementPolicyNotInUseFromInfoSchema(is, policy); err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionDropPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: policyName.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}

	args := &model.PlacementPolicyArgs{
		PolicyName: policyName,
		PolicyID:   policy.ID,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) (err error) {
	if checkIgnorePlacementDDL(ctx) {
		return nil
	}
	policyName := stmt.PolicyName
	is := e.infoCache.GetLatest()
	// Check policy existence.
	policy, ok := is.PolicyByName(policyName)
	if !ok {
		return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName)
	}

	newPolicyInfo, err := buildPolicyInfo(policy.Name, stmt.PlacementOptions)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkPolicyValidation(newPolicyInfo.PlacementSettings)
	if err != nil {
		return err
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       policy.ID,
		SchemaName:     policy.Name.L,
		Type:           model.ActionAlterPlacementPolicy,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{
			Policy: newPolicyInfo.Name.L,
		}},
		SQLMode: ctx.GetSessionVars().SQLMode,
	}
	args := &model.PlacementPolicyArgs{
		Policy:   newPolicyInfo,
		PolicyID: policy.ID,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterTableCache(sctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return err
	}
	// if a table is already in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusEnable {
		return nil
	}

	// forbidden cache table in system database.
	if metadef.IsMemOrSysDB(schema.Name.L) {
		return errors.Trace(dbterror.ErrUnsupportedAlterCacheForSysTable)
	} else if t.Meta().TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache")
	}

	if t.Meta().Partition != nil {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("partition mode")
	}

	succ, err := checkCacheTableSize(e.store, t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	if !succ {
		return dbterror.ErrOptOnCacheTable.GenWithStackByArgs("table too large")
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	ddlQuery, _ := sctx.Value(sessionctx.QueryString).(string)
	// Initialize the cached table meta lock info in `mysql.table_cache_meta`.
	// The operation shouldn't fail in most cases, and if it does, return the error directly.
	// This DML and the following DDL is not atomic, that's not a problem.
	_, _, err = sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		"replace into mysql.table_cache_meta values (%?, 'NONE', 0, 0)", t.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}

	sctx.SetValue(sessionctx.QueryString, ddlQuery)

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
	}

	return e.doDDLJob2(sctx, job, &model.EmptyArgs{})
}

func checkCacheTableSize(store kv.Storage, tableID int64) (bool, error) {
	const cacheTableSizeLimit = 64 * (1 << 20) // 64M
	succ := true
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnCacheTable)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		txn.SetOption(kv.RequestSourceType, kv.InternalTxnCacheTable)
		prefix := tablecodec.GenTablePrefix(tableID)
		it, err := txn.Iter(prefix, prefix.PrefixNext())
		if err != nil {
			return errors.Trace(err)
		}
		defer it.Close()

		totalSize := 0
		for it.Valid() && it.Key().HasPrefix(prefix) {
			key := it.Key()
			value := it.Value()
			totalSize += len(key)
			totalSize += len(value)

			if totalSize > cacheTableSizeLimit {
				succ = false
				break
			}

			err = it.Next()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return succ, err
}

func (e *executor) AlterTableNoCache(ctx sessionctx.Context, ti ast.Ident) (err error) {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return err
	}
	// if a table is not in cache state, return directly
	if t.Meta().TableCacheStatusType == model.TableCacheStatusDisable {
		return nil
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		SchemaName:     schema.Name.L,
		TableName:      t.Meta().Name.L,
		TableID:        t.Meta().ID,
		Type:           model.ActionAlterNoCacheTable,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	return e.doDDLJob2(ctx, job, &model.EmptyArgs{})
}

func (e *executor) CreateCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName ast.CIStr, constr *ast.Constraint) error {
	schema, t, err := e.getSchemaAndTableByIdent(ti)
	if err != nil {
		return errors.Trace(err)
	}
	if constraintInfo := t.Meta().FindConstraintInfoByName(constrName.L); constraintInfo != nil {
		return infoschema.ErrCheckConstraintDupName.GenWithStackByArgs(constrName.L)
	}

	// allocate the temporary constraint name for dependency-check-error-output below.
	constrNames := map[string]bool{}
	for _, constr := range t.Meta().Constraints {
		constrNames[constr.Name.L] = true
	}
	setEmptyCheckConstraintName(t.Meta().Name.L, constrNames, []*ast.Constraint{constr})

	// existedColsMap can be used to check the existence of depended.
	existedColsMap := make(map[string]struct{})
	cols := t.Cols()
	for _, v := range cols {
		existedColsMap[v.Name.L] = struct{}{}
	}
	// check expression if supported
	if ok, err := table.IsSupportedExpr(constr); !ok {
		return err
	}

	dependedColsMap := findDependentColsInExpr(constr.Expr)
	dependedCols := make([]ast.CIStr, 0, len(dependedColsMap))
	for k := range dependedColsMap {
		if _, ok := existedColsMap[k]; !ok {
			// The table constraint depended on a non-existed column.
			return dbterror.ErrBadField.GenWithStackByArgs(k, "check constraint "+constr.Name+" expression")
		}
		dependedCols = append(dependedCols, ast.NewCIStr(k))
	}

	// build constraint meta info.
	tblInfo := t.Meta()

	// check auto-increment column
	if table.ContainsAutoIncrementCol(dependedCols, tblInfo) {
		return dbterror.ErrCheckConstraintRefersAutoIncrementColumn.GenWithStackByArgs(constr.Name)
	}
	// check foreign key
	if err := table.HasForeignKeyRefAction(tblInfo.ForeignKeys, nil, constr, dependedCols); err != nil {
		return err
	}
	constraintInfo, err := buildConstraintInfo(tblInfo, dependedCols, constr, model.StateNone)
	if err != nil {
		return errors.Trace(err)
	}
	// check if the expression is bool type
	if err := table.IfCheckConstraintExprBoolType(ctx.GetExprCtx(), constraintInfo, tblInfo); err != nil {
		return err
	}
	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAddCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		Priority:       ctx.GetSessionVars().DDLReorgPriority,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.AddCheckConstraintArgs{
		Constraint: constraintInfo,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) DropCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName ast.CIStr) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionDropCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.CheckConstraintArgs{
		ConstraintName: constrName,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) AlterCheckConstraint(ctx sessionctx.Context, ti ast.Ident, constrName ast.CIStr, enforced bool) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ti.Schema)
	if !ok {
		return errors.Trace(infoschema.ErrDatabaseNotExists)
	}
	t, err := is.TableByName(context.Background(), ti.Schema, ti.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ti.Schema, ti.Name))
	}
	tblInfo := t.Meta()

	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		return dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tblInfo.ID,
		SchemaName:     schema.Name.L,
		TableName:      tblInfo.Name.L,
		Type:           model.ActionAlterCheckConstraint,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}

	args := &model.CheckConstraintArgs{
		ConstraintName: constrName,
		Enforced:       enforced,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

func (e *executor) genPlacementPolicyID() (int64, error) {
	var ret int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err := kv.RunInNewTxn(ctx, e.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		var err error
		ret, err = m.GenPlacementPolicyID()
		return err
	})

	return ret, err
}

// DoDDLJob will return
// - nil: found in history DDL job and no job error
// - context.Cancel: job has been sent to worker, but not found in history DDL job before cancel
// - other: found in history DDL job and return that job error
func (e *executor) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapper(job, false))
}

func (e *executor) doDDLJob2(ctx sessionctx.Context, job *model.Job, args model.JobArgs) error {
	return e.DoDDLJobWrapper(ctx, NewJobWrapperWithArgs(job, args, false))
}

// DoDDLJobWrapper submit DDL job and wait it finishes.
// When fast create is enabled, we might merge multiple jobs into one, so do not
// depend on job.ID, use JobID from jobSubmitResult.
func (e *executor) DoDDLJobWrapper(ctx sessionctx.Context, jobW *JobWrapper) (resErr error) {
	if traceCtx := ctx.GetTraceCtx(); traceCtx != nil {
		r := tracing.StartRegion(traceCtx, "ddl.DoDDLJobWrapper")
		defer r.End()
	}

	job := jobW.Job
	job.TraceInfo = &tracing.TraceInfo{
		ConnectionID: ctx.GetSessionVars().ConnectionID,
		SessionAlias: ctx.GetSessionVars().SessionAlias,
		TraceID:      traceevent.TraceIDFromContext(ctx.GetTraceCtx()),
	}
	if mci := ctx.GetSessionVars().StmtCtx.MultiSchemaInfo; mci != nil {
		// In multiple schema change, we don't run the job.
		// Instead, we merge all the jobs into one pending job.
		return appendToSubJobs(mci, jobW)
	}
	if err := job.CheckInvolvingSchemaInfo(); err != nil {
		return err
	}
	// Get a global job ID and put the DDL job in the queue.
	setDDLJobQuery(ctx, job)

	if traceevent.IsEnabled(tracing.DDLJob) && ctx.GetTraceCtx() != nil {
		traceevent.TraceEvent(ctx.GetTraceCtx(), tracing.DDLJob, "ddlDelieverJobTask",
			zap.Uint64("ConnID", job.TraceInfo.ConnectionID),
			zap.String("SessionAlias", job.TraceInfo.SessionAlias))
	}
	e.deliverJobTask(jobW)

	failpoint.Inject("mockParallelSameDDLJobTwice", func(val failpoint.Value) {
		if val.(bool) {
			<-jobW.ResultCh[0]
			// The same job will be put to the DDL queue twice.
			job = job.Clone()
			newJobW := NewJobWrapperWithArgs(job, jobW.JobArgs, jobW.IDAllocated)
			e.deliverJobTask(newJobW)
			// The second job result is used for test.
			jobW = newJobW
		}
	})

	var result jobSubmitResult
	select {
	case <-e.ctx.Done():
		logutil.DDLLogger().Info("DoDDLJob will quit because context done")
		return e.ctx.Err()
	case res := <-jobW.ResultCh[0]:
		// worker should restart to continue handling tasks in limitJobCh, and send back through jobW.err
		result = res
	}
	// job.ID must be allocated after previous channel receive returns nil.
	jobID, err := result.jobID, result.err
	defer e.delJobDoneCh(jobID)
	if err != nil {
		// The transaction of enqueuing job is failed.
		return errors.Trace(err)
	}
	failpoint.InjectCall("waitJobSubmitted")

	sessVars := ctx.GetSessionVars()
	sessVars.StmtCtx.IsDDLJobInQueue.Store(true)

	ddlAction := job.Type
	if result.merged {
		logutil.DDLLogger().Info("DDL job submitted", zap.Int64("job_id", jobID), zap.String("query", job.Query), zap.String("merged", "true"))
	} else {
		logutil.DDLLogger().Info("DDL job submitted", zap.Stringer("job", job), zap.String("query", job.Query))
	}

	// lock tables works on table ID, for some DDLs which changes table ID, we need
	// make sure the session still tracks it.
	// we need add it here to avoid this ddl job was executed successfully but the
	// session was killed before return. The session will release all table locks
	// it holds, if we don't add the new locking table id here, the session may forget
	// to release the new locked table id when this ddl job was executed successfully
	// but the session was killed before return.
	if config.TableLockEnabled() {
		HandleLockTablesOnSuccessSubmit(ctx, jobW)
		defer func() {
			HandleLockTablesOnFinish(ctx, jobW, resErr)
		}()
	}

	var historyJob *model.Job

	// Attach the context of the jobId to the calling session so that
	// KILL can cancel this DDL job.
	ctx.GetSessionVars().StmtCtx.DDLJobID = jobID

	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	initInterval, _ := getJobCheckInterval(ddlAction, 0)
	ticker := time.NewTicker(chooseLeaseTime(10*e.lease, initInterval))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(ddlAction.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(ddlAction.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(ddlAction.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		recordLastDDLInfo(ctx, historyJob)
	}()
	i := 0
	notifyCh, _ := e.getJobDoneCh(jobID)
	for {
		failpoint.InjectCall("storeCloseInLoop")
		select {
		case _, ok := <-notifyCh:
			if !ok {
				// when fast create enabled, jobs might be merged, and we broadcast
				// the result by closing the channel, to avoid this loop keeps running
				// without sleeping on retryable error, we set it to nil.
				notifyCh = nil
			}
		case <-ticker.C:
			i++
			ticker = updateTickerInterval(ticker, 10*e.lease, ddlAction, i)
		case <-e.ctx.Done():
			logutil.DDLLogger().Info("DoDDLJob will quit because context done")
			return e.ctx.Err()
		}

		// If the connection being killed, we need to CANCEL the DDL job.
		if sessVars.SQLKiller.HandleSignal() == exeerrors.ErrQueryInterrupted {
			if atomic.LoadInt32(&sessVars.ConnectionStatus) == variable.ConnStatusShutdown {
				logutil.DDLLogger().Info("DoDDLJob will quit because context done")
				return context.Canceled
			}
			if sessVars.StmtCtx.DDLJobID != 0 {
				se, err := e.sessPool.Get()
				if err != nil {
					logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
					continue
				}
				sessVars.StmtCtx.DDLJobID = 0 // Avoid repeat.
				errs, err := CancelJobsBySystem(se, []int64{jobID})
				e.sessPool.Put(se)
				if len(errs) > 0 {
					logutil.DDLLogger().Warn("error canceling DDL job", zap.Error(errs[0]))
				}
				if err != nil {
					logutil.DDLLogger().Warn("Kill command could not cancel DDL job", zap.Error(err))
					continue
				}
			}
		}

		se, err := e.sessPool.Get()
		if err != nil {
			logutil.DDLLogger().Error("get session failed, check again", zap.Error(err))
			continue
		}
		historyJob, err = GetHistoryJobByID(se, jobID)
		e.sessPool.Put(se)
		if err != nil {
			logutil.DDLLogger().Error("get history DDL job failed, check again", zap.Error(err))
			continue
		}
		if historyJob == nil {
			logutil.DDLLogger().Debug("DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		e.checkHistoryJobInTest(ctx, historyJob)

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			// Judge whether there are some warnings when executing DDL under the certain SQL mode.
			if historyJob.ReorgMeta != nil && len(historyJob.ReorgMeta.Warnings) != 0 {
				if len(historyJob.ReorgMeta.Warnings) != len(historyJob.ReorgMeta.WarningsCount) {
					logutil.DDLLogger().Info("DDL warnings doesn't match the warnings count", zap.Int64("jobID", jobID))
				} else {
					for key, warning := range historyJob.ReorgMeta.Warnings {
						keyCount := historyJob.ReorgMeta.WarningsCount[key]
						if keyCount == 1 {
							ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
						} else {
							newMsg := fmt.Sprintf("%d warnings with this error code, first warning: "+warning.GetMsg(), keyCount)
							newWarning := dbterror.ClassTypes.Synthesize(terror.ErrCode(warning.Code()), newMsg)
							ctx.GetSessionVars().StmtCtx.AppendWarning(newWarning)
						}
					}
				}
			}
			appendMultiChangeWarningsToOwnerCtx(ctx, historyJob)

			logutil.DDLLogger().Info("DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			logutil.DDLLogger().Info("DDL job is failed", zap.Int64("jobID", jobID))
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

// HandleLockTablesOnSuccessSubmit handles the table lock for the job which is submitted
// successfully. exported for testing purpose.
func HandleLockTablesOnSuccessSubmit(ctx sessionctx.Context, jobW *JobWrapper) {
	if jobW.Type == model.ActionTruncateTable {
		if ok, lockTp := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.AddTableLock([]model.TableLockTpInfo{{
				SchemaID: jobW.SchemaID,
				TableID:  jobW.JobArgs.(*model.TruncateTableArgs).NewTableID,
				Tp:       lockTp,
			}})
		}
	}
}

// HandleLockTablesOnFinish handles the table lock for the job which is finished.
// exported for testing purpose.
func HandleLockTablesOnFinish(ctx sessionctx.Context, jobW *JobWrapper, ddlErr error) {
	if jobW.Type == model.ActionTruncateTable {
		if ddlErr != nil {
			newTableID := jobW.JobArgs.(*model.TruncateTableArgs).NewTableID
			ctx.ReleaseTableLockByTableIDs([]int64{newTableID})
			return
		}
		if ok, _ := ctx.CheckTableLocked(jobW.TableID); ok {
			ctx.ReleaseTableLockByTableIDs([]int64{jobW.TableID})
		}
	}
}

func (e *executor) getJobDoneCh(jobID int64) (chan struct{}, bool) {
	return e.ddlJobDoneChMap.Load(jobID)
}

func (e *executor) delJobDoneCh(jobID int64) {
	e.ddlJobDoneChMap.Delete(jobID)
}

func (e *executor) deliverJobTask(task *JobWrapper) {
	// TODO this might block forever, as the consumer part considers context cancel.
	e.limitJobCh <- task
}

func updateTickerInterval(ticker *time.Ticker, lease time.Duration, action model.ActionType, i int) *time.Ticker {
	interval, changed := getJobCheckInterval(action, i)
	if !changed {
		return ticker
	}
	// For now we should stop old ticker and create a new ticker
	ticker.Stop()
	return time.NewTicker(chooseLeaseTime(lease, interval))
}

func recordLastDDLInfo(ctx sessionctx.Context, job *model.Job) {
	if job == nil {
		return
	}
	ctx.GetSessionVars().LastDDLInfo.Query = job.Query
	ctx.GetSessionVars().LastDDLInfo.SeqNum = job.SeqNum
}

func setDDLJobQuery(ctx sessionctx.Context, job *model.Job) {
	switch job.Type {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionUnlockTable:
		job.Query = ""
	default:
		job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	}
}

var (
	fastDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
	}
	normalDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}
	slowDDLIntervalPolicy = []time.Duration{
		500 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		1 * time.Second,
		3 * time.Second,
	}
)

func getIntervalFromPolicy(policy []time.Duration, i int) (time.Duration, bool) {
	plen := len(policy)
	if i < plen {
		return policy[i], true
	}
	return policy[plen-1], false
}

func getJobCheckInterval(action model.ActionType, i int) (time.Duration, bool) {
	switch action {
	case model.ActionAddIndex, model.ActionAddPrimaryKey, model.ActionModifyColumn,
		model.ActionReorganizePartition,
		model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		return getIntervalFromPolicy(slowDDLIntervalPolicy, i)
	case model.ActionCreateTable, model.ActionCreateSchema:
		return getIntervalFromPolicy(fastDDLIntervalPolicy, i)
	default:
		return getIntervalFromPolicy(normalDDLIntervalPolicy, i)
	}
}

// NewDDLReorgMeta create a DDL ReorgMeta.
func NewDDLReorgMeta(ctx sessionctx.Context) *model.DDLReorgMeta {
	tzName, tzOffset := ddlutil.GetTimeZone(ctx)
	return &model.DDLReorgMeta{
		SQLMode:           ctx.GetSessionVars().SQLMode,
		Warnings:          make(map[errors.ErrorID]*terror.Error),
		WarningsCount:     make(map[errors.ErrorID]int64),
		Location:          &model.TimeZoneLocation{Name: tzName, Offset: tzOffset},
		ResourceGroupName: ctx.GetSessionVars().StmtCtx.ResourceGroupName,
		Version:           model.CurrentReorgMetaVersion,
	}
}

// RefreshMeta is a internal DDL job. In some cases, BR log restore will EXCHANGE
// PARTITION\DROP TABLE by write meta kv directly, and table info in meta kv
// is inconsistent with info schema. So when BR call AlterTableMode for new table
// will failure. RefreshMeta will reload schema diff to update info schema by
// schema ID and table ID to make sure data in meta kv and info schema is consistent.
func (e *executor) RefreshMeta(sctx sessionctx.Context, args *model.RefreshMetaArgs) error {
	job := &model.Job{
		Version:        model.JobVersion2,
		SchemaID:       args.SchemaID,
		TableID:        args.TableID,
		SchemaName:     args.InvolvedDB,
		Type:           model.ActionRefreshMeta,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: sctx.GetSessionVars().CDCWriteSource,
		SQLMode:        sctx.GetSessionVars().SQLMode,
		InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
			{
				Database: args.InvolvedDB,
				Table:    args.InvolvedTable,
			},
		},
	}
	sctx.SetValue(sessionctx.QueryString, "skip")
	err := e.doDDLJob2(sctx, job, args)
	return errors.Trace(err)
}

func getScatterScopeFromSessionctx(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBScatterRegion); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_scatter_region not found, use default value")
	return vardef.DefTiDBScatterRegion
}

func getEnableDDLAnalyze(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBEnableDDLAnalyze); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_stats_update_during_ddl not found, use default value")
	return variable.BoolToOnOff(vardef.DefTiDBEnableDDLAnalyze)
}

func getAnalyzeVersion(sctx sessionctx.Context) string {
	if val, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBAnalyzeVersion); ok {
		return val
	}
	logutil.DDLLogger().Info("system variable tidb_analyze_version not found, use default value")
	return strconv.Itoa(vardef.DefTiDBAnalyzeVersion)
}

// checkColumnReferencedByPartialCondition checks whether alter column is referenced by a partial index condition
func checkColumnReferencedByPartialCondition(t *model.TableInfo, colName ast.CIStr) error {
	for _, idx := range t.Indices {
		_, ic := model.FindIndexColumnByName(idx.AffectColumn, colName.L)
		if ic != nil {
			return dbterror.ErrModifyColumnReferencedByPartialCondition.GenWithStackByArgs(colName.O, idx.Name.O)
		}
	}

	return nil
}

func isReservedSchemaObjInNextGen(id int64) bool {
	failpoint.Inject("skipCheckReservedSchemaObjInNextGen", func() {
		failpoint.Return(false)
	})
	return kerneltype.IsNextGen() && metadef.IsReservedID(id)
}
