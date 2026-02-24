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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package tables

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// TableCommon is shared by both Table and partition.
// NOTE: when copying this struct, use Copy() to clear its columns cache.
type TableCommon struct {
	// TODO: Why do we need tableID, when it is already in meta.ID ?
	tableID int64
	// physicalTableID is a unique int64 to identify a physical table.
	physicalTableID int64
	Columns         []*table.Column

	// column caches
	// They are pointers to support copying TableCommon to CachedTable and PartitionedTable
	publicColumns                   []*table.Column
	visibleColumns                  []*table.Column
	hiddenColumns                   []*table.Column
	writableColumns                 []*table.Column
	fullHiddenColsAndVisibleColumns []*table.Column
	writableConstraints             []*table.Constraint

	indices     []table.Index
	meta        *model.TableInfo
	allocs      autoid.Allocators
	sequence    *sequenceCommon
	Constraints []*table.Constraint

	// recordPrefix and indexPrefix are generated using physicalTableID.
	recordPrefix kv.Key
	indexPrefix  kv.Key

	// skipAssert is used for partitions that are in WriteOnly/DeleteOnly state.
	skipAssert bool
}

// ResetColumnsCache implements testingKnob interface.
func (t *TableCommon) ResetColumnsCache() {
	t.publicColumns = t.getCols(full)
	t.visibleColumns = t.getCols(visible)
	t.hiddenColumns = t.getCols(hidden)

	t.writableColumns = make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		t.writableColumns = append(t.writableColumns, col)
	}

	t.fullHiddenColsAndVisibleColumns = make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.Hidden || col.State == model.StatePublic {
			t.fullHiddenColsAndVisibleColumns = append(t.fullHiddenColsAndVisibleColumns, col)
		}
	}

	if t.Constraints != nil {
		t.writableConstraints = make([]*table.Constraint, 0, len(t.Constraints))
		for _, con := range t.Constraints {
			if !con.Enforced {
				continue
			}
			if con.State == model.StateDeleteOnly || con.State == model.StateDeleteReorganization {
				continue
			}
			t.writableConstraints = append(t.writableConstraints, con)
		}
	}
}

// Copy copies a TableCommon struct, and reset its column cache. This is not a deep copy.
func (t *TableCommon) Copy() TableCommon {
	newTable := *t
	return newTable
}

// MockTableFromMeta only serves for test.
func MockTableFromMeta(tblInfo *model.TableInfo) table.Table {
	columns := make([]*table.Column, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		col := table.ToColumn(colInfo)
		columns = append(columns, col)
	}

	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil
	}
	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns, autoid.NewAllocators(false), constraints)
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		ret, err := newCachedTable(&t)
		if err != nil {
			return nil
		}
		return ret
	}
	if tblInfo.GetPartitionInfo() == nil {
		if err := initTableIndices(&t); err != nil {
			return nil
		}
		return &t
	}

	ret, err := newPartitionedTable(&t, tblInfo)
	if err != nil {
		return nil
	}
	return ret
}

// TableFromMeta creates a Table instance from model.TableInfo.
func TableFromMeta(allocs autoid.Allocators, tblInfo *model.TableInfo) (table.Table, error) {
	if tblInfo.State == model.StateNone {
		return nil, table.ErrTableStateCantNone.GenWithStackByArgs(tblInfo.Name)
	}

	colsLen := len(tblInfo.Columns)
	columns := make([]*table.Column, 0, colsLen)
	for i, colInfo := range tblInfo.Columns {
		if colInfo.State == model.StateNone {
			return nil, table.ErrColumnStateCantNone.GenWithStackByArgs(colInfo.Name)
		}

		// Print some information when the column's offset isn't equal to i.
		if colInfo.Offset != i {
			logutil.BgLogger().Error("wrong table schema", zap.Any("table", tblInfo), zap.Any("column", colInfo), zap.Int("index", i), zap.Int("offset", colInfo.Offset), zap.Int("columnNumber", colsLen))
		}

		col := table.ToColumn(colInfo)
		if col.IsGenerated() {
			genStr := colInfo.GeneratedExprString
			expr, err := buildGeneratedExpr(tblInfo, genStr)
			if err != nil {
				return nil, err
			}
			col.GeneratedExpr = table.NewClonableExprNode(func() ast.ExprNode {
				newExpr, err1 := buildGeneratedExpr(tblInfo, genStr)
				if err1 != nil {
					logutil.BgLogger().Warn("unexpected parse generated string error",
						zap.String("generatedStr", genStr),
						zap.Error(err1))
					return expr
				}
				return newExpr
			}, expr)
		}
		// default value is expr.
		if col.DefaultIsExpr {
			expr, err := generatedexpr.ParseExpression(colInfo.DefaultValue.(string))
			if err != nil {
				return nil, err
			}
			col.DefaultExpr = expr
		}
		columns = append(columns, col)
	}

	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil, err
	}
	var t TableCommon
	initTableCommon(&t, tblInfo, tblInfo.ID, columns, allocs, constraints)
	if tblInfo.GetPartitionInfo() == nil {
		if err := initTableIndices(&t); err != nil {
			return nil, err
		}
		if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
			return newCachedTable(&t)
		}
		return &t, nil
	}
	return newPartitionedTable(&t, tblInfo)
}

func buildGeneratedExpr(tblInfo *model.TableInfo, genExpr string) (ast.ExprNode, error) {
	expr, err := generatedexpr.ParseExpression(genExpr)
	if err != nil {
		return nil, err
	}
	expr, err = generatedexpr.SimpleResolveName(expr, tblInfo)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// initTableCommon initializes a TableCommon struct.
func initTableCommon(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, allocs autoid.Allocators, constraints []*table.Constraint) {
	t.tableID = tblInfo.ID
	t.physicalTableID = physicalTableID
	t.allocs = allocs
	t.meta = tblInfo
	t.Columns = cols
	t.Constraints = constraints
	t.recordPrefix = tablecodec.GenTableRecordPrefix(physicalTableID)
	t.indexPrefix = tablecodec.GenTableIndexPrefix(physicalTableID)
	if tblInfo.IsSequence() {
		t.sequence = &sequenceCommon{meta: tblInfo.Sequence}
	}
	t.ResetColumnsCache()
}

// initTableIndices initializes the indices of the TableCommon.
func initTableIndices(t *TableCommon) error {
	tblInfo := t.meta
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.State == model.StateNone {
			return table.ErrIndexStateCantNone.GenWithStackByArgs(idxInfo.Name)
		}

		// Use partition ID for index, because TableCommon may be table or partition.
		idx, err := NewIndex(t.physicalTableID, tblInfo, idxInfo)
		if err != nil {
			return err
		}
		intest.AssertFunc(func() bool {
			// `TableCommon.indices` is type of `[]table.Index` to implement interface method `Table.Indices`.
			// However, we have an assumption that the specific type of each element in it should always be `*index`.
			// We have this assumption because some codes access the inner method of `*index`,
			// and they use `asIndex` to cast `table.Index` to `*index`.
			_, ok := idx.(*index)
			intest.Assert(ok, "index should be type of `*index`")
			return true
		})
		t.indices = append(t.indices, idx)
	}
	return nil
}

// checkDataForModifyColumn checks if the data can be stored in the column with changingType.
// It's used to prevent illegal data being inserted if we want to skip reorg.
func checkDataForModifyColumn(row []types.Datum, col *table.Column) error {
	if col.ChangingFieldType == nil {
		return nil
	}

	data := row[col.Offset]
	value, err := table.CastColumnValueWithStrictMode(data, col.ChangingFieldType)
	if err != nil {
		return err
	}

	// For the case from VARCHAR -> CHAR
	if col.ChangingFieldType.GetType() == mysql.TypeString && value.GetString() != data.GetString() {
		return errors.New("data truncation error during modify column")
	}
	return nil
}

// asIndex casts a table.Index to *index which is the actual type of index in TableCommon.
func asIndex(idx table.Index) *index {
	return idx.(*index)
}

func initTableCommonWithIndices(t *TableCommon, tblInfo *model.TableInfo, physicalTableID int64, cols []*table.Column, allocs autoid.Allocators, constraints []*table.Constraint) error {
	initTableCommon(t, tblInfo, physicalTableID, cols, allocs, constraints)
	return initTableIndices(t)
}

// Indices implements table.Table Indices interface.
func (t *TableCommon) Indices() []table.Index {
	return t.indices
}

// GetWritableIndexByName gets the index meta from the table by the index name.
func GetWritableIndexByName(idxName string, t table.Table) table.Index {
	for _, idx := range t.Indices() {
		if !IsIndexWritable(idx) {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}
		if idxName == idx.Meta().Name.L {
			return idx
		}
	}
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (t *TableCommon) DeletableIndices() []table.Index {
	// All indices are deletable because we don't need to check StateNone.
	return t.indices
}

// Meta implements table.Table Meta interface.
func (t *TableCommon) Meta() *model.TableInfo {
	return t.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (t *TableCommon) GetPhysicalID() int64 {
	return t.physicalTableID
}

// GetPartitionedTable implements table.Table GetPhysicalID interface.
func (t *TableCommon) GetPartitionedTable() table.PartitionedTable {
	return nil
}

type getColsMode int64

const (
	_ getColsMode = iota
	visible
	hidden
	full
)

func (t *TableCommon) getCols(mode getColsMode) []*table.Column {
	columns := make([]*table.Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		if col.State != model.StatePublic {
			continue
		}
		if (mode == visible && col.Hidden) || (mode == hidden && !col.Hidden) {
			continue
		}
		columns = append(columns, col)
	}
	return columns
}

// Cols implements table.Table Cols interface.
func (t *TableCommon) Cols() []*table.Column {
	return t.publicColumns
}

// VisibleCols implements table.Table VisibleCols interface.
func (t *TableCommon) VisibleCols() []*table.Column {
	return t.visibleColumns
}

// HiddenCols implements table.Table HiddenCols interface.
func (t *TableCommon) HiddenCols() []*table.Column {
	return t.hiddenColumns
}

// WritableCols implements table WritableCols interface.
func (t *TableCommon) WritableCols() []*table.Column {
	return t.writableColumns
}

// DeletableCols implements table DeletableCols interface.
func (t *TableCommon) DeletableCols() []*table.Column {
	return t.Columns
}

// WritableConstraint returns constraints of the table in writable states.
func (t *TableCommon) WritableConstraint() []*table.Constraint {
	if t.Constraints == nil {
		return nil
	}
	return t.writableConstraints
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (t *TableCommon) FullHiddenColsAndVisibleCols() []*table.Column {
	return t.fullHiddenColsAndVisibleColumns
}

// RecordPrefix implements table.Table interface.
func (t *TableCommon) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table interface.
func (t *TableCommon) IndexPrefix() kv.Key {
	return t.indexPrefix
}

// RecordKey implements table.Table interface.
func (t *TableCommon) RecordKey(h kv.Handle) kv.Key {
	return tablecodec.EncodeRecordKey(t.recordPrefix, h)
}

// UpdateRecord implements table.Table UpdateRecord interface.
// `touched` means which columns are really modified, used for secondary indices.
// Length of `oldData` and `newData` equals to length of `t.WritableCols()`.
func (t *TableCommon) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	opt := table.NewUpdateRecordOpt(opts...)
	return t.updateRecord(ctx, txn, h, oldData, newData, touched, opt)
}

func (t *TableCommon) updateRecord(sctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opt *table.UpdateRecordOpt) error {
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(sctx, m); ok {
			if err := checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	numColsCap := len(newData) + 1 // +1 for the extra handle column that we may need to append.

	// a reusable buffer to save malloc
	// Note: The buffer should not be referenced or modified outside this function.
	// It can only act as a temporary buffer for the current function call.
	mutateBuffers := sctx.GetMutateBuffers()
	encodeRowBuffer := mutateBuffers.GetEncodeRowBufferWithCap(numColsCap)
	checkRowBuffer := mutateBuffers.GetCheckRowBufferWithCap(numColsCap)

	for _, col := range t.Columns {
		var value types.Datum
		var err error
		if err := checkDataForModifyColumn(newData, col); err != nil {
			return err
		}

		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			if col.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = table.CastColumnValue(sctx.GetExprCtx(), oldData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
				if err != nil {
					logutil.BgLogger().Info("update record cast value failed", zap.Any("col", col), zap.Uint64("txnStartTS", txn.StartTS()),
						zap.String("handle", h.String()), zap.Any("val", oldData[col.DependencyColumnOffset]), zap.Error(err))
					return err
				}
				oldData = append(oldData, value)
				touched = append(touched, touched[col.DependencyColumnOffset])
			}
			continue
		}
		if col.State != model.StatePublic {
			// If col is in write only or write reorganization state we should keep the oldData.
			// Because the oldData must be the original data(it's changed by other TiDBs.) or the original default value.
			// TODO: Use newData directly.
			value = oldData[col.Offset]
			if col.ChangeStateInfo != nil {
				// TODO: Check overflow or ignoreTruncate.
				value, err = table.CastColumnValue(sctx.GetExprCtx(), newData[col.DependencyColumnOffset], col.ColumnInfo, false, false)
				if err != nil {
					return err
				}
				newData[col.Offset] = value
				touched[col.Offset] = touched[col.DependencyColumnOffset]
			}
		} else {
			value = newData[col.Offset]
		}
		if !t.canSkip(col, &value) {
			encodeRowBuffer.AddColVal(col.ID, value)
		}
		checkRowBuffer.AddColVal(value)
	}
	// check data constraint
	if constraints := t.WritableConstraint(); len(constraints) > 0 {
		if err := table.CheckRowConstraint(sctx.GetExprCtx(), constraints, checkRowBuffer.GetRowToCheck(), t.Meta()); err != nil {
			return err
		}
	}
	// rebuild index
	err := t.rebuildUpdateRecordIndices(sctx, txn, h, touched, oldData, newData, opt)
	if err != nil {
		return err
	}

	key := t.RecordKey(h)
	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
	err = encodeRowBuffer.WriteMemBufferEncoded(sctx.GetRowEncodingConfig(), tc.Location(), ec, memBuffer, key, h)
	if err != nil {
		return err
	}

	failpoint.Inject("updateRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting not exist on UpdateRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = setAssertion(txn, key, kv.AssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})

	if t.skipAssert {
		err = setAssertion(txn, key, kv.AssertUnknown)
	} else {
		err = setAssertion(txn, key, kv.AssertExist)
	}
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	if sctx.EnableMutationChecker() {
		if err = CheckDataConsistency(txn, tc, t, newData, oldData, memBuffer, sh); err != nil {
			return errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if s, ok := sctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(t.physicalTableID, 0, 1)
	}
	return nil
}

func (t *TableCommon) rebuildUpdateRecordIndices(
	ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, touched []bool, oldData []types.Datum, newData []types.Datum,
	opt *table.UpdateRecordOpt,
) error {
	for _, idx := range t.DeletableIndices() {
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}

		oldDataMeetPartialCondition, err := idx.MeetPartialCondition(oldData)
		if err != nil {
			return err
		}
		if !oldDataMeetPartialCondition {
			// If the partial index condition is not met, we don't need to delete it because
			// it has never been written.
			continue
		}
		untouched := true
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		for _, ic := range idx.Meta().AffectColumn {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}

		if !untouched {
			oldVs, err := idx.FetchValues(oldData, nil)
			if err != nil {
				return err
			}
			if err = idx.Delete(ctx, txn, oldVs, h); err != nil {
				return err
			}
		}
	}
	createIdxOpt := opt.GetCreateIdxOpt()
	for _, idx := range t.Indices() {
		if !IsIndexWritable(idx) {
			continue
		}
		if idx.Meta().IsColumnarIndex() {
			continue
		}
		if t.meta.IsCommonHandle && idx.Meta().Primary {
			continue
		}
		untouched := true
		for _, ic := range idx.Meta().Columns {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		for _, ic := range idx.Meta().AffectColumn {
			if !touched[ic.Offset] {
				continue
			}
			untouched = false
			break
		}
		if untouched && opt.SkipWriteUntouchedIndices() {
			continue
		}
		newDataMeetPartialCondition, err := idx.MeetPartialCondition(newData)
		if err != nil {
			return err
		}
		if !newDataMeetPartialCondition {
			// If the partial index condition is not met, we don't need to build the new index.
			continue
		}

		newVs, err := idx.FetchValues(newData, nil)
		if err != nil {
			return err
		}
		if err := t.buildIndexForRow(ctx, h, newVs, newData, asIndex(idx), txn, untouched, createIdxOpt); err != nil {
			return err
		}
	}
	return nil
}

// FindPrimaryIndex uses to find primary index in tableInfo.
func FindPrimaryIndex(tblInfo *model.TableInfo) *model.IndexInfo {
	var pkIdx *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	return pkIdx
}

// TryGetCommonPkColumnIds get the IDs of primary key column if the table has common handle.
func TryGetCommonPkColumnIds(tbl *model.TableInfo) []int64 {
	if !tbl.IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl)
	pkColIDs := make([]int64, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkColIDs = append(pkColIDs, tbl.Columns[idxCol.Offset].ID)
	}
	return pkColIDs
}

// PrimaryPrefixColumnIDs get prefix column ids in primary key.
func PrimaryPrefixColumnIDs(tbl *model.TableInfo) (prefixCols []int64) {
	for _, idx := range tbl.Indices {
		if !idx.Primary {
			continue
		}
		for _, col := range idx.Columns {
			if col.Length > 0 && tbl.Columns[col.Offset].GetFlen() > col.Length {
				prefixCols = append(prefixCols, tbl.Columns[col.Offset].ID)
			}
		}
	}
	return
}

// TryGetCommonPkColumns get the primary key columns if the table has common handle.
func TryGetCommonPkColumns(tbl table.Table) []*table.Column {
	if !tbl.Meta().IsCommonHandle {
		return nil
	}
	pkIdx := FindPrimaryIndex(tbl.Meta())
	cols := tbl.Cols()
	pkCols := make([]*table.Column, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkCols = append(pkCols, cols[idxCol.Offset])
	}
	return pkCols
}

func addTemporaryTable(sctx table.MutateContext, tblInfo *model.TableInfo) (tblctx.TemporaryTableHandler, int64, bool) {
	if s, ok := sctx.GetTemporaryTableSupport(); ok {
		if h, ok := s.AddTemporaryTableToTxn(tblInfo); ok {
			return h, s.GetTemporaryTableSizeLimit(), ok
		}
	}
	return tblctx.TemporaryTableHandler{}, 0, false
}

// The size of a temporary table is calculated by accumulating the transaction size delta.
func handleTempTableSize(t tblctx.TemporaryTableHandler, txnSizeBefore int, txn kv.Transaction) {
	t.UpdateTxnDeltaSize(txn.Size() - txnSizeBefore)
}

func checkTempTableSize(tmpTable tblctx.TemporaryTableHandler, sizeLimit int64) error {
	if tmpTable.GetCommittedSize()+tmpTable.GetDirtySize() > sizeLimit {
		return table.ErrTempTableFull.GenWithStackByArgs(tmpTable.Meta().Name.O)
	}
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *TableCommon) AddRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	// TODO: optimize the allocation (and calculation) of opt.
	opt := table.NewAddRecordOpt(opts...)
	return t.addRecord(sctx, txn, r, opt)
}

func (t *TableCommon) addRecord(sctx table.MutateContext, txn kv.Transaction, r []types.Datum, opt *table.AddRecordOpt) (recordID kv.Handle, err error) {
	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(sctx, m); ok {
			if err = checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return nil, err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	var ctx context.Context
	if ctx = opt.Ctx(); ctx != nil {
		var r tracing.Region
		r, ctx = tracing.StartRegionEx(ctx, "table.AddRecord")
		defer r.End()
	} else {
		ctx = context.Background()
	}

	evalCtx := sctx.GetExprCtx().GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()

	var hasRecordID bool
	cols := t.Cols()
	// opt.GenerateRecordID is used for normal update.
	// If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
	// Currently, insert can set _tidb_rowid.
	// Update can only update _tidb_rowid during reorganize partition, to keep the generated _tidb_rowid
	// the same between the old/new sets of partitions, where possible.
	if len(r) > len(cols) && !opt.GenerateRecordID() {
		// The last value is _tidb_rowid.
		recordID = kv.IntHandle(r[len(r)-1].GetInt64())
		hasRecordID = true
	} else {
		tblInfo := t.Meta()
		txn.CacheTableInfo(t.physicalTableID, tblInfo)
		if tblInfo.PKIsHandle {
			recordID = kv.IntHandle(r[tblInfo.GetPkColInfo().Offset].GetInt64())
			hasRecordID = true
		} else if tblInfo.IsCommonHandle {
			pkIdx := FindPrimaryIndex(tblInfo)
			pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
			for _, idxCol := range pkIdx.Columns {
				pkDts = append(pkDts, r[idxCol.Offset])
			}
			tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
			var handleBytes []byte
			handleBytes, err = codec.EncodeKey(tc.Location(), nil, pkDts...)
			err = ec.HandleError(err)
			if err != nil {
				return
			}
			recordID, err = kv.NewCommonHandle(handleBytes)
			if err != nil {
				return
			}
			hasRecordID = true
		}
	}
	if !hasRecordID {
		if reserveAutoID := opt.ReserveAutoID(); reserveAutoID > 0 {
			// Reserve a batch of auto ID in the statement context.
			// The reserved ID could be used in the future within this statement, by the
			// following AddRecord() operation.
			// Make the IDs continuous benefit for the performance of TiKV.
			if reserved, ok := sctx.GetReservedRowIDAlloc(); ok {
				var baseRowID, maxRowID int64
				if baseRowID, maxRowID, err = AllocHandleIDs(ctx, sctx, t, uint64(reserveAutoID)); err != nil {
					return nil, err
				}
				reserved.Reset(baseRowID, maxRowID)
			}
		}

		recordID, err = AllocHandle(ctx, sctx, t)
		if err != nil {
			return nil, err
		}
	}

	// a reusable buffer to save malloc
	// Note: The buffer should not be referenced or modified outside this function.
	// It can only act as a temporary buffer for the current function call.
	mutateBuffers := sctx.GetMutateBuffers()
	encodeRowBuffer := mutateBuffers.GetEncodeRowBufferWithCap(len(r))
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	for _, col := range t.Columns {
		if err := checkDataForModifyColumn(r, col); err != nil {
			return nil, err
		}

		var value types.Datum
		if col.State == model.StateDeleteOnly || col.State == model.StateDeleteReorganization {
			continue
		}
		// In column type change, since we have set the origin default value for changing col, but
		// for the new insert statement, we should use the casted value of relative column to insert.
		if col.ChangeStateInfo != nil && col.State != model.StatePublic {
			// TODO: Check overflow or ignoreTruncate.
			value, err = table.CastColumnValue(sctx.GetExprCtx(), r[col.DependencyColumnOffset], col.ColumnInfo, false, false)
			if err != nil {
				return nil, err
			}
			if len(r) < len(t.WritableCols()) {
				r = append(r, value)
			} else {
				r[col.Offset] = value
			}
			encodeRowBuffer.AddColVal(col.ID, value)
			continue
		}
		if col.State == model.StatePublic {
			value = r[col.Offset]
		} else {
			// col.ChangeStateInfo must be nil here.
			// because `col.State != model.StatePublic` is true here, if col.ChangeStateInfo is not nil, the col should
			// be handle by the previous if-block.

			if opt.IsUpdate() {
				// If `AddRecord` is called by an update, the default value should be handled the update.
				value = r[col.Offset]
			} else {
				// If `AddRecord` is called by an insert and the col is in write only or write reorganization state, we must
				// add it with its default value.
				value, err = table.GetColOriginDefaultValue(sctx.GetExprCtx(), col.ToInfo())
				if err != nil {
					return nil, err
				}
				// add value to `r` for dirty db in transaction.
				// Otherwise when update will panic cause by get value of column in write only state from dirty db.
				if col.Offset < len(r) {
					r[col.Offset] = value
				} else {
					r = append(r, value)
				}
			}
		}
		if !t.canSkip(col, &value) {
			encodeRowBuffer.AddColVal(col.ID, value)
		}
	}
	if err = table.CheckRowConstraintWithDatum(sctx.GetExprCtx(), t.WritableConstraint(), r, t.meta); err != nil {
		return nil, err
	}
	key := t.RecordKey(recordID)
	var setPresume bool
	if opt.DupKeyCheck() != table.DupKeyCheckSkip {
		if t.meta.TempTableType != model.TempTableNone {
			// Always check key for temporary table because it does not write to TiKV
			_, err = txn.Get(ctx, key)
		} else if opt.DupKeyCheck() == table.DupKeyCheckLazy {
			var v []byte
			v, err = txn.GetMemBuffer().GetLocal(ctx, key)
			if err != nil {
				setPresume = true
			}
			if err == nil && len(v) == 0 {
				err = kv.ErrNotExist
			}
		} else {
			_, err = txn.Get(ctx, key)
		}
		if err == nil {
			// If Global Index and reorganization truncate/drop partition, old partition,
			// Accept and set Assertion key to kv.AssertUnknown for overwrite instead
			dupErr := getDuplicateError(t.Meta(), recordID, r)
			return recordID, dupErr
		} else if !kv.ErrNotExist.Equal(err) {
			return recordID, err
		}
	}

	var flags []kv.FlagsOp
	if setPresume {
		flags = []kv.FlagsOp{kv.SetPresumeKeyNotExists}
		if opt.PessimisticLazyDupKeyCheck() == table.DupKeyCheckInPrewrite && txn.IsPessimistic() {
			flags = append(flags, kv.SetNeedConstraintCheckInPrewrite)
		}
	}

	err = encodeRowBuffer.WriteMemBufferEncoded(sctx.GetRowEncodingConfig(), tc.Location(), ec, memBuffer, key, recordID, flags...)
	if err != nil {
		return nil, err
	}

	failpoint.Inject("addRecordForceAssertExist", func() {
		// Assert the key exists while it actually doesn't. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if sctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting exist on AddRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = setAssertion(txn, key, kv.AssertExist); err != nil {
				failpoint.Return(nil, err)
			}
		}
	})
	if setPresume && !txn.IsPessimistic() {
		err = setAssertion(txn, key, kv.AssertUnknown)
	} else {
		err = setAssertion(txn, key, kv.AssertNotExist)
	}
	if err != nil {
		return nil, err
	}

	// Insert new entries into indices.
	h, err := t.addIndices(sctx, recordID, r, txn, opt.GetCreateIdxOpt())
	if err != nil {
		return h, err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return nil, err
	}
	if sctx.EnableMutationChecker() {
		if err = CheckDataConsistency(txn, tc, t, r, nil, memBuffer, sh); err != nil {
			return nil, errors.Trace(err)
		}
	}

	memBuffer.Release(sh)

	if s, ok := sctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(t.physicalTableID, 1, 1)
	}
	return recordID, nil
}

// genIndexKeyStrs generates index content strings representation.
func genIndexKeyStrs(colVals []types.Datum) ([]string, error) {
	// Pass pre-composed error to txn.
	strVals := make([]string, 0, len(colVals))
	for _, cv := range colVals {
		cvs := "NULL"
		var err error
		if !cv.IsNull() {
			cvs, err = types.ToString(cv.GetValue())
			if err != nil {
				return nil, err
			}
		}
		strVals = append(strVals, cvs)
	}
	return strVals, nil
}

// addIndices adds data into indices. If any key is duplicated, returns the original handle.
func (t *TableCommon) addIndices(sctx table.MutateContext, recordID kv.Handle, r []types.Datum, txn kv.Transaction, opt *table.CreateIdxOpt) (kv.Handle, error) {
	writeBufs := sctx.GetMutateBuffers().GetWriteStmtBufs()
	indexVals := writeBufs.IndexValsBuf
	skipCheck := opt.DupKeyCheck() == table.DupKeyCheckSkip
	for _, v := range t.Indices() {
		if !IsIndexWritable(v) {
			continue
		}
		if v.Meta().IsColumnarIndex() {
			continue
		}
		if t.meta.IsCommonHandle && v.Meta().Primary {
			continue
		}

		meetPartialCondition, err := v.MeetPartialCondition(r)
		if err != nil {
			return nil, err
		}
		if !meetPartialCondition {
			continue
		}

		// We should make sure `indexVals` is assigned with `=` instead of `:=`.
		// The latter one will create a new variable that shadows the outside `indexVals` that makes `indexVals` outside
		// always nil, and we cannot reuse it.
		indexVals, err = v.FetchValues(r, indexVals)
		if err != nil {
			return nil, err
		}
		var dupErr error
		if !skipCheck && v.Meta().Unique {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, v.Meta(), indexVals)
			colStrVals, err := genIndexKeyStrs(indexVals)
			if err != nil {
				return nil, err
			}
			dupErr = kv.GenKeyExistsErr(colStrVals, fmt.Sprintf("%s.%s", v.TableMeta().Name.String(), v.Meta().Name.String()))
		}
		rsData := TryGetHandleRestoredDataWrapper(t.meta, r, nil, v.Meta())
		if dupHandle, err := asIndex(v).create(sctx, txn, indexVals, recordID, rsData, false, opt); err != nil {
			if kv.ErrKeyExists.Equal(err) {
				return dupHandle, dupErr
			}
			return nil, err
		}
	}
	// save the buffer, multi rows insert can use it.
	writeBufs.IndexValsBuf = indexVals
	return nil, nil
}
