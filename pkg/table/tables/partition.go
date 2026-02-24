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

package tables

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	btreeDegree = 32
)

// Both partition and partitionedTable implement the table.Table interface.
var _ table.PhysicalTable = &partition{}
var _ table.Table = &partitionedTable{}

// partitionedTable implements the table.PartitionedTable interface.
var _ table.PartitionedTable = &partitionedTable{}

// partition is a feature from MySQL:
// See https://dev.mysql.com/doc/refman/8.0/en/partitioning.html
// A partition table may contain many partitions, each partition has a unique partition
// id. The underlying representation of a partition and a normal table (a table with no
// partitions) is basically the same.
// partition also implements the table.Table interface.
type partition struct {
	TableCommon
	table *partitionedTable
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (p *partition) GetPhysicalID() int64 {
	return p.physicalTableID
}

// GetPartitionedTable implements table.Table GetPartitionedTable interface.
func (p *partition) GetPartitionedTable() table.PartitionedTable {
	return p.table
}

// GetPartitionedTable implements table.Table GetPartitionedTable interface.
func (t *partitionedTable) GetPartitionedTable() table.PartitionedTable {
	return t
}

// partitionedTable implements the table.PartitionedTable interface.
// partitionedTable is a table, it contains many Partitions.
type partitionedTable struct {
	TableCommon
	partitionExpr   *PartitionExpr
	partitions      map[int64]*partition
	evalBufferTypes []*types.FieldType
	evalBufferPool  sync.Pool

	// Only used during Reorganize partition
	// reorganizePartitions is the currently used partitions that are reorganized
	reorganizePartitions map[int64]any
	// doubleWritePartitions are the partitions not visible, but we should double write to
	doubleWritePartitions map[int64]any
	reorgPartitionExpr    *PartitionExpr
}

// TODO: Check which data structures that can be shared between all partitions and which
// needs to be copies
func newPartitionedTable(tbl *TableCommon, tblInfo *model.TableInfo) (table.PartitionedTable, error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || len(pi.Definitions) == 0 {
		return nil, table.ErrUnknownPartition
	}
	ret := &partitionedTable{TableCommon: tbl.Copy()}
	partitionExpr, err := newPartitionExpr(tblInfo, pi.Type, pi.Expr, pi.Columns, pi.Definitions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.partitionExpr = partitionExpr
	initEvalBufferType(ret)
	ret.evalBufferPool = sync.Pool{
		New: func() any {
			return initEvalBuffer(ret)
		},
	}
	if err := initTableIndices(&ret.TableCommon); err != nil {
		return nil, errors.Trace(err)
	}
	origIndices := ret.meta.Indices
	DroppingDefinitionIndices := make([]*model.IndexInfo, 0, len(origIndices))
	AddingDefinitionIndices := make([]*model.IndexInfo, 0, len(origIndices))
	changesArePublic := pi.DDLState == model.StateDeleteReorganization || pi.DDLState == model.StatePublic
	for _, idx := range origIndices {
		newIdx, ok := pi.DDLChangedIndex[idx.ID]
		if !ok {
			// Untouched index
			clonedIdx := idx.Clone()
			if changesArePublic {
				// Adding partitions are now public, so we should assert on them.
				AddingDefinitionIndices = append(AddingDefinitionIndices, idx)
				// Dropping partitions are no longer public, so we cannot assert on them.
				// Using WriteOnly, since DeleteOnly/DeleteReorganization is not classified
				// as Writable, see tables.IsIndexWritable().
				clonedIdx.State = model.StateWriteOnly
				DroppingDefinitionIndices = append(DroppingDefinitionIndices, clonedIdx)
				continue
			}
			// Currently used partitions, continue use StatePublic for assertions
			DroppingDefinitionIndices = append(DroppingDefinitionIndices, idx)
			// new partitions, use current state for skipping assertions
			clonedIdx.State = pi.DDLState
			AddingDefinitionIndices = append(AddingDefinitionIndices, clonedIdx)
			continue
		}
		if newIdx {
			AddingDefinitionIndices = append(AddingDefinitionIndices, idx)
		} else {
			DroppingDefinitionIndices = append(DroppingDefinitionIndices, idx)
		}
	}
	tblInfo.Indices = origIndices
	defer func() { ret.meta.Indices = origIndices }()
	dropMap := make(map[int64]struct{})
	for _, def := range pi.DroppingDefinitions {
		dropMap[def.ID] = struct{}{}
	}
	addMap := make(map[int64]struct{})
	for _, def := range pi.AddingDefinitions {
		addMap[def.ID] = struct{}{}
	}
	partitions := make(map[int64]*partition, len(pi.Definitions))
	for _, p := range pi.Definitions {
		var t partition
		if _, drop := dropMap[p.ID]; drop {
			tblInfo.Indices = DroppingDefinitionIndices
		} else if _, add := addMap[p.ID]; add {
			tblInfo.Indices = AddingDefinitionIndices
		} else {
			tblInfo.Indices = origIndices
		}
		err := initTableCommonWithIndices(&t.TableCommon, tblInfo, p.ID, tbl.Columns, tbl.allocs, tbl.Constraints)
		if err != nil {
			return nil, errors.Trace(err)
		}
		t.table = ret
		partitions[p.ID] = &t
	}
	ret.partitions = partitions
	switch pi.DDLAction {
	case model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		// continue after switch!
	case model.ActionTruncateTablePartition:
		for _, def := range pi.DroppingDefinitions {
			p, err := initPartition(ret, def)
			if err != nil {
				return nil, err
			}
			partitions[def.ID] = p
		}
		fallthrough
	default:
		return ret, nil
	}
	// In WriteReorganization we are using the 'old' partition definitions
	// and if any new change happens in DroppingDefinitions, it needs to be done
	// also in AddingDefinitions (with new evaluation of the new expression)
	// In DeleteReorganization/Public we are using the 'new' partition definitions
	// and if any new change happens in AddingDefinitions, it needs to be done
	// also in DroppingDefinitions (since session running on schema version -1)
	// should also see the changes.
	if pi.DDLState == model.StateDeleteReorganization || pi.DDLState == model.StatePublic {
		// TODO: Explicitly explain the different DDL/New fields!
		if pi.NewTableID != 0 {
			ret.reorgPartitionExpr, err = newPartitionExpr(tblInfo, pi.DDLType, pi.DDLExpr, pi.DDLColumns, pi.DroppingDefinitions)
		} else {
			ret.reorgPartitionExpr, err = newPartitionExpr(tblInfo, pi.Type, pi.Expr, pi.Columns, pi.DroppingDefinitions)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.reorganizePartitions = make(map[int64]any, len(pi.AddingDefinitions))
		for _, def := range pi.AddingDefinitions {
			ret.reorganizePartitions[def.ID] = nil
		}
		ret.doubleWritePartitions = make(map[int64]any, len(pi.DroppingDefinitions))
		tblInfo.Indices = DroppingDefinitionIndices
		for _, def := range pi.DroppingDefinitions {
			p, err := initPartition(ret, def)
			if err != nil {
				return nil, err
			}
			p.skipAssert = true
			partitions[def.ID] = p
			ret.doubleWritePartitions[def.ID] = nil
		}
	} else {
		if len(pi.AddingDefinitions) > 0 {
			if pi.NewTableID != 0 {
				// REMOVE PARTITIONING or PARTITION BY
				ret.reorgPartitionExpr, err = newPartitionExpr(tblInfo, pi.DDLType, pi.DDLExpr, pi.DDLColumns, pi.AddingDefinitions)
			} else {
				// REORGANIZE PARTITION
				ret.reorgPartitionExpr, err = newPartitionExpr(tblInfo, pi.Type, pi.Expr, pi.Columns, pi.AddingDefinitions)
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
			ret.doubleWritePartitions = make(map[int64]any, len(pi.AddingDefinitions))
			tblInfo.Indices = AddingDefinitionIndices
			for _, def := range pi.AddingDefinitions {
				ret.doubleWritePartitions[def.ID] = nil
				p, err := initPartition(ret, def)
				if err != nil {
					return nil, err
				}
				p.skipAssert = true
				partitions[def.ID] = p
			}
		}
		if len(pi.DroppingDefinitions) > 0 {
			ret.reorganizePartitions = make(map[int64]any, len(pi.DroppingDefinitions))
			for _, def := range pi.DroppingDefinitions {
				ret.reorganizePartitions[def.ID] = nil
			}
		}
	}
	return ret, nil
}

func initPartition(t *partitionedTable, def model.PartitionDefinition) (*partition, error) {
	var newPart partition
	err := initTableCommonWithIndices(&newPart.TableCommon, t.meta, def.ID, t.Columns, t.allocs, t.Constraints)
	if err != nil {
		return nil, err
	}
	newPart.table = t
	return &newPart, nil
}

// NewPartitionExprBuildCtx returns a context to build partition expression.
func NewPartitionExprBuildCtx() expression.BuildContext {
	return exprstatic.NewExprContext(
		exprstatic.WithEvalCtx(exprstatic.NewEvalContext(
			// Set a non-strict SQL mode and allow all date values if possible to make sure constant fold can work to
			// estimate some undetermined result when locating a row to a partition.
			// See issue: https://github.com/pingcap/tidb/issues/54271 for details.
			exprstatic.WithSQLMode(mysql.ModeAllowInvalidDates),
			exprstatic.WithTypeFlags(types.StrictFlags.
				WithIgnoreTruncateErr(true).
				WithIgnoreZeroDateErr(true).
				WithIgnoreZeroInDate(true).
				WithIgnoreInvalidDateErr(true),
			),
			exprstatic.WithErrLevelMap(errctx.LevelMap{
				errctx.ErrGroupTruncate: errctx.LevelIgnore,
			}),
		)),
	)
}

func newPartitionExpr(tblInfo *model.TableInfo, tp ast.PartitionType, expr string, partCols []ast.CIStr, defs []model.PartitionDefinition) (*PartitionExpr, error) {
	ctx := NewPartitionExprBuildCtx()
	dbName := ast.NewCIStr(ctx.GetEvalCtx().CurrentDB())
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(ctx, dbName, tblInfo.Name, tblInfo.Cols(), tblInfo)
	if err != nil {
		return nil, err
	}
	switch tp {
	case ast.PartitionTypeNone:
		// Nothing to do
		return nil, nil
	case ast.PartitionTypeRange:
		return generateRangePartitionExpr(ctx, expr, partCols, defs, columns, names)
	case ast.PartitionTypeHash:
		return generateHashPartitionExpr(ctx, expr, columns, names)
	case ast.PartitionTypeKey:
		return generateKeyPartitionExpr(ctx, expr, partCols, columns, names)
	case ast.PartitionTypeList:
		return generateListPartitionExpr(ctx, tblInfo, expr, partCols, defs, columns, names)
	}
	panic("cannot reach here")
}

// PartitionExpr is the partition definition expressions.
type PartitionExpr struct {
	// UpperBounds: (x < y1); (x < y2); (x < y3), used by locatePartition.
	UpperBounds []expression.Expression
	// OrigExpr is the partition expression ast used in point get.
	OrigExpr ast.ExprNode
	// Expr is the hash partition expression.
	Expr expression.Expression
	// Used in the key partition
	*ForKeyPruning
	// Used in the range pruning process.
	*ForRangePruning
	// Used in the range column pruning process.
	*ForRangeColumnsPruning
	// ColOffset is the offsets of partition columns.
	ColumnOffset []int
	*ForListPruning
}

// GetPartColumnsForKeyPartition is used to get partition columns for key partition table
func (pe *PartitionExpr) GetPartColumnsForKeyPartition(columns []*expression.Column) ([]*expression.Column, []int) {
	schema := expression.NewSchema(columns...)
	partCols := make([]*expression.Column, len(pe.ColumnOffset))
	colLen := make([]int, 0, len(pe.ColumnOffset))
	for i, offset := range pe.ColumnOffset {
		partCols[i] = schema.Columns[offset]
		partCols[i].Index = i
		colLen = append(colLen, partCols[i].RetType.GetFlen())
	}
	return partCols, colLen
}

// LocateKeyPartition is the common interface used to locate the destination partition
func (kp *ForKeyPruning) LocateKeyPartition(numParts uint64, r []types.Datum) (int, error) {
	h := crc32.NewIEEE()
	for _, col := range kp.KeyPartCols {
		val := r[col.Index]
		if val.Kind() == types.KindNull {
			h.Write([]byte{0})
		} else {
			data, err := val.ToHashKey()
			if err != nil {
				return 0, err
			}
			h.Write(data)
		}
	}
	return int(h.Sum32() % uint32(numParts)), nil
}

func initEvalBufferType(t *partitionedTable) {
	hasExtraHandle := false
	numCols := len(t.WritableCols())
	if !t.Meta().PKIsHandle {
		hasExtraHandle = true
		numCols++
	}
	t.evalBufferTypes = make([]*types.FieldType, numCols)
	for i, col := range t.WritableCols() {
		t.evalBufferTypes[i] = &col.FieldType
	}

	if hasExtraHandle {
		t.evalBufferTypes[len(t.evalBufferTypes)-1] = types.NewFieldType(mysql.TypeLonglong)
	}
}

func initEvalBuffer(t *partitionedTable) *chunk.MutRow {
	evalBuffer := chunk.MutRowFromTypes(t.evalBufferTypes)
	return &evalBuffer
}

func generateHashPartitionExpr(ctx expression.BuildContext, exprStr string,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	schema := expression.NewSchema(columns...)
	origExpr, err := parseExpr(parser.New(), exprStr)
	if err != nil {
		return nil, err
	}
	exprs, err := expression.BuildSimpleExpr(ctx, origExpr, expression.WithInputSchemaAndNames(schema, names, nil))
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", exprStr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	// build column offset.
	partitionCols := expression.ExtractColumns(exprs)
	offset := make([]int, len(partitionCols))
	for i, col := range columns {
		for j, partitionCol := range partitionCols {
			if partitionCol.UniqueID == col.UniqueID {
				offset[j] = i
			}
		}
	}
	exprs.HashCode()
	return &PartitionExpr{
		Expr:         exprs,
		OrigExpr:     origExpr,
		ColumnOffset: offset,
	}, nil
}

// PartitionExpr returns the partition expression.
func (t *partitionedTable) PartitionExpr() *PartitionExpr {
	return t.partitionExpr
}

func (t *partitionedTable) GetPartitionColumnIDs() []int64 {
	// PARTITION BY {LIST|RANGE} COLUMNS uses columns directly without expressions
	pi := t.Meta().Partition
	if len(pi.Columns) > 0 {
		colIDs := make([]int64, 0, len(pi.Columns))
		for _, name := range pi.Columns {
			col := table.FindColLowerCase(t.Cols(), name.L)
			if col == nil {
				// For safety, should not happen
				continue
			}
			colIDs = append(colIDs, col.ID)
		}
		return colIDs
	}
	if t.partitionExpr == nil {
		return nil
	}

	partitionCols := expression.ExtractColumns(t.partitionExpr.Expr)
	colIDs := make([]int64, 0, len(partitionCols))
	for _, col := range partitionCols {
		colIDs = append(colIDs, col.ID)
	}
	return colIDs
}

func (t *partitionedTable) GetPartitionColumnNames() []ast.CIStr {
	pi := t.Meta().Partition
	if len(pi.Columns) > 0 {
		return pi.Columns
	}
	colIDs := t.GetPartitionColumnIDs()
	colNames := make([]ast.CIStr, 0, len(colIDs))
	for _, colID := range colIDs {
		for _, col := range t.Cols() {
			if col.ID == colID {
				colNames = append(colNames, col.Name)
			}
		}
	}
	return colNames
}

// PartitionRecordKey is exported for test.
func PartitionRecordKey(pid int64, handle int64) kv.Key {
	recordPrefix := tablecodec.GenTableRecordPrefix(pid)
	return tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(handle))
}

func (t *partitionedTable) CheckForExchangePartition(ctx expression.EvalContext, pi *model.PartitionInfo, r []types.Datum, partID, ntID int64) error {
	defID, err := t.locatePartition(ctx, r)
	if err != nil {
		return err
	}
	if defID != partID && defID != ntID {
		return errors.WithStack(table.ErrRowDoesNotMatchGivenPartitionSet)
	}
	return nil
}

// locatePartitionCommon returns the partition idx of the input record.
func (t *partitionedTable) locatePartitionCommon(ctx expression.EvalContext, tp ast.PartitionType, partitionExpr *PartitionExpr, num uint64, columnsPartitioned bool, r []types.Datum) (int, error) {
	var err error
	var idx int
	switch tp {
	case ast.PartitionTypeRange:
		if columnsPartitioned {
			idx, err = t.locateRangeColumnPartition(ctx, partitionExpr, r)
		} else {
			idx, err = t.locateRangePartition(ctx, partitionExpr, r)
		}
		if err != nil {
			return -1, err
		}
		pi := t.Meta().Partition
		if pi.CanHaveOverlappingDroppingPartition() {
			if pi.IsDropping(idx) {
				// Give an error, since it should not be written to!
				// For read it can check the Overlapping partition and ignore the error.
				// One should use the next non-dropping partition for range, or the default
				// partition for list partitioned table with default partition, for read.
				return idx, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(fmt.Sprintf("matching a partition being dropped, '%s'", pi.Definitions[idx].Name.String()))
			}
		}
	case ast.PartitionTypeHash:
		// Note that only LIST and RANGE supports REORGANIZE PARTITION
		idx, err = t.locateHashPartition(ctx, partitionExpr, num, r)
	case ast.PartitionTypeKey:
		idx, err = partitionExpr.LocateKeyPartition(num, r)
	case ast.PartitionTypeList:
		idx, err = partitionExpr.locateListPartition(ctx, r)
		pi := t.Meta().Partition
		if idx != pi.GetOverlappingDroppingPartitionIdx(idx) {
			return idx, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(fmt.Sprintf("matching a partition being dropped, '%s'", pi.Definitions[idx].Name.String()))
		}
	case ast.PartitionTypeNone:
		idx = 0
	}
	if err != nil {
		return -1, errors.Trace(err)
	}
	return idx, nil
}

func (t *partitionedTable) locatePartitionIdx(ctx expression.EvalContext, r []types.Datum) (int, error) {
	pi := t.Meta().GetPartitionInfo()
	columnsSet := len(t.meta.Partition.Columns) > 0
	return t.locatePartitionCommon(ctx, pi.Type, t.partitionExpr, pi.Num, columnsSet, r)
}

func (t *partitionedTable) locatePartition(ctx expression.EvalContext, r []types.Datum) (int64, error) {
	idx, err := t.locatePartitionIdx(ctx, r)
	if err != nil {
		return 0, errors.Trace(err)
	}
	pi := t.Meta().GetPartitionInfo()
	return pi.Definitions[idx].ID, nil
}

func (t *partitionedTable) locateReorgPartition(ctx expression.EvalContext, r []types.Datum) (int64, error) {
	pi := t.Meta().GetPartitionInfo()
	columnsSet := len(pi.DDLColumns) > 0
	// Note that for KEY/HASH partitioning, since we do not support LINEAR,
	// all partitions will be reorganized,
	// so we can use the number in Dropping or AddingDefinitions,
	// depending on current state.
	reorgDefs := pi.AddingDefinitions
	switch pi.DDLAction {
	case model.ActionReorganizePartition, model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		if pi.DDLState == model.StatePublic {
			reorgDefs = pi.DroppingDefinitions
		}
		fallthrough
	default:
		if pi.DDLState == model.StateDeleteReorganization {
			reorgDefs = pi.DroppingDefinitions
		}
	}
	idx, err := t.locatePartitionCommon(ctx, pi.DDLType, t.reorgPartitionExpr, uint64(len(reorgDefs)), columnsSet, r)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return reorgDefs[idx].ID, nil
}

func (t *partitionedTable) locateRangeColumnPartition(ctx expression.EvalContext, partitionExpr *PartitionExpr, r []types.Datum) (int, error) {
	upperBounds := partitionExpr.UpperBounds
	var lastError error
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	idx := sort.Search(len(upperBounds), func(i int) bool {
		evalBuffer.SetDatums(r...)
		ret, isNull, err := upperBounds[i].EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			lastError = err
			return true // Does not matter, will propagate the last error anyway.
		}
		if isNull {
			// If the column value used to determine the partition is NULL, the row is inserted into the lowest partition.
			// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-handling-nulls.html
			return true // Always less than any other value (NULL cannot be in the partition definition VALUE LESS THAN).
		}
		return ret > 0
	})
	if lastError != nil {
		return 0, errors.Trace(lastError)
	}
	if idx >= len(upperBounds) {
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs("from column_list")
	}
	return idx, nil
}

func (pe *PartitionExpr) locateListPartition(ctx expression.EvalContext, r []types.Datum) (int, error) {
	lp := pe.ForListPruning
	if len(lp.ColPrunes) == 0 {
		return lp.locateListPartitionByRow(ctx, r)
	}
	tc, ec := ctx.TypeCtx(), ctx.ErrCtx()
	return lp.locateListColumnsPartitionByRow(tc, ec, r)
}

func (t *partitionedTable) locateRangePartition(ctx expression.EvalContext, partitionExpr *PartitionExpr, r []types.Datum) (int, error) {
	var (
		ret    int64
		val    int64
		isNull bool
		err    error
	)
	if col, ok := partitionExpr.Expr.(*expression.Column); ok {
		if r[col.Index].IsNull() {
			isNull = true
		}
		ret = r[col.Index].GetInt64()
	} else {
		evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
		defer t.evalBufferPool.Put(evalBuffer)
		evalBuffer.SetDatums(r...)
		val, isNull, err = partitionExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
		if err != nil {
			return 0, err
		}
		ret = val
	}
	unsigned := mysql.HasUnsignedFlag(partitionExpr.Expr.GetType(ctx).GetFlag())
	ranges := partitionExpr.ForRangePruning
	length := len(ranges.LessThan)
	pos := sort.Search(length, func(i int) bool {
		if isNull {
			return true
		}
		return ranges.Compare(i, ret, unsigned) > 0
	})
	if isNull {
		pos = 0
	}
	if pos < 0 || pos >= length {
		// The data does not belong to any of the partition returns `table has no partition for value %s`.
		var valueMsg string
		if unsigned {
			valueMsg = fmt.Sprintf("%d", uint64(ret))
		} else {
			valueMsg = fmt.Sprintf("%d", ret)
		}
		return 0, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
	}
	return pos, nil
}

// TODO: supports linear hashing
func (t *partitionedTable) locateHashPartition(ctx expression.EvalContext, partExpr *PartitionExpr, numParts uint64, r []types.Datum) (int, error) {
	if col, ok := partExpr.Expr.(*expression.Column); ok {
		var data types.Datum
		switch r[col.Index].Kind() {
		case types.KindInt64, types.KindUint64:
			data = r[col.Index]
		default:
			var err error
			data, err = r[col.Index].ConvertTo(ctx.TypeCtx(), types.NewFieldType(mysql.TypeLonglong))
			if err != nil {
				return 0, err
			}
		}
		ret := data.GetInt64()
		ret = ret % int64(numParts)
		if ret < 0 {
			ret = -ret
		}
		return int(ret), nil
	}
	evalBuffer := t.evalBufferPool.Get().(*chunk.MutRow)
	defer t.evalBufferPool.Put(evalBuffer)
	evalBuffer.SetDatums(r...)
	ret, isNull, err := partExpr.Expr.EvalInt(ctx, evalBuffer.ToRow())
	if err != nil {
		return 0, err
	}
	if isNull {
		return 0, nil
	}
	ret = ret % int64(numParts)
	if ret < 0 {
		ret = -ret
	}
	return int(ret), nil
}

// GetPartition returns a Table, which is actually a partition.
func (t *partitionedTable) GetPartition(pid int64) table.PhysicalTable {
	part := t.getPartition(pid)

	// Explicitly check if the partition is nil, and return a nil interface if it is
	if part == nil {
		return nil // Return a truly nil interface instead of an interface holding a nil pointer
	}

	return part
}

// getPartition returns a Table, which is actually a partition.
func (t *partitionedTable) getPartition(pid int64) *partition {
	// Attention, can't simply use `return t.partitions[pid]` here.
	// Because A nil of type *partition is a kind of `table.PhysicalTable`
	part, ok := t.partitions[pid]
	if !ok {
		// Should never happen!
		return nil
	}
	return part
}

// GetReorganizedPartitionedTable returns the same table
// but only with the AddingDefinitions used.
func GetReorganizedPartitionedTable(t table.Table) (table.PartitionedTable, error) {
	// This is used during Reorganize partitions; All data from DroppingDefinitions
	// will be copied to AddingDefinitions, so only setup with AddingDefinitions!

	// Do not change any Definitions of t, but create a new struct.
	if t.GetPartitionedTable() == nil {
		return nil, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	tblInfo := t.Meta().Clone()
	pi := tblInfo.Partition
	pi.Definitions = pi.AddingDefinitions
	pi.Num = uint64(len(pi.Definitions))
	pi.AddingDefinitions = nil
	pi.DroppingDefinitions = nil

	// Reorganized status, use the new values
	pi.Type = pi.DDLType
	pi.Expr = pi.DDLExpr
	pi.Columns = pi.DDLColumns
	if pi.NewTableID != 0 {
		tblInfo.ID = pi.NewTableID
	}

	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil, err
	}
	var tc TableCommon
	initTableCommon(&tc, tblInfo, tblInfo.ID, t.Cols(), t.Allocators(nil), constraints)

	// and rebuild the partitioning structure
	return newPartitionedTable(&tc, tblInfo)
}

// GetPartitionByRow returns a Table, which is actually a Partition.
func (t *partitionedTable) GetPartitionByRow(ctx expression.EvalContext, r []types.Datum) (table.PhysicalTable, error) {
	pid, err := t.locatePartition(ctx, r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.partitions[pid], nil
}

// GetPartitionIdxByRow returns the index in PartitionDef for the matching partition
func (t *partitionedTable) GetPartitionIdxByRow(ctx expression.EvalContext, r []types.Datum) (int, error) {
	return t.locatePartitionIdx(ctx, r)
}
