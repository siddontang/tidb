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

package rule

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/set"
)

func (*PartitionProcessor) resolveAccessPaths(ds *logicalop.DataSource) error {
	possiblePaths, err := utilfuncp.GetPossibleAccessPaths(
		ds.SCtx(), &h.PlanHints{IndexMergeHintList: ds.IndexMergeHints, IndexHintList: ds.IndexHints},
		ds.AstIndexHints, ds.Table, ds.DBName, ds.TableInfo.Name, ds.IsForUpdateRead, true)
	if err != nil {
		return err
	}
	possiblePaths, err = util.FilterPathByIsolationRead(ds.SCtx(), possiblePaths, ds.TableInfo.Name, ds.DBName)
	if err != nil {
		return err
	}
	// partition processor path pruning should affect the all paths.
	allPaths := make([]*util.AccessPath, len(possiblePaths))
	copy(allPaths, possiblePaths)
	ds.AllPossibleAccessPaths = allPaths
	ds.PossibleAccessPaths = possiblePaths
	return nil
}

func (s *PartitionProcessor) resolveOptimizeHint(ds *logicalop.DataSource, partitionName ast.CIStr) error {
	// index hint
	if len(ds.IndexHints) > 0 {
		newIndexHint := make([]h.HintedIndex, 0, len(ds.IndexHints))
		for _, idxHint := range ds.IndexHints {
			if len(idxHint.Partitions) == 0 {
				newIndexHint = append(newIndexHint, idxHint)
			} else {
				for _, p := range idxHint.Partitions {
					if p.String() == partitionName.String() {
						newIndexHint = append(newIndexHint, idxHint)
						break
					}
				}
			}
		}
		ds.IndexHints = newIndexHint
	}

	// index merge hint
	if len(ds.IndexMergeHints) > 0 {
		newIndexMergeHint := make([]h.HintedIndex, 0, len(ds.IndexMergeHints))
		for _, idxHint := range ds.IndexMergeHints {
			if len(idxHint.Partitions) == 0 {
				newIndexMergeHint = append(newIndexMergeHint, idxHint)
			} else {
				for _, p := range idxHint.Partitions {
					if p.String() == partitionName.String() {
						newIndexMergeHint = append(newIndexMergeHint, idxHint)
						break
					}
				}
			}
		}
		ds.IndexMergeHints = newIndexMergeHint
	}

	// read from storage hint
	if ds.PreferStoreType&h.PreferTiKV > 0 {
		if len(ds.PreferPartitions[h.PreferTiKV]) > 0 {
			ds.PreferStoreType ^= h.PreferTiKV
			for _, p := range ds.PreferPartitions[h.PreferTiKV] {
				if p.String() == partitionName.String() {
					ds.PreferStoreType |= h.PreferTiKV
				}
			}
		}
	}
	if ds.PreferStoreType&h.PreferTiFlash > 0 {
		if len(ds.PreferPartitions[h.PreferTiFlash]) > 0 {
			ds.PreferStoreType ^= h.PreferTiFlash
			for _, p := range ds.PreferPartitions[h.PreferTiFlash] {
				if p.String() == partitionName.String() {
					ds.PreferStoreType |= h.PreferTiFlash
				}
			}
		}
	}
	if ds.PreferStoreType&h.PreferTiFlash != 0 && ds.PreferStoreType&h.PreferTiKV != 0 {
		ds.SCtx().GetSessionVars().StmtCtx.AppendWarning(
			errors.NewNoStackError("hint `read_from_storage` has conflict storage type for the partition " + partitionName.L))
	}

	return s.resolveAccessPaths(ds)
}

func checkTableHintsApplicableForPartition(partitions []ast.CIStr, partitionSet set.StringSet) []string {
	var unknownPartitions []string
	for _, p := range partitions {
		if !partitionSet.Exist(p.L) {
			unknownPartitions = append(unknownPartitions, p.L)
		}
	}
	return unknownPartitions
}

func appendWarnForUnknownPartitions(ctx base.PlanContext, hintName string, unknownPartitions []string) {
	if len(unknownPartitions) == 0 {
		return
	}

	warning := fmt.Errorf("unknown partitions (%s) in optimizer hint %s", strings.Join(unknownPartitions, ","), hintName)
	ctx.GetSessionVars().StmtCtx.SetHintWarningFromError(warning)
}

func (*PartitionProcessor) checkHintsApplicable(ds *logicalop.DataSource, partitionSet set.StringSet) {
	for _, idxHint := range ds.IndexHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxHint.Partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.SCtx(), h.Restore2IndexHint(idxHint.HintTypeString(), idxHint), unknownPartitions)
	}
	for _, idxMergeHint := range ds.IndexMergeHints {
		unknownPartitions := checkTableHintsApplicableForPartition(idxMergeHint.Partitions, partitionSet)
		appendWarnForUnknownPartitions(ds.SCtx(), h.Restore2IndexHint(h.HintIndexMerge, idxMergeHint), unknownPartitions)
	}
	unknownPartitions := checkTableHintsApplicableForPartition(ds.PreferPartitions[h.PreferTiKV], partitionSet)
	unknownPartitions = append(unknownPartitions,
		checkTableHintsApplicableForPartition(ds.PreferPartitions[h.PreferTiFlash], partitionSet)...)
	appendWarnForUnknownPartitions(ds.SCtx(), h.HintReadFromStorage, unknownPartitions)
}

func (s *PartitionProcessor) makeUnionAllChildren(ds *logicalop.DataSource, pi *model.PartitionInfo, or PartitionRangeOR) (base.LogicalPlan, error) {
	children := make([]base.LogicalPlan, 0, len(pi.Definitions))
	partitionNameSet := make(set.StringSet)
	usedDefinition := make(map[int64]model.PartitionDefinition)
	for _, r := range or {
		for i := r.Start; i < r.End; i++ {
			partIdx := pi.GetOverlappingDroppingPartitionIdx(i)
			if partIdx < 0 {
				continue
			}

			// This is for `table partition (p0,p1)` syntax, only union the specified partition if has specified partitions.
			if len(ds.PartitionNames) != 0 {
				if !s.FindByName(ds.PartitionNames, pi.Definitions[partIdx].Name.L) {
					continue
				}
			}
			if _, found := usedDefinition[pi.Definitions[partIdx].ID]; found {
				continue
			}
			// Not a deep copy.
			newDataSource := *ds
			newDataSource.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ds.SCtx(), plancodec.TypeTableScan, &newDataSource, ds.QueryBlockOffset())
			newDataSource.SetSchema(ds.Schema().Clone())
			newDataSource.Columns = make([]*model.ColumnInfo, len(ds.Columns))
			copy(newDataSource.Columns, ds.Columns)
			newDataSource.PartitionDefIdx = &partIdx
			newDataSource.PhysicalTableID = pi.Definitions[partIdx].ID

			// There are many expression nodes in the plan tree use the original datasource
			// id as FromID. So we set the id of the newDataSource with the original one to
			// avoid traversing the whole plan tree to update the references.
			newDataSource.SetID(ds.ID())
			err := s.resolveOptimizeHint(&newDataSource, pi.Definitions[partIdx].Name)
			partitionNameSet.Insert(pi.Definitions[partIdx].Name.L)
			if err != nil {
				return nil, err
			}
			children = append(children, &newDataSource)
			usedDefinition[pi.Definitions[partIdx].ID] = pi.Definitions[partIdx]
		}
	}
	s.checkHintsApplicable(ds, partitionNameSet)

	ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Static partition pruning mode")
	if len(children) == 0 {
		// No result after table pruning.
		tableDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ds.SCtx(), ds.QueryBlockOffset())
		tableDual.SetSchema(ds.Schema())
		return tableDual, nil
	}
	if len(children) == 1 {
		// No need for the union all.
		return children[0], nil
	}
	unionAll := logicalop.LogicalPartitionUnionAll{}.Init(ds.SCtx(), ds.QueryBlockOffset())
	unionAll.SetChildren(children...)
	unionAll.SetSchema(ds.Schema().Clone())
	return unionAll, nil
}

func (*PartitionProcessor) pruneRangeColumnsPartition(ctx base.PlanContext, conds []expression.Expression, pi *model.PartitionInfo, pe *tables.PartitionExpr, columns []*expression.Column) (PartitionRangeOR, error) {
	result := GetFullRange(len(pi.Definitions))

	if len(pi.Columns) < 1 {
		return result, nil
	}

	pruner, err := makeRangeColumnPruner(columns, pi, pe.ForRangeColumnsPruning, pe.ColumnOffset)
	if err == nil {
		result = PartitionRangeForCNFExpr(ctx, conds, pruner, result)
	}
	return result, nil
}

var _ partitionRangePruner = &RangeColumnsPruner{}

// RangeColumnsPruner is used by 'partition by range columns'.
type RangeColumnsPruner struct {
	LessThan [][]*expression.Expression
	PartCols []*expression.Column
}

func makeRangeColumnPruner(columns []*expression.Column, pi *model.PartitionInfo, from *tables.ForRangeColumnsPruning, offsets []int) (*RangeColumnsPruner, error) {
	if len(pi.Definitions) != len(from.LessThan) {
		return nil, errors.Trace(fmt.Errorf("internal error len(pi.Definitions) != len(from.LessThan) %d != %d", len(pi.Definitions), len(from.LessThan)))
	}
	partCols := make([]*expression.Column, len(offsets))
	for i, offset := range offsets {
		partCols[i] = columns[offset]
	}
	lessThan := make([][]*expression.Expression, 0, len(from.LessThan))
	for i := range from.LessThan {
		colVals := make([]*expression.Expression, 0, len(from.LessThan[i]))
		for j := range from.LessThan[i] {
			if from.LessThan[i][j] != nil {
				tmp := (*from.LessThan[i][j]).Clone()
				colVals = append(colVals, &tmp)
			} else {
				colVals = append(colVals, nil)
			}
		}
		lessThan = append(lessThan, colVals)
	}
	return &RangeColumnsPruner{lessThan, partCols}, nil
}

func (p *RangeColumnsPruner) fullRange() PartitionRangeOR {
	return GetFullRange(len(p.LessThan))
}

func (p *RangeColumnsPruner) getPartCol(colID int64) *expression.Column {
	for i := range p.PartCols {
		if colID == p.PartCols[i].ID {
			return p.PartCols[i]
		}
	}
	return nil
}

func (p *RangeColumnsPruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool) {
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return 0, len(p.LessThan), false
	}

	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE, ast.NullEQ:
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && len(p.PartCols) == 1 && arg0.ID == p.PartCols[0].ID {
			// Single column RANGE COLUMNS, NULL sorts before all other values: match first partition
			return 0, 1, true
		}
		return 0, len(p.LessThan), false
	default:
		return 0, len(p.LessThan), false
	}
	opName := op.FuncName.L

	var col *expression.Column
	var con *expression.Constant
	var argCol0, argCol1 *expression.Column
	var argCon0, argCon1 *expression.Constant
	var okCol0, okCol1, okCon0, okCon1 bool
	args := op.GetArgs()
	argCol1, okCol1 = args[1].(*expression.Column)
	argCon1, okCon1 = args[1].(*expression.Constant)
	argCol0, okCol0 = args[0].(*expression.Column)
	argCon0, okCon0 = args[0].(*expression.Constant)
	if okCol0 && okCon1 {
		col, con = argCol0, argCon1
	} else if okCol1 && okCon0 {
		col, con = argCol1, argCon0
		opName = opposite(opName)
	} else {
		return 0, len(p.LessThan), false
	}
	partCol := p.getPartCol(col.ID)
	if partCol == nil {
		return 0, len(p.LessThan), false
	}

	if opName == ast.NullEQ {
		if con.Value.IsNull() {
			return 0, 1, true
		}
		opName = ast.EQ
	}
	// If different collation, we can only prune if:
	// - expression is binary collation (can only be found in one partition)
	// - EQ operator, consider values 'a','b','ä' where 'ä' would be in the same partition as 'a' if general_ci, but is binary after 'b'
	// otherwise return all partitions / no pruning
	_, exprColl := expr.CharsetAndCollation()
	colColl := partCol.RetType.GetCollate()
	if exprColl != colColl && (opName != ast.EQ || !collate.IsBinCollation(exprColl)) {
		return 0, len(p.LessThan), true
	}
	start, end = p.pruneUseBinarySearch(sctx, opName, con)
	return start, end, true
}

// PruneUseBinarySearch returns the start and end of which partitions will match.
// If no match (i.e. value > last partition) the start partition will be the number of partition, not the first partition!
func (p *RangeColumnsPruner) pruneUseBinarySearch(sctx base.PlanContext, op string, data *expression.Constant) (start int, end int) {
	var savedError error
	var isNull bool
	if len(p.PartCols) > 1 {
		// Only one constant in the input, this will never be called with
		// multi-column RANGE COLUMNS :)
		return 0, len(p.LessThan)
	}
	charSet, collation := p.PartCols[0].RetType.GetCharset(), p.PartCols[0].RetType.GetCollate()
	compare := func(ith int, op string, v *expression.Constant) bool {
		for i := range p.PartCols {
			if p.LessThan[ith][i] == nil { // MAXVALUE
				return true
			}
			expr, err := expression.NewFunctionBase(sctx.GetExprCtx(), op, types.NewFieldType(mysql.TypeLonglong), *p.LessThan[ith][i], v)
			if err != nil {
				savedError = err
				return true
			}
			expr.SetCharsetAndCollation(charSet, collation)
			var val int64
			val, isNull, err = expr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				savedError = err
				return true
			}
			if val > 0 {
				return true
			}
		}
		return false
	}

	length := len(p.LessThan)
	switch op {
	case ast.EQ:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = pos, pos+1
	case ast.LT:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GE, data) })
		start, end = 0, pos+1
	case ast.GE, ast.GT:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = pos, length
	case ast.LE:
		pos := sort.Search(length, func(i int) bool { return compare(i, ast.GT, data) })
		start, end = 0, pos+1
	default:
		start, end = 0, length
	}

	// Something goes wrong, abort this pruning.
	if savedError != nil || isNull {
		return 0, len(p.LessThan)
	}

	if end > length {
		end = length
	}
	return start, end
}

// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from conds, the condition like
// 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
func PushDownNot(ctx expression.BuildContext, conds []expression.Expression) []expression.Expression {
	if len(conds) == 0 {
		return conds
	}
	for i, cond := range conds {
		conds[i] = expression.PushDownNot(ctx, cond)
	}
	return conds
}
