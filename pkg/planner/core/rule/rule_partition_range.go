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
	"cmp"
	"slices"
	"sort"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// LessThanDataInt is the less than structure in partition def.
type LessThanDataInt struct {
	Data     []int64
	Unsigned bool
	Maxvalue bool
}

// Length exported for test usage.
func (lt *LessThanDataInt) Length() int {
	return len(lt.Data)
}

func (lt *LessThanDataInt) compare(ith int, v int64, unsigned bool) int {
	// TODO: get an extra partition when `v` bigger than `lt.maxvalue``, but the result still correct.
	if ith == lt.Length()-1 && lt.Maxvalue {
		return 1
	}

	return types.CompareInt(lt.Data[ith], lt.Unsigned, v, unsigned)
}

// PartitionRange represents [start, end)
type PartitionRange struct {
	Start int
	End   int
}

// Cmp compare between two partition range.
func (p *PartitionRange) Cmp(a PartitionRange) int {
	return cmp.Compare(p.Start, a.Start)
}

// PartitionRangeOR represents OR(range1, range2, ...)
type PartitionRangeOR []PartitionRange

// GetFullRange get the full range.
func GetFullRange(end int) PartitionRangeOR {
	var reduceAllocation [3]PartitionRange
	reduceAllocation[0] = PartitionRange{0, end}
	return reduceAllocation[:1]
}

// IntersectionRange intersect the ranges.
func (or PartitionRangeOR) IntersectionRange(start, end int) PartitionRangeOR {
	// Let M = intersection, U = union, then
	// a M (b U c) == (a M b) U (a M c)
	ret := or[:0]
	for _, r1 := range or {
		newStart, newEnd := intersectionRange(r1.Start, r1.End, start, end)
		// Exclude the empty one.
		if newEnd > newStart {
			ret = append(ret, PartitionRange{newStart, newEnd})
		}
	}
	return ret
}

// Len returns the length.
func (or PartitionRangeOR) Len() int {
	return len(or)
}

// Union returns the union range.
func (or PartitionRangeOR) Union(x PartitionRangeOR) PartitionRangeOR {
	or = append(or, x...)
	return or.simplify()
}

func (or PartitionRangeOR) simplify() PartitionRangeOR {
	// if the length of the `or` is zero. We should return early.
	if len(or) == 0 {
		return or
	}
	// Make the ranges order by start.
	slices.SortFunc(or, func(i, j PartitionRange) int {
		return i.Cmp(j)
	})

	// Iterate the sorted ranges, merge the adjacent two when their range overlap.
	// For example, [0, 1), [2, 7), [3, 5), ... => [0, 1), [2, 7) ...
	res := or[:1]
	for _, curr := range or[1:] {
		last := &res[len(res)-1]
		if curr.Start > last.End {
			res = append(res, curr)
		} else {
			// Merge two.
			if curr.End > last.End {
				last.End = curr.End
			}
		}
	}
	return res
}

// Intersection intersect the ranges.
func (or PartitionRangeOR) Intersection(x PartitionRangeOR) PartitionRangeOR {
	if or.Len() == 1 {
		return x.IntersectionRange(or[0].Start, or[0].End)
	}
	if x.Len() == 1 {
		return or.IntersectionRange(x[0].Start, x[0].End)
	}

	// Rename to x, y where len(x) > len(y)
	var y PartitionRangeOR
	if or.Len() > x.Len() {
		x, y = or, x
	} else {
		y = or
	}

	// (a U b) M (c U d) => (x M c) U (x M d), x = (a U b)
	res := make(PartitionRangeOR, 0, len(y))
	for _, r := range y {
		// As IntersectionRange modify the raw data, we have to make a copy.
		tmp := make(PartitionRangeOR, len(x))
		copy(tmp, x)
		tmp = tmp.IntersectionRange(r.Start, r.End)
		res = append(res, tmp...)
	}
	return res.simplify()
}

// intersectionRange calculate the intersection of [start, end) and [newStart, newEnd)
func intersectionRange(start, end, newStart, newEnd int) (s int, e int) {
	s = max(start, newStart)

	e = min(end, newEnd)
	return s, e
}

// PruneRangePartition prune range partitions.
func (s *PartitionProcessor) PruneRangePartition(ctx base.PlanContext, pi *model.PartitionInfo, tbl table.PartitionedTable, conds []expression.Expression,
	columns []*expression.Column, names types.NameSlice) (PartitionRangeOR, error) {
	partExpr := tbl.(base.PartitionTable).PartitionExpr()

	// Partition by range columns.
	if len(pi.Columns) > 0 {
		result, err := s.pruneRangeColumnsPartition(ctx, conds, pi, partExpr, columns)
		return result, err
	}

	// Partition by range.
	col, fn, mono, err := MakePartitionByFnCol(ctx, columns, names, pi.Expr)
	if err != nil {
		return nil, err
	}
	result := GetFullRange(len(pi.Definitions))
	if col == nil {
		return result, nil
	}

	// Extract the partition column, if the column is not null, it's possible to prune.
	pruner := RangePruner{
		LessThan: LessThanDataInt{
			Data:     partExpr.ForRangePruning.LessThan,
			Unsigned: mysql.HasUnsignedFlag(col.GetStaticType().GetFlag()),
			Maxvalue: partExpr.ForRangePruning.MaxValue,
		},
		Col:        col,
		PartFn:     fn,
		Monotonous: mono,
	}
	result = PartitionRangeForCNFExpr(ctx, conds, &pruner, result)

	return result, nil
}

func (s *PartitionProcessor) processRangePartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error) {
	used, err := s.PruneRangePartition(ds.SCtx(), pi, ds.Table.(table.PartitionedTable), ds.AllConds, ds.TblCols, ds.OutputNames())
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, used)
}

func (s *PartitionProcessor) processListPartition(ds *logicalop.DataSource, pi *model.PartitionInfo) (base.LogicalPlan, error) {
	used, err := s.PruneListPartition(ds.SCtx(), ds.Table, ds.PartitionNames, ds.AllConds, ds.TblCols)
	if err != nil {
		return nil, err
	}
	return s.makeUnionAllChildren(ds, pi, convertToRangeOr(used, pi))
}

// MakePartitionByFnCol extracts the column and function information in 'partition by ... fn(col)'.
func MakePartitionByFnCol(sctx base.PlanContext, columns []*expression.Column, names types.NameSlice, partitionExpr string) (*expression.Column, *expression.ScalarFunction, monotoneMode, error) {
	monotonous := MonotoneModeInvalid
	schema := expression.NewSchema(columns...)
	// Increase the PlanID to make sure some tests will pass. The old implementation to rewrite AST builds a `TableDual`
	// that causes the `PlanID` increases, and many test cases hardcoded the output plan ID in the expected result.
	// Considering the new `ParseSimpleExpr` does not do the same thing and to make the test pass,
	// we have to increase the `PlanID` here. But it is safe to remove this line without introducing any bug.
	// TODO: remove this line after fixing the test cases.
	sctx.GetSessionVars().PlanID.Add(1)
	partExpr, err := expression.ParseSimpleExpr(sctx.GetExprCtx(), partitionExpr, expression.WithInputSchemaAndNames(schema, names, nil))
	if err != nil {
		return nil, nil, monotonous, err
	}
	var col *expression.Column
	var fn *expression.ScalarFunction
	switch raw := partExpr.(type) {
	case *expression.ScalarFunction:
		args := raw.GetArgs()
		// Optimizations for a limited set of functions
		switch raw.FuncName.L {
		case ast.Floor:
			// Special handle for floor(unix_timestamp(ts)) as partition expression.
			// This pattern is so common for timestamp(3) column as partition expression that it deserve an optimization.
			if ut, ok := args[0].(*expression.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
				args1 := ut.GetArgs()
				if len(args1) == 1 {
					if c, ok1 := args1[0].(*expression.Column); ok1 {
						return c, raw, MonotoneModeNonStrict, nil
					}
				}
			}
		case ast.Extract:
			con, ok := args[0].(*expression.Constant)
			if !ok {
				break
			}
			col, ok = args[1].(*expression.Column)
			if !ok {
				// Special case where CastTimeToDuration is added
				expr, ok := args[1].(*expression.ScalarFunction)
				if !ok {
					break
				}
				if expr.Function.PbCode() != tipb.ScalarFuncSig_CastTimeAsDuration {
					break
				}
				castArgs := expr.GetArgs()
				col, ok = castArgs[0].(*expression.Column)
				if !ok {
					break
				}
			}
			if con.Value.Kind() != types.KindString {
				break
			}
			val := con.Value.GetString()
			colType := col.GetStaticType().GetType()
			switch colType {
			case mysql.TypeDate, mysql.TypeDatetime:
				switch val {
				// Only YEAR, YEAR_MONTH can be considered monotonic, the rest will wrap around!
				case "YEAR", "YEAR_MONTH":
					// Note, this function will not have the column as first argument,
					// so in replaceColumnWithConst it will replace the second argument, which
					// is special handling there too!
					return col, raw, MonotoneModeNonStrict, nil
				default:
					return col, raw, monotonous, nil
				}
			case mysql.TypeDuration:
				switch val {
				// Only HOUR* can be considered monotonic, the rest will wrap around!
				// TODO: if fsp match for HOUR_SECOND or HOUR_MICROSECOND we could
				// mark it as MonotoneModeStrict
				case "HOUR", "HOUR_MINUTE", "HOUR_SECOND", "HOUR_MICROSECOND":
					// Note, this function will not have the column as first argument,
					// so in replaceColumnWithConst it will replace the second argument, which
					// is special handling there too!
					return col, raw, MonotoneModeNonStrict, nil
				default:
					return col, raw, monotonous, nil
				}
			}
		}

		fn = raw
		monotonous = getMonotoneMode(raw.FuncName.L)
		// Check the partitionExpr is in the form: fn(col, ...)
		// There should be only one column argument, and it should be the first parameter.
		if expression.ExtractColumnSet(args...).Len() == 1 {
			if col1, ok := args[0].(*expression.Column); ok {
				col = col1
			}
		}
	case *expression.Column:
		col = raw
	}
	return col, fn, monotonous, nil
}

func minCmp(ctx base.PlanContext, lowVal []types.Datum, columnsPruner *RangeColumnsPruner, comparer []collate.Collator, lowExclude bool, gotError *bool) func(i int) bool {
	return func(i int) bool {
		for j := range lowVal {
			expr := columnsPruner.LessThan[i][j]

			if expr == nil {
				// MAXVALUE
				return true
			}
			con, ok := (*expr).(*expression.Constant)
			if !ok {
				// Not a constant, pruning not possible, so value is considered less than all partitions
				return true
			}
			// Add Null as point here?
			cmp, err := con.Value.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &lowVal[j], comparer[j])
			if err != nil {
				*gotError = true
			}
			if cmp > 0 {
				return true
			}
			if cmp < 0 {
				return false
			}
		}
		if len(lowVal) < len(columnsPruner.LessThan[i]) {
			// Not all columns given
			if lowExclude {
				// prefix cols > const, do not include this partition
				return false
			}

			colIdx := len(lowVal)
			col := columnsPruner.PartCols[colIdx]
			conExpr := columnsPruner.LessThan[i][colIdx]
			if conExpr == nil {
				// MAXVALUE
				return true
			}

			// Possible to optimize by getting minvalue of the column type
			// and if lessThan is equal to that
			// we can return false, since the partition definition is
			// LESS THAN (..., colN, minValOfColM, ... ) which cannot match colN == LowVal
			if !mysql.HasNotNullFlag(col.RetType.GetFlag()) {
				// NULL cannot be part of the partitioning expression: VALUES LESS THAN (NULL...)
				// NULL is allowed in the column and will be considered as lower than any other value
				// so this partition needs to be included!
				return true
			}
			if con, ok := (*conExpr).(*expression.Constant); ok && col != nil {
				switch col.RetType.EvalType() {
				case types.ETInt:
					if mysql.HasUnsignedFlag(col.RetType.GetFlag()) {
						if con.Value.GetUint64() == 0 {
							return false
						}
					} else {
						if con.Value.GetInt64() == types.IntegerSignedLowerBound(col.GetStaticType().GetType()) {
							return false
						}
					}
				case types.ETDatetime:
					if con.Value.GetMysqlTime().IsZero() {
						return false
					}
				case types.ETString:
					if len(con.Value.GetString()) == 0 {
						return false
					}
				}
			}
			// Also if not a constant, pruning not possible, so value is considered less than all partitions
			return true
		}
		return false
	}
}

func maxCmp(ctx base.PlanContext, hiVal []types.Datum, columnsPruner *RangeColumnsPruner, comparer []collate.Collator, hiExclude bool, gotError *bool) func(i int) bool {
	return func(i int) bool {
		for j := range hiVal {
			expr := columnsPruner.LessThan[i][j]
			if expr == nil {
				// MAXVALUE
				return true
			}
			con, ok := (*expr).(*expression.Constant)
			if !ok {
				// Not a constant, include every partition, i.e. value is not less than any partition
				return false
			}
			// Add Null as point here?
			cmp, err := con.Value.Compare(ctx.GetSessionVars().StmtCtx.TypeCtx(), &hiVal[j], comparer[j])
			if err != nil {
				*gotError = true
				// error pushed, we will still use the cmp value
			}
			if cmp > 0 {
				return true
			}
			if cmp < 0 {
				return false
			}
		}
		// All hiVal == columnsPruner.lessThan
		if len(hiVal) < len(columnsPruner.LessThan[i]) {
			// Not all columns given
			if columnsPruner.LessThan[i][len(hiVal)] == nil {
				// MAXVALUE
				return true
			}
		}
		// if point is included, then false, due to LESS THAN
		return hiExclude
	}
}

func multiColumnRangeColumnsPruner(sctx base.PlanContext, exprs []expression.Expression,
	columnsPruner *RangeColumnsPruner, result PartitionRangeOR) PartitionRangeOR {
	lens := make([]int, 0, len(columnsPruner.PartCols))
	for i := range columnsPruner.PartCols {
		lens = append(lens, columnsPruner.PartCols[i].RetType.GetFlen())
	}

	res, err := ranger.DetachCondAndBuildRangeForPartition(sctx.GetRangerCtx(), exprs, columnsPruner.PartCols, lens, sctx.GetSessionVars().RangeMaxSize)
	if err != nil {
		return GetFullRange(len(columnsPruner.LessThan))
	}
	if len(res.Ranges) == 0 {
		if len(res.AccessConds) == 0 && len(res.RemainedConds) == 0 {
			// Impossible conditions, like: a > 2 AND a < 1
			return PartitionRangeOR{}
		}
		// Could not extract any valid range, use all partitions
		return GetFullRange(len(columnsPruner.LessThan))
	}

	rangeOr := make([]PartitionRange, 0, len(res.Ranges))

	gotError := false
	// Create a sort.Search where the compare loops over ColumnValues
	// Loop over the different ranges and extend/include all the partitions found
	for idx := range res.Ranges {
		minComparer := minCmp(sctx, res.Ranges[idx].LowVal, columnsPruner, res.Ranges[idx].Collators, res.Ranges[idx].LowExclude, &gotError)
		maxComparer := maxCmp(sctx, res.Ranges[idx].HighVal, columnsPruner, res.Ranges[idx].Collators, res.Ranges[idx].HighExclude, &gotError)
		if gotError {
			// the compare function returned error, use all partitions.
			return GetFullRange(len(columnsPruner.LessThan))
		}
		// Can optimize if the range start is types.KindNull/types.MinNotNull
		// or range end is types.KindMaxValue
		start := sort.Search(len(columnsPruner.LessThan), minComparer)
		end := sort.Search(len(columnsPruner.LessThan), maxComparer)

		if end < len(columnsPruner.LessThan) {
			end++
		}
		rangeOr = append(rangeOr, PartitionRange{start, end})
	}
	return result.Intersection(rangeOr).simplify()
}

// PartitionRangeForCNFExpr calculates the partitions for the CNF expression.
func PartitionRangeForCNFExpr(sctx base.PlanContext, exprs []expression.Expression,
	pruner partitionRangePruner, result PartitionRangeOR) PartitionRangeOR {
	// TODO: When the ranger/detacher handles varchar_col_general_ci cmp constant bin collation
	// remove the check for single column RANGE COLUMNS and remove the single column implementation
	if columnsPruner, ok := pruner.(*RangeColumnsPruner); ok && len(columnsPruner.PartCols) > 1 {
		return multiColumnRangeColumnsPruner(sctx, exprs, columnsPruner, result)
	}
	for i := range exprs {
		result = PartitionRangeForExpr(sctx, exprs[i], pruner, result)
	}
	return result
}

// PartitionRangeForExpr calculate the partitions for the expression.
func PartitionRangeForExpr(sctx base.PlanContext, expr expression.Expression,
	pruner partitionRangePruner, result PartitionRangeOR) PartitionRangeOR {
	// Handle AND, OR respectively.
	if op, ok := expr.(*expression.ScalarFunction); ok {
		switch op.FuncName.L {
		case ast.LogicAnd:
			return PartitionRangeForCNFExpr(sctx, op.GetArgs(), pruner, result)
		case ast.LogicOr:
			args := op.GetArgs()
			newRange := partitionRangeForOrExpr(sctx, args[0], args[1], pruner)
			return result.Intersection(newRange)
		case ast.In:
			if p, ok := pruner.(*RangePruner); ok {
				newRange := partitionRangeForInExpr(sctx, op.GetArgs(), p)
				return result.Intersection(newRange)
			} else if p, ok := pruner.(*RangeColumnsPruner); ok {
				newRange := partitionRangeColumnForInExpr(sctx, op.GetArgs(), p)
				return result.Intersection(newRange)
			}
			return result
		}
	}

	// Handle a single expression.
	start, end, ok := pruner.partitionRangeForExpr(sctx, expr)
	if !ok {
		// Can't prune, return the whole range.
		return result
	}
	return result.IntersectionRange(start, end)
}

type partitionRangePruner interface {
	partitionRangeForExpr(base.PlanContext, expression.Expression) (start, end int, succ bool)
	fullRange() PartitionRangeOR
}

var _ partitionRangePruner = &RangePruner{}

// RangePruner is used by 'partition by range'.
type RangePruner struct {
	LessThan LessThanDataInt
	Col      *expression.Column
	PartFn   *expression.ScalarFunction
	// If PartFn is not nil, monotonous indicates PartFn is monotonous or not.
	Monotonous monotoneMode
}

func (p *RangePruner) partitionRangeForExpr(sctx base.PlanContext, expr expression.Expression) (start int, end int, ok bool) {
	if constExpr, ok := expr.(*expression.Constant); ok {
		if b, err := constExpr.Value.ToBool(sctx.GetSessionVars().StmtCtx.TypeCtx()); err == nil && b == 0 {
			// A constant false expression.
			return 0, 0, true
		}
	}

	dataForPrune, ok := p.extractDataForPrune(sctx, expr)
	if !ok {
		return 0, 0, false
	}

	start, end = PruneUseBinarySearch(p.LessThan, dataForPrune)
	return start, end, true
}

func (p *RangePruner) fullRange() PartitionRangeOR {
	return GetFullRange(p.LessThan.Length())
}

// partitionRangeForOrExpr calculate the partitions for or(expr1, expr2)
func partitionRangeForOrExpr(sctx base.PlanContext, expr1, expr2 expression.Expression,
	pruner partitionRangePruner) PartitionRangeOR {
	tmp1 := PartitionRangeForExpr(sctx, expr1, pruner, pruner.fullRange())
	tmp2 := PartitionRangeForExpr(sctx, expr2, pruner, pruner.fullRange())
	return tmp1.Union(tmp2)
}

func partitionRangeColumnForInExpr(sctx base.PlanContext, args []expression.Expression,
	pruner *RangeColumnsPruner) PartitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.PartCols[0].ID {
		return pruner.fullRange()
	}

	var result PartitionRangeOR
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		switch constExpr.Value.Kind() {
		case types.KindInt64, types.KindUint64, types.KindMysqlTime, types.KindString: // for safety, only support string,int and datetime now
		case types.KindNull:
			continue
		default:
			return pruner.fullRange()
		}

		// convert all elements to EQ-exprs and prune them one by one
		sf, err := expression.NewFunction(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(types.KindInt64), []expression.Expression{col, args[i]}...)
		if err != nil {
			return pruner.fullRange()
		}
		start, end, ok := pruner.partitionRangeForExpr(sctx, sf)
		if !ok {
			return pruner.fullRange()
		}
		result = append(result, PartitionRange{start, end})
	}

	return result.simplify()
}

func partitionRangeForInExpr(sctx base.PlanContext, args []expression.Expression,
	pruner *RangePruner) PartitionRangeOR {
	col, ok := args[0].(*expression.Column)
	if !ok || col.ID != pruner.Col.ID {
		return pruner.fullRange()
	}

	var result PartitionRangeOR
	for i := 1; i < len(args); i++ {
		constExpr, ok := args[i].(*expression.Constant)
		if !ok {
			return pruner.fullRange()
		}
		if constExpr.Value.Kind() == types.KindNull {
			continue
		}

		var val int64
		var err error
		var unsigned bool
		if pruner.PartFn != nil {
			// replace fn(col) to fn(const)
			partFnConst := replaceColumnWithConst(pruner.PartFn, constExpr)
			val, _, err = partFnConst.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			unsigned = mysql.HasUnsignedFlag(partFnConst.GetStaticType().GetFlag())
		} else {
			val, _, err = constExpr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			unsigned = mysql.HasUnsignedFlag(constExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).GetFlag())
		}
		if err != nil {
			return pruner.fullRange()
		}

		start, end := PruneUseBinarySearch(pruner.LessThan, DataForPrune{Op: ast.EQ, C: val, Unsigned: unsigned})
		result = append(result, PartitionRange{start, end})
	}
	return result.simplify()
}

type monotoneMode int

const (
	// MonotoneModeInvalid indicate the invalid mode.
	MonotoneModeInvalid monotoneMode = iota
	// MonotoneModeStrict indicate the strict mode.
	MonotoneModeStrict
	// MonotoneModeNonStrict indicate the non-strict mode.
	MonotoneModeNonStrict
)

// monotoneIncFuncs are those functions that are monotone increasing.
// For any x y, if x > y => f(x) > f(y), function f is strict monotone .
// For any x y, if x > y => f(x) >= f(y), function f is non-strict monotone.
var monotoneIncFuncs = map[string]monotoneMode{
	ast.Year:          MonotoneModeNonStrict,
	ast.ToDays:        MonotoneModeNonStrict,
	ast.UnixTimestamp: MonotoneModeStrict,
	// Only when the function form is fn(column, const)
	ast.Plus:  MonotoneModeStrict,
	ast.Minus: MonotoneModeStrict,
}

func getMonotoneMode(fnName string) monotoneMode {
	mode, ok := monotoneIncFuncs[fnName]
	if !ok {
		return MonotoneModeInvalid
	}
	return mode
}

// DataForPrune f(x) op const, op is > = <
type DataForPrune struct {
	Op       string
	C        int64
	Unsigned bool
}

// extractDataForPrune extracts data from the expression for pruning.
// The expression should have this form:  'f(x) op const', otherwise it can't be pruned.
func (p *RangePruner) extractDataForPrune(sctx base.PlanContext, expr expression.Expression) (DataForPrune, bool) {
	var ret DataForPrune
	op, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return ret, false
	}
	switch op.FuncName.L {
	case ast.EQ, ast.LT, ast.GT, ast.LE, ast.GE, ast.NullEQ:
		ret.Op = op.FuncName.L
	case ast.IsNull:
		// isnull(col)
		if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.Col.ID {
			ret.Op = ast.IsNull
			return ret, true
		}
		return ret, false
	default:
		return ret, false
	}

	var col *expression.Column
	var con *expression.Constant
	if arg0, ok := op.GetArgs()[0].(*expression.Column); ok && arg0.ID == p.Col.ID {
		if arg1, ok := op.GetArgs()[1].(*expression.Constant); ok {
			col, con = arg0, arg1
		}
	} else if arg0, ok := op.GetArgs()[1].(*expression.Column); ok && arg0.ID == p.Col.ID {
		if arg1, ok := op.GetArgs()[0].(*expression.Constant); ok {
			ret.Op = opposite(ret.Op)
			col, con = arg0, arg1
		}
	}
	if col == nil || con == nil {
		return ret, false
	}

	// Current expression is 'col op const'
	var constExpr expression.Expression
	if p.PartFn != nil {
		// If the partition function is not monotone, only EQ condition can be pruning.
		if p.Monotonous == MonotoneModeInvalid && ret.Op != ast.EQ {
			return ret, false
		}

		// If the partition expression is fn(col), change constExpr to fn(constExpr).
		constExpr = replaceColumnWithConst(p.PartFn, con)

		// When the PartFn is not strict monotonous, we need to relax the condition < to <=, > to >=.
		// For example, the following case doesn't hold:
		// col < '2020-02-11 17:34:11' => to_days(col) < to_days(2020-02-11 17:34:11)
		// The correct transform should be:
		// col < '2020-02-11 17:34:11' => to_days(col) <= to_days(2020-02-11 17:34:11)
		if p.Monotonous == MonotoneModeNonStrict {
			ret.Op = relaxOP(ret.Op)
		}
	} else {
		// If the partition expression is col, use constExpr.
		constExpr = con
	}
	c, isNull, err := constExpr.EvalInt(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil {
		return ret, false
	}
	if !isNull {
		if ret.Op == ast.NullEQ {
			ret.Op = ast.EQ
		}
		ret.C = c
		ret.Unsigned = mysql.HasUnsignedFlag(constExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).GetFlag())
		return ret, true
	} else if ret.Op == ast.NullEQ {
		// Mark it as IsNull, which is already handled in PruneUseBinarySearch.
		ret.Op = ast.IsNull
		return ret, true
	}
	return ret, false
}

// replaceColumnWithConst change fn(col) to fn(const)
func replaceColumnWithConst(partFn *expression.ScalarFunction, con *expression.Constant) *expression.ScalarFunction {
	args := partFn.GetArgs()
	// The partition function may be floor(unix_timestamp(ts)) instead of a simple fn(col).
	if partFn.FuncName.L == ast.Floor {
		if ut, ok := args[0].(*expression.ScalarFunction); ok && ut.FuncName.L == ast.UnixTimestamp {
			args = ut.GetArgs()
			args[0] = con
			return partFn
		}
	} else if partFn.FuncName.L == ast.Extract {
		if expr, ok := args[1].(*expression.ScalarFunction); ok && expr.Function.PbCode() == tipb.ScalarFuncSig_CastTimeAsDuration {
			// Special handing if Cast is added
			funcArgs := expr.GetArgs()
			funcArgs[0] = con
			return partFn
		}
		args[1] = con
		return partFn
	}

	// No 'copy on write' for the expression here, this is a dangerous operation.
	args[0] = con
	return partFn
}

// opposite turns > to <, >= to <= and so on.
func opposite(op string) string {
	switch op {
	case ast.EQ:
		return ast.EQ
	case ast.LT:
		return ast.GT
	case ast.GT:
		return ast.LT
	case ast.LE:
		return ast.GE
	case ast.GE:
		return ast.LE
	}
	panic("invalid input parameter" + op)
}

// relaxOP relax the op > to >= and < to <=
// Sometime we need to relax the condition, for example:
// col < const => f(col) <= const
// datetime < 2020-02-11 16:18:42 => to_days(datetime) <= to_days(2020-02-11)
// We can't say:
// datetime < 2020-02-11 16:18:42 => to_days(datetime) < to_days(2020-02-11)
func relaxOP(op string) string {
	switch op {
	case ast.LT:
		return ast.LE
	case ast.GT:
		return ast.GE
	}
	return op
}

// PruneUseBinarySearch returns the start and end of which partitions will match.
// If no match (i.e. value > last partition) the start partition will be the number of partition, not the first partition!
func PruneUseBinarySearch(lessThan LessThanDataInt, data DataForPrune) (start int, end int) {
	length := lessThan.Length()
	switch data.Op {
	case ast.EQ:
		// col = 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col = 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col = 10, lessThan = [4 7 11 14 17] => [2, 3)
		// col = 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) > 0 })
		start, end = pos, pos+1
	case ast.LT:
		// col < 66, lessThan = [4 7 11 14 17] => [0, 5)
		// col < 14, lessThan = [4 7 11 14 17] => [0, 4)
		// col < 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col < 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) >= 0 })
		start, end = 0, pos+1
	case ast.GE:
		// col >= 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col >= 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col >= 10, lessThan = [4 7 11 14 17] => [2, 5)
		// col >= 3, lessThan = [4 7 11 14 17] => [0, 5)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) > 0 })
		start, end = pos, length
	case ast.GT:
		// col > 66, lessThan = [4 7 11 14 17] => [5, 5)
		// col > 14, lessThan = [4 7 11 14 17] => [4, 5)
		// col > 10, lessThan = [4 7 11 14 17] => [3, 5)
		// col > 3, lessThan = [4 7 11 14 17] => [1, 5)
		// col > 2, lessThan = [4 7 11 14 17] => [0, 5)

		// Although `data.c+1` will overflow in sometime, this does not affect the correct results obtained.
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C+1, data.Unsigned) > 0 })
		start, end = pos, length
	case ast.LE:
		// col <= 66, lessThan = [4 7 11 14 17] => [0, 6)
		// col <= 14, lessThan = [4 7 11 14 17] => [0, 5)
		// col <= 10, lessThan = [4 7 11 14 17] => [0, 3)
		// col <= 3, lessThan = [4 7 11 14 17] => [0, 1)
		pos := sort.Search(length, func(i int) bool { return lessThan.compare(i, data.C, data.Unsigned) > 0 })
		start, end = 0, pos+1
	case ast.IsNull:
		start, end = 0, 1
	default:
		start, end = 0, length
	}

	if end > length {
		end = length
	}
	return start, end
}
