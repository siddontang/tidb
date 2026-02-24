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
	"bytes"
	stderr "errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/zap"
)

// ForRangeColumnsPruning is used for range partition pruning.
type ForRangeColumnsPruning struct {
	// LessThan contains expressions for [Partition][column].
	// If Maxvalue, then nil
	LessThan [][]*expression.Expression
}

func dataForRangeColumnsPruning(ctx expression.BuildContext, defs []model.PartitionDefinition, schema *expression.Schema, names []*types.FieldName, p *parser.Parser, colOffsets []int) (*ForRangeColumnsPruning, error) {
	var res ForRangeColumnsPruning
	res.LessThan = make([][]*expression.Expression, 0, len(defs))
	for i := range defs {
		lessThanCols := make([]*expression.Expression, 0, len(defs[i].LessThan))
		for j := range defs[i].LessThan {
			if strings.EqualFold(defs[i].LessThan[j], "MAXVALUE") {
				// Use a nil pointer instead of math.MaxInt64 to avoid the corner cases.
				lessThanCols = append(lessThanCols, nil)
				// No column after MAXVALUE matters
				break
			}
			tmp, err := parseSimpleExprWithNames(p, ctx, defs[i].LessThan[j], schema, names)
			if err != nil {
				return nil, err
			}
			_, ok := tmp.(*expression.Constant)
			if !ok {
				return nil, dbterror.ErrPartitionConstDomain
			}
			// TODO: Enable this for all types!
			// Currently it will trigger changes for collation differences
			switch schema.Columns[colOffsets[j]].RetType.GetType() {
			case mysql.TypeDatetime, mysql.TypeDate:
				// Will also fold constant
				tmp = expression.BuildCastFunction(ctx, tmp, schema.Columns[colOffsets[j]].RetType)
			}
			lessThanCols = append(lessThanCols, &tmp)
		}
		res.LessThan = append(res.LessThan, lessThanCols)
	}
	return &res, nil
}

// parseSimpleExprWithNames parses simple expression string to Expression.
// The expression string must only reference the column in the given NameSlice.
func parseSimpleExprWithNames(p *parser.Parser, ctx expression.BuildContext, exprStr string, schema *expression.Schema, names types.NameSlice) (expression.Expression, error) {
	exprNode, err := parseExpr(p, exprStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return expression.BuildSimpleExpr(ctx, exprNode, expression.WithInputSchemaAndNames(schema, names, nil))
}

// ForKeyPruning is used for key partition pruning.
type ForKeyPruning struct {
	KeyPartCols []*expression.Column
}

// ForListPruning is used for list partition pruning.
type ForListPruning struct {
	// LocateExpr uses to locate list partition by row.
	LocateExpr expression.Expression
	// PruneExpr uses to prune list partition in partition pruner.
	PruneExpr expression.Expression
	// PruneExprCols is the columns of PruneExpr, it has removed the duplicate columns.
	PruneExprCols []*expression.Column
	// valueToPartitionIdxBTree is column value -> partition idx, uses to locate list partition.
	valueToPartitionIdxBTree *btree.BTreeG[*btreeListItem]
	// nullPartitionIdx is the partition idx for null value.
	nullPartitionIdx int
	// defaultPartitionIdx is the partition idx for default value/fallback.
	defaultPartitionIdx int

	// For list columns partition pruning
	ColPrunes []*ForListColumnPruning
}

type btreeListItem struct {
	key          uint64
	partitionIdx int
}

// btreeListColumnItem is BTree's Item that uses string to compare.
type btreeListColumnItem struct {
	key      string
	location ListPartitionLocation
}

func newBtreeListColumnItem(key string, location ListPartitionLocation) *btreeListColumnItem {
	return &btreeListColumnItem{
		key:      key,
		location: location,
	}
}

func newBtreeListColumnSearchItem(key string) *btreeListColumnItem {
	return &btreeListColumnItem{
		key: key,
	}
}

func (item *btreeListColumnItem) Less(other btree.Item) bool {
	return item.key < other.(*btreeListColumnItem).key
}

func lessBtreeListColumnItem(a, b *btreeListColumnItem) bool {
	return a.key < b.key
}

// ForListColumnPruning is used for list columns partition pruning.
type ForListColumnPruning struct {
	ExprCol  *expression.Column
	valueTp  *types.FieldType
	valueMap map[string]ListPartitionLocation
	sorted   *btree.BTreeG[*btreeListColumnItem]

	// To deal with the location partition failure caused by inconsistent NewCollationEnabled values(see issue #32416).
	// The following fields are used to delay building valueMap.
	ctx     expression.BuildContext
	tblInfo *model.TableInfo
	schema  *expression.Schema
	names   types.NameSlice
	colIdx  int

	// catch-all partition / DEFAULT
	defaultPartID int64
}

// ListPartitionGroup indicate the group index of the column value in a partition.
type ListPartitionGroup struct {
	// Such as: list columns (a,b) (partition p0 values in ((1,5),(1,6)));
	// For the column a which value is 1, the ListPartitionGroup is:
	// ListPartitionGroup {
	//     PartIdx: 0,            // 0 is the partition p0 index in all partitions.
	//     GroupIdxs: []int{0,1}, // p0 has 2 value group: (1,5) and (1,6), and they both contain the column a where value is 1;
	// }                          // the value of GroupIdxs `0,1` is the index of the value group that contain the column a which value is 1.
	PartIdx   int
	GroupIdxs []int
}

// ListPartitionLocation indicate the partition location for the column value in list columns partition.
// Here is an example:
// Suppose the list columns partition is: list columns (a,b) (partition p0 values in ((1,5),(1,6)), partition p1 values in ((1,7),(9,9)));
// How to express the location of the column a which value is 1?
// For the column a which value is 1, both partition p0 and p1 contain the column a which value is 1.
// In partition p0, both value group0 (1,5) and group1 (1,6) are contain the column a which value is 1.
// In partition p1, value group0 (1,7) contains the column a which value is 1.
// So, the ListPartitionLocation of column a which value is 1 is:
//
//	[]ListPartitionGroup{
//		{
//			PartIdx: 0,               // `0` is the partition p0 index in all partitions.
//			GroupIdxs: []int{0, 1}    // `0,1` is the index of the value group0, group1.
//		},
//		{
//			PartIdx: 1,               // `1` is the partition p1 index in all partitions.
//			GroupIdxs: []int{0}       // `0` is the index of the value group0.
//		},
//	}
type ListPartitionLocation []ListPartitionGroup

// IsEmpty returns true if the ListPartitionLocation is empty.
func (ps ListPartitionLocation) IsEmpty() bool {
	for _, pg := range ps {
		if len(pg.GroupIdxs) > 0 {
			return false
		}
	}
	return true
}

func (ps ListPartitionLocation) findByPartitionIdx(partIdx int) int {
	for i, p := range ps {
		if p.PartIdx == partIdx {
			return i
		}
	}
	return -1
}

type listPartitionLocationHelper struct {
	initialized bool
	location    ListPartitionLocation
}

// NewListPartitionLocationHelper returns a new listPartitionLocationHelper.
func NewListPartitionLocationHelper() *listPartitionLocationHelper {
	return &listPartitionLocationHelper{}
}

// GetLocation gets the list partition location.
func (p *listPartitionLocationHelper) GetLocation() ListPartitionLocation {
	return p.location
}

// UnionPartitionGroup unions with the list-partition-value-group.
func (p *listPartitionLocationHelper) UnionPartitionGroup(pg ListPartitionGroup) {
	idx := p.location.findByPartitionIdx(pg.PartIdx)
	if idx < 0 {
		// copy the group idx.
		groupIdxs := make([]int, len(pg.GroupIdxs))
		copy(groupIdxs, pg.GroupIdxs)
		p.location = append(p.location, ListPartitionGroup{
			PartIdx:   pg.PartIdx,
			GroupIdxs: groupIdxs,
		})
		return
	}
	p.location[idx].union(pg)
}

// Union unions with the other location.
func (p *listPartitionLocationHelper) Union(location ListPartitionLocation) {
	for _, pg := range location {
		p.UnionPartitionGroup(pg)
	}
}

// Intersect intersect with other location.
func (p *listPartitionLocationHelper) Intersect(location ListPartitionLocation) bool {
	if !p.initialized {
		p.initialized = true
		p.location = make([]ListPartitionGroup, 0, len(location))
		p.location = append(p.location, location...)
		return true
	}
	currPgs := p.location
	remainPgs := make([]ListPartitionGroup, 0, len(location))
	for _, pg := range location {
		idx := currPgs.findByPartitionIdx(pg.PartIdx)
		if idx < 0 {
			continue
		}
		if !currPgs[idx].intersect(pg) {
			continue
		}
		remainPgs = append(remainPgs, currPgs[idx])
	}
	p.location = remainPgs
	return len(remainPgs) > 0
}

func (pg *ListPartitionGroup) intersect(otherPg ListPartitionGroup) bool {
	if pg.PartIdx != otherPg.PartIdx {
		return false
	}
	var groupIdxs []int
	for _, gidx := range otherPg.GroupIdxs {
		if pg.findGroupIdx(gidx) {
			groupIdxs = append(groupIdxs, gidx)
		}
	}
	pg.GroupIdxs = groupIdxs
	return len(groupIdxs) > 0
}

func (pg *ListPartitionGroup) union(otherPg ListPartitionGroup) {
	if pg.PartIdx != otherPg.PartIdx {
		return
	}
	pg.GroupIdxs = append(pg.GroupIdxs, otherPg.GroupIdxs...)
}

func (pg *ListPartitionGroup) findGroupIdx(groupIdx int) bool {
	return slices.Contains(pg.GroupIdxs, groupIdx)
}

// ForRangePruning is used for range partition pruning.
type ForRangePruning struct {
	LessThan []int64
	MaxValue bool
	Unsigned bool
}

// dataForRangePruning extracts the less than parts from 'partition p0 less than xx ... partition p1 less than ...'
func dataForRangePruning(sctx expression.BuildContext, defs []model.PartitionDefinition) (*ForRangePruning, error) {
	var maxValue bool
	var unsigned bool
	lessThan := make([]int64, len(defs))
	for i := range defs {
		if strings.EqualFold(defs[i].LessThan[0], "MAXVALUE") {
			// Use a bool flag instead of math.MaxInt64 to avoid the corner cases.
			maxValue = true
		} else {
			var err error
			lessThan[i], err = strconv.ParseInt(defs[i].LessThan[0], 10, 64)
			var numErr *strconv.NumError
			if stderr.As(err, &numErr) && numErr.Err == strconv.ErrRange {
				var tmp uint64
				tmp, err = strconv.ParseUint(defs[i].LessThan[0], 10, 64)
				lessThan[i] = int64(tmp)
				unsigned = true
			}
			if err != nil {
				val, ok := fixOldVersionPartitionInfo(sctx, defs[i].LessThan[0])
				if !ok {
					logutil.BgLogger().Error("wrong partition definition", zap.String("less than", defs[i].LessThan[0]))
					return nil, errors.WithStack(err)
				}
				lessThan[i] = val
			}
		}
	}
	return &ForRangePruning{
		LessThan: lessThan,
		MaxValue: maxValue,
		Unsigned: unsigned,
	}, nil
}

func fixOldVersionPartitionInfo(sctx expression.BuildContext, str string) (int64, bool) {
	// less than value should be calculate to integer before persistent.
	// Old version TiDB may not do it and store the raw expression.
	tmp, err := parseSimpleExprWithNames(parser.New(), sctx, str, nil, nil)
	if err != nil {
		return 0, false
	}
	ret, isNull, err := tmp.EvalInt(sctx.GetEvalCtx(), chunk.Row{})
	if err != nil || isNull {
		return 0, false
	}
	return ret, true
}

func rangePartitionExprStrings(cols []ast.CIStr, expr string) []string {
	var s []string
	if len(cols) > 0 {
		s = make([]string, 0, len(cols))
		for _, col := range cols {
			s = append(s, stringutil.Escape(col.O, mysql.ModeNone))
		}
	} else {
		s = []string{expr}
	}
	return s
}

func generateKeyPartitionExpr(ctx expression.BuildContext, expr string, partCols []ast.CIStr,
	columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	ret := &PartitionExpr{
		ForKeyPruning: &ForKeyPruning{},
	}
	_, partColumns, offset, err := extractPartitionExprColumns(ctx, expr, partCols, columns, names)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.ColumnOffset = offset
	ret.KeyPartCols = partColumns

	return ret, nil
}

func generateRangePartitionExpr(ctx expression.BuildContext, expr string, partCols []ast.CIStr,
	defs []model.PartitionDefinition, columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	p := parser.New()
	schema := expression.NewSchema(columns...)
	partStrs := rangePartitionExprStrings(partCols, expr)
	locateExprs, err := getRangeLocateExprs(ctx, p, defs, partStrs, schema, names)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := &PartitionExpr{
		UpperBounds: locateExprs,
	}

	partExpr, _, offset, err := extractPartitionExprColumns(ctx, expr, partCols, columns, names)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.ColumnOffset = offset

	if len(partCols) < 1 {
		tmp, err := dataForRangePruning(ctx, defs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.Expr = partExpr
		ret.ForRangePruning = tmp
	} else {
		tmp, err := dataForRangeColumnsPruning(ctx, defs, schema, names, p, offset)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret.ForRangeColumnsPruning = tmp
	}
	return ret, nil
}

func getRangeLocateExprs(ctx expression.BuildContext, p *parser.Parser, defs []model.PartitionDefinition, partStrs []string, schema *expression.Schema, names types.NameSlice) ([]expression.Expression, error) {
	var buf bytes.Buffer
	locateExprs := make([]expression.Expression, 0, len(defs))
	for i := range defs {
		if strings.EqualFold(defs[i].LessThan[0], "MAXVALUE") {
			// Expr less than maxvalue is always true.
			fmt.Fprintf(&buf, "true")
		} else {
			maxValueFound := false
			for j := range partStrs[1:] {
				if strings.EqualFold(defs[i].LessThan[j+1], "MAXVALUE") {
					// if any column will be less than MAXVALUE, so change < to <= of the previous prefix of columns
					fmt.Fprintf(&buf, "((%s) <= (%s))", strings.Join(partStrs[:j+1], ","), strings.Join(defs[i].LessThan[:j+1], ","))
					maxValueFound = true
					break
				}
			}
			if !maxValueFound {
				fmt.Fprintf(&buf, "((%s) < (%s))", strings.Join(partStrs, ","), strings.Join(defs[i].LessThan, ","))
			}
		}

		expr, err := parseSimpleExprWithNames(p, ctx, buf.String(), schema, names)
		if err != nil {
			// If it got an error here, ddl may hang forever, so this error log is important.
			logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", buf.String()), zap.Error(err))
			return nil, errors.Trace(err)
		}
		locateExprs = append(locateExprs, expr)
		buf.Reset()
	}
	return locateExprs, nil
}

func getColumnsOffset(cols, columns []*expression.Column) []int {
	colsOffset := make([]int, len(cols))
	for i, col := range columns {
		if idx := findIdxByColUniqueID(cols, col); idx >= 0 {
			colsOffset[idx] = i
		}
	}
	return colsOffset
}

func findIdxByColUniqueID(cols []*expression.Column, col *expression.Column) int {
	for idx, c := range cols {
		if c.UniqueID == col.UniqueID {
			return idx
		}
	}
	return -1
}

func extractPartitionExprColumns(ctx expression.BuildContext, expr string, partCols []ast.CIStr, columns []*expression.Column, names types.NameSlice) (expression.Expression, []*expression.Column, []int, error) {
	var cols []*expression.Column
	var partExpr expression.Expression
	if len(partCols) == 0 {
		if expr == "" {
			return nil, nil, nil, errors.New("expression should not be an empty string")
		}
		schema := expression.NewSchema(columns...)
		expr, err := expression.ParseSimpleExpr(ctx, expr, expression.WithInputSchemaAndNames(schema, names, nil))
		if err != nil {
			return nil, nil, nil, err
		}
		cols = expression.ExtractColumns(expr)
		partExpr = expr
	} else {
		for _, col := range partCols {
			idx := expression.FindFieldNameIdxByColName(names, col.L)
			if idx < 0 {
				panic("should never happen")
			}
			cols = append(cols, columns[idx])
		}
	}
	offset := getColumnsOffset(cols, columns)
	deDupCols := make([]*expression.Column, 0, len(cols))
	for _, col := range cols {
		if findIdxByColUniqueID(deDupCols, col) < 0 {
			c := col.Clone().(*expression.Column)
			deDupCols = append(deDupCols, c)
		}
	}
	return partExpr, deDupCols, offset, nil
}

func generateListPartitionExpr(ctx expression.BuildContext, tblInfo *model.TableInfo, expr string, partCols []ast.CIStr,
	defs []model.PartitionDefinition, columns []*expression.Column, names types.NameSlice) (*PartitionExpr, error) {
	// The caller should assure partition info is not nil.
	partExpr, exprCols, offset, err := extractPartitionExprColumns(ctx, expr, partCols, columns, names)
	if err != nil {
		return nil, err
	}
	listPrune := &ForListPruning{}
	if len(partCols) == 0 {
		err = listPrune.buildListPruner(ctx, expr, defs, exprCols, columns, names)
	} else {
		err = listPrune.buildListColumnsPruner(ctx, tblInfo, partCols, defs, columns, names)
	}
	if err != nil {
		return nil, err
	}
	ret := &PartitionExpr{
		ForListPruning: listPrune,
		ColumnOffset:   offset,
		Expr:           partExpr,
	}
	return ret, nil
}

// Clone a copy of ForListPruning
func (lp *ForListPruning) Clone() *ForListPruning {
	ret := *lp
	if ret.LocateExpr != nil {
		ret.LocateExpr = lp.LocateExpr.Clone()
	}
	if ret.PruneExpr != nil {
		ret.PruneExpr = lp.PruneExpr.Clone()
	}
	ret.PruneExprCols = make([]*expression.Column, 0, len(lp.PruneExprCols))
	for i := range lp.PruneExprCols {
		c := lp.PruneExprCols[i].Clone().(*expression.Column)
		ret.PruneExprCols = append(ret.PruneExprCols, c)
	}
	ret.ColPrunes = make([]*ForListColumnPruning, 0, len(lp.ColPrunes))
	for i := range lp.ColPrunes {
		l := *lp.ColPrunes[i]
		l.ExprCol = l.ExprCol.Clone().(*expression.Column)
		ret.ColPrunes = append(ret.ColPrunes, &l)
	}
	return &ret
}

func (lp *ForListPruning) buildListPruner(ctx expression.BuildContext, exprStr string, defs []model.PartitionDefinition, exprCols []*expression.Column,
	columns []*expression.Column, names types.NameSlice) error {
	schema := expression.NewSchema(columns...)
	p := parser.New()
	expr, err := parseSimpleExprWithNames(p, ctx, exprStr, schema, names)
	if err != nil {
		// If it got an error here, ddl may hang forever, so this error log is important.
		logutil.BgLogger().Error("wrong table partition expression", zap.String("expression", exprStr), zap.Error(err))
		return errors.Trace(err)
	}
	// Since need to change the column index of the expression, clone the expression first.
	lp.LocateExpr = expr.Clone()
	lp.PruneExprCols = exprCols
	lp.PruneExpr = expr.Clone()
	cols := expression.ExtractColumns(lp.PruneExpr)
	for _, c := range cols {
		idx := findIdxByColUniqueID(exprCols, c)
		if idx < 0 {
			return table.ErrUnknownColumn.GenWithStackByArgs(c.OrigName)
		}
		c.Index = idx
	}
	err = lp.buildListPartitionValueMap(ctx, defs, schema, names, p)
	if err != nil {
		return err
	}
	return nil
}

func (lp *ForListPruning) buildListColumnsPruner(ctx expression.BuildContext,
	tblInfo *model.TableInfo, partCols []ast.CIStr, defs []model.PartitionDefinition,
	columns []*expression.Column, names types.NameSlice) error {
	schema := expression.NewSchema(columns...)
	p := parser.New()
	colPrunes := make([]*ForListColumnPruning, 0, len(partCols))
	lp.defaultPartitionIdx = -1
	for colIdx := range partCols {
		colInfo := model.FindColumnInfo(tblInfo.Columns, partCols[colIdx].L)
		if colInfo == nil {
			return table.ErrUnknownColumn.GenWithStackByArgs(partCols[colIdx].L)
		}
		idx := expression.FindFieldNameIdxByColName(names, partCols[colIdx].L)
		if idx < 0 {
			return table.ErrUnknownColumn.GenWithStackByArgs(partCols[colIdx].L)
		}
		colPrune := &ForListColumnPruning{
			ctx:      ctx,
			tblInfo:  tblInfo,
			schema:   schema,
			names:    names,
			colIdx:   colIdx,
			ExprCol:  columns[idx],
			valueTp:  &colInfo.FieldType,
			valueMap: make(map[string]ListPartitionLocation),
			sorted:   btree.NewG[*btreeListColumnItem](btreeDegree, lessBtreeListColumnItem),
		}
		err := colPrune.buildPartitionValueMapAndSorted(p, defs)
		if err != nil {
			return err
		}
		if colPrune.defaultPartID > 0 {
			for i := range defs {
				if defs[i].ID == colPrune.defaultPartID {
					if lp.defaultPartitionIdx >= 0 && i != lp.defaultPartitionIdx {
						// Should be same for all columns, i.e. should never happen!
						return table.ErrUnknownPartition
					}
					lp.defaultPartitionIdx = i
				}
			}
		}
		colPrunes = append(colPrunes, colPrune)
	}
	lp.ColPrunes = colPrunes
	return nil
}

// buildListPartitionValueMap builds list partition value map.
// The map is column value -> partition index.
// colIdx is the column index in the list columns.
func (lp *ForListPruning) buildListPartitionValueMap(ctx expression.BuildContext, defs []model.PartitionDefinition,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) error {
	lp.valueToPartitionIdxBTree = btree.NewG[*btreeListItem](btreeDegree, func(a, b *btreeListItem) bool { return a.key < b.key })
	lp.nullPartitionIdx = -1
	lp.defaultPartitionIdx = -1
	for partitionIdx, def := range defs {
		for _, vs := range def.InValues {
			if strings.EqualFold(vs[0], "DEFAULT") {
				lp.defaultPartitionIdx = partitionIdx
				continue
			}
			expr, err := parseSimpleExprWithNames(p, ctx, vs[0], schema, names)
			if err != nil {
				return errors.Trace(err)
			}
			v, isNull, err := expr.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
			if err != nil {
				return errors.Trace(err)
			}
			if isNull {
				lp.nullPartitionIdx = partitionIdx
				continue
			}
			if mysql.HasUnsignedFlag(lp.PruneExpr.GetType(ctx.GetEvalCtx()).GetFlag()) {
				lp.valueToPartitionIdxBTree.ReplaceOrInsert(&btreeListItem{uint64(v), partitionIdx})
			} else {
				lp.valueToPartitionIdxBTree.ReplaceOrInsert(&btreeListItem{codec.EncodeIntToCmpUint(v), partitionIdx})
			}
		}
	}
	return nil
}

// LocatePartition locates partition by the column value
func (lp *ForListPruning) LocatePartition(ctx exprctx.EvalContext, value int64, isNull bool) int {
	if isNull {
		if lp.nullPartitionIdx >= 0 {
			return lp.nullPartitionIdx
		}
		return lp.defaultPartitionIdx
	}
	var key uint64
	if mysql.HasUnsignedFlag(lp.PruneExpr.GetType(ctx).GetFlag()) {
		key = uint64(value)
	} else {
		key = codec.EncodeIntToCmpUint(value)
	}
	partitionIdx, ok := lp.valueToPartitionIdxBTree.Get(&btreeListItem{key: key})
	if !ok {
		return lp.defaultPartitionIdx
	}
	return partitionIdx.partitionIdx
}

// LocatePartitionByRange locates partition by the range
// Only could process `column op value` right now.
func (lp *ForListPruning) LocatePartitionByRange(ctx exprctx.EvalContext, r *ranger.Range) (idxs map[int]struct{}, err error) {
	idxs = make(map[int]struct{})
	lowVal, highVal := r.LowVal[0], r.HighVal[0]
	if r.LowVal[0].Kind() == types.KindMinNotNull {
		lowVal = types.GetMinValue(lp.PruneExpr.GetType(ctx))
	}

	if r.HighVal[0].Kind() == types.KindMaxValue {
		highVal = types.GetMaxValue(lp.PruneExpr.GetType(ctx))
	}

	highInt64, isNull, err := lp.PruneExpr.EvalInt(ctx, chunk.MutRowFromDatums([]types.Datum{highVal}).ToRow())
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, errors.Errorf("Internal error, `r.HighVal` cannot be null")
	}

	lowInt64, isNull, err := lp.PruneExpr.EvalInt(ctx, chunk.MutRowFromDatums([]types.Datum{lowVal}).ToRow())
	if err != nil {
		return nil, err
	}
	if isNull {
		// If low value is null, add `lp.nullPartitionIdx` into idxs map.
		if !r.LowExclude && lp.nullPartitionIdx != -1 {
			idxs[lp.nullPartitionIdx] = struct{}{}
		} else {
			dt := types.GetMinValue(lp.PruneExpr.GetType(ctx))
			lowInt64 = dt.GetInt64()
		}
	}

	var lowKey, highKey uint64

	if mysql.HasUnsignedFlag(lp.PruneExpr.GetType(ctx).GetFlag()) {
		lowKey, highKey = uint64(lowInt64), uint64(highInt64)
	} else {
		lowKey, highKey = codec.EncodeIntToCmpUint(lowInt64), codec.EncodeIntToCmpUint(highInt64)
	}

	lp.valueToPartitionIdxBTree.AscendRange(&btreeListItem{key: lowKey}, &btreeListItem{key: highKey}, func(item *btreeListItem) bool {
		if item.key == lowKey && r.LowExclude {
			return true
		}
		idxs[item.partitionIdx] = struct{}{}
		return true
	})

	if item, ok := lp.valueToPartitionIdxBTree.Get(&btreeListItem{key: highKey}); ok && !r.HighExclude {
		idxs[item.partitionIdx] = struct{}{}
	}

	idxs[lp.defaultPartitionIdx] = struct{}{}
	return idxs, nil
}

func (lp *ForListPruning) locateListPartitionByRow(ctx expression.EvalContext, r []types.Datum) (int, error) {
	value, isNull, err := lp.LocateExpr.EvalInt(ctx, chunk.MutRowFromDatums(r).ToRow())
	if err != nil {
		return -1, errors.Trace(err)
	}
	idx := lp.LocatePartition(ctx, value, isNull)
	if idx >= 0 {
		return idx, nil
	}
	if isNull {
		return -1, table.ErrNoPartitionForGivenValue.GenWithStackByArgs("NULL")
	}
	var valueMsg string
	if mysql.HasUnsignedFlag(lp.LocateExpr.GetType(ctx).GetFlag()) {
		// Handle unsigned value
		valueMsg = fmt.Sprintf("%d", uint64(value))
	} else {
		valueMsg = fmt.Sprintf("%d", value)
	}
	return -1, table.ErrNoPartitionForGivenValue.GenWithStackByArgs(valueMsg)
}

func (lp *ForListPruning) locateListColumnsPartitionByRow(tc types.Context, ec errctx.Context, r []types.Datum) (int, error) {
	helper := NewListPartitionLocationHelper()
	for _, colPrune := range lp.ColPrunes {
		location, err := colPrune.LocatePartition(tc, ec, r[colPrune.ExprCol.Index])
		if err != nil {
			return -1, errors.Trace(err)
		}
		if !helper.Intersect(location) {
			break
		}
	}
	location := helper.GetLocation()
	if location.IsEmpty() {
		if lp.defaultPartitionIdx >= 0 {
			return lp.defaultPartitionIdx, nil
		}
		return -1, table.ErrNoPartitionForGivenValue.GenWithStackByArgs("from column_list")
	}
	return location[0].PartIdx, nil
}

// GetDefaultIdx return the Default partitions index.
func (lp *ForListPruning) GetDefaultIdx() int {
	return lp.defaultPartitionIdx
}

// buildPartitionValueMapAndSorted builds list columns partition value map for the specified column.
// It also builds list columns partition value btree for the specified column.
// colIdx is the specified column index in the list columns.
func (lp *ForListColumnPruning) buildPartitionValueMapAndSorted(p *parser.Parser,
	defs []model.PartitionDefinition) error {
	l := len(lp.valueMap)
	if l != 0 {
		return nil
	}

	return lp.buildListPartitionValueMapAndSorted(p, defs)
}

// HasDefault return true if the partition has the DEFAULT value
func (lp *ForListColumnPruning) HasDefault() bool {
	return lp.defaultPartID > 0
}

// RebuildPartitionValueMapAndSorted rebuilds list columns partition value map for the specified column.
func (lp *ForListColumnPruning) RebuildPartitionValueMapAndSorted(p *parser.Parser,
	defs []model.PartitionDefinition) error {
	lp.valueMap = make(map[string]ListPartitionLocation, len(lp.valueMap))
	lp.sorted.Clear(false)
	return lp.buildListPartitionValueMapAndSorted(p, defs)
}

func (lp *ForListColumnPruning) buildListPartitionValueMapAndSorted(p *parser.Parser, defs []model.PartitionDefinition) error {
DEFS:
	for partitionIdx, def := range defs {
		for groupIdx, vs := range def.InValues {
			if len(vs) == 1 && vs[0] == "DEFAULT" {
				lp.defaultPartID = def.ID
				continue DEFS
			}
			keyBytes, err := lp.genConstExprKey(lp.ctx, vs[lp.colIdx], lp.schema, lp.names, p)
			if err != nil {
				return errors.Trace(err)
			}
			key := string(keyBytes)
			location, ok := lp.valueMap[key]
			if ok {
				idx := location.findByPartitionIdx(partitionIdx)
				if idx != -1 {
					location[idx].GroupIdxs = append(location[idx].GroupIdxs, groupIdx)
					continue
				}
			}
			location = append(location, ListPartitionGroup{
				PartIdx:   partitionIdx,
				GroupIdxs: []int{groupIdx},
			})
			lp.valueMap[key] = location
			lp.sorted.ReplaceOrInsert(newBtreeListColumnItem(key, location))
		}
	}
	return nil
}

func (lp *ForListColumnPruning) genConstExprKey(ctx expression.BuildContext, exprStr string,
	schema *expression.Schema, names types.NameSlice, p *parser.Parser) ([]byte, error) {
	expr, err := parseSimpleExprWithNames(p, ctx, exprStr, schema, names)
	if err != nil {
		return nil, errors.Trace(err)
	}
	v, err := expr.Eval(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	evalCtx := ctx.GetEvalCtx()
	tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
	key, err := lp.genKey(tc, ec, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return key, nil
}

func (lp *ForListColumnPruning) genKey(tc types.Context, ec errctx.Context, v types.Datum) ([]byte, error) {
	v, err := v.ConvertTo(tc, lp.valueTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	valByte, err := codec.EncodeKey(tc.Location(), nil, v)
	err = ec.HandleError(err)
	return valByte, err
}

// LocatePartition locates partition by the column value
func (lp *ForListColumnPruning) LocatePartition(tc types.Context, ec errctx.Context, v types.Datum) (ListPartitionLocation, error) {
	key, err := lp.genKey(tc, ec, v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	location, ok := lp.valueMap[string(key)]
	if !ok {
		return nil, nil
	}
	return location, nil
}

// LocateRanges locates partition ranges by the column range
func (lp *ForListColumnPruning) LocateRanges(tc types.Context, ec errctx.Context, r *ranger.Range, defaultPartIdx int) ([]ListPartitionLocation, error) {
	var lowKey, highKey []byte
	var err error
	lowVal := r.LowVal[0]
	if r.LowVal[0].Kind() == types.KindMinNotNull {
		lowVal = types.GetMinValue(lp.ExprCol.GetType(lp.ctx.GetEvalCtx()))
	}
	highVal := r.HighVal[0]
	if r.HighVal[0].Kind() == types.KindMaxValue {
		highVal = types.GetMaxValue(lp.ExprCol.GetType(lp.ctx.GetEvalCtx()))
	}

	// For string type, values returned by GetMinValue and GetMaxValue are already encoded,
	// so it's unnecessary to invoke genKey to encode them.
	if lp.ExprCol.GetType(lp.ctx.GetEvalCtx()).EvalType() == types.ETString && r.LowVal[0].Kind() == types.KindMinNotNull {
		lowKey = (&lowVal).GetBytes()
	} else {
		lowKey, err = lp.genKey(tc, ec, lowVal)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if lp.ExprCol.GetType(lp.ctx.GetEvalCtx()).EvalType() == types.ETString && r.HighVal[0].Kind() == types.KindMaxValue {
		highKey = (&highVal).GetBytes()
	} else {
		highKey, err = lp.genKey(tc, ec, highVal)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if r.LowExclude {
		lowKey = kv.Key(lowKey).PrefixNext()
	}
	if !r.HighExclude {
		highKey = kv.Key(highKey).PrefixNext()
	}

	locations := make([]ListPartitionLocation, 0, lp.sorted.Len())
	lp.sorted.AscendRange(newBtreeListColumnSearchItem(string(hack.String(lowKey))), newBtreeListColumnSearchItem(string(hack.String(highKey))), func(item *btreeListColumnItem) bool {
		locations = append(locations, item.location)
		return true
	})
	if lp.HasDefault() {
		// Add the default partition since there may be a gap
		// between the conditions range and the LIST COLUMNS values
		locations = append(locations, ListPartitionLocation{
			ListPartitionGroup{
				PartIdx:   defaultPartIdx,
				GroupIdxs: []int{-1}, // Special group!
			},
		})
	}
	return locations, nil
}
