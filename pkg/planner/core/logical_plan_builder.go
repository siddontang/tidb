// Copyright 2016 PingCAP, Inc.
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

package core

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	// ErrExprInSelect  is in select fields for the error of ErrFieldNotInGroupBy
	ErrExprInSelect = "SELECT list"
	// ErrExprInOrderBy  is in order by items for the error of ErrFieldNotInGroupBy
	ErrExprInOrderBy = "ORDER BY"
)

// aggOrderByResolver is currently resolving expressions of order by clause
// in aggregate function GROUP_CONCAT.
type aggOrderByResolver struct {
	ctx       base.PlanContext
	err       error
	args      []ast.ExprNode
	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (a *aggOrderByResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	a.exprDepth++
	if n, ok := inNode.(*driver.ParamMarkerExpr); ok {
		if a.exprDepth == 1 {
			_, isNull, isExpectedType := getUintFromNode(a.ctx.GetExprCtx(), n, false)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
	}
	return inNode, false
}

func (a *aggOrderByResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	if v, ok := inNode.(*ast.PositionExpr); ok {
		pos, isNull, err := expression.PosFromPositionExpr(a.ctx.GetExprCtx(), v)
		if err != nil {
			a.err = err
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(a.args) {
			errPos := strconv.Itoa(pos)
			if v.P != nil {
				errPos = "?"
			}
			a.err = plannererrors.ErrUnknownColumn.FastGenByArgs(errPos, "order clause")
			return inNode, false
		}
		ret := a.args[pos-1]
		return ret, true
	}
	return inNode, true
}

func (b *PlanBuilder) buildExpand(p base.LogicalPlan, gbyItems []expression.Expression) (base.LogicalPlan, []expression.Expression, error) {
	ectx := p.SCtx().GetExprCtx().GetEvalCtx()
	b.optFlag |= rule.FlagResolveExpand

	// Rollup syntax require expand OP to do the data expansion, different data replica supply the different grouping layout.
	distinctGbyExprs, gbyExprsRefPos := expression.DeduplicateGbyExpression(gbyItems)
	// build another projection below.
	proj := logicalop.LogicalProjection{Exprs: make([]expression.Expression, 0, p.Schema().Len()+len(distinctGbyExprs))}.Init(b.ctx, b.getSelectOffset())
	// project: child's output and distinct GbyExprs in advance. (make every group-by item to be a column)
	projSchema := p.Schema().Clone()
	names := p.OutputNames()
	for _, col := range projSchema.Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	distinctGbyColNames := make(types.NameSlice, 0, len(distinctGbyExprs))
	distinctGbyCols := make([]*expression.Column, 0, len(distinctGbyExprs))
	for _, expr := range distinctGbyExprs {
		// distinct group expr has been resolved in resolveGby.
		proj.Exprs = append(proj.Exprs, expr)

		// add the newly appended names.
		var name *types.FieldName
		if c, ok := expr.(*expression.Column); ok {
			name = buildExpandFieldName(ectx, c, names[p.Schema().ColumnIndex(c)], "")
		} else {
			name = buildExpandFieldName(ectx, expr, nil, "")
		}
		names = append(names, name)
		distinctGbyColNames = append(distinctGbyColNames, name)

		// since we will change the nullability of source col, proj it with a new col id.
		col := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			// clone it rather than using it directly,
			RetType: expr.GetType(b.ctx.GetExprCtx().GetEvalCtx()).Clone(),
		}

		projSchema.Append(col)
		distinctGbyCols = append(distinctGbyCols, col)
	}
	proj.SetSchema(projSchema)
	proj.SetChildren(p)
	proj.Proj4Expand = true
	newGbyItems := expression.RestoreGbyExpression(distinctGbyCols, gbyExprsRefPos)

	// build expand.
	rollupGroupingSets := expression.RollupGroupingSets(newGbyItems)
	// eg: <a,b,c> with rollup => {},{a},{a,b},{a,b,c}
	// for every grouping set above, we should individually set those not-needed grouping-set col as null value.
	// eg: let's say base schema is <a,b,c,d>, d is unrelated col, keep it real in every grouping set projection.
	// 		for grouping set {a,b,c}, project it as: [a,    b,    c,    d,   gid]
	//      for grouping set {a,b},   project it as: [a,    b,    null, d,   gid]
	//      for grouping set {a},     project it as: [a,    null, null, d,   gid]
	// 		for grouping set {},      project it as: [null, null, null, d,   gid]
	expandSchema := proj.Schema().Clone()
	expression.AdjustNullabilityFromGroupingSets(rollupGroupingSets, expandSchema)
	expand := logicalop.LogicalExpand{
		RollupGroupingSets:  rollupGroupingSets,
		DistinctGroupByCol:  distinctGbyCols,
		DistinctGbyColNames: distinctGbyColNames,
		// for resolving grouping function args.
		DistinctGbyExprs: distinctGbyExprs,

		// fill the gen col names when building level projections.
	}.Init(b.ctx, b.getSelectOffset())

	// if we want to use bitAnd for the quick computation of grouping function, then the maximum capacity of num of grouping is about 64.
	expand.GroupingMode = tipb.GroupingMode_ModeBitAnd
	if len(expand.RollupGroupingSets) > 64 {
		expand.GroupingMode = tipb.GroupingMode_ModeNumericSet
	}

	expand.DistinctSize, expand.RollupGroupingIDs, expand.RollupID2GIDS = expand.RollupGroupingSets.DistinctSize()
	hasDuplicateGroupingSet := len(expand.RollupGroupingSets) != expand.DistinctSize
	// append the generated column for logical Expand.
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlag(mysql.UnsignedFlag | mysql.NotNullFlag)
	gid := &expression.Column{
		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  tp,
		OrigName: "gid",
	}
	expand.GID = gid
	expandSchema.Append(gid)
	expand.ExtraGroupingColNames = append(expand.ExtraGroupingColNames, gid.OrigName)
	names = append(names, buildExpandFieldName(ectx, gid, nil, "gid_"))
	expand.GIDName = names[len(names)-1]
	if hasDuplicateGroupingSet {
		// the last two col of the schema should be gid & gpos
		gpos := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  tp.Clone(),
			OrigName: "gpos",
		}
		expand.GPos = gpos
		expandSchema.Append(gpos)
		expand.ExtraGroupingColNames = append(expand.ExtraGroupingColNames, gpos.OrigName)
		names = append(names, buildExpandFieldName(ectx, gpos, nil, "gpos_"))
		expand.GPosName = names[len(names)-1]
	}
	expand.SetChildren(proj)
	expand.SetSchema(expandSchema)
	expand.SetOutputNames(names)

	// register current rollup Expand operator in current select block.
	b.currentBlockExpand = expand

	// defer generating level-projection as last logical optimization rule.
	return expand, newGbyItems, nil
}

func (b *PlanBuilder) buildAggregation(ctx context.Context, p base.LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression,
	correlatedAggMap map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, map[int]int, error) {
	b.optFlag |= rule.FlagBuildKeyInfo
	b.optFlag |= rule.FlagPushDownAgg
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag |= rule.FlagMaxMinEliminate
	b.optFlag |= rule.FlagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag |= rule.FlagPredicatePushDown
	b.optFlag |= rule.FlagEliminateAgg
	b.optFlag |= rule.FlagEliminateProjection

	if b.ctx.GetSessionVars().EnableSkewDistinctAgg {
		b.optFlag |= rule.FlagSkewDistinctAgg
	}
	// flag it if cte contain aggregation
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containRecursiveForbiddenOperator = true
	}
	var rollupExpand *logicalop.LogicalExpand
	if expand, ok := p.(*logicalop.LogicalExpand); ok {
		rollupExpand = expand
	}

	plan4Agg := logicalop.LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx, b.getSelectOffset())
	if hintinfo := b.TableHints(); hintinfo != nil {
		plan4Agg.PreferAggType = hintinfo.PreferAggType
		plan4Agg.PreferAggToCop = hintinfo.PreferAggToCop
	}
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(aggFuncList)+p.Schema().Len())
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	allAggsFirstRow := true
	for i, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, nil, err
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			return nil, nil, err
		}
		if newFunc.Name != ast.AggFuncFirstRow {
			allAggsFirstRow = false
		}
		if aggFunc.Order != nil {
			trueArgs := aggFunc.Args[:len(aggFunc.Args)-1] // the last argument is SEPARATOR, remote it.
			resolver := &aggOrderByResolver{
				ctx:  b.ctx,
				args: trueArgs,
			}
			for _, byItem := range aggFunc.Order.Items {
				resolver.exprDepth = 0
				resolver.err = nil
				retExpr, _ := byItem.Expr.Accept(resolver)
				if resolver.err != nil {
					return nil, nil, errors.Trace(resolver.err)
				}
				newByItem, np, err := b.rewrite(ctx, retExpr.(ast.ExprNode), p, nil, true)
				if err != nil {
					return nil, nil, err
				}
				p = np
				newFunc.OrderByItems = append(newFunc.OrderByItems, &util.ByItems{Expr: newByItem, Desc: byItem.Desc})
			}
		}
		// combine identical aggregate functions
		combined := false
		for j := range i {
			oldFunc := plan4Agg.AggFuncs[aggIndexMap[j]]
			if oldFunc.Equal(b.ctx.GetExprCtx().GetEvalCtx(), newFunc) {
				aggIndexMap[i] = aggIndexMap[j]
				combined = true
				if _, ok := correlatedAggMap[aggFunc]; ok {
					if _, ok = b.correlatedAggMapper[aggFuncList[j]]; !ok {
						b.correlatedAggMapper[aggFuncList[j]] = &expression.CorrelatedColumn{
							Column: *schema4Agg.Columns[aggIndexMap[j]],
							Data:   new(types.Datum),
						}
					}
					b.correlatedAggMapper[aggFunc] = b.correlatedAggMapper[aggFuncList[j]]
				}
				break
			}
		}
		// create new columns for aggregate functions which show up first
		if !combined {
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			column := expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  newFunc.RetTp,
			}
			schema4Agg.Append(&column)
			names = append(names, types.EmptyName)
			if _, ok := correlatedAggMap[aggFunc]; ok {
				b.correlatedAggMapper[aggFunc] = &expression.CorrelatedColumn{
					Column: column,
					Data:   new(types.Datum),
				}
			}
		}
	}
	for i, col := range p.Schema().Columns {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
		names = append(names, p.OutputNames()[i])
	}
	var (
		join            *logicalop.LogicalJoin
		isJoin          bool
		isSelectionJoin bool
	)
	join, isJoin = p.(*logicalop.LogicalJoin)
	selection, isSelection := p.(*logicalop.LogicalSelection)
	if isSelection {
		join, isSelectionJoin = selection.Children()[0].(*logicalop.LogicalJoin)
	}
	if (isJoin && join.FullSchema != nil) || (isSelectionJoin && join.FullSchema != nil) {
		for i, col := range join.FullSchema.Columns {
			if p.Schema().Contains(col) {
				continue
			}
			newFunc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
			if err != nil {
				return nil, nil, err
			}
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			newCol, _ := col.Clone().(*expression.Column)
			newCol.RetType = newFunc.RetTp
			schema4Agg.Append(newCol)
			names = append(names, join.FullNames[i])
		}
	}
	hasGroupBy := len(gbyItems) > 0
	for i, aggFunc := range plan4Agg.AggFuncs {
		err := aggFunc.UpdateNotNullFlag4RetType(hasGroupBy, allAggsFirstRow)
		if err != nil {
			return nil, nil, err
		}
		schema4Agg.Columns[i].RetType = aggFunc.RetTp
	}
	plan4Agg.SetOutputNames(names)
	plan4Agg.SetChildren(p)
	if rollupExpand != nil {
		// append gid and gpos as the group keys if any.
		plan4Agg.GroupByItems = append(gbyItems, rollupExpand.GID)
		if rollupExpand.GPos != nil {
			plan4Agg.GroupByItems = append(plan4Agg.GroupByItems, rollupExpand.GPos)
		}
	} else {
		plan4Agg.GroupByItems = gbyItems
	}
	plan4Agg.SetSchema(schema4Agg)
	return plan4Agg, aggIndexMap, nil
}

func (b *PlanBuilder) buildTableRefs(ctx context.Context, from *ast.TableRefsClause) (p base.LogicalPlan, err error) {
	if from == nil {
		p = b.buildTableDual()
		return
	}
	defer func() {
		// After build the resultSetNode, need to reset it so that it can be referenced by outer level.
		for _, cte := range b.outerCTEs {
			cte.recursiveRef = false
		}
	}()
	return b.buildResultSetNode(ctx, from.TableRefs, false)
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode, isCTE bool) (p base.LogicalPlan, err error) {
	//If it is building the CTE queries, we will mark them.
	b.isCTE = isCTE
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.TableSource:
		var isTableName bool
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			ci := b.prepareCTECheckForSubQuery()
			defer resetCTECheckForSubQuery(ci)
			b.optFlag = b.optFlag | rule.FlagConstantPropagation
			p, err = b.buildSelect(ctx, v)
		case *ast.SetOprStmt:
			ci := b.prepareCTECheckForSubQuery()
			defer resetCTECheckForSubQuery(ci)
			p, err = b.buildSetOpr(ctx, v)
		case *ast.TableName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
			isTableName = true
		default:
			err = plannererrors.ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, err
		}

		for _, name := range p.OutputNames() {
			if name.Hidden {
				continue
			}
			if x.AsName.L != "" {
				name.TblName = x.AsName
			}
		}
		// `TableName` is not a select block, so we do not need to handle it.
		var plannerSelectBlockAsName []ast.HintTable
		if p := b.ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
			plannerSelectBlockAsName = *p
		}
		if len(plannerSelectBlockAsName) > 0 && !isTableName {
			plannerSelectBlockAsName[p.QueryBlockOffset()] = ast.HintTable{DBName: p.OutputNames()[0].DBName, TableName: p.OutputNames()[0].TblName}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, name := range p.OutputNames() {
			colName := name.ColName.O
			if _, ok := dupNames[colName]; ok {
				return nil, plannererrors.ErrDupFieldName.GenWithStackByArgs(colName)
			}
			dupNames[colName] = struct{}{}
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, x)
	default:
		return nil, plannererrors.ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

func setPreferredStoreType(ds *logicalop.DataSource, hintInfo *h.PlanHints) {
	if hintInfo == nil {
		return
	}

	var alias *h.HintedTable
	if len(ds.TableAsName.L) != 0 {
		alias = &h.HintedTable{DBName: ds.DBName, TblName: *ds.TableAsName, SelectOffset: ds.QueryBlockOffset()}
	} else {
		alias = &h.HintedTable{DBName: ds.DBName, TblName: ds.TableInfo.Name, SelectOffset: ds.QueryBlockOffset()}
	}
	if hintTbl := hintInfo.IfPreferTiKV(alias); hintTbl != nil {
		for _, path := range ds.AllPossibleAccessPaths {
			if path.StoreType == kv.TiKV {
				ds.PreferStoreType |= h.PreferTiKV
				ds.PreferPartitions[h.PreferTiKV] = hintTbl.Partitions
				break
			}
		}
		if ds.PreferStoreType&h.PreferTiKV == 0 {
			errMsg := fmt.Sprintf("No available path for table %s.%s with the store type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the table replica and variable value of tidb_isolation_read_engines(%v)",
				ds.DBName.O, ds.Table.Meta().Name.O, kv.TiKV.Name(), ds.SCtx().GetSessionVars().GetIsolationReadEngines())
			ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		} else {
			ds.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have set a hint to read table `" + hintTbl.TblName.O + "` from TiKV.")
		}
	}
	if hintTbl := hintInfo.IfPreferTiFlash(alias); hintTbl != nil {
		// `ds.PreferStoreType != 0`, which means there's a hint hit the both TiKV value and TiFlash value for table.
		// We can't support read a table from two different storages, even partition table.
		if ds.PreferStoreType != 0 {
			ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
				fmt.Sprintf("Storage hints are conflict, you can only specify one storage type of table %s.%s",
					alias.DBName.L, alias.TblName.L))
			ds.PreferStoreType = 0
			return
		}
		for _, path := range ds.AllPossibleAccessPaths {
			if path.StoreType == kv.TiFlash {
				ds.PreferStoreType |= h.PreferTiFlash
				ds.PreferPartitions[h.PreferTiFlash] = hintTbl.Partitions
				break
			}
		}
		if ds.PreferStoreType&h.PreferTiFlash == 0 {
			errMsg := fmt.Sprintf("No available path for table %s.%s with the store type %s of the hint /*+ read_from_storage */, "+
				"please check the status of the table replica and variable value of tidb_isolation_read_engines(%v)",
				ds.DBName.O, ds.Table.Meta().Name.O, kv.TiFlash.Name(), ds.SCtx().GetSessionVars().GetIsolationReadEngines())
			ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (base.LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left, false)
	}

	b.optFlag = b.optFlag | rule.FlagPredicatePushDown
	// Add join reorder flag regardless of inner join or outer join.
	b.optFlag = b.optFlag | rule.FlagJoinReOrder
	b.optFlag |= rule.FlagPredicateSimplification
	b.optFlag |= rule.FlagEmptySelectionEliminator

	leftPlan, err := b.buildResultSetNode(ctx, joinNode.Left, false)
	if err != nil {
		return nil, err
	}

	rightPlan, err := b.buildResultSetNode(ctx, joinNode.Right, false)
	if err != nil {
		return nil, err
	}

	// The recursive part in CTE must not be on the right side of a LEFT JOIN.
	if lc, ok := rightPlan.(*logicalop.LogicalCTETable); ok && joinNode.Tp == ast.LeftJoin {
		return nil, plannererrors.ErrCTERecursiveForbiddenJoinOrder.GenWithStackByArgs(lc.Name)
	}

	handleMap1 := b.handleHelper.popMap()
	handleMap2 := b.handleHelper.popMap()
	b.handleHelper.mergeAndPush(handleMap1, handleMap2)

	joinPlan := logicalop.LogicalJoin{StraightJoin: joinNode.StraightJoin || b.inStraightJoin}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.SetOutputNames(make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len()))
	copy(joinPlan.OutputNames(), leftPlan.OutputNames())
	copy(joinPlan.OutputNames()[leftPlan.Schema().Len():], rightPlan.OutputNames())

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | rule.FlagEliminateOuterJoin | rule.FlagOuterJoinToSemiJoin
		joinPlan.JoinType = base.LeftOuterJoin
		util.ResetNotNullFlag(joinPlan.Schema(), leftPlan.Schema().Len(), joinPlan.Schema().Len())
	case ast.RightJoin:
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | rule.FlagEliminateOuterJoin | rule.FlagOuterJoinToSemiJoin
		joinPlan.JoinType = base.RightOuterJoin
		util.ResetNotNullFlag(joinPlan.Schema(), 0, leftPlan.Schema().Len())
	default:
		joinPlan.JoinType = base.InnerJoin
	}

	// Merge sub-plan's FullSchema into this join plan.
	// Please read the comment of LogicalJoin.FullSchema for the details.
	var (
		lFullSchema, rFullSchema *expression.Schema
		lFullNames, rFullNames   types.NameSlice
	)
	if left, ok := leftPlan.(*logicalop.LogicalJoin); ok && left.FullSchema != nil {
		lFullSchema = left.FullSchema
		lFullNames = left.FullNames
	} else {
		lFullSchema = leftPlan.Schema()
		lFullNames = leftPlan.OutputNames()
	}
	if right, ok := rightPlan.(*logicalop.LogicalJoin); ok && right.FullSchema != nil {
		rFullSchema = right.FullSchema
		rFullNames = right.FullNames
	} else {
		rFullSchema = rightPlan.Schema()
		rFullNames = rightPlan.OutputNames()
	}
	if joinNode.Tp == ast.RightJoin {
		// Make sure lFullSchema means outer full schema and rFullSchema means inner full schema.
		lFullSchema, rFullSchema = rFullSchema, lFullSchema
		lFullNames, rFullNames = rFullNames, lFullNames
	}
	joinPlan.FullSchema = expression.MergeSchema(lFullSchema, rFullSchema)

	// Clear NotNull flag for the inner side schema if it's an outer join.
	if joinNode.Tp == ast.LeftJoin || joinNode.Tp == ast.RightJoin {
		util.ResetNotNullFlag(joinPlan.FullSchema, lFullSchema.Len(), joinPlan.FullSchema.Len())
	}

	// Merge sub-plan's FullNames into this join plan, similar to the FullSchema logic above.
	joinPlan.FullNames = make([]*types.FieldName, 0, len(lFullNames)+len(rFullNames))
	for _, lName := range lFullNames {
		name := *lName
		joinPlan.FullNames = append(joinPlan.FullNames, &name)
	}
	for _, rName := range rFullNames {
		name := *rName
		joinPlan.FullNames = append(joinPlan.FullNames, &name)
	}

	// Set preferred join algorithm if some join hints is specified by user.
	joinPlan.SetPreferredJoinTypeAndOrder(b.TableHints())

	// "NATURAL JOIN" doesn't have "ON" or "USING" conditions.
	//
	// The "NATURAL [LEFT] JOIN" of two tables is defined to be semantically
	// equivalent to an "INNER JOIN" or a "LEFT JOIN" with a "USING" clause
	// that names all columns that exist in both tables.
	//
	// See https://dev.mysql.com/doc/refman/5.7/en/join.html for more detail.
	if joinNode.NaturalJoin {
		err = b.buildNaturalJoin(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.Using != nil {
		err = b.buildUsingClause(joinPlan, leftPlan, rightPlan, joinNode)
		if err != nil {
			return nil, err
		}
	} else if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(ctx, joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, err
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		// Keep these expressions as a LogicalSelection upon the inner join, in order to apply
		// possible decorrelate optimizations. The ON clause is actually treated as a WHERE clause now.
		if joinPlan.JoinType == base.InnerJoin {
			sel := logicalop.LogicalSelection{Conditions: onCondition}.Init(b.ctx, b.getSelectOffset())
			sel.SetChildren(joinPlan)
			return sel, nil
		}
		joinPlan.AttachOnConds(onCondition)
	}
	return joinPlan, nil
}

// buildUsingClause eliminate the redundant columns and ordering columns based
// on the "USING" clause.
//
// According to the standard SQL, columns are ordered in the following way:
//  1. coalesced common columns of "leftPlan" and "rightPlan", in the order they
//     appears in "leftPlan".
//  2. the rest columns in "leftPlan", in the order they appears in "leftPlan".
//  3. the rest columns in "rightPlan", in the order they appears in "rightPlan".
func (b *PlanBuilder) buildUsingClause(p *logicalop.LogicalJoin, leftPlan, rightPlan base.LogicalPlan, join *ast.Join) error {
	filter := make(map[string]bool, len(join.Using))
	for _, col := range join.Using {
		filter[col.Name.L] = true
	}
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, filter)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.SetSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// buildNaturalJoin builds natural join output schema. It finds out all the common columns
// then using the same mechanism as buildUsingClause to eliminate redundant columns and build join conditions.
// According to standard SQL, producing this display order:
//
//	All the common columns
//	Every column in the first (left) table that is not a common column
//	Every column in the second (right) table that is not a common column
func (b *PlanBuilder) buildNaturalJoin(p *logicalop.LogicalJoin, leftPlan, rightPlan base.LogicalPlan, join *ast.Join) error {
	err := b.coalesceCommonColumns(p, leftPlan, rightPlan, join.Tp, nil)
	if err != nil {
		return err
	}
	// We do not need to coalesce columns for update and delete.
	if b.inUpdateStmt || b.inDeleteStmt {
		p.SetSchemaAndNames(expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema()),
			append(p.Children()[0].OutputNames(), p.Children()[1].OutputNames()...))
	}
	return nil
}

// coalesceCommonColumns is used by buildUsingClause and buildNaturalJoin. The filter is used by buildUsingClause.
func (b *PlanBuilder) coalesceCommonColumns(p *logicalop.LogicalJoin, leftPlan, rightPlan base.LogicalPlan, joinTp ast.JoinType, filter map[string]bool) error {
	lsc := leftPlan.Schema().Clone()
	rsc := rightPlan.Schema().Clone()
	if joinTp == ast.LeftJoin {
		util.ResetNotNullFlag(rsc, 0, rsc.Len())
	} else if joinTp == ast.RightJoin {
		util.ResetNotNullFlag(lsc, 0, lsc.Len())
	}
	lColumns, rColumns := lsc.Columns, rsc.Columns
	lNames, rNames := leftPlan.OutputNames().Shallow(), rightPlan.OutputNames().Shallow()
	if joinTp == ast.RightJoin {
		leftPlan, rightPlan = rightPlan, leftPlan
		lNames, rNames = rNames, lNames
		lColumns, rColumns = rsc.Columns, lsc.Columns
	}

	// Check using clause with ambiguous columns.
	if filter != nil {
		checkAmbiguous := func(names types.NameSlice) error {
			columnNameInFilter := set.StringSet{}
			for _, name := range names {
				if _, ok := filter[name.ColName.L]; !ok {
					continue
				}
				if columnNameInFilter.Exist(name.ColName.L) {
					return plannererrors.ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				columnNameInFilter.Insert(name.ColName.L)
			}
			return nil
		}
		err := checkAmbiguous(lNames)
		if err != nil {
			return err
		}
		err = checkAmbiguous(rNames)
		if err != nil {
			return err
		}
	} else {
		// Even with no using filter, we still should check the checkAmbiguous name before we try to find the common column from both side.
		// (t3 cross join t4) natural join t1
		//  t1 natural join (t3 cross join t4)
		// t3 and t4 may generate the same name column from cross join.
		// for every common column of natural join, the name from right or left should be exactly one.
		commonNames := make([]string, 0, len(lNames))
		lNameMap := make(map[string]int, len(lNames))
		rNameMap := make(map[string]int, len(rNames))
		for _, name := range lNames {
			// Natural join should ignore _tidb_rowid and _tidb_commit_ts
			if name.ColName.L == model.ExtraHandleName.L ||
				name.ColName.L == model.ExtraCommitTSName.L ||
				name.ColName.L == model.ExtraPhysTblIDName.L {
				continue
			}
			// record left map
			if cnt, ok := lNameMap[name.ColName.L]; ok {
				lNameMap[name.ColName.L] = cnt + 1
			} else {
				lNameMap[name.ColName.L] = 1
			}
		}
		for _, name := range rNames {
			// Natural join should ignore _tidb_rowid and _tidb_commit_ts
			if name.ColName.L == model.ExtraHandleName.L ||
				name.ColName.L == model.ExtraCommitTSName.L ||
				name.ColName.L == model.ExtraPhysTblIDName.L {
				continue
			}
			// record right map
			if cnt, ok := rNameMap[name.ColName.L]; ok {
				rNameMap[name.ColName.L] = cnt + 1
			} else {
				rNameMap[name.ColName.L] = 1
			}
			// check left map
			if cnt, ok := lNameMap[name.ColName.L]; ok {
				if cnt > 1 {
					return plannererrors.ErrAmbiguous.GenWithStackByArgs(name.ColName.L, "from clause")
				}
				commonNames = append(commonNames, name.ColName.L)
			}
		}
		// check right map
		for _, commonName := range commonNames {
			if rNameMap[commonName] > 1 {
				return plannererrors.ErrAmbiguous.GenWithStackByArgs(commonName, "from clause")
			}
		}
	}

	// Find out all the common columns and put them ahead.
	commonLen := 0
	for i, lName := range lNames {
		// Natural join should ignore _tidb_rowid and _tidb_commit_ts
		if lName.ColName.L == model.ExtraHandleName.L ||
			lName.ColName.L == model.ExtraCommitTSName.L ||
			lName.ColName.L == model.ExtraPhysTblIDName.L {
			continue
		}
		for j := commonLen; j < len(rNames); j++ {
			if lName.ColName.L != rNames[j].ColName.L {
				continue
			}

			if len(filter) > 0 {
				if !filter[lName.ColName.L] {
					break
				}
				// Mark this column exist.
				filter[lName.ColName.L] = false
			}

			col := lColumns[i]
			copy(lColumns[commonLen+1:i+1], lColumns[commonLen:i])
			lColumns[commonLen] = col

			name := lNames[i]
			copy(lNames[commonLen+1:i+1], lNames[commonLen:i])
			lNames[commonLen] = name

			col = rColumns[j]
			copy(rColumns[commonLen+1:j+1], rColumns[commonLen:j])
			rColumns[commonLen] = col

			name = rNames[j]
			copy(rNames[commonLen+1:j+1], rNames[commonLen:j])
			rNames[commonLen] = name

			commonLen++
			break
		}
	}

	if len(filter) > 0 && len(filter) != commonLen {
		for col, notExist := range filter {
			if notExist {
				return plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "from clause")
			}
		}
	}

	schemaCols := make([]*expression.Column, len(lColumns)+len(rColumns)-commonLen)
	copy(schemaCols[:len(lColumns)], lColumns)
	copy(schemaCols[len(lColumns):], rColumns[commonLen:])
	names := make(types.NameSlice, len(schemaCols))
	copy(names, lNames)
	copy(names[len(lNames):], rNames[commonLen:])

	conds := make([]expression.Expression, 0, commonLen)
	for i := range commonLen {
		lc, rc := lsc.Columns[i], rsc.Columns[i]
		cond, err := expression.NewFunction(b.ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), lc, rc)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
		if p.FullSchema != nil {
			// since FullSchema is derived from left and right schema in upper layer, so rc/lc must be in FullSchema.
			if joinTp == ast.RightJoin {
				p.FullNames[p.FullSchema.ColumnIndex(lc)].Redundant = true
			} else {
				p.FullNames[p.FullSchema.ColumnIndex(rc)].Redundant = true
			}
		}
	}

	p.SetSchema(expression.NewSchema(schemaCols...))
	p.SetOutputNames(names)

	p.OtherConditions = append(conds, p.OtherConditions...)

	return nil
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p base.LogicalPlan, where ast.ExprNode, aggMapper map[*ast.AggregateFuncExpr]int) (base.LogicalPlan, error) {
	b.optFlag |= rule.FlagPredicatePushDown
	b.optFlag |= rule.FlagDeriveTopNFromWindow
	b.optFlag |= rule.FlagPredicateSimplification
	if b.curClause != havingClause {
		b.curClause = whereClause
	}

	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := logicalop.LogicalSelection{}.Init(b.ctx, b.getSelectOffset())
	for _, cond := range conditions {
		expr, np, err := b.rewrite(ctx, cond, p, aggMapper, false)
		if err != nil {
			return nil, err
		}
		p = np
		if expr == nil {
			continue
		}
		// for case: explain SELECT year+2 as y, SUM(profit) AS profit FROM sales GROUP BY year+2, year+profit WITH ROLLUP having y > 2002;
		// currently, we succeed to resolve y to (year+2), but fail to resolve (year+2) to grouping col, and to base column function: plus(year, 2) instead.
		// which will cause this selection being pushed down through Expand OP itself.
		//
		// In expand, we will additionally project (year+2) out as a new column, let's say grouping_col here, and we wanna it can substitute any upper layer's (year+2)
		expr = b.replaceGroupingFunc(expr)

		expressions = append(expressions, expr)
	}
	cnfExpres := make([]expression.Expression, 0)
	useCache := b.ctx.GetSessionVars().StmtCtx.UseCache()
	for _, expr := range expressions {
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok && expression.ConstExprConsiderPlanCache(con, useCache) {
				ret, _, err := expression.EvalBool(b.ctx.GetExprCtx().GetEvalCtx(), expression.CNFExprs{con}, chunk.Row{})
				if err != nil {
					return nil, errors.Trace(err)
				}
				if ret {
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				dual := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
				if join, ok := p.(*logicalop.LogicalJoin); ok && join.FullSchema != nil {
					dual.SetOutputNames(join.FullNames)
					dual.SetSchema(join.FullSchema)
				} else {
					dual.SetOutputNames(p.OutputNames())
					dual.SetSchema(p.Schema())
				}

				return dual, nil
			}
			cnfExpres = append(cnfExpres, item)
		}
	}
	if len(cnfExpres) == 0 {
		return p, nil
	}
	evalCtx := b.ctx.GetExprCtx().GetEvalCtx()
	// check expr field types.
	for i, expr := range cnfExpres {
		if expr.GetType(evalCtx).EvalType() == types.ETString {
			tp := &types.FieldType{}
			tp.SetType(mysql.TypeDouble)
			tp.SetFlag(expr.GetType(evalCtx).GetFlag())
			tp.SetFlen(mysql.MaxRealWidth)
			tp.SetDecimal(types.UnspecifiedLength)
			types.SetBinChsClnFlag(tp)
			cnfExpres[i] = expression.TryPushCastIntoControlFunctionForHybridType(b.ctx.GetExprCtx(), expr, tp)
		}
	}
	selection.Conditions = cnfExpres
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (*PlanBuilder) buildProjectionFieldNameFromColumns(origField *ast.SelectField, colNameField *ast.ColumnNameExpr, name *types.FieldName) (colName, origColName, tblName, origTblName, dbName ast.CIStr) {
	origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
	if origField.AsName.L == "" {
		colName = colNameField.Name.Name
	} else {
		colName = origField.AsName
	}
	if tblName.L == "" {
		tblName = name.TblName
	} else {
		tblName = colNameField.Name.Table
	}
	return
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(_ context.Context, field *ast.SelectField) (ast.CIStr, error) {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name, nil
	}

	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	funcCall, isFuncCall := innerExpr.(*ast.FuncCallExpr)
	// When used to produce a result set column, NAME_CONST() causes the column to have the given name.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_name-const for details
	if isFuncCall && funcCall.FnName.L == ast.NameConst {
		if v, err := evalAstExpr(b.ctx.GetExprCtx(), funcCall.Args[0]); err == nil {
			if s, err := v.ToString(); err == nil {
				return ast.NewCIStr(s), nil
			}
		}
		return ast.NewCIStr(""), plannererrors.ErrWrongArguments.GenWithStackByArgs("NAME_CONST")
	}
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return ast.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment)), nil
	}

	// Literal: Need special processing
	switch valueExpr.Kind() {
	case types.KindString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		return ast.NewCIStr(fieldName), nil
	case types.KindNull:
		// See #4053, #3685
		return ast.NewCIStr("NULL"), nil
	case types.KindBinaryLiteral:
		// Don't rewrite BIT literal or HEX literals
		return ast.NewCIStr(field.Text()), nil
	case types.KindInt64:
		// See #9683
		// TRUE or FALSE can be a int64
		if mysql.HasIsBooleanFlag(valueExpr.Type.GetFlag()) {
			if i := valueExpr.GetValue().(int64); i == 0 {
				return ast.NewCIStr("FALSE"), nil
			}
			return ast.NewCIStr("TRUE"), nil
		}
		fallthrough

	default:
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return ast.NewCIStr(fieldName), nil
	}
}

func buildExpandFieldName(ctx expression.EvalContext, expr expression.Expression, name *types.FieldName, genName string) *types.FieldName {
	_, isCol := expr.(*expression.Column)
	var origTblName, origColName, dbName, colName, tblName ast.CIStr
	if genName != "" {
		// for case like: gid_, gpos_
		colName = ast.NewCIStr(expr.StringWithCtx(ctx, errors.RedactLogDisable))
	} else if isCol {
		// col ref to original col, while its nullability may be changed.
		origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
		colName = ast.NewCIStr("ex_" + name.ColName.O)
		tblName = ast.NewCIStr("ex_" + name.TblName.O)
	} else {
		// Other: complicated expression.
		colName = ast.NewCIStr("ex_" + expr.StringWithCtx(ctx, errors.RedactLogDisable))
	}
	newName := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
	}
	return newName
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(ctx context.Context, p base.LogicalPlan, field *ast.SelectField, expr expression.Expression) (*expression.Column, *types.FieldName, error) {
	var origTblName, tblName, origColName, colName, dbName ast.CIStr
	innerNode := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	col, isCol := expr.(*expression.Column)
	// Correlated column won't affect the final output names. So we can put it in any of the three logic block.
	// Don't put it into the first block just for simplifying the codes.
	if colNameField, ok := innerNode.(*ast.ColumnNameExpr); ok && isCol {
		// Field is a column reference.
		idx := p.Schema().ColumnIndex(col)
		var name *types.FieldName
		// The column maybe the one from join's redundant part.
		if idx == -1 {
			name = findColFromNaturalUsingJoin(p, col)
		} else {
			name = p.OutputNames()[idx]
		}
		colName, origColName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, colNameField, name)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		var err error
		if colName, err = b.buildProjectionFieldNameFromExpressions(ctx, field); err != nil {
			return nil, nil, err
		}
	}
	name := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
	}
	if isCol {
		return col, name, nil
	}
	if expr == nil {
		return nil, name, nil
	}
	// invalid unique id
	correlatedColUniqueID := int64(0)
	if cc, ok := expr.(*expression.CorrelatedColumn); ok {
		correlatedColUniqueID = cc.UniqueID
	}
	// for expr projection, we should record the map relationship <hashcode, uniqueID> down.
	newCol := &expression.Column{
		UniqueID:              b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:               expr.GetType(b.ctx.GetExprCtx().GetEvalCtx()),
		CorrelatedColUniqueID: correlatedColUniqueID,
	}
	if b.ctx.GetSessionVars().OptimizerEnableNewOnlyFullGroupByCheck {
		if b.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol == nil {
			b.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol = make(map[string]int, 1)
		}
		b.ctx.GetSessionVars().MapHashCode2UniqueID4ExtendedCol[string(expr.HashCode())] = int(newCol.UniqueID)
	}
	newCol.SetCoercibility(expr.Coercibility())
	return newCol, name, nil
}

type userVarTypeProcessor struct {
	ctx     context.Context
	plan    base.LogicalPlan
	builder *PlanBuilder
	mapper  map[*ast.AggregateFuncExpr]int
	err     error
}

func (p *userVarTypeProcessor) Enter(in ast.Node) (ast.Node, bool) {
	v, ok := in.(*ast.VariableExpr)
	if !ok {
		return in, false
	}
	if v.IsSystem || v.Value == nil {
		return in, true
	}
	_, p.plan, p.err = p.builder.rewrite(p.ctx, v, p.plan, p.mapper, true)
	return in, true
}

func (p *userVarTypeProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, p.err == nil
}

func (b *PlanBuilder) preprocessUserVarTypes(ctx context.Context, p base.LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) error {
	aggMapper := make(map[*ast.AggregateFuncExpr]int)
	maps.Copy(aggMapper, mapper)
	processor := userVarTypeProcessor{
		ctx:     ctx,
		plan:    p,
		builder: b,
		mapper:  aggMapper,
	}
	for _, field := range fields {
		field.Expr.Accept(&processor)
		if processor.err != nil {
			return processor.err
		}
	}
	return nil
}

// findColFromNaturalUsingJoin is used to recursively find the column from the
// underlying natural-using-join.
// e.g. For SQL like `select t2.a from t1 join t2 using(a) where t2.a > 0`, the
// plan will be `join->selection->projection`. The schema of the `selection`
// will be `[t1.a]`, thus we need to recursively retrieve the `t2.a` from the
// underlying join.
func findColFromNaturalUsingJoin(p base.LogicalPlan, col *expression.Column) (name *types.FieldName) {
	switch x := p.(type) {
	case *logicalop.LogicalLimit, *logicalop.LogicalSelection, *logicalop.LogicalTopN, *logicalop.LogicalSort, *logicalop.LogicalMaxOneRow:
		return findColFromNaturalUsingJoin(p.Children()[0], col)
	case *logicalop.LogicalJoin:
		if x.FullSchema != nil {
			idx := x.FullSchema.ColumnIndex(col)
			return x.FullNames[idx]
		}
	}
	return nil
}

type resolveGroupingTraverseAction struct {
	CurrentBlockExpand *logicalop.LogicalExpand
}

func (r resolveGroupingTraverseAction) Transform(expr expression.Expression) (res expression.Expression) {
	switch x := expr.(type) {
	case *expression.Column:
		// when meeting a column, judge whether it's a relate grouping set col.
		// eg: select a, b from t group by a, c with rollup, here a is, while b is not.
		// in underlying Expand schema (a,b,c,a',c'), a select list should be resolved to a'.
		res, _ = r.CurrentBlockExpand.TrySubstituteExprWithGroupingSetCol(x)
	case *expression.CorrelatedColumn:
		// select 1 in (select t2.a from t group by t2.a, b with rollup) from t2;
		// in this case: group by item has correlated column t2.a, and it's select list contains t2.a as well.
		res, _ = r.CurrentBlockExpand.TrySubstituteExprWithGroupingSetCol(x)
	case *expression.Constant:
		// constant just keep it real: select 1 from t group by a, b with rollup.
		res = x
	case *expression.ScalarFunction:
		// scalar function just try to resolve itself first, then if not changed, trying resolve its children.
		var substituted bool
		res, substituted = r.CurrentBlockExpand.TrySubstituteExprWithGroupingSetCol(x)
		if !substituted {
			// if not changed, try to resolve it children.
			// select a+1, grouping(b) from t group by a+1 (projected as c), b with rollup: in this case, a+1 is resolved as c as a whole.
			// select a+1, grouping(b) from t group by a(projected as a'), b with rollup  : in this case, a+1 is resolved as a'+ 1.
			newArgs := x.GetArgs()
			for i, arg := range newArgs {
				newArgs[i] = r.Transform(arg)
			}
			res = x
		}
	default:
		res = expr
	}
	return res
}

func (b *PlanBuilder) replaceGroupingFunc(expr expression.Expression) expression.Expression {
	// current block doesn't have an expand OP, just return it.
	// expr can be nil when rewrite eliminates a predicate in non-scalar contexts.
	if b.currentBlockExpand == nil || expr == nil {
		return expr
	}
	// curExpand can supply the DistinctGbyExprs and gid col.
	traverseAction := resolveGroupingTraverseAction{CurrentBlockExpand: b.currentBlockExpand}
	return expr.Traverse(traverseAction)
}

func (b *PlanBuilder) implicitProjectGroupingSetCols(projSchema *expression.Schema, projNames []*types.FieldName, projExprs []expression.Expression) (*expression.Schema, []*types.FieldName, []expression.Expression) {
	if b.currentBlockExpand == nil {
		return projSchema, projNames, projExprs
	}
	m := make(map[int64]struct{}, len(b.currentBlockExpand.DistinctGroupByCol))
	for _, col := range projSchema.Columns {
		m[col.UniqueID] = struct{}{}
	}
	for idx, gCol := range b.currentBlockExpand.DistinctGroupByCol {
		if _, ok := m[gCol.UniqueID]; ok {
			// grouping col has been explicitly projected, not need to reserve it here for later order-by item (a+1)
			// like: select a+1, b from t group by a+1 order by a+1.
			continue
		}
		// project the grouping col out implicitly here. If it's not used by later OP, it will be cleaned in column pruner.
		projSchema.Append(gCol)
		projExprs = append(projExprs, gCol)
		projNames = append(projNames, b.currentBlockExpand.DistinctGbyColNames[idx])
	}
	// project GID.
	projSchema.Append(b.currentBlockExpand.GID)
	projExprs = append(projExprs, b.currentBlockExpand.GID)
	projNames = append(projNames, b.currentBlockExpand.GIDName)
	// project GPos if any.
	if b.currentBlockExpand.GPos != nil {
		projSchema.Append(b.currentBlockExpand.GPos)
		projExprs = append(projExprs, b.currentBlockExpand.GPos)
		projNames = append(projNames, b.currentBlockExpand.GPosName)
	}
	return projSchema, projNames, projExprs
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *PlanBuilder) buildProjection(ctx context.Context, p base.LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int,
	windowMapper map[*ast.WindowFuncExpr]int, considerWindow bool, expandGenerateColumn bool) (base.LogicalPlan, []expression.Expression, int, error) {
	err := b.preprocessUserVarTypes(ctx, p, fields, mapper)
	if err != nil {
		return nil, nil, 0, err
	}
	b.optFlag |= rule.FlagEliminateProjection
	b.curClause = fieldList
	proj := logicalop.LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.Init(b.ctx, b.getSelectOffset())
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	newNames := make([]*types.FieldName, 0, len(fields))
	for i, field := range fields {
		if !field.Auxiliary {
			oldLen++
		}

		isWindowFuncField := ast.HasWindowFlag(field.Expr)
		// Although window functions occurs in the select fields, but it has to be processed after having clause.
		// So when we build the projection for select fields, we need to skip the window function.
		// When `considerWindow` is false, we will only build fields for non-window functions, so we add fake placeholders.
		// for window functions. These fake placeholders will be erased in column pruning.
		// When `considerWindow` is true, all the non-window fields have been built, so we just use the schema columns.
		if considerWindow && !isWindowFuncField {
			col := p.Schema().Columns[i]
			proj.Exprs = append(proj.Exprs, col)
			schema.Append(col)
			newNames = append(newNames, p.OutputNames()[i])
			continue
		} else if !considerWindow && isWindowFuncField {
			expr := expression.NewZero()
			proj.Exprs = append(proj.Exprs, expr)
			col, name, err := b.buildProjectionField(ctx, p, field, expr)
			if err != nil {
				return nil, nil, 0, err
			}
			schema.Append(col)
			newNames = append(newNames, name)
			continue
		}
		newExpr, np, err := b.rewriteWithPreprocess(ctx, field.Expr, p, mapper, windowMapper, true, nil)
		if err != nil {
			return nil, nil, 0, err
		}

		// for case: select a+1, b, sum(b), grouping(a) from t group by a, b with rollup.
		// the column inside aggregate (only sum(b) here) should be resolved to original source column,
		// while for others, just use expanded columns if exists: a'+ 1, b', group(gid)
		newExpr = b.replaceGroupingFunc(newExpr)

		// For window functions in the order by clause, we will append an field for it.
		// We need rewrite the window mapper here so order by clause could find the added field.
		if considerWindow && isWindowFuncField && field.Auxiliary {
			if windowExpr, ok := field.Expr.(*ast.WindowFuncExpr); ok {
				windowMapper[windowExpr] = i
			}
		}

		p = np
		proj.Exprs = append(proj.Exprs, newExpr)

		col, name, err := b.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return nil, nil, 0, err
		}
		schema.Append(col)
		newNames = append(newNames, name)
	}
	// implicitly project expand grouping set cols, if not used later, it will being pruned out in logical column pruner.
	schema, newNames, proj.Exprs = b.implicitProjectGroupingSetCols(schema, newNames, proj.Exprs)

	proj.SetSchema(schema)
	proj.SetOutputNames(newNames)
	if expandGenerateColumn {
		// Sometimes we need to add some fields to the projection so that we can use generate column substitute
		// optimization. For example: select a+1 from t order by a+1, with a virtual generate column c as (a+1) and
		// an index on c. We need to add c into the projection so that we can replace a+1 with c.
		exprToColumn := make(ExprColumnMap)
		collectGenerateColumn(p, exprToColumn)
		for expr, col := range exprToColumn {
			idx := p.Schema().ColumnIndex(col)
			if idx == -1 {
				continue
			}
			if proj.Schema().Contains(col) {
				continue
			}
			proj.Schema().Columns = append(proj.Schema().Columns, col)
			proj.Exprs = append(proj.Exprs, expr)
			proj.SetOutputNames(append(proj.OutputNames(), p.OutputNames()[idx]))
		}
	}
	proj.SetChildren(p)
	// delay the only-full-group-by-check in create view statement to later query.
	if !b.isCreateView && b.ctx.GetSessionVars().OptimizerEnableNewOnlyFullGroupByCheck && b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() {
		fds := proj.ExtractFD()
		// Projection -> Children -> ...
		// Let the projection itself to evaluate the whole FD, which will build the connection
		// 1: from select-expr to registered-expr
		// 2: from base-column to select-expr
		// After that
		if fds.HasAggBuilt {
			for offset, expr := range proj.Exprs[:len(fields)] {
				// skip the auxiliary column in agg appended to select fields, which mainly comes from two kind of cases:
				// 1: having agg(t.a), this will append t.a to the select fields, if it isn't here.
				// 2: order by agg(t.a), this will append t.a to the select fields, if it isn't here.
				if fields[offset].AuxiliaryColInAgg {
					continue
				}
				item := intset.NewFastIntSet()
				switch x := expr.(type) {
				case *expression.Column:
					item.Insert(int(x.UniqueID))
				case *expression.ScalarFunction:
					if expression.CheckFuncInExpr(x, ast.AnyValue) {
						continue
					}
					scalarUniqueID, ok := fds.IsHashCodeRegistered(string(hack.String(x.HashCode())))
					if !ok {
						logutil.BgLogger().Warn("Error occurred while maintaining the functional dependency")
						continue
					}
					item.Insert(scalarUniqueID)
				default:
				}
				// Rule #1, if there are no group cols, the col in the order by shouldn't be limited.
				if fds.GroupByCols.Only1Zero() && fields[offset].AuxiliaryColInOrderBy {
					continue
				}

				// Rule #2, if select fields are constant, it's ok.
				if item.SubsetOf(fds.ConstantCols()) {
					continue
				}

				// Rule #3, if select fields are subset of group by items, it's ok.
				if item.SubsetOf(fds.GroupByCols) {
					continue
				}

				// Rule #4, if select fields are dependencies of Strict FD with determinants in group-by items, it's ok.
				// lax FD couldn't be done here, eg: for unique key (b), index key NULL & NULL are different rows with
				// uncertain other column values.
				strictClosure := fds.ClosureOfStrict(fds.GroupByCols)
				if item.SubsetOf(strictClosure) {
					continue
				}
				// locate the base col that are not in (constant list / group by list / strict fd closure) for error show.
				baseCols := expression.ExtractColumns(expr)
				errShowCol := baseCols[0]
				for _, col := range baseCols {
					colSet := intset.NewFastIntSet(int(col.UniqueID))
					if !colSet.SubsetOf(strictClosure) {
						errShowCol = col
						break
					}
				}
				// better use the schema alias name firstly if any.
				name := ""
				for idx, schemaCol := range proj.Schema().Columns {
					if schemaCol.UniqueID == errShowCol.UniqueID {
						name = proj.OutputNames()[idx].String()
						break
					}
				}
				if name == "" {
					name = errShowCol.OrigName
				}
				// Only1Zero is to judge whether it's no-group-by-items case.
				if !fds.GroupByCols.Only1Zero() {
					return nil, nil, 0, plannererrors.ErrFieldNotInGroupBy.GenWithStackByArgs(offset+1, ErrExprInSelect, name)
				}
				return nil, nil, 0, plannererrors.ErrMixOfGroupFuncAndFields.GenWithStackByArgs(offset+1, name)
			}
			if fds.GroupByCols.Only1Zero() {
				// maxOneRow is delayed from agg's ExtractFD logic since some details listed in it.
				projectionUniqueIDs := intset.NewFastIntSet()
				for _, expr := range proj.Exprs {
					switch x := expr.(type) {
					case *expression.Column:
						projectionUniqueIDs.Insert(int(x.UniqueID))
					case *expression.ScalarFunction:
						scalarUniqueID, ok := fds.IsHashCodeRegistered(string(hack.String(x.HashCode())))
						if !ok {
							logutil.BgLogger().Warn("Error occurred while maintaining the functional dependency")
							continue
						}
						projectionUniqueIDs.Insert(scalarUniqueID)
					}
				}
				fds.MaxOneRow(projectionUniqueIDs)
			}
			// for select * from view (include agg), outer projection don't have to check select list with the inner group-by flag.
			fds.HasAggBuilt = false
		}
	}
	return proj, proj.Exprs, oldLen, nil
}

func (b *PlanBuilder) buildDistinct(child base.LogicalPlan, length int) (*logicalop.LogicalAggregation, error) {
	b.optFlag = b.optFlag | rule.FlagBuildKeyInfo
	b.optFlag = b.optFlag | rule.FlagPushDownAgg
	// flag it if cte contain distinct
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containRecursiveForbiddenOperator = true
	}
	plan4Agg := logicalop.LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.Init(b.ctx, child.QueryBlockOffset())
	if hintinfo := b.TableHints(); hintinfo != nil {
		plan4Agg.PreferAggType = hintinfo.PreferAggType
		plan4Agg.PreferAggToCop = hintinfo.PreferAggToCop
	}
	for _, col := range child.Schema().Columns {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	plan4Agg.SetOutputNames(child.OutputNames())
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.Schema().Columns {
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	return plan4Agg, nil
}

// unionJoinFieldType finds the type which can carry the given types in Union.
// Note that unionJoinFieldType doesn't handle charset and collation, caller need to handle it by itself.
func unionJoinFieldType(a, b *types.FieldType) *types.FieldType {
	// We ignore the pure NULL type.
	if a.GetType() == mysql.TypeNull {
		return b
	} else if b.GetType() == mysql.TypeNull {
		return a
	}
	resultTp := types.AggFieldType([]*types.FieldType{a, b})
	// This logic will be intelligible when it is associated with the buildProjection4Union logic.
	if resultTp.GetType() == mysql.TypeNewDecimal {
		// The decimal result type will be unsigned only when all the decimals to be united are unsigned.
		resultTp.AndFlag(b.GetFlag() & mysql.UnsignedFlag)
	} else {
		// Non-decimal results will be unsigned when a,b both unsigned.
		// ref1: https://dev.mysql.com/doc/refman/5.7/en/union.html#union-result-set
		// ref2: https://github.com/pingcap/tidb/issues/24953
		resultTp.AddFlag((a.GetFlag() & mysql.UnsignedFlag) & (b.GetFlag() & mysql.UnsignedFlag))
	}
	resultTp.SetDecimalUnderLimit(max(a.GetDecimal(), b.GetDecimal()))
	// `flen - decimal` is the fraction before '.'
	if a.GetFlen() == -1 || b.GetFlen() == -1 {
		resultTp.SetFlenUnderLimit(-1)
	} else {
		resultTp.SetFlenUnderLimit(max(a.GetFlen()-a.GetDecimal(), b.GetFlen()-b.GetDecimal()) + resultTp.GetDecimal())
	}
	types.TryToFixFlenOfDatetime(resultTp)
	if resultTp.EvalType() != types.ETInt && (a.EvalType() == types.ETInt || b.EvalType() == types.ETInt) &&
		(resultTp.GetFlen() < mysql.MaxIntWidth && resultTp.GetFlen() != types.UnspecifiedLength) {
		resultTp.SetFlen(mysql.MaxIntWidth)
	}
	expression.SetBinFlagOrBinStr(b, resultTp)
	return resultTp
}

// Set the flen of the union column using the max flen in children.
func (b *PlanBuilder) setUnionFlen(resultTp *types.FieldType, cols []expression.Expression) {
	if resultTp.GetFlen() == -1 {
		return
	}
	isBinary := resultTp.GetCharset() == charset.CharsetBin
	for i := range cols {
		childTp := cols[i].GetType(b.ctx.GetExprCtx().GetEvalCtx())
		childTpCharLen := 1
		if isBinary {
			if charsetInfo, ok := charset.CharacterSetInfos[childTp.GetCharset()]; ok {
				childTpCharLen = charsetInfo.Maxlen
			}
		}
		resultTp.SetFlen(max(resultTp.GetFlen(), childTpCharLen*childTp.GetFlen()))
	}
}

func (b *PlanBuilder) buildProjection4Union(_ context.Context, u *logicalop.LogicalUnionAll) error {
	unionCols := make([]*expression.Column, 0, u.Children()[0].Schema().Len())
	names := make([]*types.FieldName, 0, u.Children()[0].Schema().Len())

	// Infer union result types by its children's schema.
	for i, col := range u.Children()[0].Schema().Columns {
		tmpExprs := make([]expression.Expression, 0, len(u.Children()))
		tmpExprs = append(tmpExprs, col)
		resultTp := col.RetType
		for j := 1; j < len(u.Children()); j++ {
			tmpExprs = append(tmpExprs, u.Children()[j].Schema().Columns[i])
			childTp := u.Children()[j].Schema().Columns[i].RetType
			resultTp = unionJoinFieldType(resultTp, childTp)
		}
		collation, err := expression.CheckAndDeriveCollationFromExprs(b.ctx.GetExprCtx(), "UNION", resultTp.EvalType(), tmpExprs...)
		if err != nil || collation.Coer == expression.CoercibilityNone {
			return collate.ErrIllegalMixCollation.GenWithStackByArgs("UNION")
		}
		resultTp.SetCharset(collation.Charset)
		resultTp.SetCollate(collation.Collation)
		b.setUnionFlen(resultTp, tmpExprs)
		names = append(names, &types.FieldName{ColName: u.Children()[0].OutputNames()[i].ColName})
		unionCols = append(unionCols, &expression.Column{
			RetType:  resultTp,
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		})
	}
	u.SetSchema(expression.NewSchema(unionCols...))
	u.SetOutputNames(names)
	// Process each child and add a projection above original child.
	// So the schema of `UnionAll` can be the same with its children's.
	for childID, child := range u.Children() {
		exprs := make([]expression.Expression, len(child.Schema().Columns))
		for i, srcCol := range child.Schema().Columns {
			dstType := unionCols[i].RetType
			srcType := srcCol.RetType
			if !srcType.Equal(dstType) {
				exprs[i] = expression.BuildCastFunction4Union(b.ctx.GetExprCtx(), srcCol, dstType)
			} else {
				exprs[i] = srcCol
			}
		}
		b.optFlag |= rule.FlagEliminateProjection
		proj := logicalop.LogicalProjection{Exprs: exprs}.Init(b.ctx, b.getSelectOffset())
		proj.SetSchema(u.Schema().Clone())
		// reset the schema type to make the "not null" flag right.
		for i, expr := range exprs {
			proj.Schema().Columns[i].RetType = expr.GetType(b.ctx.GetExprCtx().GetEvalCtx())
		}
		proj.SetChildren(child)
		u.Children()[childID] = proj
	}
	return nil
}

func (b *PlanBuilder) buildSetOpr(ctx context.Context, setOpr *ast.SetOprStmt) (base.LogicalPlan, error) {
	if setOpr.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		_, err := b.buildWith(ctx, setOpr.With)
		if err != nil {
			return nil, err
		}
	}

	// Because INTERSECT has higher precedence than UNION and EXCEPT. We build it first.
	selectPlans := make([]base.LogicalPlan, 0, len(setOpr.SelectList.Selects))
	afterSetOprs := make([]*ast.SetOprType, 0, len(setOpr.SelectList.Selects))
	selects := setOpr.SelectList.Selects
	for i := 0; i < len(selects); i++ {
		intersects := []ast.Node{selects[i]}
		for i+1 < len(selects) {
			breakIteration := false
			switch x := selects[i+1].(type) {
			case *ast.SelectStmt:
				if *x.AfterSetOperator != ast.Intersect && *x.AfterSetOperator != ast.IntersectAll {
					breakIteration = true
				}
			case *ast.SetOprSelectList:
				if *x.AfterSetOperator != ast.Intersect && *x.AfterSetOperator != ast.IntersectAll {
					breakIteration = true
				}
				if x.Limit != nil || x.OrderBy != nil {
					// when SetOprSelectList's limit and order-by is not nil, it means itself is converted from
					// an independent ast.SetOprStmt in parser, its data should be evaluated first, and ordered
					// by given items and conduct a limit on it, then it can only be integrated with other brothers.
					breakIteration = true
				}
			}
			if breakIteration {
				break
			}
			intersects = append(intersects, selects[i+1])
			i++
		}
		selectPlan, afterSetOpr, err := b.buildIntersect(ctx, intersects)
		if err != nil {
			return nil, err
		}
		selectPlans = append(selectPlans, selectPlan)
		afterSetOprs = append(afterSetOprs, afterSetOpr)
	}
	setOprPlan, err := b.buildExcept(ctx, selectPlans, afterSetOprs)
	if err != nil {
		return nil, err
	}

	oldLen := setOprPlan.Schema().Len()

	for range setOpr.SelectList.Selects {
		b.handleHelper.popMap()
	}
	b.handleHelper.pushMap(nil)

	if setOpr.OrderBy != nil {
		setOprPlan, err = b.buildSort(ctx, setOprPlan, setOpr.OrderBy.Items, nil, nil)
		if err != nil {
			return nil, err
		}
	}

	if setOpr.Limit != nil {
		setOprPlan, err = b.buildLimit(setOprPlan, setOpr.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Fix issue #8189 (https://github.com/pingcap/tidb/issues/8189).
	// If there are extra expressions generated from `ORDER BY` clause, generate a `Projection` to remove them.
	if oldLen != setOprPlan.Schema().Len() {
		proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(setOprPlan.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(setOprPlan)
		schema := expression.NewSchema(setOprPlan.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.SetOutputNames(setOprPlan.OutputNames()[:oldLen])
		proj.SetSchema(schema)
		return proj, nil
	}
	return setOprPlan, nil
}

func (b *PlanBuilder) buildSemiJoinForSetOperator(
	leftOriginPlan base.LogicalPlan,
	rightPlan base.LogicalPlan,
	joinType base.JoinType) (leftPlan base.LogicalPlan, err error) {
	leftPlan, err = b.buildDistinct(leftOriginPlan, leftOriginPlan.Schema().Len())
	if err != nil {
		return nil, err
	}

	joinPlan := logicalop.LogicalJoin{JoinType: joinType}.Init(b.ctx, b.getSelectOffset())
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(leftPlan.Schema())
	joinPlan.SetOutputNames(make([]*types.FieldName, leftPlan.Schema().Len()))
	copy(joinPlan.OutputNames(), leftPlan.OutputNames())
	for j := range rightPlan.Schema().Columns {
		leftCol, rightCol := leftPlan.Schema().Columns[j], rightPlan.Schema().Columns[j]
		eqCond, err := expression.NewFunction(b.ctx.GetExprCtx(), ast.NullEQ, types.NewFieldType(mysql.TypeTiny), leftCol, rightCol)
		if err != nil {
			return nil, err
		}
		sf := eqCond.(*expression.ScalarFunction)
		_, _, ok := expression.IsColOpCol(sf)
		if leftCol.RetType.GetType() != rightCol.RetType.GetType() || !ok {
			joinPlan.OtherConditions = append(joinPlan.OtherConditions, eqCond)
		} else {
			joinPlan.EqualConditions = append(joinPlan.EqualConditions, sf)
		}
	}
	return joinPlan, nil
}

// buildIntersect build the set operator for 'intersect'. It is called before buildExcept and buildUnion because of its
// higher precedence.
func (b *PlanBuilder) buildIntersect(ctx context.Context, selects []ast.Node) (base.LogicalPlan, *ast.SetOprType, error) {
	var leftPlan base.LogicalPlan
	var err error
	var afterSetOperator *ast.SetOprType
	switch x := selects[0].(type) {
	case *ast.SelectStmt:
		afterSetOperator = x.AfterSetOperator
		leftPlan, err = b.buildSelect(ctx, x)
	case *ast.SetOprSelectList:
		afterSetOperator = x.AfterSetOperator
		leftPlan, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: x, With: x.With, Limit: x.Limit, OrderBy: x.OrderBy})
	}
	if err != nil {
		return nil, nil, err
	}
	if len(selects) == 1 {
		return leftPlan, afterSetOperator, nil
	}

	columnNums := leftPlan.Schema().Len()
	for i := 1; i < len(selects); i++ {
		var rightPlan base.LogicalPlan
		switch x := selects[i].(type) {
		case *ast.SelectStmt:
			if *x.AfterSetOperator == ast.IntersectAll {
				// TODO: support intersect all
				return nil, nil, errors.Errorf("TiDB do not support intersect all")
			}
			rightPlan, err = b.buildSelect(ctx, x)
		case *ast.SetOprSelectList:
			if *x.AfterSetOperator == ast.IntersectAll {
				// TODO: support intersect all
				return nil, nil, errors.Errorf("TiDB do not support intersect all")
			}
			rightPlan, err = b.buildSetOpr(ctx, &ast.SetOprStmt{SelectList: x, With: x.With, Limit: x.Limit, OrderBy: x.OrderBy})
		}
		if err != nil {
			return nil, nil, err
		}
		if rightPlan.Schema().Len() != columnNums {
			return nil, nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, base.SemiJoin)
		if err != nil {
			return nil, nil, err
		}
	}
	return leftPlan, afterSetOperator, nil
}

// buildExcept build the set operators for 'except', and in this function, it calls buildUnion at the same time. Because
// Union and except has the same precedence.
func (b *PlanBuilder) buildExcept(ctx context.Context, selects []base.LogicalPlan, afterSetOpts []*ast.SetOprType) (base.LogicalPlan, error) {
	unionPlans := []base.LogicalPlan{selects[0]}
	tmpAfterSetOpts := []*ast.SetOprType{nil}
	columnNums := selects[0].Schema().Len()
	for i := 1; i < len(selects); i++ {
		rightPlan := selects[i]
		if rightPlan.Schema().Len() != columnNums {
			return nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
		if *afterSetOpts[i] == ast.Except {
			leftPlan, err := b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
			if err != nil {
				return nil, err
			}
			leftPlan, err = b.buildSemiJoinForSetOperator(leftPlan, rightPlan, base.AntiSemiJoin)
			if err != nil {
				return nil, err
			}
			unionPlans = []base.LogicalPlan{leftPlan}
			tmpAfterSetOpts = []*ast.SetOprType{nil}
		} else if *afterSetOpts[i] == ast.ExceptAll {
			// TODO: support except all.
			return nil, errors.Errorf("TiDB do not support except all")
		} else {
			unionPlans = append(unionPlans, rightPlan)
			tmpAfterSetOpts = append(tmpAfterSetOpts, afterSetOpts[i])
		}
	}
	return b.buildUnion(ctx, unionPlans, tmpAfterSetOpts)
}

func (b *PlanBuilder) buildUnion(ctx context.Context, selects []base.LogicalPlan, afterSetOpts []*ast.SetOprType) (base.LogicalPlan, error) {
	if len(selects) == 1 {
		return selects[0], nil
	}
	distinctSelectPlans, allSelectPlans, err := b.divideUnionSelectPlans(ctx, selects, afterSetOpts)
	if err != nil {
		return nil, err
	}
	unionDistinctPlan, err := b.buildUnionAll(ctx, distinctSelectPlans)
	if err != nil {
		return nil, err
	}
	if unionDistinctPlan != nil {
		unionDistinctPlan, err = b.buildDistinct(unionDistinctPlan, unionDistinctPlan.Schema().Len())
		if err != nil {
			return nil, err
		}
		if len(allSelectPlans) > 0 {
			// Can't change the statements order in order to get the correct column info.
			allSelectPlans = append([]base.LogicalPlan{unionDistinctPlan}, allSelectPlans...)
		}
	}

	unionAllPlan, err := b.buildUnionAll(ctx, allSelectPlans)
	if err != nil {
		return nil, err
	}
	unionPlan := unionDistinctPlan
	if unionAllPlan != nil {
		unionPlan = unionAllPlan
	}

	return unionPlan, nil
}

// divideUnionSelectPlans resolves union's select stmts to logical plans.
// and divide result plans into "union-distinct" and "union-all" parts.
// divide rule ref:
//
//	https://dev.mysql.com/doc/refman/5.7/en/union.html
//
// "Mixed UNION types are treated such that a DISTINCT union overrides any ALL union to its left."
func (*PlanBuilder) divideUnionSelectPlans(_ context.Context, selects []base.LogicalPlan, setOprTypes []*ast.SetOprType) (distinctSelects []base.LogicalPlan, allSelects []base.LogicalPlan, err error) {
	firstUnionAllIdx := 0
	columnNums := selects[0].Schema().Len()
	for i := len(selects) - 1; i > 0; i-- {
		if firstUnionAllIdx == 0 && *setOprTypes[i] != ast.UnionAll {
			firstUnionAllIdx = i + 1
		}
		if selects[i].Schema().Len() != columnNums {
			return nil, nil, plannererrors.ErrWrongNumberOfColumnsInSelect.GenWithStackByArgs()
		}
	}
	return selects[:firstUnionAllIdx], selects[firstUnionAllIdx:], nil
}

func (b *PlanBuilder) buildUnionAll(ctx context.Context, subPlan []base.LogicalPlan) (base.LogicalPlan, error) {
	if len(subPlan) == 0 {
		return nil, nil
	}
	b.optFlag |= rule.FlagEliminateUnionAllDualItem
	u := logicalop.LogicalUnionAll{}.Init(b.ctx, b.getSelectOffset())
	u.SetChildren(subPlan...)
	err := b.buildProjection4Union(ctx, u)
	return u, err
}

// itemTransformer transforms ParamMarkerExpr to PositionExpr in the context of ByItem
type itemTransformer struct{}

func (*itemTransformer) Enter(inNode ast.Node) (ast.Node, bool) {
	if n, ok := inNode.(*driver.ParamMarkerExpr); ok {
		newNode := expression.ConstructPositionExpr(n)
		return newNode, true
	}
	return inNode, false
}

func (*itemTransformer) Leave(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (b *PlanBuilder) buildSort(ctx context.Context, p base.LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int) (*logicalop.LogicalSort, error) {
	return b.buildSortWithCheck(ctx, p, byItems, aggMapper, windowMapper, nil, 0, false)
}

func (b *PlanBuilder) buildSortWithCheck(ctx context.Context, p base.LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int, windowMapper map[*ast.WindowFuncExpr]int,
	projExprs []expression.Expression, oldLen int, hasDistinct bool) (*logicalop.LogicalSort, error) {
	if _, isUnion := p.(*logicalop.LogicalUnionAll); isUnion {
		b.curClause = globalOrderByClause
	} else {
		b.curClause = orderByClause
	}
	sort := logicalop.LogicalSort{}.Init(b.ctx, b.getSelectOffset())
	exprs := make([]*util.ByItems, 0, len(byItems))
	transformer := &itemTransformer{}
	for i, item := range byItems {
		newExpr, _ := item.Expr.Accept(transformer)
		item.Expr = newExpr.(ast.ExprNode)
		it, np, err := b.rewriteWithPreprocess(ctx, item.Expr, p, aggMapper, windowMapper, true, nil)
		if err != nil {
			return nil, err
		}
		// for case: select a+1, b, sum(b) from t group by a+1, b with rollup order by a+1.
		// currently, we fail to resolve (a+1) in order-by to projection item (a+1), and adding
		// another a' in the select fields instead, leading finally resolved expr is a'+1 here.
		//
		// Anyway, a and a' has the same column unique id, so we can do the replacement work like
		// we did in build projection phase.
		it = b.replaceGroupingFunc(it)

		// check whether ORDER BY items show up in SELECT DISTINCT fields, see #12442
		if hasDistinct && projExprs != nil {
			err = b.checkOrderByInDistinct(item, i, it, p, projExprs, oldLen)
			if err != nil {
				return nil, err
			}
		}

		p = np
		exprs = append(exprs, &util.ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// checkOrderByInDistinct checks whether ORDER BY has conflicts with DISTINCT, see #12442
func (b *PlanBuilder) checkOrderByInDistinct(byItem *ast.ByItem, idx int, expr expression.Expression, p base.LogicalPlan, originalExprs []expression.Expression, length int) error {
	// Check if expressions in ORDER BY whole match some fields in DISTINCT.
	// e.g.
	// select distinct count(a) from t group by b order by count(a);  ✔
	// select distinct a+1 from t order by a+1;                       ✔
	// select distinct a+1 from t order by a+2;                       ✗
	evalCtx := b.ctx.GetExprCtx().GetEvalCtx()
	for j := range length {
		// both check original expression & as name
		if expr.Equal(evalCtx, originalExprs[j]) || expr.Equal(evalCtx, p.Schema().Columns[j]) {
			return nil
		}
	}

	// Check if referenced columns of expressions in ORDER BY whole match some fields in DISTINCT,
	// both original expression and alias can be referenced.
	// e.g.
	// select distinct sin(a) from t order by a;                            ✔
	// select distinct a, b from t order by a+b;                            ✔
	// select distinct count(a), sum(a) from t group by b order by sum(a);  ✔
	cols := expression.ExtractColumns(expr)
CheckReferenced:
	for _, col := range cols {
		for j := range length {
			if col.Equal(evalCtx, originalExprs[j]) || col.Equal(evalCtx, p.Schema().Columns[j]) {
				continue CheckReferenced
			}
		}

		// Failed cases
		// e.g.
		// select distinct sin(a) from t order by a;                            ✗
		// select distinct a from t order by a+b;                               ✗
		if _, ok := byItem.Expr.(*ast.AggregateFuncExpr); ok {
			return plannererrors.ErrAggregateInOrderNotSelect.GenWithStackByArgs(idx+1, "DISTINCT")
		}
		// select distinct count(a) from t group by b order by sum(a);          ✗
		return plannererrors.ErrFieldInOrderNotSelect.GenWithStackByArgs(idx+1, col.OrigName, "DISTINCT")
	}
	return nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
// For prepared statement, node is string. We should convert it to uint64.
func getUintFromNode(ctx expression.BuildContext, n ast.Node, mustInt64orUint64 bool) (uVal uint64, isNull bool, isExpectedType bool) {
	var val any
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	case *driver.ParamMarkerExpr:
		if !v.InExecute {
			return 0, false, true
		}
		if mustInt64orUint64 {
			if expected, _ := CheckParamTypeInt64orUint64(v); !expected {
				return 0, false, false
			}
		}
		param, err := expression.ParamMarkerExpression(ctx, v, false)
		if err != nil {
			return 0, false, false
		}
		str, isNull, err := expression.GetStringFromConstant(ctx.GetEvalCtx(), param)
		if err != nil {
			return 0, false, false
		}
		if isNull {
			return 0, true, true
		}
		val = str
	default:
		return 0, false, false
	}
	switch v := val.(type) {
	case uint64:
		return v, false, true
	case int64:
		if v >= 0 {
			return uint64(v), false, true
		}
	case string:
		uVal, err := types.StrToUint(ctx.GetEvalCtx().TypeCtx(), v, false)
		if err != nil {
			return 0, false, false
		}
		return uVal, false, true
	}
	return 0, false, false
}

// CheckParamTypeInt64orUint64 check param type for plan cache limit, only allow int64 and uint64 now
// eg: set @a = 1;
func CheckParamTypeInt64orUint64(param *driver.ParamMarkerExpr) (bool, uint64) {
	val := param.GetValue()
	switch v := val.(type) {
	case int64:
		if v >= 0 {
			return true, uint64(v)
		}
	case uint64:
		return true, v
	}
	return false, 0
}

func extractLimitCountOffset(ctx expression.BuildContext, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	var isExpectedType bool
	if limit.Count != nil {
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count, true)
		if !isExpectedType {
			return 0, 0, plannererrors.ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset, true)
		if !isExpectedType {
			return 0, 0, plannererrors.ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (b *PlanBuilder) buildLimit(src base.LogicalPlan, limit *ast.Limit) (base.LogicalPlan, error) {
	b.optFlag = b.optFlag | rule.FlagPushDownTopN
	// flag it if cte contain limit
	if b.buildingCTE {
		b.outerCTEs[len(b.outerCTEs)-1].containRecursiveForbiddenOperator = true
	}
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx.GetExprCtx(), limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	if offset+count == 0 {
		tableDual := logicalop.LogicalTableDual{RowCount: 0}.Init(b.ctx, b.getSelectOffset())
		tableDual.SetSchema(src.Schema())
		tableDual.SetOutputNames(src.OutputNames())
		return tableDual, nil
	}
	li := logicalop.LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(b.ctx, b.getSelectOffset())
	if hint := b.TableHints(); hint != nil {
		li.PreferLimitToCop = hint.PreferLimitToCop
	}
	li.SetChildren(src)
	return li, nil
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if field.Match(v, ignoreAsName) {
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curCol
				index = i
			} else if !matchedExpr.(*ast.ColumnNameExpr).Name.Match(curCol.Name) &&
				!curCol.Name.Match(matchedExpr.(*ast.ColumnNameExpr).Name) {
				return -1, plannererrors.ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts ColumnNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	err          error
	p            base.LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	curClause    clauseCode
	prevClause   []clauseCode
}

func (a *havingWindowAndOrderbyExprResolver) pushCurClause(newClause clauseCode) {
	a.prevClause = append(a.prevClause, a.curClause)
	a.curClause = newClause
}

func (a *havingWindowAndOrderbyExprResolver) popCurClause() {
	a.curClause = a.prevClause[len(a.prevClause)-1]
	a.prevClause = a.prevClause[:len(a.prevClause)-1]
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		a.inWindowFunc = true
	case *ast.WindowSpec:
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	case *ast.PartitionByClause:
		a.pushCurClause(partitionByClause)
	case *ast.OrderByClause:
		if a.inWindowSpec {
			a.pushCurClause(windowOrderByClause)
		}
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromPlan(v *ast.ColumnNameExpr, p base.LogicalPlan, resolveFieldsFirst bool) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	schemaCols, outputNames := p.Schema().Columns, p.OutputNames()
	if idx < 0 {
		// For SQL like `select t2.a from t1 join t2 using(a) where t2.a > 0
		// order by t2.a`, the query plan will be `join->selection->sort`. The
		// schema of selection will be `[t1.a]`, thus we need to recursively
		// retrieve the `t2.a` from the underlying join.
		switch x := p.(type) {
		case *logicalop.LogicalLimit, *logicalop.LogicalSelection, *logicalop.LogicalTopN, *logicalop.LogicalSort, *logicalop.LogicalMaxOneRow:
			return a.resolveFromPlan(v, p.Children()[0], resolveFieldsFirst)
		case *logicalop.LogicalJoin:
			if len(x.FullNames) != 0 {
				idx, err = expression.FindFieldName(x.FullNames, v.Name)
				schemaCols, outputNames = x.FullSchema.Columns, x.FullNames
			}
		}
		if err != nil || idx < 0 {
			// nowhere to be found.
			return -1, err
		}
	}
	col := schemaCols[idx]
	if col.IsHidden {
		return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[a.curClause])
	}
	name := outputNames[idx]
	newColName := &ast.ColumnName{
		Schema: name.DBName,
		Table:  name.TblName,
		Name:   name.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && c.Name.Match(newColName) {
			return i, nil
		}
	}
	// From https://github.com/pingcap/tidb/issues/51107
	// You should make the column in the having clause as the correlated column
	// which is not relation with select's fields and GroupBy's fields.
	// For SQLs like:
	//     SELECT * FROM `t1` WHERE NOT (`t1`.`col_1`>= (
	//          SELECT `t2`.`col_7`
	//             FROM (`t1`)
	//             JOIN `t2`
	//             WHERE ISNULL(`t2`.`col_3`) HAVING `t1`.`col_6`>1951988)
	//     ) ;
	//
	// if resolveFieldsFirst is false, the groupby is not nil.
	if resolveFieldsFirst && a.curClause == havingClause {
		return -1, nil
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	// appended with new select fields. set them with flag.
	if a.inAggFunc {
		// should skip check in FD for only full group by.
		sf.AuxiliaryColInAgg = true
	} else if a.curClause == orderByClause {
		// should skip check in FD for only full group by only when group by item are empty.
		sf.AuxiliaryColInOrderBy = true
	}
	sf.Expr.SetType(col.GetStaticType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    ast.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		a.inWindowFunc = false
		if a.curClause == havingClause {
			a.err = plannererrors.ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.Name))
			return node, false
		}
		if a.curClause == orderByClause {
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    ast.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		a.inWindowSpec = false
	case *ast.PartitionByClause:
		a.popCurClause()
	case *ast.OrderByClause:
		if a.inWindowSpec {
			a.popCurClause()
		}
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.curClause == orderByClause && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && a.curClause != orderByClause {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(v.Name.Match(col.Name) || col.Name.Match(v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				a.err = plannererrors.ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			if index == -1 {
				if a.curClause == orderByClause {
					index, a.err = a.resolveFromPlan(v, a.p, resolveFieldsFirst)
				} else if a.curClause == havingClause && v.Name.Table.L != "" {
					// For SQLs like:
					//   select a from t b having b.a;
					index, a.err = a.resolveFromPlan(v, a.p, resolveFieldsFirst)
					if a.err != nil {
						return node, false
					}
					if index != -1 {
						// For SQLs like:
						//   select a+1 from t having t.a;
						field := a.selectFields[index]
						if field.Auxiliary { // having can't use auxiliary field
							index = -1
						}
					}
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p, resolveFieldsFirst)
			_ = err
			if index == -1 && a.curClause != fieldList &&
				a.curClause != windowOrderByClause && a.curClause != partitionByClause {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
				if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
					a.err = plannererrors.ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
					return node, false
				}
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, names := range a.outerNames {
				idx, err1 := expression.FindFieldName(names, v.Name)
				if err1 != nil {
					a.err = err1
					return node, false
				}
				if idx >= 0 {
					return n, true
				}
			}
			a.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(ctx context.Context, sel *ast.SelectStmt, p base.LogicalPlan) (
	havingAggMapper, _ map[*ast.AggregateFuncExpr]int, err error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			return nil, nil, errors.Trace(extractor.err)
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper = extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			extractor.inExpr = false
			if ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	// this part is used to fetch correlated column from sub-query item in order-by clause, and append the origin
	// auxiliary select filed in select list, otherwise, sub-query itself won't get the name resolved in outer schema.
	if sel.OrderBy != nil {
		for _, byItem := range sel.OrderBy.Items {
			if _, ok := byItem.Expr.(*ast.SubqueryExpr); ok {
				// correlated agg will be extracted completely latter.
				_, np, err := b.rewrite(ctx, byItem.Expr, p, nil, true)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				correlatedCols := coreusage.ExtractCorrelatedCols4LogicalPlan(np)
				for _, cone := range correlatedCols {
					var colName *ast.ColumnName
					for idx, pone := range p.Schema().Columns {
						if cone.UniqueID == pone.UniqueID {
							pname := p.OutputNames()[idx]
							colName = &ast.ColumnName{
								Schema: pname.DBName,
								Table:  pname.TblName,
								Name:   pname.ColName,
							}
							break
						}
					}
					if colName != nil {
						columnNameExpr := &ast.ColumnNameExpr{Name: colName}
						for _, field := range sel.Fields.Fields {
							if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && c.Name.Match(columnNameExpr.Name) && field.AsName.L == "" {
								// deduplicate select fields: don't append it once it already has one.
								// TODO: we add the field if it has alias, but actually they are the same column. We should not have two duplicate one.
								columnNameExpr = nil
								break
							}
						}
						if columnNameExpr != nil {
							sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
								Auxiliary: true,
								Expr:      columnNameExpr,
							})
						}
					}
				}
			}
		}
	}
	return havingAggMapper, extractor.aggMapper, nil
}

func (b *PlanBuilder) extractAggFuncsInExprs(exprs []ast.ExprNode) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, expr := range exprs {
		expr.Accept(extractor)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

func (b *PlanBuilder) extractAggFuncsInSelectFields(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

func (b *PlanBuilder) extractAggFuncsInByItems(byItems []*ast.ByItem) []*ast.AggregateFuncExpr {
	extractor := &AggregateFuncExtractor{skipAggMap: b.correlatedAggMapper}
	for _, f := range byItems {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	return extractor.AggFuncs
}

// extractCorrelatedAggFuncs extracts correlated aggregates which belong to outer query from aggregate function list.
func (b *PlanBuilder) extractCorrelatedAggFuncs(ctx context.Context, p base.LogicalPlan, aggFuncs []*ast.AggregateFuncExpr) (outer []*ast.AggregateFuncExpr, err error) {
	corCols := make([]*expression.CorrelatedColumn, 0, len(aggFuncs))
	cols := make([]*expression.Column, 0, len(aggFuncs))
	aggMapper := make(map[*ast.AggregateFuncExpr]int)
	for _, agg := range aggFuncs {
		for _, arg := range agg.Args {
			expr, _, err := b.rewrite(ctx, arg, p, aggMapper, true)
			if err != nil {
				return nil, err
			}
			corCols = append(corCols, expression.ExtractCorColumns(expr)...)
			cols = append(cols, expression.ExtractColumns(expr)...)
		}
		// If decorrelation is disabled, don't extract correlated aggregates
		if b.noDecorrelate && len(corCols) > 0 {
			continue
		}
		if len(corCols) > 0 && len(cols) == 0 {
			outer = append(outer, agg)
		}
		aggMapper[agg] = -1
		corCols, cols = corCols[:0], cols[:0]
	}
	return
}

// resolveWindowFunction will process window functions and resolve the columns that don't exist in select fields.
func (b *PlanBuilder) resolveWindowFunction(sel *ast.SelectStmt, p base.LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	extractor.curClause = fieldList
	for _, field := range sel.Fields.Fields {
		if !ast.HasWindowFlag(field.Expr) {
			continue
		}
		n, ok := field.Expr.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
		field.Expr = n.(ast.ExprNode)
	}
	for _, spec := range sel.WindowSpecs {
		_, ok := spec.Accept(extractor)
		if !ok {
			return nil, extractor.err
		}
	}
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if !ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, extractor.err
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return extractor.aggMapper, nil
}

// correlatedAggregateResolver visits Expr tree.
// It finds and collects all correlated aggregates which should be evaluated in the outer query.
type correlatedAggregateResolver struct {
	ctx       context.Context
	err       error
	b         *PlanBuilder
	outerPlan base.LogicalPlan

	// correlatedAggFuncs stores aggregate functions which belong to outer query
	correlatedAggFuncs []*ast.AggregateFuncExpr

	// noDecorrelate indicates whether decorrelation should be disabled for this resolver
	noDecorrelate bool
}

// Enter implements Visitor interface.
func (r *correlatedAggregateResolver) Enter(n ast.Node) (ast.Node, bool) {
	if v, ok := n.(*ast.SelectStmt); ok {
		if r.outerPlan != nil {
			outerSchema := r.outerPlan.Schema()
			r.b.outerSchemas = append(r.b.outerSchemas, outerSchema)
			r.b.outerNames = append(r.b.outerNames, r.outerPlan.OutputNames())
			r.b.outerBlockExpand = append(r.b.outerBlockExpand, r.b.currentBlockExpand)
		}
		r.err = r.resolveSelect(v)
		return n, true
	}
	return n, false
}

// resolveSelect finds and collects correlated aggregates within the SELECT stmt.
// It resolves and builds FROM clause first to get a source plan, from which we can decide
// whether a column is correlated or not.
// Then it collects correlated aggregate from SELECT fields (including sub-queries), HAVING,
// ORDER BY, WHERE & GROUP BY.
// Finally it restore the original SELECT stmt.
func (r *correlatedAggregateResolver) resolveSelect(sel *ast.SelectStmt) (err error) {
	if sel.With != nil {
		l := len(r.b.outerCTEs)
		defer func() {
			r.b.outerCTEs = r.b.outerCTEs[:l]
		}()
		_, err := r.b.buildWith(r.ctx, sel.With)
		if err != nil {
			return err
		}
	}
	// collect correlated aggregate from sub-queries inside FROM clause.
	if err := r.collectFromTableRefs(sel.From); err != nil {
		return err
	}
	p, err := r.b.buildTableRefs(r.ctx, sel.From)
	if err != nil {
		return err
	}

	// similar to process in PlanBuilder.buildSelect
	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = r.b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return err
	}
	if r.b.capFlag&canExpandAST != 0 {
		originalFields = sel.Fields.Fields
	}

	hasWindowFuncField := r.b.detectSelectWindow(sel)
	if hasWindowFuncField {
		_, err = r.b.resolveWindowFunction(sel, p)
		if err != nil {
			return err
		}
	}

	_, _, err = r.b.resolveHavingAndOrderBy(r.ctx, sel, p)
	if err != nil {
		return err
	}

	// find and collect correlated aggregates recursively in sub-queries
	_, err = r.b.resolveCorrelatedAggregates(r.ctx, sel, p)
	if err != nil {
		return err
	}

	// collect from SELECT fields, HAVING, ORDER BY and window functions
	if r.b.detectSelectAgg(sel) {
		err = r.collectFromSelectFields(p, sel.Fields.Fields)
		if err != nil {
			return err
		}
	}

	// collect from WHERE
	err = r.collectFromWhere(p, sel.Where)
	if err != nil {
		return err
	}

	// collect from GROUP BY
	err = r.collectFromGroupBy(p, sel.GroupBy)
	if err != nil {
		return err
	}

	// restore the sub-query
	sel.Fields.Fields = originalFields
	r.b.handleHelper.popMap()
	return nil
}

func (r *correlatedAggregateResolver) collectFromTableRefs(from *ast.TableRefsClause) error {
	if from == nil {
		return nil
	}
	subResolver := &correlatedAggregateResolver{
		ctx: r.ctx,
		b:   r.b,
	}
	_, ok := from.TableRefs.Accept(subResolver)
	if !ok {
		return subResolver.err
	}
	if len(subResolver.correlatedAggFuncs) == 0 {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, subResolver.correlatedAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromSelectFields(p base.LogicalPlan, fields []*ast.SelectField) error {
	aggList, _ := r.b.extractAggFuncsInSelectFields(fields)
	r.b.curClause = fieldList
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, aggList)
	if err != nil {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromGroupBy(p base.LogicalPlan, groupBy *ast.GroupByClause) error {
	if groupBy == nil {
		return nil
	}
	aggList := r.b.extractAggFuncsInByItems(groupBy.Items)
	r.b.curClause = groupByClause
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, aggList)
	if err != nil {
		return nil
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

func (r *correlatedAggregateResolver) collectFromWhere(p base.LogicalPlan, where ast.ExprNode) error {
	if where == nil {
		return nil
	}
	extractor := &AggregateFuncExtractor{skipAggMap: r.b.correlatedAggMapper}
	_, _ = where.Accept(extractor)
	r.b.curClause = whereClause
	outerAggFuncs, err := r.b.extractCorrelatedAggFuncs(r.ctx, p, extractor.AggFuncs)
	if err != nil {
		return err
	}
	r.correlatedAggFuncs = append(r.correlatedAggFuncs, outerAggFuncs...)
	return nil
}

// Leave implements Visitor interface.
func (r *correlatedAggregateResolver) Leave(n ast.Node) (ast.Node, bool) {
	if _, ok := n.(*ast.SelectStmt); ok {
		if r.outerPlan != nil {
			r.b.outerSchemas = r.b.outerSchemas[0 : len(r.b.outerSchemas)-1]
			r.b.outerNames = r.b.outerNames[0 : len(r.b.outerNames)-1]
			r.b.currentBlockExpand = r.b.outerBlockExpand[len(r.b.outerBlockExpand)-1]
			r.b.outerBlockExpand = r.b.outerBlockExpand[0 : len(r.b.outerBlockExpand)-1]
		}
	}
	return n, r.err == nil
}

// resolveCorrelatedAggregates finds and collects all correlated aggregates which should be evaluated
// in the outer query from all the sub-queries inside SELECT fields.
func (b *PlanBuilder) resolveCorrelatedAggregates(ctx context.Context, sel *ast.SelectStmt, p base.LogicalPlan) (map[*ast.AggregateFuncExpr]int, error) {
	resolver := &correlatedAggregateResolver{
		ctx:           ctx,
		b:             b,
		outerPlan:     p,
		noDecorrelate: b.noDecorrelate,
	}
	correlatedAggList := make([]*ast.AggregateFuncExpr, 0)
	for _, field := range sel.Fields.Fields {
		_, ok := field.Expr.Accept(resolver)
		if !ok {
			return nil, resolver.err
		}
		correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
	}
	if sel.Having != nil {
		_, ok := sel.Having.Expr.Accept(resolver)
		if !ok {
			return nil, resolver.err
		}
		correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			_, ok := item.Expr.Accept(resolver)
			if !ok {
				return nil, resolver.err
			}
			correlatedAggList = append(correlatedAggList, resolver.correlatedAggFuncs...)
		}
	}
	correlatedAggMap := make(map[*ast.AggregateFuncExpr]int)
	for _, aggFunc := range correlatedAggList {
		colMap := make(map[*types.FieldName]struct{}, len(p.Schema().Columns))
		allColFromAggExprNode(p, aggFunc, colMap)
		for k := range colMap {
			colName := &ast.ColumnName{
				Schema: k.DBName,
				Table:  k.TblName,
				Name:   k.ColName,
			}
			// Add the column referred in the agg func into the select list. So that we can resolve the agg func correctly.
			// And we need set the AuxiliaryColInAgg to true to help our only_full_group_by checker work correctly.
			sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
				Auxiliary:         true,
				AuxiliaryColInAgg: true,
				Expr:              &ast.ColumnNameExpr{Name: colName},
			})
		}
		correlatedAggMap[aggFunc] = len(sel.Fields.Fields)
		sel.Fields.Fields = append(sel.Fields.Fields, &ast.SelectField{
			Auxiliary: true,
			Expr:      aggFunc,
			AsName:    ast.NewCIStr(fmt.Sprintf("sel_subq_agg_%d", len(sel.Fields.Fields))),
		})
	}
	return correlatedAggMap, nil
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	ctx        base.PlanContext
	fields     []*ast.SelectField
	schema     *expression.Schema
	names      []*types.FieldName
	err        error
	inExpr     bool
	isParam    bool
	skipAggMap map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn

	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	g.exprDepth++
	switch n := inNode.(type) {
	case *ast.SubqueryExpr, *ast.CompareSubqueryExpr, *ast.ExistsSubqueryExpr:
		return inNode, true
	case *driver.ParamMarkerExpr:
		g.isParam = true
		if g.exprDepth == 1 && !n.UseAsValueInGbyByClause {
			_, isNull, isExpectedType := getUintFromNode(g.ctx.GetExprCtx(), n, false)
			// For constant uint expression in top level, it should be treated as position expression.
			if !isNull && isExpectedType {
				return expression.ConstructPositionExpr(n), true
			}
		}
		return n, true
	case *driver.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
	default:
		g.inExpr = true
	}
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	extractor := &AggregateFuncExtractor{skipAggMap: g.skipAggMap}
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		idx, err := expression.FindFieldName(g.names, v.Name)
		if idx < 0 || !g.inExpr {
			var index int
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				g.err = plannererrors.ErrAmbiguous.GenWithStackByArgs(v.Name.Name.L, clauseMsg[groupByClause])
				return inNode, false
			}
			if idx >= 0 {
				return inNode, true
			}
			if index != -1 {
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					err = plannererrors.ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to group function")
				} else if ast.HasWindowFlag(ret) {
					err = plannererrors.ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to window function")
				} else {
					if isParam, ok := ret.(*driver.ParamMarkerExpr); ok {
						isParam.UseAsValueInGbyByClause = true
					}
					return ret, true
				}
			}
			g.err = err
			return inNode, false
		}
	case *ast.PositionExpr:
		pos, isNull, err := expression.PosFromPositionExpr(g.ctx.GetExprCtx(), v)
		if err != nil {
			g.err = plannererrors.ErrUnknown.GenWithStackByArgs()
		}
		if err != nil || isNull {
			return inNode, false
		}
		if pos < 1 || pos > len(g.fields) {
			g.err = errors.Errorf("Unknown column '%d' in 'group statement'", pos)
			return inNode, false
		}
		ret := g.fields[pos-1].Expr
		ret.Accept(extractor)
		if len(extractor.AggFuncs) != 0 || ast.HasWindowFlag(ret) {
			fieldName := g.fields[pos-1].AsName.String()
			if fieldName == "" {
				fieldName = g.fields[pos-1].Text()
			}
			g.err = plannererrors.ErrWrongGroupField.GenWithStackByArgs(fieldName)
			return inNode, false
		}
		return ret, true
	case *ast.ValuesExpr:
		if v.Column == nil {
			g.err = plannererrors.ErrUnknownColumn.GenWithStackByArgs("", "VALUES() function")
		}
	}
	return inNode, true
}

func (b *PlanBuilder) tblInfoFromCol(from ast.ResultSetNode, name *types.FieldName) *model.TableInfo {
	nodeW := resolve.NewNodeWWithCtx(from, b.resolveCtx)
	tableList := ExtractTableList(nodeW, true)
	for _, field := range tableList {
		if field.Name.L == name.TblName.L {
			tnW := b.resolveCtx.GetTableName(field)
			if tnW != nil {
				return tnW.TableInfo
			}
			// when the Select is inside a view, it's not pre-processed, tnW is nil.
			if b.isCreateView {
				// Ignore during create
				return nil
			}
			tblInfo, err := b.is.TableInfoByName(name.DBName, name.TblName)
			if err != nil {
				return nil
			}
			return tblInfo
		}
	}
	return nil
}

func buildFuncDependCol(p base.LogicalPlan, cond ast.ExprNode) (_, _ *types.FieldName, err error) {
	binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
	if !ok {
		return nil, nil, nil
	}
	if binOpExpr.Op != opcode.EQ {
		return nil, nil, nil
	}
	lColExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil, nil
	}
	rColExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr)
	if !ok {
		return nil, nil, nil
	}
	lIdx, err := expression.FindFieldName(p.OutputNames(), lColExpr.Name)
	if err != nil {
		return nil, nil, err
	}
	rIdx, err := expression.FindFieldName(p.OutputNames(), rColExpr.Name)
	if err != nil {
		return nil, nil, err
	}
	if lIdx == -1 {
		return nil, nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(lColExpr.Name, "where clause")
	}
	if rIdx == -1 {
		return nil, nil, plannererrors.ErrUnknownColumn.GenWithStackByArgs(rColExpr.Name, "where clause")
	}
	return p.OutputNames()[lIdx], p.OutputNames()[rIdx], nil
}

func buildWhereFuncDepend(p base.LogicalPlan, where ast.ExprNode) (map[*types.FieldName]*types.FieldName, error) {
	whereConditions := splitWhere(where)
	colDependMap := make(map[*types.FieldName]*types.FieldName, 2*len(whereConditions))
	for _, cond := range whereConditions {
		lCol, rCol, err := buildFuncDependCol(p, cond)
		if err != nil {
			return nil, err
		}
		if lCol == nil || rCol == nil {
			continue
		}
		colDependMap[lCol] = rCol
		colDependMap[rCol] = lCol
	}
	return colDependMap, nil
}

func (b *PlanBuilder) buildJoinFuncDepend(p base.LogicalPlan, from ast.ResultSetNode) (map[*types.FieldName]*types.FieldName, error) {
	switch x := from.(type) {
	case *ast.Join:
		if x.On == nil {
			return nil, nil
		}
		onConditions := splitWhere(x.On.Expr)
		colDependMap := make(map[*types.FieldName]*types.FieldName, len(onConditions))
		for _, cond := range onConditions {
			lCol, rCol, err := buildFuncDependCol(p, cond)
			if err != nil {
				return nil, err
			}
			if lCol == nil || rCol == nil {
				continue
			}
			lTbl := b.tblInfoFromCol(x.Left, lCol)
			if lTbl == nil {
				lCol, rCol = rCol, lCol
			}
			switch x.Tp {
			case ast.CrossJoin:
				colDependMap[lCol] = rCol
				colDependMap[rCol] = lCol
			case ast.LeftJoin:
				colDependMap[rCol] = lCol
			case ast.RightJoin:
				colDependMap[lCol] = rCol
			}
		}
		return colDependMap, nil
	default:
		return nil, nil
	}
}

func checkColFuncDepend(
	p base.LogicalPlan,
	name *types.FieldName,
	tblInfo *model.TableInfo,
	gbyOrSingleValueColNames map[*types.FieldName]struct{},
	whereDependNames, joinDependNames map[*types.FieldName]*types.FieldName,
) bool {
	for _, index := range tblInfo.Indices {
		if !index.Unique {
			continue
		}
		funcDepend := true
		// if all columns of some unique/pri indexes are determined, all columns left are check-passed.
		for _, indexCol := range index.Columns {
			iColInfo := tblInfo.Columns[indexCol.Offset]
			if !mysql.HasNotNullFlag(iColInfo.GetFlag()) {
				funcDepend = false
				break
			}
			cn := &ast.ColumnName{
				Schema: name.DBName,
				Table:  name.TblName,
				Name:   iColInfo.Name,
			}
			iIdx, err := expression.FindFieldName(p.OutputNames(), cn)
			if err != nil || iIdx < 0 {
				funcDepend = false
				break
			}
			iName := p.OutputNames()[iIdx]
			if _, ok := gbyOrSingleValueColNames[iName]; ok {
				continue
			}
			if wCol, ok := whereDependNames[iName]; ok {
				if _, ok = gbyOrSingleValueColNames[wCol]; ok {
					continue
				}
			}
			if jCol, ok := joinDependNames[iName]; ok {
				if _, ok = gbyOrSingleValueColNames[jCol]; ok {
					continue
				}
			}
			funcDepend = false
			break
		}
		if funcDepend {
			return true
		}
	}
	primaryFuncDepend := true
	hasPrimaryField := false
	for _, colInfo := range tblInfo.Columns {
		if !mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			continue
		}
		hasPrimaryField = true
		pkName := &ast.ColumnName{
			Schema: name.DBName,
			Table:  name.TblName,
			Name:   colInfo.Name,
		}
		pIdx, err := expression.FindFieldName(p.OutputNames(), pkName)
		// It is possible that `pIdx < 0` and here is a case.
		// ```
		// CREATE TABLE `BB` (
		//   `pk` int(11) NOT NULL AUTO_INCREMENT,
		//   `col_int_not_null` int NOT NULL,
		//   PRIMARY KEY (`pk`)
		// );
		//
		// SELECT OUTR . col2 AS X
		// FROM
		//   BB AS OUTR2
		// INNER JOIN
		//   (SELECT col_int_not_null AS col1,
		//     pk AS col2
		//   FROM BB) AS OUTR ON OUTR2.col_int_not_null = OUTR.col1
		// GROUP BY OUTR2.col_int_not_null;
		// ```
		// When we enter `checkColFuncDepend`, `pkName.Table` is `OUTR` which is an alias, while `pkName.Name` is `pk`
		// which is a original name. Hence `expression.FindFieldName` will fail and `pIdx` will be less than 0.
		// Currently, when we meet `pIdx < 0`, we directly regard `primaryFuncDepend` as false and jump out. This way is
		// easy to implement but makes only-full-group-by checker not smart enough. Later we will refactor only-full-group-by
		// checker and resolve the inconsistency between the alias table name and the original column name.
		if err != nil || pIdx < 0 {
			primaryFuncDepend = false
			break
		}
		pCol := p.OutputNames()[pIdx]
		if _, ok := gbyOrSingleValueColNames[pCol]; ok {
			continue
		}
		if wCol, ok := whereDependNames[pCol]; ok {
			if _, ok = gbyOrSingleValueColNames[wCol]; ok {
				continue
			}
		}
		if jCol, ok := joinDependNames[pCol]; ok {
			if _, ok = gbyOrSingleValueColNames[jCol]; ok {
				continue
			}
		}
		primaryFuncDepend = false
		break
	}
	return primaryFuncDepend && hasPrimaryField
}

// ErrExprLoc is for generate the ErrFieldNotInGroupBy error info
type ErrExprLoc struct {
	Offset int
	Loc    string
}

func checkExprInGroupByOrIsSingleValue(
	p base.LogicalPlan,
	expr ast.ExprNode,
	offset int,
	loc string,
	gbyOrSingleValueColNames map[*types.FieldName]struct{},
	gbyExprs []ast.ExprNode,
	notInGbyOrSingleValueColNames map[*types.FieldName]ErrExprLoc,
) {
	if _, ok := expr.(*ast.AggregateFuncExpr); ok {
		return
	}
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.Grouping {
			// just skip grouping function check here, because later in building plan phase, we
			// will do the grouping function valid check.
			return
		}
	}
	if _, ok := expr.(*ast.ColumnNameExpr); !ok {
		for _, gbyExpr := range gbyExprs {
			if ast.ExpressionDeepEqual(gbyExpr, expr) {
				return
			}
		}
	}
	// Function `any_value` can be used in aggregation, even `ONLY_FULL_GROUP_BY` is set.
	// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_any-value for details
	if f, ok := expr.(*ast.FuncCallExpr); ok {
		if f.FnName.L == ast.AnyValue {
			return
		}
	}
	colMap := make(map[*types.FieldName]struct{}, len(p.Schema().Columns))
	allColFromExprNode(p, expr, colMap)
	for col := range colMap {
		if _, ok := gbyOrSingleValueColNames[col]; !ok {
			notInGbyOrSingleValueColNames[col] = ErrExprLoc{Offset: offset, Loc: loc}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupBy(p base.LogicalPlan, sel *ast.SelectStmt) (err error) {
	if sel.GroupBy != nil {
		err = b.checkOnlyFullGroupByWithGroupClause(p, sel)
	} else {
		err = b.checkOnlyFullGroupByWithOutGroupClause(p, sel)
	}
	return err
}

func addGbyOrSingleValueColName(p base.LogicalPlan, colName *ast.ColumnName, gbyOrSingleValueColNames map[*types.FieldName]struct{}) {
	idx, err := expression.FindFieldName(p.OutputNames(), colName)
	if err != nil || idx < 0 {
		return
	}
	gbyOrSingleValueColNames[p.OutputNames()[idx]] = struct{}{}
}

func extractSingeValueColNamesFromWhere(p base.LogicalPlan, where ast.ExprNode, gbyOrSingleValueColNames map[*types.FieldName]struct{}) {
	whereConditions := splitWhere(where)
	for _, cond := range whereConditions {
		binOpExpr, ok := cond.(*ast.BinaryOperationExpr)
		if !ok || binOpExpr.Op != opcode.EQ {
			continue
		}
		if colExpr, ok := binOpExpr.L.(*ast.ColumnNameExpr); ok {
			// a = value
			if _, ok := binOpExpr.R.(ast.ValueExpr); ok {
				addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
			}
			// a = - value
			if u, ok := binOpExpr.R.(*ast.UnaryOperationExpr); ok {
				if _, ok := u.V.(ast.ValueExpr); ok {
					addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
				}
			}
		} else if colExpr, ok := binOpExpr.R.(*ast.ColumnNameExpr); ok {
			// value = a
			if _, ok := binOpExpr.L.(ast.ValueExpr); ok {
				addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
			}
			// - value = a
			if u, ok := binOpExpr.L.(*ast.UnaryOperationExpr); ok {
				if _, ok := u.V.(ast.ValueExpr); ok {
					addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
				}
			}
		}
	}
}

func (b *PlanBuilder) checkOnlyFullGroupByWithGroupClause(p base.LogicalPlan, sel *ast.SelectStmt) error {
	gbyOrSingleValueColNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	gbyExprs := make([]ast.ExprNode, 0, len(sel.Fields.Fields))
	for _, byItem := range sel.GroupBy.Items {
		expr := getInnerFromParenthesesAndUnaryPlus(byItem.Expr)
		if colExpr, ok := expr.(*ast.ColumnNameExpr); ok {
			addGbyOrSingleValueColName(p, colExpr.Name, gbyOrSingleValueColNames)
		} else {
			gbyExprs = append(gbyExprs, expr)
		}
	}
	// MySQL permits a nonaggregate column not named in a GROUP BY clause when ONLY_FULL_GROUP_BY SQL mode is enabled,
	// provided that this column is limited to a single value.
	// See https://dev.mysql.com/doc/refman/5.7/en/group-by-handling.html for details.
	extractSingeValueColNamesFromWhere(p, sel.Where, gbyOrSingleValueColNames)

	notInGbyOrSingleValueColNames := make(map[*types.FieldName]ErrExprLoc, len(sel.Fields.Fields))
	for offset, field := range sel.Fields.Fields {
		if field.Auxiliary {
			continue
		}
		checkExprInGroupByOrIsSingleValue(p, getInnerFromParenthesesAndUnaryPlus(field.Expr), offset, ErrExprInSelect, gbyOrSingleValueColNames, gbyExprs, notInGbyOrSingleValueColNames)
	}

	if sel.OrderBy != nil {
		for offset, item := range sel.OrderBy.Items {
			if colName, ok := item.Expr.(*ast.ColumnNameExpr); ok {
				index, err := resolveFromSelectFields(colName, sel.Fields.Fields, false)
				if err != nil {
					return err
				}
				// If the ByItem is in fields list, it has been checked already in above.
				if index >= 0 {
					continue
				}
			}
			checkExprInGroupByOrIsSingleValue(p, getInnerFromParenthesesAndUnaryPlus(item.Expr), offset, ErrExprInOrderBy, gbyOrSingleValueColNames, gbyExprs, notInGbyOrSingleValueColNames)
		}
	}
	if len(notInGbyOrSingleValueColNames) == 0 {
		return nil
	}

	whereDepends, err := buildWhereFuncDepend(p, sel.Where)
	if err != nil {
		return err
	}
	joinDepends, err := b.buildJoinFuncDepend(p, sel.From.TableRefs)
	if err != nil {
		return err
	}
	tblMap := make(map[*model.TableInfo]struct{}, len(notInGbyOrSingleValueColNames))
	for name, errExprLoc := range notInGbyOrSingleValueColNames {
		tblInfo := b.tblInfoFromCol(sel.From.TableRefs, name)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, name, tblInfo, gbyOrSingleValueColNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		switch errExprLoc.Loc {
		case ErrExprInSelect:
			if sel.GroupBy.Rollup {
				return plannererrors.ErrFieldInGroupingNotGroupBy.GenWithStackByArgs(strconv.Itoa(errExprLoc.Offset + 1))
			}
			return plannererrors.ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, name.DBName.O+"."+name.TblName.O+"."+name.OrigColName.O)
		case ErrExprInOrderBy:
			return plannererrors.ErrFieldNotInGroupBy.GenWithStackByArgs(errExprLoc.Offset+1, errExprLoc.Loc, sel.OrderBy.Items[errExprLoc.Offset].Expr.Text())
		}
		return nil
	}
	return nil
}

func (b *PlanBuilder) checkOnlyFullGroupByWithOutGroupClause(p base.LogicalPlan, sel *ast.SelectStmt) error {
	resolver := colResolverForOnlyFullGroupBy{
		firstOrderByAggColIdx: -1,
	}
	resolver.curClause = fieldList
	for idx, field := range sel.Fields.Fields {
		resolver.exprIdx = idx
		field.Accept(&resolver)
	}
	if len(resolver.nonAggCols) > 0 {
		if sel.Having != nil {
			sel.Having.Expr.Accept(&resolver)
		}
		if sel.OrderBy != nil {
			resolver.curClause = orderByClause
			for idx, byItem := range sel.OrderBy.Items {
				resolver.exprIdx = idx
				byItem.Expr.Accept(&resolver)
			}
		}
	}
	if resolver.firstOrderByAggColIdx != -1 && len(resolver.nonAggCols) > 0 {
		// SQL like `select a from t where a = 1 order by count(b)` is illegal.
		return plannererrors.ErrAggregateOrderNonAggQuery.GenWithStackByArgs(resolver.firstOrderByAggColIdx + 1)
	}
	if !resolver.hasAggFuncOrAnyValue || len(resolver.nonAggCols) == 0 {
		return nil
	}
	singleValueColNames := make(map[*types.FieldName]struct{}, len(sel.Fields.Fields))
	extractSingeValueColNamesFromWhere(p, sel.Where, singleValueColNames)
	whereDepends, err := buildWhereFuncDepend(p, sel.Where)
	if err != nil {
		return err
	}

	joinDepends, err := b.buildJoinFuncDepend(p, sel.From.TableRefs)
	if err != nil {
		return err
	}
	tblMap := make(map[*model.TableInfo]struct{}, len(resolver.nonAggCols))
	for i, colName := range resolver.nonAggCols {
		idx, err := expression.FindFieldName(p.OutputNames(), colName)
		if err != nil || idx < 0 {
			return plannererrors.ErrMixOfGroupFuncAndFields.GenWithStackByArgs(resolver.nonAggColIdxs[i]+1, colName.Name.O)
		}
		fieldName := p.OutputNames()[idx]
		if _, ok := singleValueColNames[fieldName]; ok {
			continue
		}
		tblInfo := b.tblInfoFromCol(sel.From.TableRefs, fieldName)
		if tblInfo == nil {
			continue
		}
		if _, ok := tblMap[tblInfo]; ok {
			continue
		}
		if checkColFuncDepend(p, fieldName, tblInfo, singleValueColNames, whereDepends, joinDepends) {
			tblMap[tblInfo] = struct{}{}
			continue
		}
		return plannererrors.ErrMixOfGroupFuncAndFields.GenWithStackByArgs(resolver.nonAggColIdxs[i]+1, colName.Name.O)
	}
	return nil
}

// colResolverForOnlyFullGroupBy visits Expr tree to find out if an Expr tree is an aggregation function.
// If so, find out the first column name that not in an aggregation function.
type colResolverForOnlyFullGroupBy struct {
	nonAggCols            []*ast.ColumnName
	exprIdx               int
	nonAggColIdxs         []int
	hasAggFuncOrAnyValue  bool
	firstOrderByAggColIdx int
	curClause             clauseCode
}

func (c *colResolverForOnlyFullGroupBy) Enter(node ast.Node) (ast.Node, bool) {
	switch t := node.(type) {
	case *ast.AggregateFuncExpr:
		c.hasAggFuncOrAnyValue = true
		if c.curClause == orderByClause {
			c.firstOrderByAggColIdx = c.exprIdx
		}
		return node, true
	case *ast.FuncCallExpr:
		// enable function `any_value` in aggregation even `ONLY_FULL_GROUP_BY` is set
		if t.FnName.L == ast.AnyValue {
			c.hasAggFuncOrAnyValue = true
			return node, true
		}
	case *ast.ColumnNameExpr:
		c.nonAggCols = append(c.nonAggCols, t.Name)
		c.nonAggColIdxs = append(c.nonAggColIdxs, c.exprIdx)
		return node, true
	case *ast.SubqueryExpr:
		return node, true
	}
	return node, false
}

func (*colResolverForOnlyFullGroupBy) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

type aggColNameResolver struct {
	colNameResolver
}

func (*aggColNameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	if _, ok := inNode.(*ast.ColumnNameExpr); ok {
		return inNode, true
	}
	return inNode, false
}

func allColFromAggExprNode(p base.LogicalPlan, n ast.Node, names map[*types.FieldName]struct{}) {
	extractor := &aggColNameResolver{
		colNameResolver: colNameResolver{
			p:     p,
			names: names,
		},
	}
	n.Accept(extractor)
}

type colNameResolver struct {
	p     base.LogicalPlan
	names map[*types.FieldName]struct{}
}

func (*colNameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	switch inNode.(type) {
	case *ast.ColumnNameExpr, *ast.SubqueryExpr, *ast.AggregateFuncExpr:
		return inNode, true
	}
	return inNode, false
}

func (c *colNameResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	if v, ok := inNode.(*ast.ColumnNameExpr); ok {
		idx, err := expression.FindFieldName(c.p.OutputNames(), v.Name)
		if err == nil && idx >= 0 {
			c.names[c.p.OutputNames()[idx]] = struct{}{}
		}
	}
	return inNode, true
}

func allColFromExprNode(p base.LogicalPlan, n ast.Node, names map[*types.FieldName]struct{}) {
	extractor := &colNameResolver{
		p:     p,
		names: names,
	}
	n.Accept(extractor)
}

// resolveGbyExprs resolves group by expressions from the group by clause of a select statement.
// The returned `[]ast.Node` may differ from the original `gby.Items` in the group by clause for params. For params, the
// `gby.Items[].Expr` will not be overwritten. However, the resolved expression is still needed for further processing, so
// it's returned out.
func (b *PlanBuilder) resolveGbyExprs(p base.LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) ([]ast.ExprNode, error) {
	b.curClause = groupByClause
	exprs := make([]ast.ExprNode, 0, len(gby.Items))
	schema := p.Schema()
	names := p.OutputNames()
	if join, ok := p.(*logicalop.LogicalJoin); ok && join.FullSchema != nil {
		schema = join.FullSchema
		names = join.FullNames
	}
	resolver := &gbyResolver{
		ctx:        b.ctx,
		fields:     fields,
		schema:     schema,
		names:      names,
		skipAggMap: b.correlatedAggMapper,
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		resolver.exprDepth = 0
		resolver.isParam = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return exprs, errors.Trace(resolver.err)
		}
		if !resolver.isParam {
			item.Expr = retExpr.(ast.ExprNode)
		}

		exprs = append(exprs, retExpr.(ast.ExprNode))
	}
	return exprs, nil
}

func (b *PlanBuilder) rewriteGbyExprs(ctx context.Context, p base.LogicalPlan, gby *ast.GroupByClause, items []ast.ExprNode) (base.LogicalPlan, []expression.Expression, bool, error) {
	exprs := make([]expression.Expression, 0, len(gby.Items))

	for _, item := range items {
		expr, np, err := b.rewrite(ctx, item, p, nil, true)
		if err != nil {
			return nil, nil, false, err
		}

		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs, gby.Rollup, nil
}

func (*PlanBuilder) unfoldWildStar(p base.LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	join, isJoin := p.(*logicalop.LogicalJoin)
	resultList = make([]*ast.SelectField, 0, max(2, len(selectFields)))
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			return nil, plannererrors.ErrInvalidWildCard
		}
		list := unfoldWildStar(field, p.OutputNames(), p.Schema().Columns)
		// For sql like `select t1.*, t2.* from t1 join t2 using(a)` or `select t1.*, t2.* from t1 natual join t2`,
		// the schema of the Join doesn't contain enough columns because the join keys are coalesced in this schema.
		// We should collect the columns from the FullSchema.
		if isJoin && join.FullSchema != nil && field.WildCard.Table.L != "" {
			list = unfoldWildStar(field, join.FullNames, join.FullSchema.Columns)
		}
		if len(list) == 0 {
			return nil, plannererrors.ErrBadTable.GenWithStackByArgs(field.WildCard.Table)
		}
		resultList = append(resultList, list...)
	}
	return resultList, nil
}

func unfoldWildStar(field *ast.SelectField, outputName types.NameSlice, column []*expression.Column) (resultList []*ast.SelectField) {
	dbName := field.WildCard.Schema
	tblName := field.WildCard.Table
	for i, name := range outputName {
		col := column[i]
		if col.IsHidden {
			continue
		}
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			col.ID != model.ExtraHandleID && col.ID != model.ExtraPhysTblID && col.ID != model.ExtraCommitTSID {
			colName := &ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Schema: name.DBName,
					Table:  name.TblName,
					Name:   name.ColName,
				}}
			colName.SetType(col.GetStaticType())
			field := &ast.SelectField{Expr: colName}
			field.SetText(nil, name.ColName.O)
			resultList = append(resultList, field)
		}
	}
	return resultList
}

func (b *PlanBuilder) addAliasName(ctx context.Context, selectStmt *ast.SelectStmt, p base.LogicalPlan) (resultList []*ast.SelectField, err error) {
	selectFields := selectStmt.Fields.Fields
	projOutNames := make([]*types.FieldName, 0, len(selectFields))
	for _, field := range selectFields {
		colNameField, isColumnNameExpr := field.Expr.(*ast.ColumnNameExpr)
		if isColumnNameExpr {
			colName := colNameField.Name.Name
			if field.AsName.L != "" {
				colName = field.AsName
			}
			projOutNames = append(projOutNames, &types.FieldName{
				TblName:     colNameField.Name.Table,
				OrigTblName: colNameField.Name.Table,
				ColName:     colName,
				OrigColName: colNameField.Name.Name,
				DBName:      colNameField.Name.Schema,
			})
		} else {
			// create view v as select name_const('col', 100);
			// The column in v should be 'col', so we call `buildProjectionField` to handle this.
			_, name, err := b.buildProjectionField(ctx, p, field, nil)
			if err != nil {
				return nil, err
			}
			projOutNames = append(projOutNames, name)
		}
	}

	// dedupMap is used for renaming a duplicated anonymous column
	dedupMap := make(map[string]int)
	anonymousFields := make([]bool, len(selectFields))

	for i, field := range selectFields {
		newField := *field
		if newField.AsName.L == "" {
			newField.AsName = projOutNames[i].ColName
		}

		if _, ok := field.Expr.(*ast.ColumnNameExpr); !ok && field.AsName.L == "" {
			anonymousFields[i] = true
		} else {
			anonymousFields[i] = false
			// dedupMap should be inited with all non-anonymous fields before renaming other duplicated anonymous fields
			dedupMap[newField.AsName.L] = 0
		}

		resultList = append(resultList, &newField)
	}

	// We should rename duplicated anonymous fields in the first SelectStmt of CreateViewStmt
	// See: https://github.com/pingcap/tidb/issues/29326
	if selectStmt.AsViewSchema {
		for i, field := range resultList {
			if !anonymousFields[i] {
				continue
			}

			oldName := field.AsName
			if dup, ok := dedupMap[field.AsName.L]; ok {
				if dup == 0 {
					field.AsName = ast.NewCIStr(fmt.Sprintf("Name_exp_%s", field.AsName.O))
				} else {
					field.AsName = ast.NewCIStr(fmt.Sprintf("Name_exp_%d_%s", dup, field.AsName.O))
				}
				dedupMap[oldName.L] = dup + 1
			} else {
				dedupMap[oldName.L] = 0
			}
		}
	}

	return resultList, nil
}

func (b *PlanBuilder) pushTableHints(hints []*ast.TableOptimizerHint, currentLevel int) {
	hints = b.hintProcessor.GetCurrentStmtHints(hints, currentLevel)
	sessionVars := b.ctx.GetSessionVars()
	currentDB := sessionVars.CurrentDB
	warnHandler := sessionVars.StmtCtx
	planHints, subQueryHintFlags, err := h.ParsePlanHints(hints, currentLevel, currentDB,
		b.hintProcessor, sessionVars.StmtCtx.StraightJoinOrder,
		b.subQueryCtx == handlingInSubquery,
		b.subQueryCtx == handlingExistsSubquery, b.subQueryCtx == notHandlingSubquery, warnHandler)
	if err != nil {
		return
	}
	b.tableHintInfo = append(b.tableHintInfo, planHints)
	b.subQueryHintFlags |= subQueryHintFlags
}

func (b *PlanBuilder) popVisitInfo() {
	if len(b.visitInfo) == 0 {
		return
	}
	b.visitInfo = b.visitInfo[:len(b.visitInfo)-1]
}

func (b *PlanBuilder) popTableHints() {
	hintInfo := b.tableHintInfo[len(b.tableHintInfo)-1]
	for _, warning := range h.CollectUnmatchedHintWarnings(hintInfo) {
		b.ctx.GetSessionVars().StmtCtx.SetHintWarning(warning)
	}
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

// TableHints returns the *TableHintInfo of PlanBuilder.
func (b *PlanBuilder) TableHints() *h.PlanHints {
	if len(b.tableHintInfo) == 0 {
		return nil
	}
	return b.tableHintInfo[len(b.tableHintInfo)-1]
}

func (b *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p base.LogicalPlan, err error) {
	b.pushSelectOffset(sel.QueryBlockOffset)
	b.pushTableHints(sel.TableHints, sel.QueryBlockOffset)
	defer func() {
		b.popSelectOffset()
		// table hints are only visible in the current SELECT statement.
		b.popTableHints()
	}()
	if b.buildingRecursivePartForCTE {
		if sel.Distinct || sel.OrderBy != nil || sel.Limit != nil {
			return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("ORDER BY / LIMIT / SELECT DISTINCT in recursive query block of Common Table Expression")
		}
		if sel.GroupBy != nil {
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.FastGenByArgs(b.genCTETableNameForError())
		}
	}
	// Handle STRAIGHT_JOIN - both keyword and hint forms
	straightJoinFromKeyword := sel.SelectStmtOpts != nil && sel.SelectStmtOpts.StraightJoin
	straightJoinFromHint := false
	if hints := b.TableHints(); hints != nil {
		straightJoinFromHint = hints.StraightJoinOrder
	}
	if straightJoinFromKeyword || straightJoinFromHint {
		origin := b.inStraightJoin
		b.inStraightJoin = true
		defer func() { b.inStraightJoin = origin }()
	}

	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		windowAggMap                  map[*ast.AggregateFuncExpr]int
		correlatedAggMap              map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
		projExprs                     []expression.Expression
		rollup                        bool
	)

	// set for update read to true before building result set node
	if isForUpdateReadSelectLock(sel.LockInfo) {
		b.isForUpdateRead = true
	}

	if hints := b.TableHints(); hints != nil && hints.CTEMerge {
		// Verify Merge hints in the current query,
		// we will update parameters for those that meet the rules, and warn those that do not.
		// If the current query uses Merge Hint and the query is a CTE,
		// we update the HINT information for the current query.
		// If the current query is not a CTE query (it may be a subquery within a CTE query
		// or an external non-CTE query), we will give a warning.
		// In particular, recursive CTE have separate warnings, so they are no longer called.
		if b.buildingCTE {
			if b.isCTE {
				b.outerCTEs[len(b.outerCTEs)-1].forceInlineByHintOrVar = true
			} else if !b.buildingRecursivePartForCTE {
				// If there has subquery which is not CTE and using `MERGE()` hint, we will show this warning;
				b.ctx.GetSessionVars().StmtCtx.SetHintWarning(
					"Hint merge() is inapplicable. " +
						"Please check whether the hint is used in the right place, " +
						"you should use this hint inside the CTE.")
			}
		} else if !b.buildingCTE && !b.isCTE {
			b.ctx.GetSessionVars().StmtCtx.SetHintWarning(
				"Hint merge() is inapplicable. " +
					"Please check whether the hint is used in the right place, " +
					"you should use this hint inside the CTE.")
		}
	}

	var currentLayerCTEs []*cteInfo
	if sel.With != nil {
		l := len(b.outerCTEs)
		defer func() {
			b.outerCTEs = b.outerCTEs[:l]
		}()
		currentLayerCTEs, err = b.buildWith(ctx, sel.With)
		if err != nil {
			return nil, err
		}
	}

	p, err = b.buildTableRefs(ctx, sel.From)
	if err != nil {
		return nil, err
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}
	if b.capFlag&canExpandAST != 0 {
		// To be compatible with MySQL, we add alias name for each select field when creating view.
		sel.Fields.Fields, err = b.addAliasName(ctx, sel, p)
		if err != nil {
			return nil, err
		}
		originalFields = sel.Fields.Fields
	}

	var gbyExprs []ast.ExprNode
	if sel.GroupBy != nil {
		gbyExprs, err = b.resolveGbyExprs(p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			return nil, err
		}
	}

	// `checkOnlyFullGroupBy` should be executed before rewrite gbyExprs, because the field type of the fields
	// may change. For example, the length of a string field may change in `adjustRetFtForCastString`
	if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() && sel.From != nil && !b.ctx.GetSessionVars().OptimizerEnableNewOnlyFullGroupByCheck {
		err = b.checkOnlyFullGroupBy(p, sel)
		if err != nil {
			return nil, err
		}
	}

	if sel.GroupBy != nil {
		p, gbyCols, rollup, err = b.rewriteGbyExprs(ctx, p, sel.GroupBy, gbyExprs)
		if err != nil {
			return nil, err
		}
	}

	hasWindowFuncField := b.detectSelectWindow(sel)
	// Some SQL statements define WINDOW but do not use them. But we also need to check the window specification list.
	// For example: select id from t group by id WINDOW w AS (ORDER BY uids DESC) ORDER BY id;
	// We don't use the WINDOW w, but if the 'uids' column is not in the table t, we still need to report an error.
	if hasWindowFuncField || sel.WindowSpecs != nil {
		if b.buildingRecursivePartForCTE {
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.FastGenByArgs(b.genCTETableNameForError())
		}

		windowAggMap, err = b.resolveWindowFunction(sel, p)
		if err != nil {
			return nil, err
		}
	}
	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(ctx, sel, p)
	if err != nil {
		return nil, err
	}

	// We have to resolve correlated aggregate inside sub-queries before building aggregation and building projection,
	// for instance, count(a) inside the sub-query of "select (select count(a)) from t" should be evaluated within
	// the context of the outer query. So we have to extract such aggregates from sub-queries and put them into
	// SELECT field list.
	correlatedAggMap, err = b.resolveCorrelatedAggregates(ctx, sel, p)
	if err != nil {
		return nil, err
	}

	// b.allNames will be used in evalDefaultExpr(). Default function is special because it needs to find the
	// corresponding column name, but does not need the value in the column.
	// For example, select a from t order by default(b), the column b will not be in select fields. Also because
	// buildSort is after buildProjection, so we need get OutputNames before BuildProjection and store in allNames.
	// Otherwise, we will get select fields instead of all OutputNames, so that we can't find the column b in the
	// above example.
	b.allNames = append(b.allNames, p.OutputNames())
	defer func() { b.allNames = b.allNames[:len(b.allNames)-1] }()

	if sel.Where != nil {
		p, err = b.buildSelection(ctx, p, sel.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	l := sel.LockInfo
	if l != nil && l.LockType != ast.SelectLockNone {
		var tableList []*ast.TableName
		var isExplicitSetTablesNames bool
		if len(l.Tables) == 0 && sel.From != nil {
			// It's for stmt like `select xxx from table t, t1 where t.a = t1.a for update`
			nodeW := resolve.NewNodeWWithCtx(sel.From, b.resolveCtx)
			tableList = ExtractTableList(nodeW, false)
		} else if len(l.Tables) != 0 {
			// It's for stmt like `select xxx from table t, t1 where t.a = t1.a for update of t`
			isExplicitSetTablesNames = true
			tableList = l.Tables
		}
		for _, tName := range tableList {
			// CTE has no *model.HintedTable, we need to skip it.
			tNameW := b.resolveCtx.GetTableName(tName)
			if tNameW == nil {
				continue
			}
			if isExplicitSetTablesNames {
				// If `LockTableIDs` map is empty, it will lock all records from all tables.
				// Besides, it will only lock the metioned in `of` part.
				b.ctx.GetSessionVars().StmtCtx.LockTableIDs[tNameW.TableInfo.ID] = struct{}{}
			}
			dbName := getLowerDB(tName.Schema, b.ctx.GetSessionVars())
			var authErr error
			if user := b.ctx.GetSessionVars().User; user != nil {
				authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT with locking clause", user.AuthUsername, user.AuthHostname, tNameW.Name.L)
			}
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.DeletePriv|mysql.UpdatePriv|mysql.LockTablesPriv, dbName, tNameW.Name.L, "", authErr)
		}
		p, err = b.buildSelectLock(p, l)
		if err != nil {
			return nil, err
		}
	}
	b.handleHelper.popMap()
	b.handleHelper.pushMap(nil)

	hasAgg := b.detectSelectAgg(sel)
	needBuildAgg := hasAgg
	if hasAgg {
		if b.buildingRecursivePartForCTE {
			return nil, plannererrors.ErrCTERecursiveForbidsAggregation.GenWithStackByArgs(b.genCTETableNameForError())
		}

		aggFuncs, totalMap = b.extractAggFuncsInSelectFields(sel.Fields.Fields)
		// len(aggFuncs) == 0 and sel.GroupBy == nil indicates that all the aggregate functions inside the SELECT fields
		// are actually correlated aggregates from the outer query, which have already been built in the outer query.
		// The only thing we need to do is to find them from b.correlatedAggMap in buildProjection.
		if len(aggFuncs) == 0 && sel.GroupBy == nil {
			needBuildAgg = false
		}
	}
	if needBuildAgg {
		// if rollup syntax is specified, Expand OP is required to replicate the data to feed different grouping layout.
		if rollup {
			p, gbyCols, err = b.buildExpand(p, gbyCols)
			if err != nil {
				return nil, err
			}
		}
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(ctx, p, aggFuncs, gbyCols, correlatedAggMap)
		if err != nil {
			return nil, err
		}
		for agg, idx := range totalMap {
			totalMap[agg] = aggIndexMap[idx]
		}
	}

	var oldLen int
	// According to https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html,
	// we can only process window functions after having clause, so `considerWindow` is false now.
	p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, totalMap, nil, false, sel.OrderBy != nil)
	if err != nil {
		return nil, err
	}

	if sel.Having != nil {
		b.curClause = havingClause
		p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
		if err != nil {
			return nil, err
		}
	}

	b.windowSpecs, err = buildWindowSpecs(sel.WindowSpecs)
	if err != nil {
		return nil, err
	}

	var windowMapper map[*ast.WindowFuncExpr]int
	if hasWindowFuncField || sel.WindowSpecs != nil {
		windowFuncs := extractWindowFuncs(sel.Fields.Fields)
		// we need to check the func args first before we check the window spec
		err := b.checkWindowFuncArgs(ctx, p, windowFuncs, windowAggMap)
		if err != nil {
			return nil, err
		}
		groupedFuncs, orderedSpec, err := b.groupWindowFuncs(windowFuncs)
		if err != nil {
			return nil, err
		}
		p, windowMapper, err = b.buildWindowFunctions(ctx, p, groupedFuncs, orderedSpec, windowAggMap)
		if err != nil {
			return nil, err
		}
		// `hasWindowFuncField == false` means there's only unused named window specs without window functions.
		// In such case plan `p` is not changed, so we don't have to build another projection.
		if hasWindowFuncField {
			// Now we build the window function fields.
			p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, windowAggMap, windowMapper, true, false)
			if err != nil {
				return nil, err
			}
		}
	}

	if sel.Distinct {
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		// flag it if cte contain order by
		if b.buildingCTE {
			b.outerCTEs[len(b.outerCTEs)-1].containRecursiveForbiddenOperator = true
		}
		// We need to keep the ORDER BY clause for the following cases:
		// 1. The select is top level query, order should be honored
		// 2. The query has LIMIT clause
		// 3. The control flag requires keeping ORDER BY explicitly
		if len(b.qbOffset) == 1 || sel.Limit != nil || !b.ctx.GetSessionVars().RemoveOrderbyInSubquery {
			if b.ctx.GetSessionVars().SQLMode.HasOnlyFullGroupBy() {
				p, err = b.buildSortWithCheck(ctx, p, sel.OrderBy.Items, orderMap, windowMapper, projExprs, oldLen, sel.Distinct)
			} else {
				p, err = b.buildSort(ctx, p, sel.OrderBy.Items, orderMap, windowMapper)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, err
		}
	}

	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx, b.getSelectOffset())
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.SetOutputNames(p.OutputNames()[:oldLen])
		proj.SetSchema(schema)
		return b.tryToBuildSequence(currentLayerCTEs, proj), nil
	}

	return b.tryToBuildSequence(currentLayerCTEs, p), nil
}

func (b *PlanBuilder) tryToBuildSequence(ctes []*cteInfo, p base.LogicalPlan) base.LogicalPlan {
	if !b.ctx.GetSessionVars().EnableMPPSharedCTEExecution {
		return p
	}
	for i := len(ctes) - 1; i >= 0; i-- {
		if !ctes[i].nonRecursive {
			return p
		}
		if ctes[i].isInline || ctes[i].cteClass == nil {
			ctes = slices.Delete(ctes, i, i+1)
		}
	}
	if len(ctes) == 0 {
		return p
	}
	lctes := make([]base.LogicalPlan, 0, len(ctes)+1)
	for _, cte := range ctes {
		lcte := logicalop.LogicalCTE{
			Cte:               cte.cteClass,
			CteAsName:         cte.def.Name,
			CteName:           cte.def.Name,
			SeedStat:          cte.seedStat,
			OnlyUsedAsStorage: true,
		}.Init(b.ctx, b.getSelectOffset())
		lcte.SetSchema(getResultCTESchema(cte.seedLP.Schema(), b.ctx.GetSessionVars()))
		lctes = append(lctes, lcte)
	}
	b.optFlag |= rule.FlagPushDownSequence
	seq := logicalop.LogicalSequence{}.Init(b.ctx, b.getSelectOffset())
	seq.SetChildren(append(lctes, p)...)
	seq.SetOutputNames(p.OutputNames().Shallow())
	return seq
}

func (b *PlanBuilder) buildTableDual() *logicalop.LogicalTableDual {
	b.handleHelper.pushMap(nil)
	return logicalop.LogicalTableDual{RowCount: 1}.Init(b.ctx, b.getSelectOffset())
}
