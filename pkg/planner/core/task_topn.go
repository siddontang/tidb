// Copyright 2017 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
)

func getPushedDownTopN(p *physicalop.PhysicalTopN, childPlan base.PhysicalPlan, storeTp kv.StoreType) (topN, newGlobalTopN *physicalop.PhysicalTopN) {
	fixValue := fixcontrol.GetBoolWithDefault(p.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix56318, true)
	// HeavyFunctionOptimize: if TopN's ByItems is a HeavyFunction (currently mainly for Vector Search), we will change
	// the ByItems in order to reuse the function result.
	byItemIndex := make([]int, 0)
	for i, byItem := range p.ByItems {
		if ContainHeavyFunction(byItem.Expr) {
			byItemIndex = append(byItemIndex, i)
		}
	}
	if fixValue && len(byItemIndex) > 0 {
		x, err := p.Clone(p.SCtx())
		if err != nil {
			return nil, nil
		}
		newGlobalTopN = x.(*physicalop.PhysicalTopN)
		// the projecton's construction cannot be create if the AllowProjectionPushDown is disable.
		if storeTp == kv.TiKV && !p.SCtx().GetSessionVars().AllowProjectionPushDown {
			newGlobalTopN = nil
		}
	}
	newByItems := make([]*util.ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}
	newCount := p.Offset + p.Count
	childProfile := childPlan.StatsInfo()
	// Strictly speaking, for the row count of pushed down TopN, we should multiply newCount with "regionNum",
	// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
	stats := property.DeriveLimitStats(childProfile, float64(newCount))

	// Add a extra physicalProjection to save the distance column, a example like :
	// select id from t order by vec_distance(vec, '[1,2,3]') limit x
	// The Plan will be modified like:
	//
	// Original: DataSource(id, vec) -> TopN(by vec->dis) -> Projection(id)
	//                                  └─Byitem: vec_distance(vec, '[1,2,3]')
	//
	// New:      DataSource(id, vec) -> Projection(id, vec->dis) -> TopN(by dis) -> Projection(id)
	//                                  └─Byitem: dis
	//
	// Note that for plan now, TopN has its own schema and does not use the schema of children.
	if newGlobalTopN != nil {
		// create a new PhysicalProjection to calculate the distance columns, and add it into plan route
		bottomProjSchemaCols := make([]*expression.Column, 0, len(childPlan.Schema().Columns))
		bottomProjExprs := make([]expression.Expression, 0, len(childPlan.Schema().Columns))
		for _, col := range newGlobalTopN.Schema().Columns {
			newCol := col.Clone().(*expression.Column)
			bottomProjSchemaCols = append(bottomProjSchemaCols, newCol)
			bottomProjExprs = append(bottomProjExprs, newCol)
		}
		type DistanceColItem struct {
			Index       int
			DistanceCol *expression.Column
		}
		distanceCols := make([]DistanceColItem, 0)
		for _, idx := range byItemIndex {
			bottomProjExprs = append(bottomProjExprs, newGlobalTopN.ByItems[idx].Expr)
			distanceCol := &expression.Column{
				UniqueID: newGlobalTopN.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  newGlobalTopN.ByItems[idx].Expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
			}
			distanceCols = append(distanceCols, DistanceColItem{
				Index:       idx,
				DistanceCol: distanceCol,
			})
		}
		for _, dis := range distanceCols {
			bottomProjSchemaCols = append(bottomProjSchemaCols, dis.DistanceCol)
		}

		bottomProj := physicalop.PhysicalProjection{
			Exprs: bottomProjExprs,
		}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
		bottomProj.SetSchema(expression.NewSchema(bottomProjSchemaCols...))
		bottomProj.SetChildren(childPlan)

		topN := physicalop.PhysicalTopN{
			ByItems:     newByItems,
			PartitionBy: newPartitionBy,
			Count:       newCount,
		}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
		// mppTask's topN
		for _, item := range distanceCols {
			topN.ByItems[item.Index].Expr = item.DistanceCol
		}

		// rootTask's topn, need reuse the distance col
		for _, expr := range distanceCols {
			newGlobalTopN.ByItems[expr.Index].Expr = expr.DistanceCol
		}
		topN.SetChildren(bottomProj)

		// orderByCol is the column `distanceCol`, so this explain always success.
		orderByCol, _ := topN.ByItems[0].Expr.(*expression.Column)
		orderByCol.Index = len(bottomProj.Exprs) - 1

		// try to Check and modify plan when it is possible to not scanning vector column at all.
		tryReturnDistanceFromIndex(topN, newGlobalTopN, childPlan, bottomProj)

		return topN, newGlobalTopN
	}

	topN = physicalop.PhysicalTopN{
		ByItems:     newByItems,
		PartitionBy: newPartitionBy,
		Count:       newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset(), p.GetChildReqProps(0))
	topN.SetChildren(childPlan)
	return topN, newGlobalTopN
}

// tryReturnDistanceFromIndex checks whether the vector in the plan can be removed and a distance column will be added.
// Consider this situation sql statement: select id from t order by vec_distance(vec, '[1,2,3]') limit x
// The plan like:
//
// DataSource(id, vec) -> Projection1(id, vec->dis) -> TopN(by dis) -> Projection2(id)
// └─Schema: id, vec
//
// In vector index, the distance result already exists, so there is no need to calculate it again in projection1.
// We can directly read the distance result. After this Optimization, the plan will be modified to:
//
// DataSource(id, dis) -> TopN(by dis) -> Projection2(id)
// └─Schema: id, dis
func tryReturnDistanceFromIndex(local, global *physicalop.PhysicalTopN, childPlan base.PhysicalPlan, proj *physicalop.PhysicalProjection) bool {
	tableScan, ok := childPlan.(*physicalop.PhysicalTableScan)
	if !ok {
		return false
	}

	orderByCol, _ := local.ByItems[0].Expr.(*expression.Column)
	var annQueryInfo *physicalop.ColumnarIndexExtra
	for _, idx := range tableScan.UsedColumnarIndexes {
		if idx != nil && idx.QueryInfo.IndexType == tipb.ColumnarIndexType_TypeVector && idx.QueryInfo != nil {
			annQueryInfo = idx
			break
		}
	}
	if annQueryInfo == nil {
		return false
	}

	// If the vector column is only used in the VectorSearch and no where
	// else, then it can be eliminated in TableScan.
	if orderByCol.Index < 0 || orderByCol.Index >= len(proj.Exprs) {
		return false
	}

	isVecColumnInUse := false
	for idx, projExpr := range proj.Exprs {
		if idx == orderByCol.Index {
			// Skip the distance function projection itself.
			continue
		}
		flag := expression.HasColumnWithCondition(projExpr, func(col *expression.Column) bool {
			return col.ID == annQueryInfo.QueryInfo.GetAnnQueryInfo().GetColumn().ColumnId
		})
		if flag {
			isVecColumnInUse = true
			break
		}
	}

	if isVecColumnInUse {
		return false
	}

	// append distance column to the table scan
	virtualDistanceColInfo := &model.ColumnInfo{
		ID:        model.VirtualColVecSearchDistanceID,
		FieldType: *types.NewFieldType(mysql.TypeFloat),
		Offset:    len(tableScan.Columns) - 1,
	}

	virtualDistanceCol := &expression.Column{
		UniqueID: tableScan.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  types.NewFieldType(mysql.TypeFloat),
	}

	// remove the vector column in order to read distance directly by virtualDistanceCol
	vectorIdx := -1
	for i, col := range tableScan.Columns {
		if col.ID == annQueryInfo.QueryInfo.GetAnnQueryInfo().GetColumn().ColumnId {
			vectorIdx = i
			break
		}
	}
	if vectorIdx == -1 {
		return false
	}

	// set the EnableDistanceProj to modify the read process of tiflash.
	annQueryInfo.QueryInfo.GetAnnQueryInfo().EnableDistanceProj = true

	// append the distance column to the last position in columns and schema.
	tableScan.Columns = slices.Delete(tableScan.Columns, vectorIdx, vectorIdx+1)
	tableScan.Columns = append(tableScan.Columns, virtualDistanceColInfo)

	tableScan.Schema().Columns = slices.Delete(tableScan.Schema().Columns, vectorIdx, vectorIdx+1)
	tableScan.Schema().Append(virtualDistanceCol)

	// The children of topN are currently projections. After optimization, we no longer
	// need the projection and directly set the children to tablescan.
	local.SetChildren(tableScan)

	// modify the topN's ByItem
	local.ByItems[0].Expr = virtualDistanceCol
	global.ByItems[0].Expr = virtualDistanceCol
	local.ByItems[0].Expr.(*expression.Column).Index = tableScan.Schema().Len() - 1

	return true
}

// ContainHeavyFunction check if the expr contains a function that need to do HeavyFunctionOptimize. Currently this only applies
// to Vector data types and their functions. The HeavyFunctionOptimize eliminate the usage of the function in TopN operators
// to avoid vector distance re-calculation of TopN in the root task.
func ContainHeavyFunction(expr expression.Expression) bool {
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	if _, ok := HeavyFunctionNameMap[sf.FuncName.L]; ok {
		return true
	}
	return slices.ContainsFunc(sf.GetArgs(), ContainHeavyFunction)
}

// canPushToIndexPlan checks if this TopN can be pushed to the index side of copTask.
// It can be pushed to the index side when all columns used by ByItems are available from the index side and there's no prefix index column.
func canPushToIndexPlan(indexPlan base.PhysicalPlan, byItemCols []*expression.Column) bool {
	// If we call canPushToIndexPlan and there's no index plan, we should go into the index merge case.
	// Index merge case is specially handled for now. So we directly return false here.
	// So we directly return false.
	if indexPlan == nil {
		return false
	}
	schema := indexPlan.Schema()
	for _, col := range byItemCols {
		pos := schema.ColumnIndex(col)
		if pos == -1 {
			return false
		}
		if schema.Columns[pos].IsPrefix {
			return false
		}
	}
	return true
}

// canExpressionConvertedToPB checks whether each of the the expression in TopN can be converted to pb.
func canExpressionConvertedToPB(p *physicalop.PhysicalTopN, storeTp kv.StoreType) bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	return expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), exprs, storeTp)
}

// containVirtualColumn checks whether TopN.ByItems contains virtual generated columns.
func containVirtualColumn(p *physicalop.PhysicalTopN, tCols []*expression.Column) bool {
	tColSet := make(map[int64]struct{}, len(tCols))
	for _, tCol := range tCols {
		if tCol.ID > 0 && tCol.VirtualExpr != nil {
			tColSet[tCol.ID] = struct{}{}
		}
	}
	for _, by := range p.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if _, ok := tColSet[col.ID]; ok {
				// A column with ID > 0 indicates that the column can be resolved by data source.
				return true
			}
		}
	}
	return false
}

// canPushDownToTiKV checks whether this topN can be pushed down to TiKV.
func canPushDownToTiKV(p *physicalop.PhysicalTopN, copTask *physicalop.CopTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiKV) {
		return false
	}
	if len(copTask.RootTaskConds) != 0 {
		return false
	}
	if !copTask.IndexPlanFinished && len(copTask.IdxMergePartPlans) > 0 {
		for _, partialPlan := range copTask.IdxMergePartPlans {
			if containVirtualColumn(p, partialPlan.Schema().Columns) {
				return false
			}
		}
	} else if containVirtualColumn(p, copTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// canPushDownToTiFlash checks whether this topN can be pushed down to TiFlash.
func canPushDownToTiFlash(p *physicalop.PhysicalTopN, mppTask *physicalop.MppTask) bool {
	if !canExpressionConvertedToPB(p, kv.TiFlash) {
		return false
	}
	if containVirtualColumn(p, mppTask.Plan().Schema().Columns) {
		return false
	}
	return true
}

// For https://github.com/pingcap/tidb/issues/51723,
// This function only supports `CLUSTER_SLOW_QUERY`,
// it will change plan from
// TopN -> TableReader -> TableFullScan[cop] to
// TopN -> TableReader -> Limit[cop] -> TableFullScan[cop] + keepOrder
func pushLimitDownToTiDBCop(p *physicalop.PhysicalTopN, copTsk *physicalop.CopTask) (base.Task, bool) {
	if copTsk.IndexPlan != nil || copTsk.TablePlan == nil {
		return nil, false
	}

	var (
		selOnTblScan   *physicalop.PhysicalSelection
		selSelectivity float64
		tblScan        *physicalop.PhysicalTableScan
		err            error
		ok             bool
	)

	copTsk.TablePlan, err = copTsk.TablePlan.Clone(p.SCtx())
	if err != nil {
		return nil, false
	}
	finalTblScanPlan := copTsk.TablePlan
	for len(finalTblScanPlan.Children()) > 0 {
		selOnTblScan, _ = finalTblScanPlan.(*physicalop.PhysicalSelection)
		finalTblScanPlan = finalTblScanPlan.Children()[0]
	}

	if tblScan, ok = finalTblScanPlan.(*physicalop.PhysicalTableScan); !ok {
		return nil, false
	}

	// Check the table is `CLUSTER_SLOW_QUERY` or not.
	if tblScan.Table.Name.O != infoschema.ClusterTableSlowLog {
		return nil, false
	}

	colsProp, ok := physicalop.GetPropByOrderByItems(p.ByItems)
	if !ok {
		return nil, false
	}
	// For cluster tables, HandleCols may be nil. Skip this optimization if HandleCols is not available.
	if tblScan.HandleCols == nil {
		return nil, false
	}
	if len(colsProp.SortItems) != 1 || !colsProp.SortItems[0].Col.Equal(p.SCtx().GetExprCtx().GetEvalCtx(), tblScan.HandleCols.GetCol(0)) {
		return nil, false
	}
	if selOnTblScan != nil && tblScan.StatsInfo().RowCount > 0 {
		selSelectivity = selOnTblScan.StatsInfo().RowCount / tblScan.StatsInfo().RowCount
	}
	tblScan.Desc = colsProp.SortItems[0].Desc
	tblScan.KeepOrder = true

	childProfile := copTsk.Plan().StatsInfo()
	newCount := p.Offset + p.Count
	stats := property.DeriveLimitStats(childProfile, float64(newCount))
	pushedLimit := physicalop.PhysicalLimit{
		Count: newCount,
	}.Init(p.SCtx(), stats, p.QueryBlockOffset())
	pushedLimit.SetSchema(copTsk.TablePlan.Schema())
	copTsk = attachPlan2Task(pushedLimit, copTsk).(*physicalop.CopTask)
	child := pushedLimit.Children()[0]
	child.SetStats(child.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), float64(newCount)))
	if selSelectivity > 0 && selSelectivity < 1 {
		scaledRowCount := child.StatsInfo().RowCount / selSelectivity
		tblScan.SetStats(tblScan.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), scaledRowCount))
	}
	rootTask := copTsk.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask), true
}

// Attach2Task implements the PhysicalPlan interface.
func attach2Task4PhysicalTopN(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalTopN)
	t := tasks[0].Copy()

	// Handle partial order TopN first: when CopTask carries PartialOrderMatchResult,
	// it means skylinePruning has found a prefix index that can provide partial order.
	if copTask, ok := t.(*physicalop.CopTask); ok {
		if copTask.PartialOrderMatchResult != nil && copTask.PartialOrderMatchResult.Matched {
			return handlePartialOrderTopN(p, copTask)
		}
	}

	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	needPushDown := len(cols) > 0
	if copTask, ok := t.(*physicalop.CopTask); ok && needPushDown && copTask.GetStoreType() == kv.TiDB && len(copTask.RootTaskConds) == 0 {
		newTask, changed := pushLimitDownToTiDBCop(p, copTask)
		if changed {
			return newTask
		}
	}
	if copTask, ok := t.(*physicalop.CopTask); ok && needPushDown && canPushDownToTiKV(p, copTask) && len(copTask.RootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *physicalop.PhysicalTopN
		var newGlobalTopN *physicalop.PhysicalTopN
		if !copTask.IndexPlanFinished && canPushToIndexPlan(copTask.IndexPlan, cols) {
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.IndexPlan, copTask.GetStoreType())
			copTask.IndexPlan = pushedDownTopN
			if newGlobalTopN != nil {
				rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
				// Skip TopN with partition on the root. This is a derived topN and window function
				// will take care of the filter.
				if len(p.GetPartitionBy()) > 0 {
					return t
				}
				return attachPlan2Task(newGlobalTopN, rootTask)
			}
		} else {
			// It works for both normal index scan and index merge scan.
			copTask.FinishIndexPlan()
			pushedDownTopN, newGlobalTopN = getPushedDownTopN(p, copTask.TablePlan, copTask.GetStoreType())
			copTask.TablePlan = pushedDownTopN
			if newGlobalTopN != nil {
				rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
				// Skip TopN with partition on the root. This is a derived topN and window function
				// will take care of the filter.
				if len(p.GetPartitionBy()) > 0 {
					return t
				}
				return attachPlan2Task(newGlobalTopN, rootTask)
			}
		}
	} else if mppTask, ok := t.(*physicalop.MppTask); ok && needPushDown && canPushDownToTiFlash(p, mppTask) {
		pushedDownTopN, newGlobalTopN := getPushedDownTopN(p, mppTask.Plan(), kv.TiFlash)
		mppTask.SetPlan(pushedDownTopN)
		if newGlobalTopN != nil {
			rootTask := t.ConvertToRootTask(newGlobalTopN.SCtx())
			// Skip TopN with partition on the root. This is a derived topN and window function
			// will take care of the filter.
			if len(p.GetPartitionBy()) > 0 {
				return t
			}
			return attachPlan2Task(newGlobalTopN, rootTask)
		}
	}
	rootTask := t.ConvertToRootTask(p.SCtx())
	// Skip TopN with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, rootTask)
}

// handlePartialOrderTopN handles the partial order TopN scenario.
// It fills the partial-order-related fields on the TopN itself and, when possible,
// pushes down a special Limit with prefix information to the index side.
// There are two different cases:
//
// Case1: Two phase TopN, where TiDB keeps TopN and TiKV applies a partial-order Limit:
//
//	TopN(with partial info)
//	  └─IndexLookUp
//	     └─Limit(with partial info)
//	... (other operators)
//
// Case2: One phase TopN, where the whole TopN can only be executed in TiDB:
//
//	TopN(with partial info)
//	  ├─IndexPlan
//	  └─TablePlan
func handlePartialOrderTopN(p *physicalop.PhysicalTopN, copTask *physicalop.CopTask) base.Task {
	matchResult := copTask.PartialOrderMatchResult

	// Init partial order params PrefixCol and PrefixLen.
	// PartialOrderedLimit = Count + Offset.
	// TopN(with partial order) executor will short-cut read when it already handle "p.Count + p.Offset" rows.
	// Also it need to read X more rows (which prefix value is same as the last line prefix value) to ensure correctness.
	partialOrderedLimit := p.Count + p.Offset
	p.PrefixLen = matchResult.PrefixLen
	// Find the corresponding prefix column in TopN's schema.
	// matchResult.PrefixCol is from IndexScan's schema, but
	// Projection operators may remap columns. We need to find the column in TopN's
	// schema that has the same UniqueID as matchResult.PrefixCol.
	// Column UniqueID remains unchanged even after Projection remapping.
	p.PrefixCol = nil
	for _, col := range p.Schema().Columns {
		if col.UniqueID == matchResult.PrefixCol.UniqueID {
			p.PrefixCol = col
			break
		}
	}
	// Fallback: if not found in rootTask schema (should not happen)
	if p.PrefixCol == nil {
		return base.InvalidTask
	}

	// Decide whether we can push a special Limit down to the index plan.
	// Conditions:
	//   - Not an IndexMerge.
	//   - IndexPlan is not finished (IndexPlanFinished == false).
	//   - No root task conditions.
	// Since the output of the table plan is not ordered. So if limit can be pushed down, it must be pushed down to the index plan.
	// Therefore, we performed this related check here.
	canPushLimit := false
	if len(copTask.IdxMergePartPlans) == 0 &&
		!copTask.IndexPlanFinished &&
		len(copTask.RootTaskConds) == 0 &&
		copTask.IndexPlan != nil {
		canPushLimit = true
	}

	if canPushLimit {
		// Two-layer mode: TiDB TopN(with partial order info.) + TiKV limit(with partial order info.)
		// The estRows of partial order TopN : N + X
		// N: The partialOrderedLimit, N means the value of TopN, N = Count + Offset.
		// X: The estimated extra rows to read to fulfill the TopN.
		// We need to read more prefix values that are the same as the last line
		// to ensure the correctness of the final calculation of the Top n rows.
		maxX := estimateMaxXForPartialOrder()
		estimatedRows := float64(partialOrderedLimit) + float64(maxX)
		childProfile := copTask.IndexPlan.StatsInfo()
		limitStats := property.DeriveLimitStats(childProfile, estimatedRows)

		pushedDownLimit := physicalop.PhysicalLimit{
			Count:     partialOrderedLimit,
			PrefixCol: p.PrefixCol,
			PrefixLen: matchResult.PrefixLen,
		}.Init(p.SCtx(), limitStats, p.QueryBlockOffset())
		pushedDownLimit.SetChildren(copTask.IndexPlan)
		pushedDownLimit.SetSchema(copTask.IndexPlan.Schema())
		copTask.IndexPlan = pushedDownLimit
	}

	// Always keep TopN in TiDB as the upper layer.
	rootTask := copTask.ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p, rootTask)
}

// estimateMaxXForPartialOrder estimates the extra rows X to read for partial order optimization.
// This value is used for statistics (row count estimation).
func estimateMaxXForPartialOrder() uint64 {
	// TODO: implement it by TopN/buckets and adjust it by session variable.
	return 0
}
