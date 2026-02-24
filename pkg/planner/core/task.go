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
	"math"
	"slices"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// HeavyFunctionNameMap stores function names that is worth to do HeavyFunctionOptimize.
// Currently this only applies to Vector data types and their functions. The HeavyFunctionOptimize
// eliminate the usage of the function in TopN operators to avoid vector distance re-calculation
// of TopN in the root task.
var HeavyFunctionNameMap = map[string]struct{}{
	"vec_cosine_distance":        {},
	"vec_l1_distance":            {},
	"vec_l2_distance":            {},
	"vec_negative_inner_product": {},
	"vec_dims":                   {},
	"vec_l2_norm":                {},
}

func attachPlan2Task(p base.PhysicalPlan, t base.Task) base.Task {
	// since almost all current physical plan will be attached to bottom encapsulated task.
	// we do the stats inheritance here for all the index join inner task.
	inheritStatsFromBottomTaskForIndexJoinInner(p, t)
	switch v := t.(type) {
	case *physicalop.CopTask:
		if v.IndexPlanFinished {
			p.SetChildren(v.TablePlan)
			v.TablePlan = p
		} else {
			p.SetChildren(v.IndexPlan)
			v.IndexPlan = p
		}
	case *physicalop.RootTask:
		p.SetChildren(v.GetPlan())
		v.SetPlan(p)
	case *physicalop.MppTask:
		p.SetChildren(v.Plan())
		v.SetPlan(p)
	}
	return t
}

// attach2Task4PhysicalUnionScan implements PhysicalPlan interface.
func attach2Task4PhysicalUnionScan(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalUnionScan)
	// when it arrives here, physical union scan will absolutely require a root task type,
	// so convert child to root task type first.
	task := tasks[0].ConvertToRootTask(p.SCtx())
	// We need to pull the projection under unionScan upon unionScan.
	// Since the projection only prunes columns, it's ok the put it upon unionScan.
	if sel, ok := task.Plan().(*physicalop.PhysicalSelection); ok {
		if pj, ok := sel.Children()[0].(*physicalop.PhysicalProjection); ok {
			// Convert unionScan->selection->projection to projection->unionScan->selection.
			// shallow clone sel
			clonedSel := *sel
			clonedSel.SetChildren(pj.Children()...)
			// set child will substitute original child slices, not an in-place change.
			p.SetChildren(&clonedSel)
			p.SetStats(task.Plan().StatsInfo())
			rt := task.(*physicalop.RootTask)
			rt.SetPlan(p) // root task plan current is p headed.
			// shallow clone proj.
			clonedProj := *pj
			// set child will substitute original child slices, not an in-place change.
			clonedProj.SetChildren(p)
			return clonedProj.Attach2Task(task)
		}
	}
	if pj, ok := task.Plan().(*physicalop.PhysicalProjection); ok {
		// Convert unionScan->projection to projection->unionScan, because unionScan can't handle projection as its children.
		p.SetChildren(pj.Children()...)
		p.SetStats(task.Plan().StatsInfo())
		rt, _ := task.(*physicalop.RootTask)
		rt.SetPlan(pj.Children()[0])
		// shallow clone proj.
		clonedProj := *pj
		// set child will substitute original child slices, not an in-place change.
		clonedProj.SetChildren(p)
		return clonedProj.Attach2Task(p.BasePhysicalPlan.Attach2Task(task))
	}
	p.SetStats(task.Plan().StatsInfo())
	// once task is copTask type here, it may be converted proj + tablePlan here.
	// then when it's connected with union-scan here, we may get as: union-scan + proj + tablePlan
	// while proj is not allowed to be built under union-scan in execution layer currently.
	return p.BasePhysicalPlan.Attach2Task(task)
}

// attach2Task4PhysicalApply implements PhysicalPlan interface.
func attach2Task4PhysicalApply(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalApply)
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	p.SetSchema(physicalop.BuildPhysicalJoinSchema(p.JoinType, p))
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	// inherit left and right child's warnings.
	t.Warnings.CopyFrom(&lTask.(*physicalop.RootTask).Warnings, &rTask.(*physicalop.RootTask).Warnings)
	return t
}

// attach2Task4PhysicalIndexMergeJoin implements PhysicalPlan interface.
func attach2Task4PhysicalIndexMergeJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalIndexMergeJoin)
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	return t
}

func indexHashJoinAttach2TaskV1(p *physicalop.PhysicalIndexHashJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	return t
}

func indexHashJoinAttach2TaskV2(p *physicalop.PhysicalIndexHashJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	innerTask := tasks[p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	// only fill the wrapped physical index join is ok.
	completePhysicalIndexJoin(&p.PhysicalIndexJoin, innerTask.(*physicalop.RootTask), innerTask.Plan().Schema(), outerTask.Plan().Schema(), true)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	t.Warnings.CopyFrom(&outerTask.(*physicalop.RootTask).Warnings, &innerTask.(*physicalop.RootTask).Warnings)
	return t
}

// attach2Task4PhysicalIndexHashJoin implements PhysicalPlan interface.
func attach2Task4PhysicalIndexHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalIndexHashJoin)
	if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
		return indexHashJoinAttach2TaskV2(p, tasks...)
	}
	return indexHashJoinAttach2TaskV1(p, tasks...)
}

func indexJoinAttach2TaskV1(p *physicalop.PhysicalIndexJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), p.InnerPlan)
	} else {
		p.SetChildren(p.InnerPlan, outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	return t
}

func indexJoinAttach2TaskV2(p *physicalop.PhysicalIndexJoin, tasks ...base.Task) base.Task {
	outerTask := tasks[1-p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	innerTask := tasks[p.InnerChildIdx].ConvertToRootTask(p.SCtx())
	completePhysicalIndexJoin(p, innerTask.(*physicalop.RootTask), innerTask.Plan().Schema(), outerTask.Plan().Schema(), true)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.Plan(), innerTask.Plan())
	} else {
		p.SetChildren(innerTask.Plan(), outerTask.Plan())
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	t.Warnings.CopyFrom(&outerTask.(*physicalop.RootTask).Warnings, &innerTask.(*physicalop.RootTask).Warnings)
	return t
}

func attach2Task4PhysicalIndexJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalIndexJoin)
	if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
		return indexJoinAttach2TaskV2(p, tasks...)
	}
	return indexJoinAttach2TaskV1(p, tasks...)
}

// RowSize for cost model ver2 is simplified, always use this function to calculate row size.
func getAvgRowSize(stats *property.StatsInfo, cols []*expression.Column) (size float64) {
	if stats.HistColl != nil {
		size = max(cardinality.GetAvgRowSizeDataInDiskByRows(stats.HistColl, cols), 0)
	} else {
		// Estimate using just the type info.
		for _, col := range cols {
			size += max(float64(chunk.EstimateTypeWidth(col.GetStaticType())), 0)
		}
	}
	return
}

// attach2Task4PhysicalHashJoin implements PhysicalPlan interface.
func attach2Task4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalHashJoin)
	if p.StoreTp == kv.TiFlash {
		return attach2TaskForTiFlash4PhysicalHashJoin(p, tasks...)
	}
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	task := &physicalop.RootTask{}
	task.SetPlan(p)
	task.Warnings.CopyFrom(&rTask.(*physicalop.RootTask).Warnings, &lTask.(*physicalop.RootTask).Warnings)
	return task
}

func attach2TaskForTiFlash4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalHashJoin)
	rTask, rok := tasks[1].(*physicalop.CopTask)
	lTask, lok := tasks[0].(*physicalop.CopTask)
	if !lok || !rok {
		return attach2TaskForMpp4PhysicalHashJoin(p, tasks...)
	}
	rRoot := rTask.ConvertToRootTask(p.SCtx())
	lRoot := lTask.ConvertToRootTask(p.SCtx())
	p.SetChildren(lRoot.Plan(), rRoot.Plan())
	p.SetSchema(physicalop.BuildPhysicalJoinSchema(p.JoinType, p))
	task := &physicalop.RootTask{}
	task.SetPlan(p)
	task.Warnings.CopyFrom(&rTask.Warnings, &lTask.Warnings)
	return task
}

// TiDB only require that the types fall into the same catalog but TiFlash require the type to be exactly the same, so
// need to check if the conversion is a must
func needConvert(tp *types.FieldType, rtp *types.FieldType) bool {
	// all the string type are mapped to the same type in TiFlash, so
	// do not need convert for string types
	if types.IsString(tp.GetType()) && types.IsString(rtp.GetType()) {
		return false
	}
	if tp.GetType() != rtp.GetType() {
		return true
	}
	if tp.GetType() != mysql.TypeNewDecimal {
		return false
	}
	if tp.GetDecimal() != rtp.GetDecimal() {
		return true
	}
	// for decimal type, TiFlash have 4 different impl based on the required precision
	if tp.GetFlen() >= 0 && tp.GetFlen() <= 9 && rtp.GetFlen() >= 0 && rtp.GetFlen() <= 9 {
		return false
	}
	if tp.GetFlen() > 9 && tp.GetFlen() <= 18 && rtp.GetFlen() > 9 && rtp.GetFlen() <= 18 {
		return false
	}
	if tp.GetFlen() > 18 && tp.GetFlen() <= 38 && rtp.GetFlen() > 18 && rtp.GetFlen() <= 38 {
		return false
	}
	if tp.GetFlen() > 38 && tp.GetFlen() <= 65 && rtp.GetFlen() > 38 && rtp.GetFlen() <= 65 {
		return false
	}
	return true
}

func negotiateCommonType(lType, rType *types.FieldType) (_ *types.FieldType, _, _ bool) {
	commonType := types.AggFieldType([]*types.FieldType{lType, rType})
	if commonType.GetType() == mysql.TypeNewDecimal {
		lExtend := 0
		rExtend := 0
		cDec := rType.GetDecimal()
		if lType.GetDecimal() < rType.GetDecimal() {
			lExtend = rType.GetDecimal() - lType.GetDecimal()
		} else if lType.GetDecimal() > rType.GetDecimal() {
			rExtend = lType.GetDecimal() - rType.GetDecimal()
			cDec = lType.GetDecimal()
		}
		lLen, rLen := lType.GetFlen()+lExtend, rType.GetFlen()+rExtend
		cLen := max(lLen, rLen)
		commonType.SetDecimalUnderLimit(cDec)
		commonType.SetFlenUnderLimit(cLen)
	} else if needConvert(lType, commonType) || needConvert(rType, commonType) {
		if mysql.IsIntegerType(commonType.GetType()) {
			// If the target type is int, both TiFlash and Mysql only support cast to Int64
			// so we need to promote the type to Int64
			commonType.SetType(mysql.TypeLonglong)
			commonType.SetFlen(mysql.MaxIntWidth)
		}
	}
	return commonType, needConvert(lType, commonType), needConvert(rType, commonType)
}

func getProj(ctx base.PlanContext, p base.PhysicalPlan) *physicalop.PhysicalProjection {
	proj := physicalop.PhysicalProjection{
		Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
	}.Init(ctx, p.StatsInfo(), p.QueryBlockOffset())
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(p.Schema().Clone())
	proj.SetChildren(p)
	return proj
}

func appendExpr(p *physicalop.PhysicalProjection, expr expression.Expression) *expression.Column {
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
	}
	col.SetCoercibility(expr.Coercibility())
	p.Schema().Append(col)
	return col
}

// TiFlash join require that partition key has exactly the same type, while TiDB only guarantee the partition key is the same catalog,
// so if the partition key type is not exactly the same, we need add a projection below the join or exchanger if exists.
func convertPartitionKeysIfNeed4PhysicalHashJoin(pp base.PhysicalPlan, lTask, rTask *physicalop.MppTask) (_, _ *physicalop.MppTask) {
	p := pp.(*physicalop.PhysicalHashJoin)
	lp := lTask.Plan()
	if _, ok := lp.(*physicalop.PhysicalExchangeReceiver); ok {
		lp = lp.Children()[0].Children()[0]
	}
	rp := rTask.Plan()
	if _, ok := rp.(*physicalop.PhysicalExchangeReceiver); ok {
		rp = rp.Children()[0].Children()[0]
	}
	// to mark if any partition key needs to convert
	lMask := make([]bool, len(lTask.HashCols))
	rMask := make([]bool, len(rTask.HashCols))
	cTypes := make([]*types.FieldType, len(lTask.HashCols))
	lChanged := false
	rChanged := false
	for i := range lTask.HashCols {
		lKey := lTask.HashCols[i]
		rKey := rTask.HashCols[i]
		cType, lConvert, rConvert := negotiateCommonType(lKey.Col.RetType, rKey.Col.RetType)
		if lConvert {
			lMask[i] = true
			cTypes[i] = cType
			lChanged = true
		}
		if rConvert {
			rMask[i] = true
			cTypes[i] = cType
			rChanged = true
		}
	}
	if !lChanged && !rChanged {
		return lTask, rTask
	}
	var lProj, rProj *physicalop.PhysicalProjection
	if lChanged {
		lProj = getProj(p.SCtx(), lp)
		lp = lProj
	}
	if rChanged {
		rProj = getProj(p.SCtx(), rp)
		rp = rProj
	}

	lPartKeys := make([]*property.MPPPartitionColumn, 0, len(rTask.HashCols))
	rPartKeys := make([]*property.MPPPartitionColumn, 0, len(lTask.HashCols))
	for i := range lTask.HashCols {
		lKey := lTask.HashCols[i]
		rKey := rTask.HashCols[i]
		if lMask[i] {
			cType := cTypes[i].Clone()
			cType.SetFlag(lKey.Col.RetType.GetFlag())
			lCast := expression.BuildCastFunction(p.SCtx().GetExprCtx(), lKey.Col, cType)
			lKey = &property.MPPPartitionColumn{Col: appendExpr(lProj, lCast), CollateID: lKey.CollateID}
		}
		if rMask[i] {
			cType := cTypes[i].Clone()
			cType.SetFlag(rKey.Col.RetType.GetFlag())
			rCast := expression.BuildCastFunction(p.SCtx().GetExprCtx(), rKey.Col, cType)
			rKey = &property.MPPPartitionColumn{Col: appendExpr(rProj, rCast), CollateID: rKey.CollateID}
		}
		lPartKeys = append(lPartKeys, lKey)
		rPartKeys = append(rPartKeys, rKey)
	}
	// if left or right child changes, we need to add enforcer.
	if lChanged {
		nlTask := lTask.Copy().(*physicalop.MppTask)
		nlTask.SetPlan(lProj)
		nlTask = nlTask.EnforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: lPartKeys,
		}, nil)
		lTask = nlTask
	}
	if rChanged {
		nrTask := rTask.Copy().(*physicalop.MppTask)
		nrTask.SetPlan(rProj)
		nrTask = nrTask.EnforceExchanger(&property.PhysicalProperty{
			TaskTp:           property.MppTaskType,
			MPPPartitionTp:   property.HashType,
			MPPPartitionCols: rPartKeys,
		}, nil)
		rTask = nrTask
	}
	return lTask, rTask
}

func enforceExchangerByBackup4PhysicalHashJoin(pp base.PhysicalPlan, task *physicalop.MppTask, idx int, expectedCols int) *physicalop.MppTask {
	p := pp.(*physicalop.PhysicalHashJoin)
	if backupHashProp := p.GetChildReqProps(idx); backupHashProp != nil {
		if len(backupHashProp.MPPPartitionCols) == expectedCols {
			return task.EnforceExchangerImpl(backupHashProp)
		}
	}
	return nil
}

func attach2TaskForMpp4PhysicalHashJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	const (
		left  = 0
		right = 1
	)
	rTask, rok := tasks[right].(*physicalop.MppTask)
	lTask, lok := tasks[left].(*physicalop.MppTask)
	if !lok || !rok {
		return base.InvalidTask
	}
	p := pp.(*physicalop.PhysicalHashJoin)
	if p.MppShuffleJoin {
		if len(lTask.HashCols) == 0 || len(rTask.HashCols) == 0 {
			// if the hash columns are empty, this is very likely a bug.
			return base.InvalidTask
		}
		if len(lTask.HashCols) != len(rTask.HashCols) {
			// if the hash columns are not the same, The most likely scenario is that
			// they have undergone exchange optimization, removing some hash columns.
			// In this case, we need to restore them on the side that is missing.
			if len(lTask.HashCols) < len(rTask.HashCols) {
				lTask = enforceExchangerByBackup4PhysicalHashJoin(p, lTask, left, len(rTask.HashCols))
			} else {
				rTask = enforceExchangerByBackup4PhysicalHashJoin(p, rTask, right, len(lTask.HashCols))
			}
			if lTask == nil || rTask == nil {
				return base.InvalidTask
			}
		}
		lTask, rTask = convertPartitionKeysIfNeed4PhysicalHashJoin(p, lTask, rTask)
	}
	p.SetChildren(lTask.Plan(), rTask.Plan())
	// outer task is the task that will pass its MPPPartitionType to the join result
	// for broadcast inner join, it should be the non-broadcast side, since broadcast side is always the build side, so
	// just use the probe side is ok.
	// for hash inner join, both side is ok, by default, we use the probe side
	// for outer join, it should always be the outer side of the join
	// for semi join, it should be the left side(the same as left out join)
	outerTaskIndex := 1 - p.InnerChildIdx
	if p.JoinType != base.InnerJoin {
		if p.JoinType == base.RightOuterJoin {
			outerTaskIndex = 1
		} else {
			outerTaskIndex = 0
		}
	}
	// can not use the task from tasks because it maybe updated.
	outerTask := lTask
	if outerTaskIndex == 1 {
		outerTask = rTask
	}
	task := physicalop.NewMppTask(p,
		outerTask.GetPartitionType(),
		outerTask.GetHashCols(),
		nil, rTask.GetWarnings(), lTask.GetWarnings())
	// Current TiFlash doesn't support receive Join executors' schema info directly from TiDB.
	// Instead, it calculates Join executors' output schema using algorithm like BuildPhysicalJoinSchema which
	// produces full semantic schema.
	// Thus, the column prune optimization achievements will be abandoned here.
	// To avoid the performance issue, add a projection here above the Join operator to prune useless columns explicitly.
	// TODO(hyb): transfer Join executors' schema to TiFlash through DagRequest, and use it directly in TiFlash.
	defaultSchema := physicalop.BuildPhysicalJoinSchema(p.JoinType, p)
	hashColArray := make([]*expression.Column, 0, len(task.HashCols))
	// For task.hashCols, these columns may not be contained in pruned columns:
	// select A.id from A join B on A.id = B.id; Suppose B is probe side, and it's hash inner join.
	// After column prune, the output schema of A join B will be A.id only; while the task's hashCols will be B.id.
	// To make matters worse, the hashCols may be used to check if extra cast projection needs to be added, then the newly
	// added projection will expect B.id as input schema. So make sure hashCols are included in task.p's schema.
	// TODO: planner should takes the hashCols attribute into consideration when perform column pruning; Or provide mechanism
	// to constraint hashCols are always chosen inside Join's pruned schema
	for _, hashCol := range task.HashCols {
		hashColArray = append(hashColArray, hashCol.Col)
	}
	if p.Schema().Len() < defaultSchema.Len() {
		if p.Schema().Len() > 0 {
			proj := physicalop.PhysicalProjection{
				Exprs: expression.Column2Exprs(p.Schema().Columns),
			}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

			proj.SetSchema(p.Schema().Clone())
			for _, hashCol := range hashColArray {
				if !proj.Schema().Contains(hashCol) && defaultSchema.Contains(hashCol) {
					joinCol := defaultSchema.Columns[defaultSchema.ColumnIndex(hashCol)]
					proj.Exprs = append(proj.Exprs, joinCol)
					proj.Schema().Append(joinCol.Clone().(*expression.Column))
				}
			}
			attachPlan2Task(proj, task)
		} else {
			if len(hashColArray) == 0 {
				constOne := expression.NewOne()
				expr := make([]expression.Expression, 0, 1)
				expr = append(expr, constOne)
				proj := physicalop.PhysicalProjection{
					Exprs: expr,
				}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

				proj.SetSchema(expression.NewSchema(&expression.Column{
					UniqueID: proj.SCtx().GetSessionVars().AllocPlanColumnID(),
					RetType:  constOne.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
				}))
				attachPlan2Task(proj, task)
			} else {
				proj := physicalop.PhysicalProjection{
					Exprs: make([]expression.Expression, 0, len(hashColArray)),
				}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())

				clonedHashColArray := make([]*expression.Column, 0, len(task.HashCols))
				for _, hashCol := range hashColArray {
					if defaultSchema.Contains(hashCol) {
						joinCol := defaultSchema.Columns[defaultSchema.ColumnIndex(hashCol)]
						proj.Exprs = append(proj.Exprs, joinCol)
						clonedHashColArray = append(clonedHashColArray, joinCol.Clone().(*expression.Column))
					}
				}

				proj.SetSchema(expression.NewSchema(clonedHashColArray...))
				attachPlan2Task(proj, task)
			}
		}
	}
	p.SetSchema(defaultSchema)
	return task
}

// attach2Task4PhysicalMergeJoin implements PhysicalPlan interface.
func attach2Task4PhysicalMergeJoin(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalMergeJoin)
	lTask := tasks[0].ConvertToRootTask(p.SCtx())
	rTask := tasks[1].ConvertToRootTask(p.SCtx())
	p.SetChildren(lTask.Plan(), rTask.Plan())
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	t.Warnings.CopyFrom(&rTask.(*physicalop.RootTask).Warnings, &lTask.(*physicalop.RootTask).Warnings)
	return t
}

func extractRows(p base.PhysicalPlan) float64 {
	f := float64(0)
	for _, c := range p.Children() {
		if len(c.Children()) != 0 {
			f += extractRows(c)
		} else {
			f += c.StatsInfo().RowCount
		}
	}
	return f
}

// calcPagingCost calculates the cost for paging processing which may increase the seekCnt and reduce scanned rows.
func calcPagingCost(ctx base.PlanContext, indexPlan base.PhysicalPlan, expectCnt uint64) float64 {
	sessVars := ctx.GetSessionVars()
	indexRows := indexPlan.StatsCount()
	sourceRows := extractRows(indexPlan)
	// with paging, the scanned rows is always less than or equal to source rows.
	if uint64(sourceRows) < expectCnt {
		expectCnt = uint64(sourceRows)
	}
	seekCnt := paging.CalculateSeekCnt(expectCnt)
	indexSelectivity := float64(1)
	if sourceRows > indexRows {
		indexSelectivity = indexRows / sourceRows
	}
	pagingCst := seekCnt*sessVars.GetSeekFactor(nil) + float64(expectCnt)*sessVars.GetCPUFactor()
	pagingCst *= indexSelectivity

	// we want the diff between idxCst and pagingCst here,
	// however, the idxCst does not contain seekFactor, so a seekFactor needs to be removed
	return math.Max(pagingCst-sessVars.GetSeekFactor(nil), 0)
}

// attach2Task4PhysicalLimit attach limit to different cases.
// For Normal Index Lookup
// 1: attach the limit to table side or index side of normal index lookup cop task. (normal case, old code, no more
// explanation here)
//
// For Index Merge:
// 2: attach the limit to **table** side for index merge intersection case, cause intersection will invalidate the
// fetched limit+offset rows from each partial index plan, you can not decide how many you want in advance for partial
// index path, actually. After we sink limit to table side, we still need an upper root limit to control the real limit
// count admission.
//
// 3: attach the limit to **index** side for index merge union case, because each index plan will output the fetched
// limit+offset (* N path) rows, you still need an embedded pushedLimit inside index merge reader to cut it down.
//
// 4: attach the limit to the TOP of root index merge operator if there is some root condition exists for index merge
// intersection/union case.
func attach2Task4PhysicalLimit(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalLimit)
	t := tasks[0].Copy()
	newPartitionBy := make([]property.SortItem, 0, len(p.GetPartitionBy()))
	for _, expr := range p.GetPartitionBy() {
		newPartitionBy = append(newPartitionBy, expr.Clone())
	}

	sunk := false
	if cop, ok := t.(*physicalop.CopTask); ok {
		suspendLimitAboveTablePlan := func() {
			newCount := p.Offset + p.Count
			childProfile := cop.TablePlan.StatsInfo()
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := property.DeriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
			pushedDownLimit.SetChildren(cop.TablePlan)
			cop.TablePlan = pushedDownLimit
			// Don't use clone() so that Limit and its children share the same schema. Otherwise, the virtual generated column may not be resolved right.
			pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
			t = cop.ConvertToRootTask(p.SCtx())
		}
		if len(cop.IdxMergePartPlans) == 0 {
			// For double read which requires order being kept, the limit cannot be pushed down to the table side,
			// because handles would be reordered before being sent to table scan.
			if (!cop.KeepOrder || !cop.IndexPlanFinished || cop.IndexPlan == nil) && len(cop.RootTaskConds) == 0 {
				// When limit is pushed down, we should remove its offset.
				newCount := p.Offset + p.Count
				childProfile := cop.Plan().StatsInfo()
				// Strictly speaking, for the row count of stats, we should multiply newCount with "regionNum",
				// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
				stats := property.DeriveLimitStats(childProfile, float64(newCount))
				pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
				cop = attachPlan2Task(pushedDownLimit, cop).(*physicalop.CopTask)
				// Don't use clone() so that Limit and its children share the same schema. Otherwise the virtual generated column may not be resolved right.
				pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
			}
			t = cop.ConvertToRootTask(p.SCtx())
			sunk = sinkIntoIndexLookUp(p, t)
		} else if !cop.IdxMergeIsIntersection {
			// We only support push part of the order prop down to index merge build case.
			if len(cop.RootTaskConds) == 0 {
				// For double read which requires order being kept, the limit cannot be pushed down to the table side,
				// because handles would be reordered before being sent to table scan.
				if cop.IndexPlanFinished && !cop.KeepOrder {
					// when the index plan is finished and index plan is not ordered, sink the limit to the index merge table side.
					suspendLimitAboveTablePlan()
				} else if !cop.IndexPlanFinished {
					// cop.IndexPlanFinished = false indicates the table side is a pure table-scan, sink the limit to the index merge index side.
					newCount := p.Offset + p.Count
					limitChildren := make([]base.PhysicalPlan, 0, len(cop.IdxMergePartPlans))
					for _, partialScan := range cop.IdxMergePartPlans {
						childProfile := partialScan.StatsInfo()
						stats := property.DeriveLimitStats(childProfile, float64(newCount))
						pushedDownLimit := physicalop.PhysicalLimit{PartitionBy: newPartitionBy, Count: newCount}.Init(p.SCtx(), stats, p.QueryBlockOffset())
						pushedDownLimit.SetChildren(partialScan)
						pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
						limitChildren = append(limitChildren, pushedDownLimit)
					}
					cop.IdxMergePartPlans = limitChildren
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = sinkIntoIndexMerge(p, t)
				} else {
					// when there are some limitations, just sink the limit upon the index merge reader.
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = sinkIntoIndexMerge(p, t)
				}
			} else {
				// when there are some root conditions, just sink the limit upon the index merge reader.
				t = cop.ConvertToRootTask(p.SCtx())
				sunk = sinkIntoIndexMerge(p, t)
			}
		} else if cop.IdxMergeIsIntersection {
			// In the index merge with intersection case, only the limit can be pushed down to the index merge table side.
			// Note Difference:
			// IndexMerge.PushedLimit is applied before table scan fetching, limiting the indexPartialPlan rows returned (it maybe ordered if orderBy items not empty)
			// TableProbeSide sink limit is applied on the top of table plan, which will quickly shut down the both fetch-back and read-back process.
			if len(cop.RootTaskConds) == 0 {
				if cop.IndexPlanFinished {
					// indicates the table side is not a pure table-scan, so we could only append the limit upon the table plan.
					suspendLimitAboveTablePlan()
				} else {
					t = cop.ConvertToRootTask(p.SCtx())
					sunk = sinkIntoIndexMerge(p, t)
				}
			} else {
				// Otherwise, suspend the limit out of index merge reader.
				t = cop.ConvertToRootTask(p.SCtx())
				sunk = sinkIntoIndexMerge(p, t)
			}
		} else {
			// Whatever the remained case is, we directly convert to it to root task.
			t = cop.ConvertToRootTask(p.SCtx())
		}
	} else if mpp, ok := t.(*physicalop.MppTask); ok {
		newCount := p.Offset + p.Count
		childProfile := mpp.Plan().StatsInfo()
		stats := property.DeriveLimitStats(childProfile, float64(newCount))
		pushedDownLimit := physicalop.PhysicalLimit{Count: newCount, PartitionBy: newPartitionBy}.Init(p.SCtx(), stats, p.QueryBlockOffset())
		mpp = attachPlan2Task(pushedDownLimit, mpp).(*physicalop.MppTask)
		pushedDownLimit.SetSchema(pushedDownLimit.Children()[0].Schema())
		t = mpp.ConvertToRootTask(p.SCtx())
	}
	if sunk {
		return t
	}
	// Skip limit with partition on the root. This is a derived topN and window function
	// will take care of the filter.
	if len(p.GetPartitionBy()) > 0 {
		return t
	}
	return attachPlan2Task(p, t)
}

func sinkIntoIndexLookUp(p *physicalop.PhysicalLimit, t base.Task) bool {
	root := t.(*physicalop.RootTask)
	reader, isDoubleRead := root.GetPlan().(*physicalop.PhysicalIndexLookUpReader)
	proj, isProj := root.GetPlan().(*physicalop.PhysicalProjection)
	if !isDoubleRead && !isProj {
		return false
	}
	if isProj {
		reader, isDoubleRead = proj.Children()[0].(*physicalop.PhysicalIndexLookUpReader)
		if !isDoubleRead {
			return false
		}
	}

	// We can sink Limit into IndexLookUpReader only if tablePlan contains no Selection.
	ts, isTableScan := reader.TablePlan.(*physicalop.PhysicalTableScan)
	if !isTableScan {
		return false
	}

	// If this happens, some Projection Operator must be inlined into this Limit. (issues/14428)
	// For example, if the original plan is `IndexLookUp(col1, col2) -> Limit(col1, col2) -> Project(col1)`,
	//  then after inlining the Project, it will be `IndexLookUp(col1, col2) -> Limit(col1)` here.
	// If the Limit is sunk into the IndexLookUp, the IndexLookUp's schema needs to be updated as well,
	// So we add an extra projection to solve the problem.
	if p.Schema().Len() != reader.Schema().Len() {
		extraProj := physicalop.PhysicalProjection{
			Exprs: expression.Column2Exprs(p.Schema().Columns),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
		extraProj.SetSchema(p.Schema())
		// If the root.p is already a Projection. We left the optimization for the later Projection Elimination.
		extraProj.SetChildren(root.GetPlan())
		root.SetPlan(extraProj)
	}

	reader.PushedLimit = &physicalop.PushedDownLimit{
		Offset: p.Offset,
		Count:  p.Count,
	}
	if originStats := ts.StatsInfo(); originStats.RowCount >= p.StatsInfo().RowCount {
		// Only reset the table scan stats when its row estimation is larger than the limit count.
		// When indexLookUp push down is enabled, some rows have been looked up in TiKV side,
		// and the rows processed by the TiDB table scan may be less than the limit count.
		ts.SetStats(p.StatsInfo())
		if originStats != nil {
			// keep the original stats version
			ts.StatsInfo().StatsVersion = originStats.StatsVersion
		}
	}
	reader.SetStats(p.StatsInfo())
	if isProj {
		proj.SetStats(p.StatsInfo())
	}
	return true
}

func sinkIntoIndexMerge(p *physicalop.PhysicalLimit, t base.Task) bool {
	root := t.(*physicalop.RootTask)
	imReader, isIm := root.GetPlan().(*physicalop.PhysicalIndexMergeReader)
	proj, isProj := root.GetPlan().(*physicalop.PhysicalProjection)
	if !isIm && !isProj {
		return false
	}
	if isProj {
		imReader, isIm = proj.Children()[0].(*physicalop.PhysicalIndexMergeReader)
		if !isIm {
			return false
		}
	}
	ts, ok := imReader.TablePlan.(*physicalop.PhysicalTableScan)
	if !ok {
		return false
	}
	imReader.PushedLimit = &physicalop.PushedDownLimit{
		Count:  p.Count,
		Offset: p.Offset,
	}
	// since ts.statsInfo.rowcount may dramatically smaller than limit.statsInfo.
	// like limit: rowcount=1
	//      ts:    rowcount=0.0025
	originStats := ts.StatsInfo()
	if originStats != nil {
		// keep the original stats version
		ts.StatsInfo().StatsVersion = originStats.StatsVersion
		if originStats.RowCount < p.StatsInfo().RowCount {
			ts.StatsInfo().RowCount = originStats.RowCount
		}
	}
	needProj := p.Schema().Len() != root.GetPlan().Schema().Len()
	if !needProj {
		for i := range p.Schema().Len() {
			if !p.Schema().Columns[i].EqualColumn(root.GetPlan().Schema().Columns[i]) {
				needProj = true
				break
			}
		}
	}
	if needProj {
		extraProj := physicalop.PhysicalProjection{
			Exprs: expression.Column2Exprs(p.Schema().Columns),
		}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), nil)
		extraProj.SetSchema(p.Schema())
		// If the root.p is already a Projection. We left the optimization for the later Projection Elimination.
		extraProj.SetChildren(root.GetPlan())
		root.SetPlan(extraProj)
	}
	return true
}

// attach2Task4PhysicalSort is basic logic of Attach2Task which implements PhysicalPlan interface.
func attach2Task4PhysicalSort(p base.PhysicalPlan, tasks ...base.Task) base.Task {
	intest.Assert(p.(*physicalop.PhysicalSort) != nil)
	t := tasks[0].Copy()
	t = attachPlan2Task(p, t)
	return t
}

// attach2Task4NominalSort implements PhysicalPlan interface.
func attach2Task4NominalSort(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.NominalSort)
	if p.OnlyColumn {
		return tasks[0]
	}
	t := tasks[0].Copy()
	t = attachPlan2Task(p, t)
	return t
}

// attach2Task4PhysicalProjection implements PhysicalPlan interface.
func attach2Task4PhysicalProjection(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalProjection)
	t := tasks[0].Copy()
	if cop, ok := t.(*physicalop.CopTask); ok {
		if (len(cop.RootTaskConds) == 0 && len(cop.IdxMergePartPlans) == 0) && expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Exprs, cop.GetStoreType()) {
			copTask := attachPlan2Task(p, cop)
			return copTask
		}
	} else if mpp, ok := t.(*physicalop.MppTask); ok {
		if expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Exprs, kv.TiFlash) {
			p.SetChildren(mpp.Plan())
			mpp.SetPlan(p)
			return mpp
		}
	}
	t = t.ConvertToRootTask(p.SCtx())
	t = attachPlan2Task(p, t)
	return t
}

// attach2Task4PhysicalExpand implements PhysicalPlan interface.
func attach2Task4PhysicalExpand(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalExpand)
	t := tasks[0].Copy()
	// current expand can only be run in MPP TiFlash mode or Root Tidb mode.
	// if expr inside could not be pushed down to tiFlash, it will error in converting to pb side.
	if mpp, ok := t.(*physicalop.MppTask); ok {
		p.SetChildren(mpp.Plan())
		mpp.SetPlan(p)
		return mpp
	}
	// For root task
	// since expand should be in root side accordingly, convert to root task now.
	root := t.ConvertToRootTask(p.SCtx())
	t = attachPlan2Task(p, root)
	return t
}

func attach2MppTasks4PhysicalUnionAll(p *physicalop.PhysicalUnionAll, tasks ...base.Task) base.Task {
	t := physicalop.NewMppTask(p, property.AnyType, nil, nil, nil)
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, tk := range tasks {
		if mpp, ok := tk.(*physicalop.MppTask); ok && !tk.Invalid() {
			childPlans = append(childPlans, mpp.Plan())
			continue
		}
		return base.InvalidTask
	}
	if len(childPlans) == 0 {
		return base.InvalidTask
	}
	p.SetChildren(childPlans...)
	return t
}

// attach2Task4PhysicalUnionAll implements PhysicalPlan interface logic.
func attach2Task4PhysicalUnionAll(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalUnionAll)
	for _, t := range tasks {
		if _, ok := t.(*physicalop.MppTask); ok {
			if p.TP() == plancodec.TypePartitionUnion {
				// In attach2MppTasks(), will attach PhysicalUnion to mppTask directly.
				// But PartitionUnion cannot pushdown to tiflash, so here disable PartitionUnion pushdown to tiflash explicitly.
				// For now, return base.InvalidTask immediately, we can refine this by letting childTask of PartitionUnion convert to rootTask.
				return base.InvalidTask
			}
			return attach2MppTasks4PhysicalUnionAll(p, tasks...)
		}
	}
	t := &physicalop.RootTask{}
	t.SetPlan(p)
	childPlans := make([]base.PhysicalPlan, 0, len(tasks))
	for _, task := range tasks {
		task = task.ConvertToRootTask(p.SCtx())
		childPlans = append(childPlans, task.Plan())
	}
	p.SetChildren(childPlans...)
	return t
}

// attach2Task4PhysicalSelection implements PhysicalPlan interface.
func attach2Task4PhysicalSelection(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	sel := pp.(*physicalop.PhysicalSelection)
	if mppTask, _ := tasks[0].(*physicalop.MppTask); mppTask != nil { // always push to mpp task.
		if expression.CanExprsPushDown(util.GetPushDownCtx(sel.SCtx()), sel.Conditions, kv.TiFlash) {
			return attachPlan2Task(sel, mppTask.Copy())
		}
	}
	t := tasks[0].ConvertToRootTask(sel.SCtx())
	return attachPlan2Task(sel, t)
}

func inheritStatsFromBottomElemForIndexJoinInner(p base.PhysicalPlan, indexJoinInfo *physicalop.IndexJoinInfo, stats *property.StatsInfo) {
	var isIndexJoin bool
	switch p.(type) {
	case *physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin, *physicalop.PhysicalIndexMergeJoin:
		isIndexJoin = true
	default:
	}
	// indexJoinInfo != nil means the child Task comes from an index join inner side.
	// !isIndexJoin means the childTask only be passed through to indexJoin as an END.
	if !isIndexJoin && indexJoinInfo != nil {
		switch p.(type) {
		case *physicalop.PhysicalSelection:
			// todo: for simplicity, we can just inherit it from child.
			// scale(1) means a cloned stats information same as the input stats.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		case *physicalop.PhysicalProjection:
			// mainly about the rowEst, proj doesn't change that.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
			// todo: for simplicity, we can just inherit it from child.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		case *physicalop.PhysicalUnionScan:
			// todo: for simplicity, we can just inherit it from child.
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		default:
			p.SetStats(stats.Scale(p.SCtx().GetSessionVars(), 1))
		}
	}
}

func inheritStatsFromBottomTaskForIndexJoinInner(p base.PhysicalPlan, t base.Task) {
	var indexJoinInfo *physicalop.IndexJoinInfo
	switch v := t.(type) {
	case *physicalop.CopTask:
		indexJoinInfo = v.IndexJoinInfo
	case *physicalop.RootTask:
		indexJoinInfo = v.IndexJoinInfo
	default:
		// index join's inner side couldn't be a mppTask, leave it.
	}
	inheritStatsFromBottomElemForIndexJoinInner(p, indexJoinInfo, t.Plan().StatsInfo())
}

func attach2TaskForMPP4PhysicalWindow(p *physicalop.PhysicalWindow, mpp *physicalop.MppTask) base.Task {
	// FIXME: currently, tiflash's join has different schema with TiDB,
	// so we have to rebuild the schema of join and operators which may inherit schema from join.
	// for window, we take the sub-plan's schema, and the schema generated by windowDescs.
	columns := p.Schema().Clone().Columns[len(p.Schema().Columns)-len(p.WindowFuncDescs):]
	p.SetSchema(expression.MergeSchema(mpp.Plan().Schema(), expression.NewSchema(columns...)))

	failpoint.Inject("CheckMPPWindowSchemaLength", func() {
		if len(p.Schema().Columns) != len(mpp.Plan().Schema().Columns)+len(p.WindowFuncDescs) {
			panic("mpp physical window has incorrect schema length")
		}
	})

	return attachPlan2Task(p, mpp)
}

func attach2Task4PhysicalWindow(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalWindow)
	if mpp, ok := tasks[0].Copy().(*physicalop.MppTask); ok && p.StoreTp == kv.TiFlash {
		return attach2TaskForMPP4PhysicalWindow(p, mpp)
	}
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return attachPlan2Task(p.Self, t)
}

// attach2Task4PhysicalCTEStorage implements the PhysicalPlan interface.
func attach2Task4PhysicalCTEStorage(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalCTEStorage)
	t := tasks[0].Copy()
	if mpp, ok := t.(*physicalop.MppTask); ok {
		p.SetChildren(t.Plan())
		nt := physicalop.NewMppTask(p,
			mpp.GetPartitionType(), mpp.GetHashCols(),
			mpp.GetTblColHists(), mpp.GetWarnings())
		return nt
	}
	t.ConvertToRootTask(p.SCtx())
	p.SetChildren(t.Plan())
	ta := &physicalop.RootTask{}
	ta.SetPlan(p)
	ta.Warnings.CopyFrom(&t.(*physicalop.RootTask).Warnings)
	return ta
}

// attach2Task4PhysicalSequence implements PhysicalSequence.Attach2Task.
func attach2Task4PhysicalSequence(pp base.PhysicalPlan, tasks ...base.Task) base.Task {
	p := pp.(*physicalop.PhysicalSequence)

	for _, t := range tasks {
		_, isMpp := t.(*physicalop.MppTask)
		if !isMpp {
			return tasks[len(tasks)-1]
		}
	}

	lastTask := tasks[len(tasks)-1].(*physicalop.MppTask)

	children := make([]base.PhysicalPlan, 0, len(tasks))
	for _, t := range tasks {
		children = append(children, t.Plan())
	}

	p.SetChildren(children...)

	mppTask := physicalop.NewMppTask(p, lastTask.GetPartitionType(), lastTask.GetHashCols(), lastTask.GetTblColHists(), nil)
	tmpWarnings := make([]*physicalop.SimpleWarnings, 0, len(tasks))
	for _, t := range tasks {
		if mpp, ok := t.(*physicalop.MppTask); ok {
			tmpWarnings = append(tmpWarnings, &mpp.Warnings)
			continue
		}
		if root, ok := t.(*physicalop.RootTask); ok {
			tmpWarnings = append(tmpWarnings, &root.Warnings)
			continue
		}
		if cop, ok := t.(*physicalop.CopTask); ok {
			tmpWarnings = append(tmpWarnings, &cop.Warnings)
		}
	}
	mppTask.Warnings.CopyFrom(tmpWarnings...)
	return mppTask
}

func collectRowSizeFromMPPPlan(mppPlan base.PhysicalPlan) (rowSize float64) {
	if mppPlan != nil && mppPlan.StatsInfo() != nil && mppPlan.StatsInfo().HistColl != nil {
		schemaCols := mppPlan.Schema().Columns
		for i, col := range schemaCols {
			if col.ID == model.ExtraCommitTSID {
				schemaCols = slices.Delete(slices.Clone(schemaCols), i, i+1)
				break
			}
		}
		return cardinality.GetAvgRowSize(mppPlan.SCtx(), mppPlan.StatsInfo().HistColl, schemaCols, false, false)
	}
	return 1 // use 1 as lower-bound for safety
}

func accumulateNetSeekCost4MPP(p base.PhysicalPlan) (cost float64) {
	if ts, ok := p.(*physicalop.PhysicalTableScan); ok {
		return float64(len(ts.Ranges)) * float64(len(ts.Columns)) * ts.SCtx().GetSessionVars().GetSeekFactor(ts.Table)
	}
	for _, c := range p.Children() {
		cost += accumulateNetSeekCost4MPP(c)
	}
	return
}
