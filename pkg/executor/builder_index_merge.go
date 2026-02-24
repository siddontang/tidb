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

package executor

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tipb/go-tipb"
)

func buildNoRangeIndexMergeReader(b *executorBuilder, v *physicalop.PhysicalIndexMergeReader) (*IndexMergeReaderExecutor, error) {
	partialPlanCount := len(v.PartialPlans)
	partialReqs := make([]*tipb.DAGRequest, 0, partialPlanCount)
	partialDataSizes := make([]float64, 0, partialPlanCount)
	indexes := make([]*model.IndexInfo, 0, partialPlanCount)
	descs := make([]bool, 0, partialPlanCount)
	ts := v.TablePlans[0].(*physicalop.PhysicalTableScan)
	isCorColInPartialFilters := make([]bool, 0, partialPlanCount)
	isCorColInPartialAccess := make([]bool, 0, partialPlanCount)
	hasGlobalIndex := false
	for i := range partialPlanCount {
		var tempReq *tipb.DAGRequest
		var err error

		if is, ok := v.PartialPlans[i][0].(*physicalop.PhysicalIndexScan); ok {
			tempReq, err = buildIndexReq(b.ctx, is.Index.Columns, ts.HandleCols.NumCols(), v.PartialPlans[i])
			descs = append(descs, is.Desc)
			indexes = append(indexes, is.Index)
			if is.Index.Global {
				hasGlobalIndex = true
			}
		} else {
			ts := v.PartialPlans[i][0].(*physicalop.PhysicalTableScan)
			tempReq, _, err = buildTableReq(b, len(ts.Columns), v.PartialPlans[i])
			descs = append(descs, ts.Desc)
			indexes = append(indexes, nil)
		}
		if err != nil {
			return nil, err
		}
		collect := false
		tempReq.CollectRangeCounts = &collect
		partialReqs = append(partialReqs, tempReq)
		isCorColInPartialFilters = append(isCorColInPartialFilters, b.corColInDistPlan(v.PartialPlans[i]))
		isCorColInPartialAccess = append(isCorColInPartialAccess, b.corColInAccess(v.PartialPlans[i][0]))
		partialDataSizes = append(partialDataSizes, v.GetPartialReaderNetDataSize(v.PartialPlans[i][0]))
	}
	tableReq, tblInfo, err := buildTableReq(b, v.Schema().Len(), v.TablePlans)
	isCorColInTableFilter := b.corColInDistPlan(v.TablePlans)
	if err != nil {
		return nil, err
	}
	startTS, err := b.getSnapshotTS()
	if err != nil {
		return nil, err
	}

	readerBuilder, err := b.newDataReaderBuilder(nil)
	if err != nil {
		return nil, err
	}

	b.ctx.GetSessionVars().StmtCtx.IsTiKV.Store(true)

	e := &IndexMergeReaderExecutor{
		BaseExecutor:             exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		indexUsageReporter:       b.buildIndexUsageReporter(v, true),
		dagPBs:                   partialReqs,
		startTS:                  startTS,
		table:                    tblInfo,
		indexes:                  indexes,
		descs:                    descs,
		tableRequest:             tableReq,
		columns:                  ts.Columns,
		partialPlans:             v.PartialPlans,
		tblPlans:                 v.TablePlans,
		partialNetDataSizes:      partialDataSizes,
		dataAvgRowSize:           v.GetAvgTableRowSize(),
		dataReaderBuilder:        readerBuilder,
		handleCols:               v.HandleCols,
		isCorColInPartialFilters: isCorColInPartialFilters,
		isCorColInTableFilter:    isCorColInTableFilter,
		isCorColInPartialAccess:  isCorColInPartialAccess,
		isIntersection:           v.IsIntersectionType,
		byItems:                  v.ByItems,
		pushedLimit:              v.PushedLimit,
		keepOrder:                v.KeepOrder,
		hasGlobalIndex:           hasGlobalIndex,
	}
	collectTable := false
	e.tableRequest.CollectRangeCounts = &collectTable
	return e, nil
}

type tableStatsPreloader interface {
	LoadTableStats(sessionctx.Context)
}

func buildIndexUsageReporter(ctx sessionctx.Context, plan tableStatsPreloader, loadStats bool) (indexUsageReporter *exec.IndexUsageReporter) {
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.GetSessionVars().StmtCtx.IndexUsageCollector != nil &&
		sc.RuntimeStatsColl != nil {
		if loadStats {
			// Preload the table stats. If the statement is a point-get or execute, the planner may not have loaded the
			// stats.
			plan.LoadTableStats(ctx)
		}

		statsMap := sc.GetUsedStatsInfo(false)
		indexUsageReporter = exec.NewIndexUsageReporter(
			sc.IndexUsageCollector,
			sc.RuntimeStatsColl, statsMap)
	}

	return indexUsageReporter
}

func (b *executorBuilder) buildIndexUsageReporter(plan tableStatsPreloader, loadStats bool) (indexUsageReporter *exec.IndexUsageReporter) {
	return buildIndexUsageReporter(b.ctx, plan, loadStats)
}

func (b *executorBuilder) buildIndexMergeReader(v *physicalop.PhysicalIndexMergeReader) exec.Executor {
	if b.Ti != nil {
		b.Ti.UseIndexMerge = true
		b.Ti.UseTableLookUp.Store(true)
	}
	ts := v.TablePlans[0].(*physicalop.PhysicalTableScan)
	assertByItemsAreColumns(ts.ByItems)
	if err := b.validCanReadTemporaryOrCacheTable(ts.Table); err != nil {
		b.err = err
		return nil
	}

	ret, err := buildNoRangeIndexMergeReader(b, v)
	if err != nil {
		b.err = err
		return nil
	}
	ret.ranges = make([][]*ranger.Range, 0, len(v.PartialPlans))
	sctx := b.ctx.GetSessionVars().StmtCtx
	hasGlobalIndex := false
	for i := range v.PartialPlans {
		if is, ok := v.PartialPlans[i][0].(*physicalop.PhysicalIndexScan); ok {
			assertByItemsAreColumns(is.ByItems)
			ret.ranges = append(ret.ranges, is.Ranges)
			sctx.IndexNames = append(sctx.IndexNames, is.Table.Name.O+":"+is.Index.Name.O)
			if is.Index.Global {
				hasGlobalIndex = true
			}
		} else {
			partialTS := v.PartialPlans[i][0].(*physicalop.PhysicalTableScan)
			assertByItemsAreColumns(partialTS.ByItems)
			ret.ranges = append(ret.ranges, partialTS.Ranges)
			if ret.table.Meta().IsCommonHandle {
				tblInfo := ret.table.Meta()
				sctx.IndexNames = append(sctx.IndexNames, tblInfo.Name.O+":"+tables.FindPrimaryIndex(tblInfo).Name.O)
			}
		}
	}
	sctx.TableIDs = append(sctx.TableIDs, ts.Table.ID)
	executor_metrics.ExecutorCounterIndexMergeReaderExecutor.Inc()

	if !b.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return ret
	}

	if pi := ts.Table.GetPartitionInfo(); pi == nil {
		return ret
	}

	tmp, _ := b.is.TableByID(context.Background(), ts.Table.ID)
	partitions, err := partitionPruning(b.ctx, tmp.(table.PartitionedTable), v.PlanPartInfo)
	if err != nil {
		b.err = err
		return nil
	}
	ret.partitionTableMode, ret.prunedPartitions = true, partitions
	if hasGlobalIndex {
		ret.partitionIDMap = make(map[int64]struct{})
		for _, p := range partitions {
			ret.partitionIDMap[p.GetPhysicalID()] = struct{}{}
		}
	}
	return ret
}

// dataReaderBuilder build an executor.
// The executor can be used to read data in the ranges which are constructed by datums.
// Differences from executorBuilder:
// 1. dataReaderBuilder calculate data range from argument, rather than plan.
// 2. the result executor is already opened.
type dataReaderBuilder struct {
	plan base.Plan
	*executorBuilder

	selectResultHook // for testing
	once             struct {
		sync.Once
		condPruneResult []table.PhysicalTable
		err             error
	}
}

type mockPhysicalIndexReader struct {
	base.PhysicalPlan

	e exec.Executor
}

// MemoryUsage of mockPhysicalIndexReader is only for testing
func (*mockPhysicalIndexReader) MemoryUsage() (sum int64) {
	return
}

func (builder *dataReaderBuilder) BuildExecutorForIndexJoin(ctx context.Context, lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value,
) (exec.Executor, error) {
	return builder.buildExecutorForIndexJoinInternal(ctx, builder.plan, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
}

func (builder *dataReaderBuilder) buildExecutorForIndexJoinInternal(ctx context.Context, plan base.Plan, lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value,
) (exec.Executor, error) {
	switch v := plan.(type) {
	case *physicalop.PhysicalTableReader:
		return builder.buildTableReaderForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	case *physicalop.PhysicalIndexReader:
		return builder.buildIndexReaderForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	case *physicalop.PhysicalIndexLookUpReader:
		return builder.buildIndexLookUpReaderForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
	case *physicalop.PhysicalUnionScan:
		return builder.buildUnionScanForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	case *physicalop.PhysicalProjection:
		return builder.buildProjectionForIndexJoin(ctx, v, lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	// Need to support physical selection because after PR 16389, TiDB will push down all the expr supported by TiKV or TiFlash
	// in predicate push down stage, so if there is an expr which only supported by TiFlash, a physical selection will be added after index read
	case *physicalop.PhysicalSelection:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := &SelectionExec{
			selectionExecutorContext: newSelectionExecutorContext(builder.ctx),
			BaseExecutorV2:           exec.NewBaseExecutorV2(builder.ctx.GetSessionVars(), v.Schema(), v.ID(), childExec),
			filters:                  v.Conditions,
		}
		err = exec.open(ctx)
		return exec, err
	case *physicalop.PhysicalHashAgg:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := builder.buildHashAggFromChildExec(childExec, v)
		err = exec.OpenSelf()
		return exec, err
	case *physicalop.PhysicalStreamAgg:
		childExec, err := builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		exec := builder.buildStreamAggFromChildExec(childExec, v)
		err = exec.OpenSelf()
		return exec, err
	case *physicalop.PhysicalHashJoin:
		// since merge join is rarely used now, we can only support hash join now.
		// we separate the child build flow out because we want to pass down the runtime constant --- lookupContents.
		// todo: support hash join in index join inner side.
		return nil, errors.New("Wrong plan type for dataReaderBuilder")
	case *mockPhysicalIndexReader:
		return v.e, nil
	}
	return nil, errors.New("Wrong plan type for dataReaderBuilder")
}

func (builder *dataReaderBuilder) buildUnionScanForIndexJoin(ctx context.Context, v *physicalop.PhysicalUnionScan,
	values []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int,
	cwc *physicalop.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value,
) (exec.Executor, error) {
	childBuilder, err := builder.newDataReaderBuilder(v.Children()[0])
	if err != nil {
		return nil, err
	}

	reader, err := childBuilder.BuildExecutorForIndexJoin(ctx, values, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	if err != nil {
		return nil, err
	}

	ret := builder.buildUnionScanFromReader(reader, v)
	if builder.err != nil {
		return nil, builder.err
	}
	if us, ok := ret.(*UnionScanExec); ok {
		err = us.open(ctx)
	}
	return ret, err
}

func (builder *dataReaderBuilder) buildTableReaderForIndexJoin(ctx context.Context, v *physicalop.PhysicalTableReader,
	lookUpContents []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int,
	cwc *physicalop.ColWithCmpFuncManager, canReorderHandles bool, memTracker *memory.Tracker, interruptSignal *atomic.Value,
) (exec.Executor, error) {
	e, err := buildNoRangeTableReader(builder.executorBuilder, v)
	if !canReorderHandles {
		// `canReorderHandles` is set to false only in IndexMergeJoin. IndexMergeJoin will trigger a dead loop problem
		// when enabling paging(tidb/issues/35831). But IndexMergeJoin is not visible to the user and is deprecated
		// for now. Thus, we disable paging here.
		e.paging = false
	}
	if err != nil {
		return nil, err
	}
	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		if v.IsCommonHandle {
			kvRanges, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, getPhysicalTableID(e.table), -1, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
			if err != nil {
				return nil, err
			}
			return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
		}
		handles, _ := dedupHandles(lookUpContents)
		return builder.buildTableReaderFromHandles(ctx, e, handles, canReorderHandles)
	}
	tbl, _ := builder.is.TableByID(ctx, tbInfo.ID)
	pt := tbl.(table.PartitionedTable)
	usedPartitionList, err := builder.partitionPruning(pt, v.PlanPartInfo)
	if err != nil {
		return nil, err
	}
	usedPartitions := make(map[int64]table.PhysicalTable, len(usedPartitionList))
	for _, p := range usedPartitionList {
		usedPartitions[p.GetPhysicalID()] = p
	}
	kvRanges, err := builder.buildPartitionedTableReaderKVRangesForIndexJoin(
		e.dctx, e.rctx, pt, usedPartitionList, usedPartitions, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal, v.IsCommonHandle)
	if err != nil {
		return nil, err
	}
	return builder.buildTableReaderFromKvRanges(ctx, e, kvRanges)
}

func dedupHandles(lookUpContents []*join.IndexJoinLookUpContent) ([]kv.Handle, []*join.IndexJoinLookUpContent) {
	handles := make([]kv.Handle, 0, len(lookUpContents))
	validLookUpContents := make([]*join.IndexJoinLookUpContent, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		isValidHandle := true
		handle := kv.IntHandle(content.Keys[0].GetInt64())
		for _, key := range content.Keys {
			if handle.IntValue() != key.GetInt64() {
				isValidHandle = false
				break
			}
		}
		if isValidHandle {
			handles = append(handles, handle)
			validLookUpContents = append(validLookUpContents, content)
		}
	}
	return handles, validLookUpContents
}

type kvRangeBuilderFromRangeAndPartition struct {
	partitions []table.PhysicalTable
}

func (h kvRangeBuilderFromRangeAndPartition) buildKeyRangeSeparately(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error) {
	ret := make([][]kv.KeyRange, len(h.partitions))
	pids := make([]int64, 0, len(h.partitions))
	for i, p := range h.partitions {
		pid := p.GetPhysicalID()
		pids = append(pids, pid)
		meta := p.Meta()
		if len(ranges) == 0 {
			continue
		}
		kvRange, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{pid}, meta != nil && meta.IsCommonHandle, ranges)
		if err != nil {
			return nil, nil, err
		}
		ret[i] = kvRange.AppendSelfTo(ret[i])
	}
	return pids, ret, nil
}

func (h kvRangeBuilderFromRangeAndPartition) buildKeyRange(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([][]kv.KeyRange, error) {
	ret := make([][]kv.KeyRange, len(h.partitions))
	if len(ranges) == 0 {
		return ret, nil
	}
	for i, p := range h.partitions {
		pid := p.GetPhysicalID()
		meta := p.Meta()
		kvRange, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{pid}, meta != nil && meta.IsCommonHandle, ranges)
		if err != nil {
			return nil, err
		}
		ret[i] = kvRange.AppendSelfTo(ret[i])
	}
	return ret, nil
}

// newClosestReadAdjuster let the request be sent to closest replica(within the same zone)
// if response size exceeds certain threshold.
func newClosestReadAdjuster(dctx *distsqlctx.DistSQLContext, req *kv.Request, netDataSize float64) kv.CoprRequestAdjuster {
	if req.ReplicaRead != kv.ReplicaReadClosestAdaptive {
		return nil
	}
	return func(req *kv.Request, copTaskCount int) bool {
		// copTaskCount is the number of coprocessor requests
		if int64(netDataSize/float64(copTaskCount)) >= dctx.ReplicaClosestReadThreshold {
			req.MatchStoreLabels = append(req.MatchStoreLabels, &metapb.StoreLabel{
				Key:   placement.DCLabelKey,
				Value: config.GetTxnScopeFromConfig(),
			})
			return true
		}
		// reset to read from leader when the data size is small.
		req.ReplicaRead = kv.ReplicaReadLeader
		return false
	}
}

func (builder *dataReaderBuilder) buildTableReaderBase(ctx context.Context, e *TableReaderExecutor, reqBuilderWithRange distsql.RequestBuilder) (*TableReaderExecutor, error) {
	startTS, err := builder.getSnapshotTS()
	if err != nil {
		return nil, err
	}
	kvReq, err := reqBuilderWithRange.
		SetDAGRequest(e.dagPB).
		SetStartTS(startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetTxnScope(e.txnScope).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.dctx).
		SetFromInfoSchema(e.GetInfoSchema()).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &reqBuilderWithRange.Request, e.netDataSize)).
		SetPaging(e.paging).
		SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias).
		Build()
	if err != nil {
		return nil, err
	}
	e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)
	e.resultHandler = &tableResultHandler{}
	result, err := builder.SelectResult(ctx, builder.ctx.GetDistSQLCtx(), kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
	if err != nil {
		return nil, err
	}
	e.resultHandler.open(nil, result)
	return e, nil
}

func (builder *dataReaderBuilder) buildTableReaderFromHandles(ctx context.Context, e *TableReaderExecutor, handles []kv.Handle, canReorderHandles bool) (*TableReaderExecutor, error) {
	if canReorderHandles {
		slices.SortFunc(handles, func(i, j kv.Handle) int {
			return i.Compare(j)
		})
	}
	var b distsql.RequestBuilder
	if len(handles) > 0 {
		if _, ok := handles[0].(kv.PartitionHandle); ok {
			b.SetPartitionsAndHandles(handles)
		} else {
			b.SetTableHandles(getPhysicalTableID(e.table), handles)
		}
	} else {
		b.SetKeyRanges(nil)
	}
	return builder.buildTableReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildTableReaderFromKvRanges(ctx context.Context, e *TableReaderExecutor, ranges []kv.KeyRange) (exec.Executor, error) {
	var b distsql.RequestBuilder
	b.SetKeyRanges(ranges)
	return builder.buildTableReaderBase(ctx, e, b)
}

func (builder *dataReaderBuilder) buildIndexReaderForIndexJoin(ctx context.Context, v *physicalop.PhysicalIndexReader,
	lookUpContents []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager, memoryTracker *memory.Tracker, interruptSignal *atomic.Value,
) (exec.Executor, error) {
	e, err := buildNoRangeIndexReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}
	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		kvRanges, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, e.physicalTableID, e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memoryTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		err = e.open(ctx, kvRanges)
		return e, err
	}

	is := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	if is.Index.Global {
		e.partitionIDMap, err = getPartitionIDsAfterPruning(builder.ctx, e.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			return nil, err
		}
		if e.ranges, err = buildRangesForIndexJoin(e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc); err != nil {
			return nil, err
		}
		if err := exec.Open(ctx, e); err != nil {
			return nil, err
		}
		return e, nil
	}

	rangeResult, err := builder.buildIndexJoinPartitionRanges(ctx, tbInfo.ID, v.PlanPartInfo, e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	if rangeResult.empty {
		ret := &TableDualExec{BaseExecutorV2: e.BaseExecutorV2}
		err = exec.Open(ctx, ret)
		return ret, err
	}
	e.partitions = rangeResult.partitions
	e.ranges = rangeResult.ranges
	if rangeResult.canPrune {
		e.partRangeMap = rangeResult.rangeMap
	}
	if err := exec.Open(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

func (builder *dataReaderBuilder) buildIndexLookUpReaderForIndexJoin(ctx context.Context, v *physicalop.PhysicalIndexLookUpReader,
	lookUpContents []*join.IndexJoinLookUpContent, indexRanges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value,
) (exec.Executor, error) {
	if builder.Ti != nil {
		builder.Ti.UseTableLookUp.Store(true)
	}
	e, err := buildNoRangeIndexLookUpReader(builder.executorBuilder, v)
	if err != nil {
		return nil, err
	}

	tbInfo := e.table.Meta()
	if tbInfo.GetPartitionInfo() == nil || !builder.ctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		kvRange, err := buildKvRangesForIndexJoin(e.dctx, e.rctx, getPhysicalTableID(e.table), e.index.ID, lookUpContents, indexRanges, keyOff2IdxOff, cwc, memTracker, interruptSignal)
		if err != nil {
			return nil, err
		}
		e.groupedKVRanges = []*kvRangesWithPhysicalTblID{{
			PhysicalTableID: getPhysicalTableID(e.table),
			KeyRanges:       kvRange,
		}}
		err = e.open(ctx)
		return e, err
	}

	is := v.IndexPlans[0].(*physicalop.PhysicalIndexScan)
	if is.Index.Global {
		e.partitionIDMap, err = getPartitionIDsAfterPruning(builder.ctx, e.table.(table.PartitionedTable), v.PlanPartInfo)
		if err != nil {
			return nil, err
		}
		e.ranges, err = buildRangesForIndexJoin(e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
		if err != nil {
			return nil, err
		}
		if err := exec.Open(ctx, e); err != nil {
			return nil, err
		}
		return e, err
	}

	rangeResult, err := builder.buildIndexJoinPartitionRanges(ctx, tbInfo.ID, v.PlanPartInfo, e.rctx, lookUpContents, indexRanges, keyOff2IdxOff, cwc)
	if err != nil {
		return nil, err
	}
	if rangeResult.empty {
		ret := &TableDualExec{BaseExecutorV2: e.BaseExecutorV2}
		err = exec.Open(ctx, ret)
		return ret, err
	}
	e.prunedPartitions = rangeResult.partitions
	e.ranges = rangeResult.ranges
	if rangeResult.canPrune {
		e.partitionRangeMap = rangeResult.rangeMap
	}
	e.partitionTableMode = true
	if err := exec.Open(ctx, e); err != nil {
		return nil, err
	}
	return e, err
}

func (builder *dataReaderBuilder) buildProjectionForIndexJoin(
	ctx context.Context,
	v *physicalop.PhysicalProjection,
	lookUpContents []*join.IndexJoinLookUpContent,
	indexRanges []*ranger.Range,
	keyOff2IdxOff []int,
	cwc *physicalop.ColWithCmpFuncManager,
	canReorderHandles bool,
	memTracker *memory.Tracker,
	interruptSignal *atomic.Value,
) (executor exec.Executor, err error) {
	var childExec exec.Executor
	childExec, err = builder.buildExecutorForIndexJoinInternal(ctx, v.Children()[0], lookUpContents, indexRanges, keyOff2IdxOff, cwc, canReorderHandles, memTracker, interruptSignal)
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
		if err != nil {
			terror.Log(exec.Close(childExec))
		}
	}()

	e := builder.newProjectionExec(childExec, v)
	failpoint.Inject("buildProjectionForIndexJoinPanic", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			panic("buildProjectionForIndexJoinPanic")
		}
	})
	err = e.open(ctx)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// buildRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildRangesForIndexJoin(rctx *rangerctx.RangerContext, lookUpContents []*join.IndexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager,
) ([]*ranger.Range, error) {
	retRanges := make([]*ranger.Range, 0, len(ranges)*len(lookUpContents))
	lastPos := len(ranges[0].LowVal) - 1
	tmpDatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.Keys[keyOff]
				ran.HighVal[idxOff] = content.Keys[keyOff]
			}
		}
		if cwc == nil {
			// A deep copy is need here because the old []*range.Range is overwritten
			for _, ran := range ranges {
				retRanges = append(retRanges, ran.Clone())
			}
			continue
		}
		nextColRanges, err := cwc.BuildRangesByRow(rctx, content.Row)
		if err != nil {
			return nil, err
		}
		for _, nextColRan := range nextColRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextColRan.LowVal[0]
				ran.HighVal[lastPos] = nextColRan.HighVal[0]
				ran.LowExclude = nextColRan.LowExclude
				ran.HighExclude = nextColRan.HighExclude
				ran.Collators = nextColRan.Collators
				tmpDatumRanges = append(tmpDatumRanges, ran.Clone())
			}
		}
	}

	if cwc == nil {
		return retRanges, nil
	}

	return ranger.UnionRanges(rctx, tmpDatumRanges, true)
}

// buildKvRangesForIndexJoin builds kv ranges for index join when the inner plan is index scan plan.
func buildKvRangesForIndexJoin(dctx *distsqlctx.DistSQLContext, pctx *rangerctx.RangerContext, tableID, indexID int64, lookUpContents []*join.IndexJoinLookUpContent,
	ranges []*ranger.Range, keyOff2IdxOff []int, cwc *physicalop.ColWithCmpFuncManager, memTracker *memory.Tracker, interruptSignal *atomic.Value,
) (_ []kv.KeyRange, err error) {
	kvRanges := make([]kv.KeyRange, 0, len(ranges)*len(lookUpContents))
	if len(ranges) == 0 {
		return []kv.KeyRange{}, nil
	}
	lastPos := len(ranges[0].LowVal) - 1
	tmpDatumRanges := make([]*ranger.Range, 0, len(lookUpContents))
	for _, content := range lookUpContents {
		for _, ran := range ranges {
			for keyOff, idxOff := range keyOff2IdxOff {
				ran.LowVal[idxOff] = content.Keys[keyOff]
				ran.HighVal[idxOff] = content.Keys[keyOff]
			}
		}
		if cwc == nil {
			// Index id is -1 means it's a common handle.
			var tmpKvRanges *kv.KeyRanges
			var err error
			if indexID == -1 {
				tmpKvRanges, err = distsql.CommonHandleRangesToKVRanges(dctx, []int64{tableID}, ranges)
			} else {
				tmpKvRanges, err = distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, tableID, indexID, ranges, memTracker, interruptSignal)
			}
			if err != nil {
				return nil, err
			}
			kvRanges = tmpKvRanges.AppendSelfTo(kvRanges)
			continue
		}
		nextColRanges, err := cwc.BuildRangesByRow(pctx, content.Row)
		if err != nil {
			return nil, err
		}
		for _, nextColRan := range nextColRanges {
			for _, ran := range ranges {
				ran.LowVal[lastPos] = nextColRan.LowVal[0]
				ran.HighVal[lastPos] = nextColRan.HighVal[0]
				ran.LowExclude = nextColRan.LowExclude
				ran.HighExclude = nextColRan.HighExclude
				ran.Collators = nextColRan.Collators
				tmpDatumRanges = append(tmpDatumRanges, ran.Clone())
			}
		}
	}
	if len(kvRanges) != 0 && memTracker != nil {
		failpoint.Inject("testIssue49033", func() {
			panic("testIssue49033")
		})
		memTracker.Consume(int64(2 * cap(kvRanges[0].StartKey) * len(kvRanges)))
	}
	if len(tmpDatumRanges) != 0 && memTracker != nil {
		memTracker.Consume(2 * types.EstimatedMemUsage(tmpDatumRanges[0].LowVal, len(tmpDatumRanges)))
	}
	if cwc == nil {
		slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		})
		return kvRanges, nil
	}

	tmpDatumRanges, err = ranger.UnionRanges(pctx, tmpDatumRanges, true)
	if err != nil {
		return nil, err
	}
	// Index id is -1 means it's a common handle.
	if indexID == -1 {
		tmpKeyRanges, err := distsql.CommonHandleRangesToKVRanges(dctx, []int64{tableID}, tmpDatumRanges)
		return tmpKeyRanges.FirstPartitionRange(), err
	}
	tmpKeyRanges, err := distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, tableID, indexID, tmpDatumRanges, memTracker, interruptSignal)
	return tmpKeyRanges.FirstPartitionRange(), err
}
