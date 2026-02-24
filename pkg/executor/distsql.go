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

package executor

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	isctx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ exec.Executor = &TableReaderExecutor{}
	_ exec.Executor = &IndexReaderExecutor{}
	_ exec.Executor = &IndexLookUpExecutor{}
)

// LookupTableTaskChannelSize represents the channel size of the index double read taskChan.
var LookupTableTaskChannelSize int32 = 50

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	id      int
	handles []kv.Handle
	rowIdx  []int // rowIdx represents the handle index for every row. Only used when keep order.
	rows    []chunk.Row
	idxRows *chunk.Chunk
	cursor  int

	// after the cop task is built, buildDone will be set to the current instant, for Next wait duration statistic.
	buildDoneTime time.Time
	doneCh        chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder *kv.HandleMap
	// duplicatedIndexOrder map likes indexOrder. But it's used when checkIndexValue isn't nil and
	// the same handle of index has multiple values.
	duplicatedIndexOrder *kv.HandleMap

	// partitionTable indicates whether this task belongs to a partition table and which partition table it is.
	partitionTable table.PhysicalTable

	// memUsage records the memory usage of this task calculated by table worker.
	// memTracker is used to release memUsage after task is done and unused.
	//
	// The sequence of function calls are:
	//   1. calculate task.memUsage.
	//   2. task.memTracker = tableWorker.memTracker
	//   3. task.memTracker.Consume(task.memUsage)
	//   4. task.memTracker.Consume(-task.memUsage)
	//
	// Step 1~3 are completed in "tableWorker.executeTask".
	// Step 4   is  completed in "IndexLookUpExecutor.Next".
	memUsage   int64
	memTracker *memory.Tracker
}

func (task *lookupTableTask) Len() int {
	return len(task.rows)
}

func (task *lookupTableTask) Less(i, j int) bool {
	return task.rowIdx[i] < task.rowIdx[j]
}

func (task *lookupTableTask) Swap(i, j int) {
	task.rowIdx[i], task.rowIdx[j] = task.rowIdx[j], task.rowIdx[i]
	task.rows[i], task.rows[j] = task.rows[j], task.rows[i]
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

// closeAll closes all objects even if an object returns an error.
// If multiple objects returns error, the first error will be returned.
func closeAll(objs ...Closeable) error {
	var err error
	for _, obj := range objs {
		if obj != nil {
			err1 := obj.Close()
			if err == nil && err1 != nil {
				err = err1
			}
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// rebuildIndexRanges will be called if there's correlated column in access conditions. We will rebuild the range
// by substituting correlated column with the constant.
func rebuildIndexRanges(ectx expression.BuildContext, rctx *rangerctx.RangerContext, is *physicalop.PhysicalIndexScan, idxCols []*expression.Column, colLens []int) (ranges []*ranger.Range, err error) {
	access := make([]expression.Expression, 0, len(is.AccessCondition))
	for _, cond := range is.AccessCondition {
		newCond, err1 := expression.SubstituteCorCol2Constant(ectx, cond)
		if err1 != nil {
			return nil, err1
		}
		access = append(access, newCond)
	}
	// All of access conditions must be used to build ranges, so we don't limit range memory usage.
	ranges, _, err = ranger.DetachSimpleCondAndBuildRangeForIndex(rctx, access, idxCols, colLens, 0)
	return ranges, err
}

type indexReaderExecutorContext struct {
	rctx       *rangerctx.RangerContext
	dctx       *distsqlctx.DistSQLContext
	ectx       expression.BuildContext
	infoSchema isctx.MetaOnlyInfoSchema
	buildPBCtx *planctx.BuildPBContext

	stmtMemTracker *memory.Tracker
}

func newIndexReaderExecutorContext(sctx sessionctx.Context) indexReaderExecutorContext {
	pctx := sctx.GetPlanCtx()

	return indexReaderExecutorContext{
		rctx:           pctx.GetRangerCtx(),
		dctx:           sctx.GetDistSQLCtx(),
		ectx:           sctx.GetExprCtx(),
		infoSchema:     pctx.GetInfoSchema(),
		buildPBCtx:     pctx.GetBuildPBCtx(),
		stmtMemTracker: sctx.GetSessionVars().StmtCtx.MemTracker,
	}
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	indexReaderExecutorContext
	exec.BaseExecutorV2
	indexUsageReporter *exec.IndexUsageReporter

	// For a partitioned table, the IndexReaderExecutor works on a partition, so
	// the type of this table field is actually `table.PhysicalTable`.
	table           table.Table
	index           *model.IndexInfo
	physicalTableID int64
	ranges          []*ranger.Range
	// groupedRanges is from AccessPath.groupedRanges, please see the comment there for more details.
	// In brief, it splits IndexReaderExecutor.ranges into groups. When it's set, we need to access them respectively
	// and use a merge sort to combine them.
	groupedRanges [][]*ranger.Range
	partitions    []table.PhysicalTable
	partRangeMap  map[int64][]*ranger.Range // each partition may have different ranges

	// kvRanges are only used for union scan.
	kvRanges         []kv.KeyRange
	dagPB            *tipb.DAGRequest
	startTS          uint64
	txnScope         string
	readReplicaScope string
	isStaleness      bool
	netDataSize      float64
	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result distsql.SelectResult
	// columns are only required by union scan.
	columns []*model.ColumnInfo
	// outputColumns are only required by union scan.
	outputColumns []*expression.Column
	// partitionIDMap are only required by union scan with global index.
	partitionIDMap map[int64]struct{}

	paging bool

	keepOrder bool
	desc      bool
	// byItems only for partition table with orderBy + pushedLimit
	byItems []*plannerutil.ByItems

	corColInFilter bool
	corColInAccess bool
	idxCols        []*expression.Column
	colLens        []int
	plans          []base.PhysicalPlan

	memTracker *memory.Tracker

	selectResultHook // for testing

	// If dummy flag is set, this is not a real IndexReader, it just provides the KV ranges for UnionScan.
	// Used by the temporary table, cached table.
	dummy bool
}

// Table implements the dataSourceExecutor interface.
func (e *IndexReaderExecutor) Table() table.Table {
	return e.table
}

func (e *IndexReaderExecutor) setDummy() {
	e.dummy = true
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() (err error) {
	if e.indexUsageReporter != nil {
		e.indexUsageReporter.ReportCopIndexUsageForTable(e.table, e.index.ID, e.plans[0].ID())
	}

	if e.result != nil {
		err = e.result.Close()
	}
	e.result = nil
	e.kvRanges = e.kvRanges[:0]
	if e.dummy {
		return nil
	}
	return err
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.dummy {
		req.Reset()
		return nil
	}

	return e.result.Next(ctx, req)
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		is := e.plans[0].(*physicalop.PhysicalIndexScan)
		e.ranges, err = rebuildIndexRanges(e.ectx, e.rctx, is, e.idxCols, e.colLens)
		if err != nil {
			return err
		}
		// Rebuild groupedRanges if it was originally set
		if len(is.GroupByColIdxs) != 0 {
			e.groupedRanges, err = plannercore.GroupRangesByCols(e.ranges, is.GroupByColIdxs)
			if err != nil {
				return err
			}
		}
	}

	// partRangeMap comes from the index join code path, while groupedRanges will not be set in that case.
	// They are two different sources of ranges, and should not appear together.
	intest.Assert(!(len(e.partRangeMap) > 0 && len(e.groupedRanges) > 0), "partRangeMap and groupedRanges should not appear together")

	// Build kvRanges considering both partitions and groupedRanges
	kvRanges, err := e.buildKVRangesForIndexReader()
	if err != nil {
		return err
	}

	return e.open(ctx, kvRanges)
}

// buildKVRangesForIndexReader builds kvRanges for IndexReaderExecutor considering both partitions and groupedRanges.
func (e *IndexReaderExecutor) buildKVRangesForIndexReader() ([]kv.KeyRange, error) {
	tableIDs := make([]int64, 0, len(e.partitions))
	for _, p := range e.partitions {
		tableIDs = append(tableIDs, p.GetPhysicalID())
	}
	if len(e.partitions) == 0 {
		tableIDs = append(tableIDs, e.physicalTableID)
	}

	groupedRanges := e.groupedRanges
	if len(groupedRanges) == 0 {
		groupedRanges = [][]*ranger.Range{e.ranges}
	}

	results := make([]kv.KeyRange, 0, len(groupedRanges))
	for _, ranges := range groupedRanges {
		kvRanges, err := buildKeyRanges(e.dctx, ranges, e.partRangeMap, tableIDs, e.index.ID, nil)
		if err != nil {
			return nil, err
		}
		for _, kvRange := range kvRanges {
			results = append(results, kvRange...)
		}
	}
	return results, nil
}

func (e *IndexReaderExecutor) buildKVReq(r []kv.KeyRange) (*kv.Request, error) {
	var builder distsql.RequestBuilder
	builder.SetKeyRanges(r).
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetTxnScope(e.txnScope).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.dctx).
		SetFromInfoSchema(e.infoSchema).
		SetMemTracker(e.memTracker).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &builder.Request, e.netDataSize)).
		SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias)
	kvReq, err := builder.Build()
	return kvReq, err
}

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	var err error
	if e.corColInFilter {
		e.dagPB.Executors, err = builder.ConstructListBasedDistExec(e.buildPBCtx, e.plans)
		if err != nil {
			return err
		}
	}

	if e.RuntimeStats() != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	e.kvRanges = kvRanges
	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	// In a test case IndexReaderExecutor is mocked and e.table is nil.
	// Avoid sending distsql request to TIKV.
	if e.dummy {
		return nil
	}

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)
	slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})
	if !needMergeSort(e.byItems, len(kvRanges)) {
		kvReq, err := e.buildKVReq(kvRanges)
		if err != nil {
			return err
		}
		e.result, err = e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		if err != nil {
			return err
		}
	} else {
		// Use sortedSelectResults for merge sort
		kvReqs := make([]*kv.Request, 0, len(kvRanges))
		for _, kvRange := range kvRanges {
			kvReq, err := e.buildKVReq([]kv.KeyRange{kvRange})
			if err != nil {
				return err
			}
			kvReqs = append(kvReqs, kvReq)
		}
		var results []distsql.SelectResult
		for _, kvReq := range kvReqs {
			result, err := e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		e.result = distsql.NewSortedSelectResults(e.ectx.GetEvalCtx(), results, e.Schema(), e.byItems, e.memTracker)
	}
	return nil
}
