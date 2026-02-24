// Copyright 2020 PingCAP, Inc.
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
	"container/heap"
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type indexMergeProcessWorker struct {
	indexMerge *IndexMergeReaderExecutor
	stats      *IndexMergeRuntimeStat
}

type rowIdx struct {
	partialID int
	taskID    int
	rowID     int
}

type handleHeap struct {
	// requiredCnt == 0 means need all handles
	requiredCnt uint64
	tracker     *memory.Tracker
	taskMap     map[int][]*indexMergeTableTask

	idx         []rowIdx
	compareFunc []chunk.CompareFunc
	byItems     []*plannerutil.ByItems
}

func (h handleHeap) Len() int {
	return len(h.idx)
}

func (h handleHeap) Less(i, j int) bool {
	rowI := h.taskMap[h.idx[i].partialID][h.idx[i].taskID].idxRows.GetRow(h.idx[i].rowID)
	rowJ := h.taskMap[h.idx[j].partialID][h.idx[j].taskID].idxRows.GetRow(h.idx[j].rowID)

	for i, compFunc := range h.compareFunc {
		cmp := compFunc(rowI, i, rowJ, i)
		if !h.byItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func (h handleHeap) Swap(i, j int) {
	h.idx[i], h.idx[j] = h.idx[j], h.idx[i]
}

func (h *handleHeap) Push(x any) {
	idx := x.(rowIdx)
	h.idx = append(h.idx, idx)
	if h.tracker != nil {
		h.tracker.Consume(int64(unsafe.Sizeof(h.idx)))
	}
}

func (h *handleHeap) Pop() any {
	idxRet := h.idx[len(h.idx)-1]
	h.idx = h.idx[:len(h.idx)-1]
	if h.tracker != nil {
		h.tracker.Consume(-int64(unsafe.Sizeof(h.idx)))
	}
	return idxRet
}

func (w *indexMergeProcessWorker) NewHandleHeap(taskMap map[int][]*indexMergeTableTask, memTracker *memory.Tracker) *handleHeap {
	compareFuncs := make([]chunk.CompareFunc, 0, len(w.indexMerge.byItems))
	for _, item := range w.indexMerge.byItems {
		keyType := item.Expr.GetType(w.indexMerge.Ctx().GetExprCtx().GetEvalCtx())
		compareFuncs = append(compareFuncs, chunk.GetCompareFunc(keyType))
	}

	requiredCnt := uint64(0)
	if w.indexMerge.pushedLimit != nil {
		// Pre-allocate up to 1024 to avoid oom
		requiredCnt = min(1024, w.indexMerge.pushedLimit.Count+w.indexMerge.pushedLimit.Offset)
	}
	return &handleHeap{
		requiredCnt: requiredCnt,
		tracker:     memTracker,
		taskMap:     taskMap,
		idx:         make([]rowIdx, 0, requiredCnt),
		compareFunc: compareFuncs,
		byItems:     w.indexMerge.byItems,
	}
}

// pruneTableWorkerTaskIdxRows prune idxRows and only keep columns that will be used in byItems.
// e.g. the common handle is (`b`, `c`) and order by with column `c`, we should make column `c` at the first.
func (w *indexMergeProcessWorker) pruneTableWorkerTaskIdxRows(task *indexMergeTableTask) {
	if task.idxRows == nil {
		return
	}
	// IndexScan no need to prune retChk, Columns required by byItems are always first.
	if plan, ok := w.indexMerge.partialPlans[task.partialPlanID][0].(*physicalop.PhysicalTableScan); ok {
		prune := make([]int, 0, len(w.indexMerge.byItems))
		for _, item := range plan.ByItems {
			c, _ := item.Expr.(*expression.Column)
			idx := plan.Schema().ColumnIndex(c)
			// couldn't equals to -1 here, if idx == -1, just let it panic
			prune = append(prune, idx)
		}
		task.idxRows = task.idxRows.Prune(prune)
	}
}

func (w *indexMergeProcessWorker) fetchLoopUnionWithOrderBy(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	memTracker := memory.NewTracker(w.indexMerge.ID(), -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)

	if w.stats != nil {
		start := time.Now()
		defer func() {
			w.stats.IndexMergeProcess += time.Since(start)
		}()
	}

	distinctHandles := kv.NewHandleMap()
	taskMap := make(map[int][]*indexMergeTableTask)
	uselessMap := make(map[int]struct{})
	taskHeap := w.NewHandleHeap(taskMap, memTracker)

	for task := range fetchCh {
		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}
		if _, ok := uselessMap[task.partialPlanID]; ok {
			continue
		}
		if _, ok := taskMap[task.partialPlanID]; !ok {
			taskMap[task.partialPlanID] = make([]*indexMergeTableTask, 0, 1)
		}
		w.pruneTableWorkerTaskIdxRows(task)
		taskMap[task.partialPlanID] = append(taskMap[task.partialPlanID], task)
		for i, h := range task.handles {
			if _, ok := distinctHandles.Get(h); !ok {
				distinctHandles.Set(h, true)
				heap.Push(taskHeap, rowIdx{task.partialPlanID, len(taskMap[task.partialPlanID]) - 1, i})
				if int(taskHeap.requiredCnt) != 0 && taskHeap.Len() > int(taskHeap.requiredCnt) {
					top := heap.Pop(taskHeap).(rowIdx)
					if top.partialID == task.partialPlanID && top.taskID == len(taskMap[task.partialPlanID])-1 && top.rowID == i {
						uselessMap[task.partialPlanID] = struct{}{}
						task.handles = task.handles[:i]
						break
					}
				}
			}
			memTracker.Consume(int64(h.MemUsage()))
		}
		memTracker.Consume(task.idxRows.MemoryUsage())
		if len(uselessMap) == len(w.indexMerge.partialPlans) {
			// consume reset tasks
			go func() {
				channel.Clear(fetchCh)
			}()
			break
		}
	}

	needCount := taskHeap.Len()
	if w.indexMerge.pushedLimit != nil {
		needCount = max(0, taskHeap.Len()-int(w.indexMerge.pushedLimit.Offset))
	}
	if needCount == 0 {
		return
	}
	fhs := make([]kv.Handle, needCount)
	for i := needCount - 1; i >= 0; i-- {
		idx := heap.Pop(taskHeap).(rowIdx)
		fhs[i] = taskMap[idx.partialID][idx.taskID].handles[idx.rowID]
	}

	batchSize := w.indexMerge.Ctx().GetSessionVars().IndexLookupSize
	tasks := make([]*indexMergeTableTask, 0, len(fhs)/batchSize+1)
	for len(fhs) > 0 {
		l := min(len(fhs), batchSize)
		// Save the index order.
		indexOrder := kv.NewHandleMap()
		for i, h := range fhs[:l] {
			indexOrder.Set(h, i)
		}
		tasks = append(tasks, &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				handles:    fhs[:l],
				indexOrder: indexOrder,
				doneCh:     make(chan error, 1),
			},
		})
		fhs = fhs[l:]
	}
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case resultCh <- task:
			failpoint.Inject("testCancelContext", func() {
				IndexMergeCancelFuncForTest()
			})
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				continue
			}
		}
	}
}

func pushedLimitCountingDown(pushedLimit *physicalop.PushedDownLimit, handles []kv.Handle) (next bool, res []kv.Handle) {
	fhsLen := uint64(len(handles))
	// The number of handles is less than the offset, discard all handles.
	if fhsLen <= pushedLimit.Offset {
		pushedLimit.Offset -= fhsLen
		return true, nil
	}
	handles = handles[pushedLimit.Offset:]
	pushedLimit.Offset = 0

	fhsLen = uint64(len(handles))
	// The number of handles is greater than the limit, only keep limit count.
	if fhsLen > pushedLimit.Count {
		handles = handles[:pushedLimit.Count]
	}
	pushedLimit.Count -= min(pushedLimit.Count, fhsLen)
	return false, handles
}

func (w *indexMergeProcessWorker) fetchLoopUnion(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	failpoint.Inject("testIndexMergeResultChCloseEarly", func(_ failpoint.Value) {
		failpoint.Return()
	})
	memTracker := memory.NewTracker(w.indexMerge.ID(), -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)
	failpoint.Inject("testIndexMergePanicProcessWorkerUnion", nil)

	var pushedLimit *physicalop.PushedDownLimit
	if w.indexMerge.pushedLimit != nil {
		pushedLimit = w.indexMerge.pushedLimit.Clone()
	}
	hMap := kv.NewHandleMap()
	for {
		var ok bool
		var task *indexMergeTableTask
		if pushedLimit != nil && pushedLimit.Count == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-fetchCh:
			if !ok {
				return
			}
		}

		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}
		start := time.Now()
		handles := task.handles
		fhs := make([]kv.Handle, 0, 8)

		memTracker.Consume(int64(cap(task.handles) * 8))
		for _, h := range handles {
			if w.indexMerge.partitionTableMode {
				if _, ok := h.(kv.PartitionHandle); !ok {
					h = kv.NewPartitionHandle(task.partitionTable.GetPhysicalID(), h)
				}
			}
			if _, ok := hMap.Get(h); !ok {
				fhs = append(fhs, h)
				hMap.Set(h, true)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		if pushedLimit != nil {
			next, res := pushedLimitCountingDown(pushedLimit, fhs)
			if next {
				continue
			}
			fhs = res
		}
		task = &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				handles: fhs,
				doneCh:  make(chan error, 1),

				partitionTable: task.partitionTable,
			},
		}
		if w.stats != nil {
			w.stats.IndexMergeProcess += time.Since(start)
		}
		failpoint.Inject("testIndexMergeProcessWorkerUnionHang", func(_ failpoint.Value) {
			for range cap(resultCh) {
				select {
				case resultCh <- &indexMergeTableTask{}:
				default:
				}
			}
		})
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case resultCh <- task:
			failpoint.Inject("testCancelContext", func() {
				IndexMergeCancelFuncForTest()
			})
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
			}
		}
	}
}

// intersectionCollectWorker is used to dispatch index-merge-table-task to original workCh and resultCh.
// a kind of interceptor to control the pushed down limit restriction. (should be no performance impact)
type intersectionCollectWorker struct {
	pushedLimit *physicalop.PushedDownLimit
	collectCh   chan *indexMergeTableTask
	limitDone   chan struct{}
}

func (w *intersectionCollectWorker) doIntersectionLimitAndDispatch(ctx context.Context, workCh chan<- *indexMergeTableTask,
	resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	var (
		ok   bool
		task *indexMergeTableTask
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-w.collectCh:
			if !ok {
				return
			}
			// receive a new intersection task here, adding limit restriction logic
			if w.pushedLimit != nil {
				if w.pushedLimit.Count == 0 {
					// close limitDone channel to notify intersectionProcessWorkers * N to exit.
					close(w.limitDone)
					return
				}
				next, handles := pushedLimitCountingDown(w.pushedLimit, task.handles)
				if next {
					continue
				}
				task.handles = handles
			}
			// dispatch the new task to workCh and resultCh.
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				select {
				case <-ctx.Done():
					return
				case <-finished:
					return
				case resultCh <- task:
				}
			}
		}
	}
}

type intersectionProcessWorker struct {
	// key: parTblIdx, val: HandleMap
	// Value of MemAwareHandleMap is *int to avoid extra Get().
	handleMapsPerWorker map[int]*kv.MemAwareHandleMap[*int]
	workerID            int
	workerCh            chan *indexMergeTableTask
	indexMerge          *IndexMergeReaderExecutor
	memTracker          *memory.Tracker
	batchSize           int

	// When rowDelta == memConsumeBatchSize, Consume(memUsage)
	rowDelta      int64
	mapUsageDelta int64

	partitionIDMap map[int64]int
}

func (w *intersectionProcessWorker) consumeMemDelta() {
	w.memTracker.Consume(w.mapUsageDelta + w.rowDelta*int64(unsafe.Sizeof(int(0))))
	w.mapUsageDelta = 0
	w.rowDelta = 0
}

// doIntersectionPerPartition fetch all the task from workerChannel, and after that, then do the intersection pruning, which
// will cause wasting a lot of time waiting for all the fetch task done.
func (w *intersectionProcessWorker) doIntersectionPerPartition(ctx context.Context, workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished, limitDone <-chan struct{}) {
	failpoint.Inject("testIndexMergePanicPartitionTableIntersectionWorker", nil)
	defer w.memTracker.Detach()

	for task := range w.workerCh {
		var ok bool
		var hMap *kv.MemAwareHandleMap[*int]
		if hMap, ok = w.handleMapsPerWorker[task.parTblIdx]; !ok {
			hMap = kv.NewMemAwareHandleMap[*int]()
			w.handleMapsPerWorker[task.parTblIdx] = hMap
		}
		var mapDelta, rowDelta int64
		for _, h := range task.handles {
			if w.indexMerge.hasGlobalIndex {
				if ph, ok := h.(kv.PartitionHandle); ok {
					if v, exists := w.partitionIDMap[ph.PartitionID]; exists {
						if hMap, ok = w.handleMapsPerWorker[v]; !ok {
							hMap = kv.NewMemAwareHandleMap[*int]()
							w.handleMapsPerWorker[v] = hMap
						}
					}
				} else {
					h = kv.NewPartitionHandle(task.partitionTable.GetPhysicalID(), h)
				}
			}
			// Use *int to avoid Get() again.
			if cntPtr, ok := hMap.Get(h); ok {
				(*cntPtr)++
			} else {
				cnt := 1
				mapDelta += hMap.Set(h, &cnt) + int64(h.ExtraMemSize())
				rowDelta++
			}
		}

		logutil.BgLogger().Debug("intersectionProcessWorker handle tasks", zap.Int("workerID", w.workerID),
			zap.Int("task.handles", len(task.handles)), zap.Int64("rowDelta", rowDelta))

		w.mapUsageDelta += mapDelta
		w.rowDelta += rowDelta
		if w.rowDelta >= int64(w.batchSize) {
			w.consumeMemDelta()
		}
		failpoint.Inject("testIndexMergeIntersectionWorkerPanic", nil)
	}
	if w.rowDelta > 0 {
		w.consumeMemDelta()
	}

	// We assume the result of intersection is small, so no need to track memory.
	intersectedMap := make(map[int][]kv.Handle, len(w.handleMapsPerWorker))
	for parTblIdx, hMap := range w.handleMapsPerWorker {
		hMap.Range(func(h kv.Handle, val *int) bool {
			if *(val) == len(w.indexMerge.partialPlans) {
				// Means all partial paths have this handle.
				intersectedMap[parTblIdx] = append(intersectedMap[parTblIdx], h)
			}
			return true
		})
	}

	tasks := make([]*indexMergeTableTask, 0, len(w.handleMapsPerWorker))
	for parTblIdx, intersected := range intersectedMap {
		// Split intersected[parTblIdx] to avoid task is too large.
		for len(intersected) > 0 {
			length := min(w.batchSize, len(intersected))
			task := &indexMergeTableTask{
				lookupTableTask: lookupTableTask{
					handles: intersected[:length],
					doneCh:  make(chan error, 1),
				},
			}
			intersected = intersected[length:]
			if w.indexMerge.partitionTableMode {
				task.partitionTable = w.indexMerge.prunedPartitions[parTblIdx]
			}
			tasks = append(tasks, task)
			logutil.BgLogger().Debug("intersectionProcessWorker build tasks",
				zap.Int("parTblIdx", parTblIdx), zap.Int("task.handles", len(task.handles)))
		}
	}
	failpoint.Inject("testIndexMergeProcessWorkerIntersectionHang", func(_ failpoint.Value) {
		if resultCh != nil {
			for range cap(resultCh) {
				select {
				case resultCh <- &indexMergeTableTask{}:
				default:
				}
			}
		}
	})
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case <-limitDone:
			// limitDone has signal means the collectWorker has collected enough results, shutdown process workers quickly here.
			return
		case workCh <- task:
			// resultCh != nil means there is no collectWorker, and we should send task to resultCh too by ourselves here.
			if resultCh != nil {
				select {
				case <-ctx.Done():
					return
				case <-finished:
					return
				case resultCh <- task:
				}
			}
		}
	}
}

// for every index merge process worker, it should be feed on a sortedSelectResult for every partial index plan (constructed on all
// table partition ranges results on that index plan path). Since every partial index path is a sorted select result, we can utilize
// K-way merge to accelerate the intersection process.
//
// partialIndexPlan-1 ---> SSR --->  +
// partialIndexPlan-2 ---> SSR --->  + ---> SSR K-way Merge ---> output IndexMergeTableTask
// partialIndexPlan-3 ---> SSR --->  +
// ...                               +
// partialIndexPlan-N ---> SSR --->  +
//
// K-way merge detail: for every partial index plan, output one row as current its representative row. Then, comparing the N representative
// rows together:
//
// Loop start:
//
//	case 1: they are all the same, intersection succeed. --- Record current handle down (already in index order).
//	case 2: distinguish among them, for the minimum value/values corresponded index plan/plans. --- Discard current representative row, fetch next.
//
// goto Loop start:
//
// encapsulate all the recorded handles (already in index order) as index merge table tasks, sending them out.
func (*indexMergeProcessWorker) fetchLoopIntersectionWithOrderBy(_ context.Context, _ <-chan *indexMergeTableTask,
	_ chan<- *indexMergeTableTask, _ chan<- *indexMergeTableTask, _ <-chan struct{}) {
	// todo: pushed sort property with partial index plan and limit.
}

// For each partition(dynamic mode), a map is used to do intersection. Key of the map is handle, and value is the number of times it occurs.
// If the value of handle equals the number of partial paths, it should be sent to final_table_scan_worker.
// To avoid too many goroutines, each intersectionProcessWorker can handle multiple partitions.
func (w *indexMergeProcessWorker) fetchLoopIntersection(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	defer close(workCh)

	if w.stats != nil {
		start := time.Now()
		defer func() {
			w.stats.IndexMergeProcess += time.Since(start)
		}()
	}

	failpoint.Inject("testIndexMergePanicProcessWorkerIntersection", nil)

	// One goroutine may handle one or multiple partitions.
	// Max number of partition number is 8192, we use ExecutorConcurrency to avoid too many goroutines.
	maxWorkerCnt := w.indexMerge.Ctx().GetSessionVars().IndexMergeIntersectionConcurrency()
	maxChannelSize := atomic.LoadInt32(&LookupTableTaskChannelSize)
	batchSize := w.indexMerge.Ctx().GetSessionVars().IndexLookupSize

	partCnt := 1
	// To avoid multi-threaded access the handle map, we only use one worker for indexMerge with global index.
	if w.indexMerge.partitionTableMode && !w.indexMerge.hasGlobalIndex {
		partCnt = len(w.indexMerge.prunedPartitions)
	}
	workerCnt := min(partCnt, maxWorkerCnt)
	failpoint.Inject("testIndexMergeIntersectionConcurrency", func(val failpoint.Value) {
		con := val.(int)
		if con != workerCnt {
			panic(fmt.Sprintf("unexpected workerCnt, expect %d, got %d", con, workerCnt))
		}
	})

	partitionIDMap := make(map[int64]int)
	if w.indexMerge.hasGlobalIndex {
		for i, p := range w.indexMerge.prunedPartitions {
			partitionIDMap[p.GetPhysicalID()] = i
		}
	}

	workers := make([]*intersectionProcessWorker, 0, workerCnt)
	var collectWorker *intersectionCollectWorker
	wg := util.WaitGroupWrapper{}
	wg2 := util.WaitGroupWrapper{}
	errCh := make(chan bool, workerCnt)
	var limitDone chan struct{}
	if w.indexMerge.pushedLimit != nil {
		// no memory cost for this code logic.
		collectWorker = &intersectionCollectWorker{
			// same size of workCh/resultCh
			collectCh:   make(chan *indexMergeTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize)),
			pushedLimit: w.indexMerge.pushedLimit.Clone(),
			limitDone:   make(chan struct{}),
		}
		limitDone = collectWorker.limitDone
		wg2.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			collectWorker.doIntersectionLimitAndDispatch(ctx, workCh, resultCh, finished)
		}, handleWorkerPanic(ctx, finished, nil, resultCh, errCh, partTblIntersectionWorkerType))
	}
	for i := range workerCnt {
		tracker := memory.NewTracker(w.indexMerge.ID(), -1)
		tracker.AttachTo(w.indexMerge.memTracker)
		worker := &intersectionProcessWorker{
			workerID:            i,
			handleMapsPerWorker: make(map[int]*kv.MemAwareHandleMap[*int]),
			workerCh:            make(chan *indexMergeTableTask, maxChannelSize),
			indexMerge:          w.indexMerge,
			memTracker:          tracker,
			batchSize:           batchSize,
			partitionIDMap:      partitionIDMap,
		}
		wg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			if collectWorker != nil {
				// workflow:
				// intersectionProcessWorker-1  --+                       (limit restriction logic)
				// intersectionProcessWorker-2  --+--------- collectCh--> intersectionCollectWorker +--> workCh --> table worker
				// ...                          --+  <--- limitDone to shut inputs ------+          +-> resultCh --> upper parent
				// intersectionProcessWorker-N  --+
				worker.doIntersectionPerPartition(ctx, collectWorker.collectCh, nil, finished, collectWorker.limitDone)
			} else {
				// workflow:
				// intersectionProcessWorker-1  --------------------------+--> workCh   --> table worker
				// intersectionProcessWorker-2  ---(same as above)        +--> resultCh --> upper parent
				// ...                          ---(same as above)
				// intersectionProcessWorker-N  ---(same as above)
				worker.doIntersectionPerPartition(ctx, workCh, resultCh, finished, nil)
			}
		}, handleWorkerPanic(ctx, finished, limitDone, resultCh, errCh, partTblIntersectionWorkerType))
		workers = append(workers, worker)
	}
	defer func() {
		for _, processWorker := range workers {
			close(processWorker.workerCh)
		}
		wg.Wait()
		// only after all the possible writer closed, can we shut down the collectCh.
		if collectWorker != nil {
			// you don't need to clear the channel before closing it, so discard all the remain tasks.
			close(collectWorker.collectCh)
		}
		wg2.Wait()
	}()
	for {
		var ok bool
		var task *indexMergeTableTask
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-fetchCh:
			if !ok {
				return
			}
		}

		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}

		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workers[task.parTblIdx%workerCnt].workerCh <- task:
		case <-errCh:
			// If got error from intersectionProcessWorker, stop processing.
			return
		}
	}
}

type partialIndexWorker struct {
	stats              *IndexMergeRuntimeStat
	sc                 sessionctx.Context
	idxID              int
	batchSize          int
	maxBatchSize       int
	maxChunkSize       int
	memTracker         *memory.Tracker
	partitionTableMode bool
	prunedPartitions   []table.PhysicalTable
	byItems            []*plannerutil.ByItems
	scannedKeys        uint64
	pushedLimit        *physicalop.PushedDownLimit
	dagPB              *tipb.DAGRequest
	plan               []base.PhysicalPlan
}

func syncErr(ctx context.Context, finished <-chan struct{}, errCh chan<- *indexMergeTableTask, err error) {
	logutil.BgLogger().Error("IndexMergeReaderExecutor.syncErr", zap.Error(err))
	doneCh := make(chan error, 1)
	doneCh <- err
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			doneCh: doneCh,
		},
	}

	// ctx.Done and finished is to avoid write channel is stuck.
	select {
	case <-ctx.Done():
		return
	case <-finished:
		return
	case errCh <- task:
		return
	}
}
