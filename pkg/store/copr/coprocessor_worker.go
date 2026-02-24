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

package copr

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/driver/options"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// CopInfo is used to expose functions of copIterator.
type CopInfo interface {
	// GetConcurrency returns the concurrency and small task concurrency.
	GetConcurrency() (int, int)
	// GetStoreBatchInfo returns the batched and fallback num.
	GetStoreBatchInfo() (uint64, uint64)
	// GetBuildTaskElapsed returns the duration of building task.
	GetBuildTaskElapsed() time.Duration
}

type copIterator struct {
	store                *Store
	req                  *kv.Request
	concurrency          int
	smallTaskConcurrency int
	// liteWorker uses to send cop request without start new goroutine, it is only work when tasks count is 1, and used to improve the performance of small cop query.
	liteWorker *liteCopIteratorWorker
	finishCh   chan struct{}

	// If keepOrder, results are stored in copTask.respChan, read them out one by one.
	tasks []*copTask
	// curr indicates the curr id of the finished copTask
	curr int

	// sendRate controls the sending rate of copIteratorTaskSender
	sendRate *util.RateLimit

	// Otherwise, results are stored in respChan.
	respChan chan *copResponse

	vars *tikv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	rpcCancel *tikv.RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to make sure the channel is not closed twice.
	closed uint32

	resolvedLocks  util.TSSet
	committedLocks util.TSSet

	actionOnExceed *rateLimitAction
	pagingTaskIdx  uint32

	buildTaskElapsed        time.Duration
	storeBatchedNum         atomic.Uint64
	storeBatchedFallbackNum atomic.Uint64

	runawayChecker resourcegroup.RunawayChecker
	stats          *copIteratorRuntimeStats
}

type liteCopIteratorWorker struct {
	// ctx contains some info(such as rpc interceptor(WithSQLKvExecCounterInterceptor)), it is used for handle cop task later.
	ctx              context.Context
	worker           *copIteratorWorker
	batchCopRespList []*copResponse
	tryCopLiteWorker *atomic2.Uint32
}

// copIteratorWorker receives tasks from copIteratorTaskSender, handles tasks and sends the copResponse to respChan.
type copIteratorWorker struct {
	taskCh   <-chan *copTask
	wg       *sync.WaitGroup
	store    *Store
	req      *kv.Request
	respChan chan<- *copResponse
	finishCh <-chan struct{}
	vars     *tikv.Variables
	kvclient *txnsnapshot.ClientHelper

	memTracker *memory.Tracker

	replicaReadSeed uint32

	pagingTaskIdx *uint32

	storeBatchedNum         *atomic.Uint64
	storeBatchedFallbackNum *atomic.Uint64
	stats                   *copIteratorRuntimeStats
}

// copIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type copIteratorTaskSender struct {
	taskCh      chan<- *copTask
	smallTaskCh chan<- *copTask
	wg          *sync.WaitGroup
	tasks       []*copTask
	finishCh    <-chan struct{}
	respChan    chan<- *copResponse
	sendRate    *util.RateLimit
}

type copResponse struct {
	pbResp   *coprocessor.Response
	detail   *CopRuntimeStats
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

type copTaskResult struct {
	resp          *copResponse
	batchRespList []*copResponse
	remains       []*copTask
}

const sizeofExecDetails = int(unsafe.Sizeof(execdetails.ExecDetails{}))

// GetData implements the kv.ResultSubset GetData interface.
func (rs *copResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *copResponse) GetStartKey() kv.Key {
	return rs.startKey
}

func (rs *copResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *copResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}
	if rs == finCopResp {
		return 0
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *copResponse) RespTime() time.Duration {
	return rs.respTime
}

const minLogCopTaskTime = 300 * time.Millisecond

// When the worker finished `handleTask`, we need to notify the copIterator that there is one task finished.
// For the non-keep-order case, we send a finCopResp into the respCh after `handleTask`. When copIterator recv
// finCopResp from the respCh, it will be aware that there is one task finished.
var finCopResp *copResponse

func init() {
	finCopResp = &copResponse{}
}

// SetLiteWorkerFallbackHookForTest installs a hook invoked when the lite worker falls back to the concurrent worker.
// Production code should not rely on this hook.
func SetLiteWorkerFallbackHookForTest(hook func()) {
	if hook != nil {
		liteWorkerFallbackHook.Store(&hook)
		return
	}
	liteWorkerFallbackHook.Store(nil)
}

func triggerLiteWorkerFallbackHook() {
	if hookPtr := liteWorkerFallbackHook.Load(); hookPtr != nil && *hookPtr != nil {
		(*hookPtr)()
	}
}

// run is a worker function that get a copTask from channel, handle it and
// send the result back.
func (worker *copIteratorWorker) run(ctx context.Context) {
	defer func() {
		failpoint.Inject("ticase-4169", func(val failpoint.Value) {
			if val.(bool) {
				worker.memTracker.Consume(10 * MockResponseSizeForTest)
				worker.memTracker.Consume(10 * MockResponseSizeForTest)
			}
		})
		worker.wg.Done()
	}()
	// 16KB ballast helps grow the stack to the requirement of copIteratorWorker.
	// This reduces the `morestack` call during the execution of `handleTask`, thus improvement the efficiency of TiDB.
	// TODO: remove ballast after global pool is applied.
	ballast := make([]byte, 16*size.KB)
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}
		worker.handleTask(ctx, task, respCh)
		if worker.respChan != nil {
			// When a task is finished by the worker, send a finCopResp into channel to notify the copIterator that
			// there is a task finished.
			worker.sendToRespCh(finCopResp, worker.respChan)
		}
		if task.respChan != nil {
			close(task.respChan)
		}
		if worker.finished() {
			return
		}
	}
	runtime.KeepAlive(ballast)
}

// open starts workers and sender goroutines.
func (it *copIterator) open(ctx context.Context, tryCopLiteWorker *atomic2.Uint32) {
	if len(it.tasks) == 1 && tryCopLiteWorker != nil && tryCopLiteWorker.CompareAndSwap(0, 1) {
		// For a query, only one `copIterator` can use `liteWorker`, otherwise it will affect the performance of multiple cop iterators executed concurrently,
		// see more detail in TestQueryWithConcurrentSmallCop.
		it.liteWorker = &liteCopIteratorWorker{
			ctx:              ctx,
			worker:           newCopIteratorWorker(it, nil),
			tryCopLiteWorker: tryCopLiteWorker,
		}
		return
	}
	taskCh := make(chan *copTask, 1)
	it.wg.Add(it.concurrency + it.smallTaskConcurrency)
	var smallTaskCh chan *copTask
	if it.smallTaskConcurrency > 0 {
		smallTaskCh = make(chan *copTask, 1)
	}
	// Start it.concurrency number of workers to handle cop requests.
	for i := range it.concurrency + it.smallTaskConcurrency {
		ch := taskCh
		if i >= it.concurrency && smallTaskCh != nil {
			ch = smallTaskCh
		}
		worker := newCopIteratorWorker(it, ch)
		go worker.run(ctx)
	}
	taskSender := &copIteratorTaskSender{
		taskCh:      taskCh,
		smallTaskCh: smallTaskCh,
		wg:          &it.wg,
		tasks:       it.tasks,
		finishCh:    it.finishCh,
		sendRate:    it.sendRate,
	}
	taskSender.respChan = it.respChan
	failpoint.Inject("ticase-4171", func(val failpoint.Value) {
		if val.(bool) {
			it.memTracker.Consume(10 * MockResponseSizeForTest)
			it.memTracker.Consume(10 * MockResponseSizeForTest)
		}
	})
	go taskSender.run(it.req.ConnID, it.req.RunawayChecker)
}

func newCopIteratorWorker(it *copIterator, taskCh <-chan *copTask) *copIteratorWorker {
	return &copIteratorWorker{
		taskCh:                  taskCh,
		wg:                      &it.wg,
		store:                   it.store,
		req:                     it.req,
		respChan:                it.respChan,
		finishCh:                it.finishCh,
		vars:                    it.vars,
		kvclient:                txnsnapshot.NewClientHelper(it.store.store, &it.resolvedLocks, &it.committedLocks, false),
		memTracker:              it.memTracker,
		replicaReadSeed:         it.replicaReadSeed,
		pagingTaskIdx:           &it.pagingTaskIdx,
		storeBatchedNum:         &it.storeBatchedNum,
		storeBatchedFallbackNum: &it.storeBatchedFallbackNum,
		stats:                   it.stats,
	}
}

func (sender *copIteratorTaskSender) run(connID uint64, checker resourcegroup.RunawayChecker) {
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		// we control the sending rate to prevent all tasks
		// being done (aka. all of the responses are buffered) by copIteratorWorker.
		// We keep the number of inflight tasks within the number of 2 * concurrency when Keep Order is true.
		// If KeepOrder is false, the number equals the concurrency.
		// It sends one more task if a task has been finished in copIterator.Next.
		exit := sender.sendRate.GetToken(sender.finishCh)
		if exit {
			break
		}
		var sendTo chan<- *copTask
		if isSmallTask(t) && sender.smallTaskCh != nil {
			sendTo = sender.smallTaskCh
		} else {
			sendTo = sender.taskCh
		}
		exit = sender.sendToTaskCh(t, sendTo)
		if exit {
			break
		}
		if connID > 0 {
			failpoint.Inject("pauseCopIterTaskSender", func() {})
		}
	}
	close(sender.taskCh)
	if sender.smallTaskCh != nil {
		close(sender.smallTaskCh)
	}

	// Wait for worker goroutines to exit.
	sender.wg.Wait()
	if sender.respChan != nil {
		close(sender.respChan)
	}
	if checker != nil {
		// runaway checker need to focus on the all processed keys of all tasks at a time.
		checker.ResetTotalProcessedKeys()
	}
}

func (it *copIterator) recvFromRespCh(ctx context.Context, respCh <-chan *copResponse) (resp *copResponse, ok bool, exit bool) {
	for {
		select {
		case resp, ok = <-respCh:
			memTrackerConsumeResp(it.memTracker, resp)
			return
		case <-it.finishCh:
			exit = true
			return
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
				close(it.finishCh)
			}
			exit = true
			return
		}
	}
}

func memTrackerConsumeResp(memTracker *memory.Tracker, resp *copResponse) {
	if memTracker != nil && resp != nil {
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		memTracker.Consume(-consumed)
	}
}

// GetConcurrency returns the concurrency and small task concurrency.
func (it *copIterator) GetConcurrency() (int, int) {
	return it.concurrency, it.smallTaskConcurrency
}

// GetStoreBatchInfo returns the batched and fallback num.
func (it *copIterator) GetStoreBatchInfo() (uint64, uint64) {
	return it.storeBatchedNum.Load(), it.storeBatchedFallbackNum.Load()
}

// GetBuildTaskElapsed returns the duration of building task.
func (it *copIterator) GetBuildTaskElapsed() time.Duration {
	return it.buildTaskElapsed
}

// GetSendRate returns the rate-limit object.
func (it *copIterator) GetSendRate() *util.RateLimit {
	return it.sendRate
}

// GetTasks returns the built tasks.
func (it *copIterator) GetTasks() []*copTask {
	return it.tasks
}

func (sender *copIteratorTaskSender) sendToTaskCh(t *copTask, sendTo chan<- *copTask) (exit bool) {
	select {
	case sendTo <- t:
	case <-sender.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) sendToRespCh(resp *copResponse, respCh chan<- *copResponse) (exit bool) {
	select {
	case respCh <- resp:
	case <-worker.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) checkRespOOM(resp *copResponse) {
	if worker.memTracker != nil {
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		failpoint.Inject("ConsumeRandomPanic", nil)
		worker.memTracker.Consume(consumed)
	}
}

// MockResponseSizeForTest mock the response size
const MockResponseSizeForTest = 100 * 1024 * 1024

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *copIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *copResponse
		ok     bool
		closed bool
	)
	defer func() {
		if resp == nil {
			failpoint.Inject("ticase-4170", func(val failpoint.Value) {
				if val.(bool) {
					it.memTracker.Consume(10 * MockResponseSizeForTest)
					it.memTracker.Consume(10 * MockResponseSizeForTest)
				}
			})
		}
	}()
	// wait unit at least 5 copResponse received.
	failpoint.Inject("testRateLimitActionMockWaitMax", func(val failpoint.Value) {
		if val.(bool) {
			// we only need to trigger oom at least once.
			if len(it.tasks) > 9 {
				for it.memTracker.MaxConsumed() < 5*MockResponseSizeForTest {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	})
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.

	failpoint.InjectCall("CtxCancelBeforeReceive", ctx)
	if it.liteWorker != nil {
		resp = it.liteWorker.liteSendReq(ctx, it)
		// after lite handle 1 task, reset tryCopLiteWorker to 0 to make future request can reuse copLiteWorker.
		it.liteWorker.tryCopLiteWorker.CompareAndSwap(1, 0)
		if resp == nil {
			it.actionOnExceed.close()
			return nil, nil
		}
		if len(it.tasks) > 0 && len(it.liteWorker.batchCopRespList) == 0 && resp.err == nil {
			// if there are remain tasks to be processed, we need to run worker concurrently to avoid blocking.
			// see more detail in https://github.com/pingcap/tidb/issues/58658 and TestDMLWithLiteCopWorker.
			it.liteWorker.runWorkerConcurrently(it)
			it.liteWorker = nil
		}
		it.actionOnExceed.destroyTokenIfNeeded(func() {})
		memTrackerConsumeResp(it.memTracker, resp)
	} else if it.respChan != nil {
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			it.actionOnExceed.close()
			return nil, errors.Trace(ctx.Err())
		}
		if resp == finCopResp {
			it.actionOnExceed.destroyTokenIfNeeded(func() {
				it.sendRate.PutToken()
			})
			return it.Next(ctx)
		}
	} else {
		for {
			if it.curr >= len(it.tasks) {
				// Resp will be nil if iterator is finishCh.
				it.actionOnExceed.close()
				return nil, nil
			}
			task := it.tasks[it.curr]
			resp, ok, closed = it.recvFromRespCh(ctx, task.respChan)
			if closed {
				// Close() is called or context cancelled/timeout, so Next() is invalid.
				return nil, errors.Trace(ctx.Err())
			}
			if ok {
				break
			}
			it.actionOnExceed.destroyTokenIfNeeded(func() {
				it.sendRate.PutToken()
			})
			// Switch to next task.
			it.tasks[it.curr] = nil
			it.curr++
		}
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := it.store.CheckVisibility(it.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (w *liteCopIteratorWorker) liteSendReq(ctx context.Context, it *copIterator) (resp *copResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Warn("copIteratorWork meet panic",
				zap.Any("r", r),
				zap.Stack("stack trace"))
			resp = &copResponse{err: util2.GetRecoverError(r)}
		}
	}()

	worker := w.worker
	if len(w.batchCopRespList) > 0 {
		resp = w.batchCopRespList[0]
		w.batchCopRespList = w.batchCopRespList[1:]
		return resp
	}
	backoffermap := make(map[uint64]*Backoffer)
	cancelFuncs := make([]context.CancelFunc, 0)
	defer func() {
		for _, cancel := range cancelFuncs {
			cancel()
		}
	}()
	for len(it.tasks) > 0 {
		curTask := it.tasks[0]
		bo, cancel := chooseBackoffer(w.ctx, backoffermap, curTask, worker)
		if cancel != nil {
			cancelFuncs = append(cancelFuncs, cancel)
		}
		result, err := worker.handleTaskOnce(bo, curTask)
		if err != nil {
			resp = &copResponse{err: errors.Trace(err)}
			worker.checkRespOOM(resp)
			return resp
		}

		if result != nil && len(result.remains) > 0 {
			it.tasks = append(result.remains, it.tasks[1:]...)
		} else {
			it.tasks = it.tasks[1:]
		}
		if result != nil {
			if result.resp != nil {
				w.batchCopRespList = result.batchRespList
				return result.resp
			}
			if len(result.batchRespList) > 0 {
				resp = result.batchRespList[0]
				w.batchCopRespList = result.batchRespList[1:]
				return resp
			}
		}
	}
	return nil
}

func (w *liteCopIteratorWorker) runWorkerConcurrently(it *copIterator) {
	triggerLiteWorkerFallbackHook()
	taskCh := make(chan *copTask, 1)
	worker := w.worker
	worker.taskCh = taskCh
	it.wg.Add(1)
	go worker.run(w.ctx)

	if it.respChan == nil {
		// If it.respChan is nil, we will read the response from task.respChan,
		// but task.respChan maybe nil when rebuilding cop task, so we need to create respChan for the task.
		for i := range it.tasks {
			if it.tasks[i].respChan == nil {
				it.tasks[i].respChan = make(chan *copResponse, 2)
			}
		}
	}

	taskSender := &copIteratorTaskSender{
		taskCh:   taskCh,
		wg:       &it.wg,
		tasks:    it.tasks,
		finishCh: it.finishCh,
		sendRate: it.sendRate,
		respChan: it.respChan,
	}
	go taskSender.run(it.req.ConnID, it.req.RunawayChecker)
}

// HasUnconsumedCopRuntimeStats indicate whether has unconsumed CopRuntimeStats.
type HasUnconsumedCopRuntimeStats interface {
	// CollectUnconsumedCopRuntimeStats returns unconsumed CopRuntimeStats.
	CollectUnconsumedCopRuntimeStats() []*CopRuntimeStats
}

func (it *copIterator) CollectUnconsumedCopRuntimeStats() []*CopRuntimeStats {
	if it == nil || it.stats == nil {
		return nil
	}
	it.stats.Lock()
	stats := make([]*CopRuntimeStats, 0, len(it.stats.stats))
	stats = append(stats, it.stats.stats...)
	it.stats.Unlock()
	return stats
}

// Associate each region with an independent backoffer. In this way, when multiple regions are
// unavailable, TiDB can execute very quickly without blocking, if the returned CancelFunc is not nil,
// the caller must call it to avoid context leak.
func chooseBackoffer(ctx context.Context, backoffermap map[uint64]*Backoffer, task *copTask, worker *copIteratorWorker) (*Backoffer, context.CancelFunc) {
	bo, ok := backoffermap[task.region.GetID()]
	if ok {
		return bo, nil
	}
	boMaxSleep := CopNextMaxBackoff
	failpoint.Inject("ReduceCopNextMaxBackoff", func(value failpoint.Value) {
		if value.(bool) {
			boMaxSleep = 2
		}
	})
	var cancel context.CancelFunc
	boCtx := ctx
	if worker.req.MaxExecutionTime > 0 {
		boCtx, cancel = context.WithTimeout(boCtx, time.Duration(worker.req.MaxExecutionTime)*time.Millisecond)
	}
	newbo := backoff.NewBackofferWithVars(boCtx, boMaxSleep, worker.vars)
	backoffermap[task.region.GetID()] = newbo
	return newbo, cancel
}

// handleTask handles single copTask, sends the result to channel, retry automatically on error.
func (worker *copIteratorWorker) handleTask(ctx context.Context, task *copTask, respCh chan<- *copResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Warn("copIteratorWork meet panic",
				zap.Any("r", r),
				zap.Stack("stack trace"))
			resp := &copResponse{err: util2.GetRecoverError(r)}
			// if panic has happened, not checkRespOOM to avoid another panic.
			worker.sendToRespCh(resp, respCh)
		}
	}()
	remainTasks := []*copTask{task}
	backoffermap := make(map[uint64]*Backoffer)
	cancelFuncs := make([]context.CancelFunc, 0)
	defer func() {
		for _, cancel := range cancelFuncs {
			cancel()
		}
	}()
	for len(remainTasks) > 0 {
		curTask := remainTasks[0]
		bo, cancel := chooseBackoffer(ctx, backoffermap, curTask, worker)
		if cancel != nil {
			cancelFuncs = append(cancelFuncs, cancel)
		}
		result, err := worker.handleTaskOnce(bo, curTask)
		if err != nil {
			resp := &copResponse{err: errors.Trace(err)}
			worker.checkRespOOM(resp)
			worker.sendToRespCh(resp, respCh)
			return
		}
		if result != nil {
			if result.resp != nil {
				worker.sendToRespCh(result.resp, respCh)
			}
			for _, resp := range result.batchRespList {
				worker.sendToRespCh(resp, respCh)
			}
		}
		if worker.finished() {
			break
		}
		if result != nil && len(result.remains) > 0 {
			remainTasks = append(result.remains, remainTasks[1:]...)
		} else {
			remainTasks = remainTasks[1:]
		}
	}
}

// handleTaskOnce handles single copTask, successful results are send to channel.
// If error happened, returns error. If region split or meet lock, returns the remain tasks.
func (worker *copIteratorWorker) handleTaskOnce(bo *Backoffer, task *copTask) (*copTaskResult, error) {
	failpoint.Inject("handleTaskOnceError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock handleTaskOnce error"))
		}
	})

	if task.paging {
		task.pagingTaskIdx = atomic.AddUint32(worker.pagingTaskIdx, 1)
	}

	copReq := coprocessor.Request{
		Tp:              worker.req.Tp,
		StartTs:         worker.req.StartTs,
		Data:            worker.req.Data,
		Ranges:          task.ranges.ToPBRanges(),
		SchemaVer:       worker.req.SchemaVar,
		PagingSize:      task.pagingSize,
		Tasks:           task.ToPBBatchTasks(),
		ConnectionId:    worker.req.ConnID,
		ConnectionAlias: worker.req.ConnAlias,
	}

	cacheKey, cacheValue := worker.buildCacheKey(task, &copReq)

	replicaRead := worker.req.ReplicaRead
	rgName := worker.req.ResourceGroupName
	if task.storeType == kv.TiFlash && !vardef.EnableResourceControl.Load() {
		// By calling variable.EnableGlobalResourceControlFunc() and setting global variables,
		// tikv/client-go can sense whether the rg function is enabled
		// But for tiflash, it check if rgName is empty to decide if resource control is enabled or not.
		rgName = ""
	}
	req := tikvrpc.NewReplicaReadRequest(task.cmdType, &copReq, options.GetTiKVReplicaReadType(replicaRead), &worker.replicaReadSeed, kvrpcpb.Context{
		IsolationLevel: isolationLevelToPB(worker.req.IsolationLevel),
		Priority:       priorityToPB(worker.req.Priority),
		NotFillCache:   worker.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         worker.req.TaskID,
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: rgName,
		},
		BusyThresholdMs: uint32(task.busyThreshold.Milliseconds()),
		BucketsVersion:  task.bucketsVer,
	})
	req.InputRequestSource = task.requestSource.GetRequestSource()
	if task.firstReadType != "" {
		req.ReadType = task.firstReadType
		req.IsRetryRequest = true
	}
	if worker.req.ResourceGroupTagger != nil {
		worker.req.ResourceGroupTagger.Build(req)
	}
	timeout := config.GetGlobalConfig().TiKVClient.CoprReqTimeout
	if task.tikvClientReadTimeout > 0 {
		timeout = time.Duration(task.tikvClientReadTimeout) * time.Millisecond
	}
	failpoint.Inject("sleepCoprRequest", func(v failpoint.Value) {
		//nolint:durationcheck
		time.Sleep(time.Millisecond * time.Duration(v.(int)))
	})

	if worker.req.RunawayChecker != nil {
		if runawayErr := worker.req.RunawayChecker.BeforeCopRequest(req); runawayErr != nil {
			return nil, runawayErr
		}
	}
	req.StoreTp = getEndPointType(task.storeType)
	startTime := time.Now()
	if worker.stats != nil && worker.kvclient.Stats == nil {
		worker.kvclient.Stats = tikv.NewRegionRequestRuntimeStats()
	}
	// set ReadReplicaScope and TxnScope so that req.IsStaleRead will be true when it's a global scope stale read.
	req.ReadReplicaScope = worker.req.ReadReplicaScope
	req.TxnScope = worker.req.TxnScope
	if task.meetLockFallback {
		req.DisableStaleReadMeetLock()
	} else if worker.req.IsStaleness {
		req.EnableStaleWithMixedReplicaRead()
	}
	ops := make([]tikv.StoreSelectorOption, 0, 2)
	if len(worker.req.MatchStoreLabels) > 0 {
		ops = append(ops, tikv.WithMatchLabels(worker.req.MatchStoreLabels))
	}
	if task.redirect2Replica != nil {
		req.ReplicaRead = true
		req.ReplicaReadType = options.GetTiKVReplicaReadType(kv.ReplicaReadFollower)
		ops = append(ops, tikv.WithMatchStores([]uint64{*task.redirect2Replica}))
	}

	failpoint.InjectCall("onBeforeSendReqCtx", req)
	resp, rpcCtx, storeAddr, err := worker.kvclient.SendReqCtx(bo.TiKVBackoffer(), req, task.region,
		timeout, getEndPointType(task.storeType), task.storeAddr, ops...)
	err = derr.ToTiDBErr(err)
	if worker.req.RunawayChecker != nil {
		err = worker.req.RunawayChecker.CheckThresholds(nil, 0, err)
	}
	if err != nil {
		if task.storeType == kv.TiDB {
			return worker.handleTiDBSendReqErr(err, task)
		}
		worker.collectUnconsumedCopRuntimeStats(bo, rpcCtx)
		return nil, errors.Trace(err)
	}

	// Set task.storeAddr field so its task.String() method have the store address information.
	task.storeAddr = storeAddr

	costTime := time.Since(startTime)
	copResp := resp.Resp.(*coprocessor.Response)

	if costTime > minLogCopTaskTime {
		worker.logTimeCopTask(costTime, task, bo, copResp)
	}

	if copResp != nil {
		tidbmetrics.DistSQLCoprRespBodySize.WithLabelValues(storeAddr).Observe(float64(len(copResp.Data) / 1024))
	}

	var result *copTaskResult
	if worker.req.Paging.Enable ||
		copResp.GetRange() != nil { // For next-gen, the storage may return paging range even if paging is not enabled.
		result, err = worker.handleCopPagingResult(bo, rpcCtx, &copResponse{pbResp: copResp}, cacheKey, cacheValue, task, costTime)
	} else {
		// Handles the response for non-paging copTask.
		result, err = worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: copResp}, cacheKey, cacheValue, task, costTime)
	}
	if req.ReadType != "" && result != nil {
		for _, remain := range result.remains {
			remain.firstReadType = req.ReadType
		}
	}
	return result, err
}

const (
	minLogBackoffTime   = 100
	minLogKVProcessTime = 100
)
