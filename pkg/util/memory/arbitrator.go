// Copyright 2025 PingCAP, Inc.
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

package memory

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// ArbitrateResult represents the results of the arbitration process
type ArbitrateResult int32

const (
	// ArbitrateOk indicates that the arbitration is successful.
	ArbitrateOk ArbitrateResult = iota
	// ArbitrateFail indicates that the arbitration is failed
	ArbitrateFail
)

// SoftLimitMode represents the mode of soft limit for the mem-arbitrator
type SoftLimitMode int32

const (
	// SoftLimitModeDisable indicates that soft-limit is same as the threshold of oom risk
	SoftLimitModeDisable SoftLimitMode = iota
	// SoftLimitModeSpecified indicates that the soft-limit is a specified num of bytes or rate of the limit
	SoftLimitModeSpecified
	// SoftLimitModeAuto indicates that the soft-limit is auto calculated by the mem-arbitrator
	SoftLimitModeAuto
)

const (
	// ArbitratorSoftLimitModDisableName is the name of the soft limit mode default
	ArbitratorSoftLimitModDisableName = "0"
	// ArbitratorSoftLimitModeAutoName is the name of the soft limit mode auto
	ArbitratorSoftLimitModeAutoName = "auto"
	// ArbitratorModeStandardName is the name of the standard mode
	ArbitratorModeStandardName = "standard"
	// ArbitratorModePriorityName is the name of the priority mode
	ArbitratorModePriorityName = "priority"
	// ArbitratorModeDisableName is the name of the disable mode
	ArbitratorModeDisableName = "disable"
	// DefMaxLimit is the default maximum limit of mem quota
	DefMaxLimit int64 = 5e15

	defTaskTickDur                            = time.Millisecond * 10
	defMinHeapFreeBPS                  int64  = 100 * byteSizeMB
	defHeapReclaimCheckDuration               = time.Second * 1
	defHeapReclaimCheckMaxDuration            = time.Second * 5
	defOOMRiskRatio                           = 0.95
	defMemRiskRatio                           = 0.9
	defTickDurMilli                           = kilo * 1             // 1s
	defStorePoolMediumCapDurMilli             = defTickDurMilli * 10 // 10s
	defTrackMemStatsDurMilli                  = kilo * 1
	defMax                             int64  = 9e15
	defServerlimitSmallLimitNum               = 1000
	defServerlimitMinUnitNum                  = 500
	defServerlimitMaxUnitNum                  = 100
	defUpdateMemConsumedTimeAlignSec          = 30
	defUpdateMemMagnifUtimeAlign              = 30
	defUpdateBufferTimeAlignSec               = 60
	defRedundancy                             = 2
	defPoolReservedQuota                      = byteSizeMB
	defAwaitFreePoolAllocAlignSize            = defPoolReservedQuota + byteSizeMB
	defAwaitFreePoolShardNum           int64  = 256
	defAwaitFreePoolShrinkDurMilli            = kilo * 2
	defPoolStatusShards                       = 128
	defPoolQuotaShards                        = 27 // quota >= BaseQuotaUnit * 2^(max_shards - 2) will be put into the last shard
	prime64                            uint64 = 1099511628211
	initHashKey                        uint64 = 14695981039346656037
	defKillCancelCheckTimeout                 = time.Second * 20
	defDigestProfileSmallMemTimeoutSec        = 60 * 60 * 24     // 1 day
	defDigestProfileMemTimeoutSec             = 60 * 60 * 24 * 7 // 1 week
	baseQuotaUnit                             = 4 * byteSizeKB   // 4KB
	defMaxMagnif                              = kilo * 10
	defMaxDigestProfileCacheLimit             = 4e4
)

// ArbitratorWorkMode represents the work mode of the arbitrator: Standard, Priority, Disable
type ArbitratorWorkMode int32

const (
	// ArbitratorModeStandard indicates the standard mode
	ArbitratorModeStandard ArbitratorWorkMode = iota
	// ArbitratorModePriority indicates the priority mode
	ArbitratorModePriority
	// ArbitratorModeDisable indicates the mem-arbitrator is disabled
	ArbitratorModeDisable

	maxArbitratorMode
)

// ArbitrationPriority represents the priority of the task: Low, Medium, High
type ArbitrationPriority int32

type entryExecState int32

const (
	execStateIdle entryExecState = iota
	execStateRunning
	execStatePrivileged
)

const (
	// ArbitrationPriorityLow indicates the low priority
	ArbitrationPriorityLow ArbitrationPriority = iota
	// ArbitrationPriorityMedium indicates the medium priority
	ArbitrationPriorityMedium
	// ArbitrationPriorityHigh indicates the high priority
	ArbitrationPriorityHigh

	minArbitrationPriority = ArbitrationPriorityLow
	maxArbitrationPriority = ArbitrationPriorityHigh + 1
	maxArbitrateMode       = maxArbitrationPriority + 1

	// ArbitrationWaitAverse indicates the wait-averse property
	ArbitrationWaitAverse = maxArbitrationPriority
)

var errArbitrateFailError = errors.New("failed to allocate resource from arbitrator")

var arbitrationPriorityNames = [maxArbitrationPriority]string{"LOW", "MEDIUM", "HIGH"}

var mockNow func() time.Time
var mockWinupCB func(*rootPoolEntry)

// String returns the string representation of the ArbitrationPriority
func (p ArbitrationPriority) String() string {
	return arbitrationPriorityNames[p]
}

var arbitratorWorkModeNames = []string{ArbitratorModeStandardName, ArbitratorModePriorityName, ArbitratorModeDisableName}

// String returns the string representation of the ArbitratorWorkMode
func (m ArbitratorWorkMode) String() string {
	return arbitratorWorkModeNames[m]
}

func (m *MemArbitrator) taskNumByPriority(priority ArbitrationPriority) int64 {
	return m.tasks.fifoByPriority[priority].approxSize()
}

func (m *MemArbitrator) taskNumOfWaitAverse() int64 {
	return m.tasks.fifoWaitAverse.approxSize()
}

func (m *MemArbitrator) firstTaskEntry(priority ArbitrationPriority) *rootPoolEntry {
	return m.tasks.fifoByPriority[priority].front()
}

func (m *MemArbitrator) removeTaskImpl(entry *rootPoolEntry) bool {
	if entry.taskMu.fifo.valid() {
		m.tasks.fifoTasks.remove(entry.taskMu.fifo)
		entry.taskMu.fifo.reset()
		m.tasks.fifoByPriority[entry.taskMu.fifoByPriority.priority].remove(entry.taskMu.fifoByPriority.wrapListElement)
		entry.taskMu.fifoByPriority.reset()
		if entry.taskMu.fifoWaitAverse.valid() {
			m.tasks.fifoWaitAverse.remove(entry.taskMu.fifoWaitAverse)
			entry.taskMu.fifoWaitAverse.reset()
		}
		return true
	}
	return false
}

// there is no need to wind up if task has been removed by cancel.
func (m *MemArbitrator) removeTask(entry *rootPoolEntry) (res bool) {
	m.tasks.Lock()

	res = m.removeTaskImpl(entry)

	m.tasks.Unlock()
	return res
}

func (m *MemArbitrator) addTask(entry *rootPoolEntry) {
	m.tasks.Lock()

	priority := entry.ctx.memPriority
	entry.taskMu.fifoByPriority.priority = priority
	entry.taskMu.fifoByPriority.wrapListElement = m.tasks.fifoByPriority[priority].pushBack(entry)
	if entry.ctx.waitAverse {
		entry.taskMu.fifoWaitAverse = m.tasks.fifoWaitAverse.pushBack(entry)
	}
	entry.taskMu.fifo = m.tasks.fifoTasks.pushBack(entry)

	m.tasks.Unlock()
}

func (m *MemArbitrator) frontTaskEntry() (entry *rootPoolEntry) {
	m.tasks.Lock()

	entry = m.tasks.fifoTasks.front()

	m.tasks.Unlock()
	return
}

func (m *MemArbitrator) extractFirstTaskEntry() (entry *rootPoolEntry) {
	m.tasks.Lock()

	if m.privilegedEntry != nil {
		if m.privilegedEntry.taskMu.fifo.valid() {
			m.tasks.fifoTasks.moveToFront(m.privilegedEntry.taskMu.fifo)
			entry = m.privilegedEntry
		}
	}

	if entry == nil {
		if m.execMu.mode == ArbitratorModePriority {
			for priority := maxArbitrationPriority - 1; priority >= minArbitrationPriority; priority-- {
				if entry = m.firstTaskEntry(priority); entry != nil {
					break
				}
			}
		} else {
			entry = m.tasks.fifoTasks.front()
		}
	}

	m.tasks.Unlock()
	return
}

type rootPoolEntry struct {
	pool   *ResourcePool
	taskMu struct { // protected by the tasks mutex of arbitrator
		fifo           wrapListElement
		fifoWaitAverse wrapListElement
		fifoByPriority struct {
			wrapListElement
			priority ArbitrationPriority
		}
	}

	// context of execution
	// mutable when entry is idle and the mutex of root pool is locked
	ctx struct {
		atomic.Pointer[ArbitrationContext]
		cancelCh <-chan struct{}
		canceled atomic.Bool

		// properties hint of the entry; data race is acceptable;
		memPriority     ArbitrationPriority
		waitAverse      bool
		preferPrivilege bool
	}
	request struct {
		// arbitrator will send result in `windup`
		resultCh chan ArbitrateResult
		// quota is readable to arbitrator when taskMu is locked
		// quota is mutable only when entry is not in task queue and taskMu is unlocked, almost like after receiving data from `<-resultCh`
		quota int64
		// task-mutex is locked when entry is under arbitration
		// task-mutex can be locked when entry is in task queue
		taskMu sync.Mutex
	}
	arbitratorMu struct { // mutable for arbitrator
		shard       *entryMapShard
		quotaShard  *entryQuotaShard
		underKill   entryKillCancelCtx
		underCancel entryKillCancelCtx
		quota       int64 // -1: uninitiated
		destroyed   bool
	}
	stateMu struct {
		quotaToReclaim atomic.Int64
		sync.Mutex
		stop atomic.Bool

		// execStateIdle -> execStateRunning -> execStatePrivileged -> execStateIdle
		// execStateIdle -> execStateRunning -> execStateIdle
		exec entryExecState
	}
}

type mapUIDEntry map[uint64]*rootPoolEntry

type entryMapShard struct {
	entries mapUIDEntry
	sync.RWMutex
}

type entryQuotaShard struct {
	entries mapUIDEntry
}

func (e *rootPoolEntry) execState() entryExecState {
	return entryExecState(atomic.LoadInt32((*int32)(&e.stateMu.exec)))
}

func (e *rootPoolEntry) setExecState(s entryExecState) {
	atomic.StoreInt32((*int32)(&e.stateMu.exec), int32(s))
}

func (e *rootPoolEntry) intoExecPrivileged() bool {
	return atomic.CompareAndSwapInt32((*int32)(&e.stateMu.exec), int32(execStateRunning), int32(execStatePrivileged))
}

func (e *rootPoolEntry) notRunning() bool {
	return e.stateMu.stop.Load() || e.execState() == execStateIdle || e.stateMu.quotaToReclaim.Load() > 0
}

type entryMap struct {
	quotaShards  [maxArbitrationPriority][]*entryQuotaShard // entries order by priority, quota
	contextCache struct {                                   // cache for traversing all entries concurrently
		sync.Map // map[uint64]*rootPoolEntry
		num      atomic.Int64
	}
	shards                    []*entryMapShard
	shardsMask                uint64
	maxQuotaShardIndex        int // for quota >= `BaseQuotaUnit * 2^(maxQuotaShard - 1)`
	minQuotaShardIndexToCheck int // ignore the pool with smaller quota
}

// controlled by arbitrator
func (m *entryMap) delete(entry *rootPoolEntry) {
	uid := entry.pool.uid

	if entry.arbitratorMu.quotaShard != nil {
		delete(entry.arbitratorMu.quotaShard.entries, uid)
		entry.arbitratorMu.quota = 0
		entry.arbitratorMu.quotaShard = nil
	}

	entry.arbitratorMu.shard.delete(uid)
	entry.arbitratorMu.shard = nil

	if _, loaded := m.contextCache.LoadAndDelete(uid); loaded {
		m.contextCache.num.Add(-1)
	}
}

// controlled by arbitrator
func (m *entryMap) addQuota(entry *rootPoolEntry, delta int64) {
	if delta == 0 {
		return
	}

	uid := entry.pool.UID()

	entry.arbitratorMu.quota += delta

	if entry.arbitratorMu.quota == 0 { // remove
		delete(entry.arbitratorMu.quotaShard.entries, uid)
		entry.arbitratorMu.quotaShard = nil
		return
	}

	newPos := getQuotaShard(entry.arbitratorMu.quota, m.maxQuotaShardIndex)
	newShard := m.quotaShards[entry.ctx.memPriority][newPos]
	if newShard != entry.arbitratorMu.quotaShard {
		if entry.arbitratorMu.quotaShard != nil {
			delete(entry.arbitratorMu.quotaShard.entries, uid)
		}
		entry.arbitratorMu.quotaShard = newShard
		entry.arbitratorMu.quotaShard.entries[uid] = entry
	}
}

func (m *entryMap) getStatusShard(key uint64) *entryMapShard {
	return m.shards[shardIndexByUID(key, m.shardsMask)]
}

func (m *entryMap) getQuotaShard(priority ArbitrationPriority, quota int64) *entryQuotaShard {
	return m.quotaShards[priority][getQuotaShard(quota, m.maxQuotaShardIndex)]
}

func (s *entryMapShard) get(key uint64) (e *rootPoolEntry, ok bool) {
	s.RLock()
	e, ok = s.entries[key]
	s.RUnlock()
	return
}

func (s *entryMapShard) delete(key uint64) {
	s.Lock()

	delete(s.entries, key)

	s.Unlock()
}

func (s *entryMapShard) emplace(key uint64, tar *rootPoolEntry) (e *rootPoolEntry, ok bool) {
	s.Lock()

	e, found := s.entries[key]
	ok = !found
	if !found {
		s.entries[key] = tar
		e = tar
	}

	s.Unlock()
	return
}

func (m *entryMap) emplace(pool *ResourcePool) (*rootPoolEntry, bool) {
	key := pool.UID()
	s := m.getStatusShard(key)
	if v, ok := s.get(key); ok {
		return v, false
	}
	tar := &rootPoolEntry{pool: pool}
	tar.arbitratorMu.shard = s
	tar.request.resultCh = make(chan ArbitrateResult, 1)

	return s.emplace(key, tar)
}

func (m *entryMap) init(shardNum uint64, maxQuotaShard int, minQuotaForReclaim int64) {
	m.shards = make([]*entryMapShard, shardNum)
	m.shardsMask = shardNum - 1
	m.maxQuotaShardIndex = maxQuotaShard
	m.minQuotaShardIndexToCheck = getQuotaShard(minQuotaForReclaim, m.maxQuotaShardIndex)
	for p := minArbitrationPriority; p < maxArbitrationPriority; p++ {
		m.quotaShards[p] = make([]*entryQuotaShard, m.maxQuotaShardIndex)
		for i := range m.maxQuotaShardIndex {
			m.quotaShards[p][i] = &entryQuotaShard{
				entries: make(mapUIDEntry),
			}
		}
	}

	for i := range shardNum {
		m.shards[i] = &entryMapShard{
			entries: make(mapUIDEntry),
		}
	}
}

// if entry is in task queue, it must have acquire the unique request lock and wait for callback
// this func only can be invoked after `removeTask`
func (e *rootPoolEntry) windUp(delta int64, r ArbitrateResult) {
	e.pool.forceAddCap(delta)
	e.request.resultCh <- r

	if intest.InTest {
		if mockWinupCB != nil {
			mockWinupCB(e)
		}
	}
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) blockingAllocate(entry *rootPoolEntry, requestedBytes int64) ArbitrateResult {
	if entry.execState() == execStateIdle {
		return ArbitrateFail
	}
	if entry.ctx.canceled.Load() {
		atomic.AddInt64(&m.execMetrics.Task.Fail, 1)
		return ArbitrateFail
	}

	m.prepareAlloc(entry, requestedBytes)
	return m.waitAlloc(entry)
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) prepareAlloc(entry *rootPoolEntry, requestedBytes int64) {
	entry.request.quota = requestedBytes
	m.tasks.waitingAlloc.Add(requestedBytes)
	m.addTask(entry)
	m.notifer.WeakWake()
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) waitAlloc(entry *rootPoolEntry) ArbitrateResult {
	res := ArbitrateOk
	select {
	case res = <-entry.request.resultCh:
		if res == ArbitrateFail {
			atomic.AddInt64(&m.execMetrics.Task.Fail, 1)
		} else {
			atomic.AddInt64(&m.execMetrics.Task.Succ, 1)
		}
	case <-entry.ctx.cancelCh:
		// 1. cancel by session
		// 2. stop by the arbitrate-helper (cancel / kill by arbitrator)
		res = ArbitrateFail
		atomic.AddInt64(&m.execMetrics.Task.Fail, 1)
		entry.ctx.canceled.Store(true)
		{ // wait for arbitration process finish
			entry.request.taskMu.Lock()

			if !m.removeTask(entry) {
				<-entry.request.resultCh
			}

			entry.request.taskMu.Unlock()
		}
	}

	m.tasks.waitingAlloc.Add(-entry.request.quota)

	return res
}

type blockedState struct {
	allocated int64
	utimeSec  int64
}

// PoolAllocProfile represents the profile of root pool allocation in the mem-arbitrator
type PoolAllocProfile struct {
	SmallPoolLimit   int64 // limit / 1000
	PoolAllocUnit    int64 // limit / 500
	MaxPoolAllocUnit int64 // limit / 100
}

// MemArbitrator represents the main structure aka `mem-arbitrator`
type MemArbitrator struct {
	execMu struct {
		startTime    time.Time          // start time of each round
		blockedState blockedState       // blocked state during arbitration
		mode         ArbitratorWorkMode // work mode of each round
	}
	actions   MemArbitratorActions // actions interfaces
	controlMu struct {             // control the async work process
		finishCh chan struct{}
		sync.Mutex
		running atomic.Bool
	}
	privilegedEntry *rootPoolEntry  // entry with privilege will always be satisfied first
	underKill       mapEntryWithMem // entries under `KILL` operation
	underCancel     mapEntryWithMem // entries under `CANCEL` operation
	notifer         Notifer         // wake up the async work process
	cleanupMu       struct {        // cleanup the state of the entry
		fifoTasks wrapList[*rootPoolEntry]
		sync.Mutex
	}
	tasks struct {
		fifoByPriority [maxArbitrationPriority]wrapList[*rootPoolEntry] // tasks by priority
		fifoTasks      wrapList[*rootPoolEntry]                         // all tasks in FIFO order
		fifoWaitAverse wrapList[*rootPoolEntry]                         // tasks with wait-averse property
		waitingAlloc   atomic.Int64                                     // total waiting allocation size
		sync.Mutex
	}
	digestProfileCache struct {
		shards     []digestProfileShard
		shardsMask uint64
		num        atomic.Int64
		limit      int64 // max number of digest profiles; shrink to limit/2 when num > limit;
	}
	entryMap  entryMap // sharded hash map & ordered quota map
	awaitFree struct { // await-free pool
		pool   *ResourcePool
		budget struct { // fixed size budget shards
			shards   []TrackedConcurrentBudget
			sizeMask uint64
		}
		lastQuotaUsage       memPoolQuotaUsage // tracked heap memory usage & quota usage
		lastShrinkUtimeMilli atomic.Int64
	}

	heapController heapController // monitor runtime mem stats; resolve mem issues; record mem profiles;

	poolAllocStats struct { // statistics of root pool allocation
		sync.RWMutex
		PoolAllocProfile
		mediumQuota atomic.Int64 // medium (max quota usage of root pool)
		timedMap    [2 + defRedundancy]struct {
			sync.RWMutex
			statisticsTimedMapElement
		}
		lastUpdateUtimeMilli atomic.Int64
	}

	buffer buffer // reserved buffer quota which only works under priority mode

	mu struct {
		sync.Mutex
		_         cpuCacheLinePad
		allocated int64  // allocated mem quota
		released  uint64 // total released mem quota
		lastGC    uint64 // total released mem quota at last GC point
		_         cpuCacheLinePad
		limit     int64 // hard limit of mem quota which is same as the server limit
		threshold struct {
			risk    int64 // threshold of mem risk
			oomRisk int64 // threshold of oom risk
		}
		softLimit struct {
			mode      SoftLimitMode
			size      int64
			specified struct {
				size  int64
				ratio int64 // ratio of soft-limit to hard-limit
			}
		}
	}
	execMetrics execMetricsCounter // execution metrics
	avoidance   struct {
		size        atomic.Int64 // size of quota cannot be allocated
		heapTracked struct {     // tracked heap memory usage
			atomic.Int64
			lastUpdateUtimeMilli atomic.Int64
		}
		memMagnif struct { // memory pressure magnification factor: ratio of runtime memory usage to quota usage
			sync.Mutex
			ratio atomic.Int64
		}
		awaitFreeBudgetKickOutIdx uint64 // round-robin index to clean await-free pool budget when quota is insufficient
	}
	tickTask struct { // periodic task
		sync.Mutex
		lastTickUtimeMilli atomic.Int64
	}
	UnixTimeSec int64 // approximate unix time in seconds
	rootPoolNum atomic.Int64
	mode        ArbitratorWorkMode
}

type buffer struct {
	size     atomic.Int64 // approximate max quota usage of root pool
	timedMap [2 + defRedundancy]struct {
		sync.RWMutex
		wrapTimeSizeQuota
	}
}

func (m *MemArbitrator) setBufferSize(v int64) {
	m.buffer.size.Store(v)
}

type digestProfileShard struct {
	sync.Map //map[uint64]*digestProfile
	num      atomic.Int64
}

// MemArbitratorActions represents the actions of the mem-arbitrator
type MemArbitratorActions struct {
	Info, Warn, Error func(format string, args ...zap.Field) // log actions

	UpdateRuntimeMemStats func() // update runtime memory statistics
	GC                    func() // garbage collection
}

type awaitFreePoolExecMetrics struct {
	pairSuccessFail
	Shrink      int64
	ForceShrink int64
}

type pairSuccessFail struct{ Succ, Fail int64 }

// NumByPriority represents the number of tasks by priority
type NumByPriority [maxArbitrationPriority]int64

type execMetricsAction struct {
	GC                    int64
	UpdateRuntimeMemStats int64
	RecordMemState        pairSuccessFail
}

type execMetricsRisk struct {
	Mem     int64
	OOM     int64
	OOMKill NumByPriority
}

type execMetricsCancel struct {
	StandardMode int64
	PriorityMode NumByPriority
	WaitAverse   int64
}

type execMetricsTask struct {
	pairSuccessFail               // all work modes
	SuccByPriority  NumByPriority // priority mode
}

type execMetricsCounter struct {
	Task         execMetricsTask
	Cancel       execMetricsCancel
	AwaitFree    awaitFreePoolExecMetrics
	Action       execMetricsAction
	Risk         execMetricsRisk
	ShrinkDigest int64
}

// ExecMetrics returns the reference of the execution metrics
//
//go:norace
func (m *MemArbitrator) ExecMetrics() execMetricsCounter {
	if m == nil {
		return execMetricsCounter{}
	}
	return m.execMetrics
}

// SetWorkMode sets the work mode of the mem-arbitrator
func (m *MemArbitrator) SetWorkMode(newMode ArbitratorWorkMode) (oriMode ArbitratorWorkMode) {
	oriMode = ArbitratorWorkMode(atomic.SwapInt32((*int32)(&m.mode), int32(newMode)))
	m.wake()
	return
}

// WorkMode returns the current work mode of the mem-arbitrator
func (m *MemArbitrator) WorkMode() ArbitratorWorkMode {
	if m == nil {
		return ArbitratorModeDisable
	}
	return m.workMode()
}

// PoolAllocProfile returns the profile of root pool allocation in the mem-arbitrator
func (m *MemArbitrator) PoolAllocProfile() (res PoolAllocProfile) {
	limit := m.limit()
	return PoolAllocProfile{
		SmallPoolLimit:   max(1, limit/defServerlimitSmallLimitNum),
		PoolAllocUnit:    max(1, limit/defServerlimitMinUnitNum),
		MaxPoolAllocUnit: max(1, limit/defServerlimitMaxUnitNum),
	}
}

func (m *MemArbitrator) workMode() ArbitratorWorkMode {
	return ArbitratorWorkMode(atomic.LoadInt32((*int32)(&m.mode)))
}

// GetDigestProfileCache returns the digest profile cache for a given digest-id and utime
func (m *MemArbitrator) GetDigestProfileCache(digestID uint64, utimeSec int64) (int64, bool) {
	d := &m.digestProfileCache.shards[digestID&m.digestProfileCache.shardsMask]
	e, ok := d.Load(digestID)
	if !ok {
		return 0, false
	}

	pf := e.(*digestProfile)

	if utimeSec > pf.lastFetchUtimeSec.Load() {
		pf.lastFetchUtimeSec.Store(utimeSec)
	}

	return pf.maxVal.Load(), true
}

func (m *MemArbitrator) shrinkDigestProfile(utimeSec int64, limit, shrinkTo int64) (shrinkedNum int64) {
	if m.digestProfileCache.num.Load() <= limit {
		return
	}

	m.execMetrics.ShrinkDigest++

	var valMap [defPoolQuotaShards]int

	for i := range m.digestProfileCache.shards {
		d := &m.digestProfileCache.shards[i]
		if d.num.Load() == 0 {
			continue
		}
		dn := int64(0)
		d.Range(func(k, v any) bool {
			pf := v.(*digestProfile)
			maxVal := pf.maxVal.Load()
			{ // try to delete timeout cache
				needDelete := false

				if maxVal > m.poolAllocStats.SmallPoolLimit {
					if utimeSec-pf.lastFetchUtimeSec.Load() > defDigestProfileMemTimeoutSec {
						needDelete = true
					}
				} else { // small max-val
					if utimeSec-pf.lastFetchUtimeSec.Load() > defDigestProfileSmallMemTimeoutSec {
						needDelete = true
					}
				}

				if needDelete {
					if _, loaded := d.LoadAndDelete(k); loaded {
						d.num.Add(-1)
						dn++
						return true
					}
				}
			}
			index := getQuotaShard(maxVal, defPoolQuotaShards)
			valMap[index]++
			return true
		})
		m.digestProfileCache.num.Add(-dn)
		shrinkedNum += dn
	}

	toShinkNum := m.digestProfileCache.num.Load() - shrinkTo
	if toShinkNum <= 0 {
		return
	}

	shrinkMaxSize := DefMaxLimit
	{ // find the max size to shrink
		n := int64(0)
		for i := range defPoolQuotaShards {
			if n += int64(valMap[i]); n >= toShinkNum {
				shrinkMaxSize = baseQuotaUnit * (1 << i)
				break
			}
		}
	}

	for i := range m.digestProfileCache.shards {
		d := &m.digestProfileCache.shards[i]
		if d.num.Load() == 0 {
			continue
		}
		dn := int64(0)
		d.Range(func(k, v any) bool {
			if pf := v.(*digestProfile); pf.maxVal.Load() < shrinkMaxSize {
				if _, loaded := d.LoadAndDelete(k); loaded {
					d.num.Add(-1)
					toShinkNum--
					dn++
				}
			}

			return toShinkNum > 0
		})
		m.digestProfileCache.num.Add(-dn)
		shrinkedNum += dn

		if toShinkNum <= 0 {
			break
		}
	}

	return
}

// UpdateDigestProfileCache updates the digest profile cache for a given digest-id
func (m *MemArbitrator) UpdateDigestProfileCache(digestID uint64, memConsumed int64, utimeSec int64) {
	d := &m.digestProfileCache.shards[digestID&m.digestProfileCache.shardsMask]
	var pf *digestProfile
	if e, ok := d.Load(digestID); ok {
		pf = e.(*digestProfile)
	} else {
		pf = &digestProfile{}
		if actual, loaded := d.LoadOrStore(digestID, pf); loaded {
			pf = actual.(*digestProfile)
		} else {
			d.num.Add(1)
			m.digestProfileCache.num.Add(1)
		}
	}

	const maxNum = int64(len(pf.timedMap))
	const maxDur = maxNum - defRedundancy

	tsAlign := utimeSec / defUpdateBufferTimeAlignSec
	tar := &pf.timedMap[tsAlign%maxNum]

	if oriTs := tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
		tar.Lock()

		if oriTs = tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.wrapTimeMaxval = wrapTimeMaxval{}
		}

		tar.Unlock()
	}

	tar.RLock()

	updateSize := false
	cleanNext := false

	if tar.tsAlign.Load() == 0 {
		if tar.tsAlign.CompareAndSwap(0, tsAlign) {
			cleanNext = true
		}
	}

	for oldVal := tar.maxVal.Load(); oldVal < memConsumed; oldVal = tar.maxVal.Load() {
		if tar.maxVal.CompareAndSwap(oldVal, memConsumed) {
			updateSize = true
			break
		}
	}

	if updateSize {
		maxv := tar.maxVal.Load()
		// tsAlign-1, tsAlign
		for i := range maxDur {
			d := &pf.timedMap[(maxNum+tsAlign-i)%maxNum]

			if ts := d.tsAlign.Load(); ts > tsAlign-maxDur && ts <= tsAlign {
				maxv = max(maxv, d.maxVal.Load())
			}
		}
		pf.maxVal.CompareAndSwap(pf.maxVal.Load(), maxv) // force update
	}

	tar.RUnlock()

	if utimeSec > pf.lastFetchUtimeSec.Load() {
		pf.lastFetchUtimeSec.Store(utimeSec)
	}

	if cleanNext {
		d := &pf.timedMap[(tsAlign+1)%maxNum]
		d.Lock()

		if ts := d.tsAlign.Load(); ts < (tsAlign+1) && ts != 0 {
			d.wrapTimeMaxval = wrapTimeMaxval{}
		}

		d.Unlock()
	}
}

type digestProfile struct {
	maxVal   atomic.Int64
	timedMap [2 + defRedundancy]struct {
		sync.RWMutex
		wrapTimeMaxval
	}
	lastFetchUtimeSec atomic.Int64
}

type wrapTimeMaxval struct {
	tsAlign atomic.Int64
	maxVal  atomic.Int64
}

type wrapTimeSizeQuota struct {
	ts    atomic.Int64
	size  atomic.Int64
	quota atomic.Int64
}

type statisticsTimedMapElement struct {
	tsAlign atomic.Int64
	slot    [defServerlimitMinUnitNum]uint32
	num     atomic.Uint64
}

type entryKillCancelCtx struct {
	startTime time.Time
	reclaim   int64
	start     bool
	fail      bool
}

type mapEntryWithMem struct {
	entries mapUIDEntry
	num     int64
}

func (x *mapEntryWithMem) delete(entry *rootPoolEntry) {
	delete(x.entries, entry.pool.uid)
	x.num--
}

//go:norace
func (x *mapEntryWithMem) approxSize() int64 {
	return x.num
}

func (x *mapEntryWithMem) init() {
	x.entries = make(mapUIDEntry)
}

func (x *mapEntryWithMem) add(entry *rootPoolEntry) {
	x.entries[entry.pool.uid] = entry
	x.num++
}

// DebugFields is used to store debug fields for logging
type DebugFields struct {
	fields [30]zap.Field
	n      int
}

// ConcurrentBudget represents a wrapped budget of the resource pool for concurrent usage
type ConcurrentBudget struct {
	Pool            *ResourcePool
	Capacity        int64
	LastUsedTimeSec int64
	sync.Mutex
	_    cpuCacheLinePad
	Used atomic.Int64
	_    cpuCacheLinePad
}

//go:norace
func (b *ConcurrentBudget) setLastUsedTimeSec(t int64) {
	b.LastUsedTimeSec = t
}

//go:norace
func (b *ConcurrentBudget) approxCapacity() int64 {
	return b.Capacity
}

func (b *ConcurrentBudget) getLastUsedTimeSec() int64 {
	return b.LastUsedTimeSec
}

// TrackedConcurrentBudget consists of ConcurrentBudget and heap inuse
type TrackedConcurrentBudget struct {
	ConcurrentBudget
	HeapInuse atomic.Int64
	_         cpuCacheLinePad
}

// Stop stops the concurrent budget and releases all the capacity and make the pool non-allocatable
func (b *ConcurrentBudget) Stop() int64 {
	b.Lock()
	defer b.Unlock()

	b.Pool.SetOutOfCapacityAction(func(OutOfCapacityActionArgs) error {
		return errArbitrateFailError
	})

	budgetCap := b.Capacity
	b.Capacity = 0
	b.Used.Store(0)
	if budgetCap > 0 {
		b.Pool.release(budgetCap)
	}

	return budgetCap
}

// Reserve reserves a given capacity for the concurrent budget
func (b *ConcurrentBudget) Reserve(newCap int64) (err error) {
	b.Lock()

	extra := max(newCap, b.Used.Load(), b.Capacity) - b.Capacity
	if err = b.Pool.allocate(extra); err == nil {
		b.Capacity += extra
	}

	b.Unlock()
	return
}

// PullFromUpstream tries to pull from the upstream pool when facing `out of capacity`
// It requires the action of the pool to be non-blocking
func (b *ConcurrentBudget) PullFromUpstream() (err error) {
	b.Lock()

	delta := b.Used.Load() - b.Capacity
	if delta > 0 {
		delta = b.Pool.roundSize(delta)
		if err = b.Pool.allocate(delta); err == nil {
			b.Capacity += delta
		}
	}

	b.Unlock()
	return
}

// AutoRun starts the work groutine of the mem-arbitrator asynchronously
func (m *MemArbitrator) AutoRun(
	actions MemArbitratorActions,
	awaitFreePoolAllocAlignSize, awaitFreePoolShardNum int64,
	taskTickDur time.Duration,
) bool {
	m.controlMu.Lock()
	defer m.controlMu.Unlock()

	if m.controlMu.running.Load() {
		return false
	}

	{ // init
		m.actions = actions
		m.refreshRuntimeMemStats()
		m.initAwaitFreePool(awaitFreePoolAllocAlignSize, awaitFreePoolShardNum)
	}
	return m.asyncRun(taskTickDur)
}

func (m *MemArbitrator) refreshRuntimeMemStats() {
	if m.actions.UpdateRuntimeMemStats != nil {
		m.actions.UpdateRuntimeMemStats() // should invoke `SetRuntimeMemStats`
	}
	atomic.AddInt64(&m.execMetrics.Action.UpdateRuntimeMemStats, 1)
}

func (m *MemArbitrator) trySetRuntimeMemStats(s memStats) bool {
	if m.heapController.TryLock() {
		m.doSetRuntimeMemStats(s)
		m.heapController.Unlock()
		return true
	}
	return false
}

func (m *MemArbitrator) setRuntimeMemStats(s memStats) {
	m.heapController.Lock()
	m.doSetRuntimeMemStats(s)
	m.heapController.Unlock()
}

func (m *MemArbitrator) doSetRuntimeMemStats(s memStats) {
	m.heapController.heapAlloc.Store(s.HeapAlloc)
	m.heapController.heapInuse.Store(s.HeapInuse)
	m.heapController.heapTotalFree.Store(s.TotalFree)
	m.heapController.memOffHeap.Store(s.MemOffHeap)
	m.heapController.memInuse.Store(s.MemOffHeap + s.HeapInuse)

	if s.LastGC > m.heapController.lastGC.utime.Load() {
		m.heapController.lastGC.heapAlloc.Store(s.HeapAlloc)
		m.heapController.lastGC.utime.Store(s.LastGC)
	}

	m.updateAvoidSize() // calc out-of-control & avoid size
}

func (m *MemArbitrator) updateAvoidSize() {
	capacity := m.softLimit()
	if m.mu.softLimit.mode == SoftLimitModeAuto {
		if ratio := m.memMagnif(); ratio != 0 {
			newCap := calcRatio(m.limit(), ratio)
			capacity = min(capacity, newCap)
		}
	}
	avoidSize := max(
		0,
		m.heapController.heapAlloc.Load()+m.heapController.memOffHeap.Load()-m.avoidance.heapTracked.Load(), // out-of-control size
		m.limit()-capacity,
	)
	m.avoidance.size.Store(avoidSize)

	if delta := m.allocated() - m.limit() + avoidSize; delta > 0 && m.awaitFree.pool.allocated() > 0 {
		reclaimed := int64(0)
		poolReleased := int64(0)
		for i := range len(m.awaitFree.budget.shards) {
			idx := (m.avoidance.awaitFreeBudgetKickOutIdx + uint64(i) + 1) & m.awaitFree.budget.sizeMask
			b := &m.awaitFree.budget.shards[idx]
			if b.approxCapacity() > 0 && b.TryLock() {
				x := min(delta-reclaimed, b.Capacity)
				b.Capacity -= x
				reclaimed += x
				b.Unlock()
			}

			if reclaimed >= delta {
				m.avoidance.awaitFreeBudgetKickOutIdx = idx
				break
			}
		}
		if reclaimed > 0 {
			poolReleased = m.awaitFree.pool.releasePopBudget(reclaimed)
		}
		if poolReleased > 0 {
			m.release(poolReleased)
			atomic.AddInt64(&m.execMetrics.AwaitFree.ForceShrink, 1)
			atomic.AddUint64(&m.mu.released, uint64(poolReleased))
		}
	}
}

func (p *ResourcePool) releasePopBudget(c int64) int64 {
	p.mu.Lock()

	p.doRelease(c)
	released := p.mu.budget.available()
	p.mu.budget.cap -= released

	p.mu.Unlock()
	return released
}

func (m *MemArbitrator) weakWake() {
	m.notifer.WeakWake()
}

func (m *MemArbitrator) wake() {
	m.notifer.Wake()
}

func (m *MemArbitrator) updatePoolMediumCapacity(utimeMilli int64) {
	s := &m.poolAllocStats
	const maxNum = int64(len(s.timedMap))
	const maxDur = maxNum - defRedundancy

	{
		s.RLock()

		tsAlign := utimeMilli / kilo / defUpdateMemConsumedTimeAlignSec
		tar1 := &s.timedMap[(maxNum+tsAlign-1)%maxNum]
		tar2 := &s.timedMap[tsAlign%maxNum]

		if ts := tar1.tsAlign.Load(); ts <= tsAlign-maxDur || ts > tsAlign {
			tar1 = nil
		}
		if ts := tar2.tsAlign.Load(); ts <= tsAlign-maxDur || ts > tsAlign {
			tar2 = nil
		}

		total := uint64(0)
		if tar1 != nil {
			tar1.RLock()
			total += tar1.num.Load()
		}
		if tar2 != nil {
			tar2.RLock()
			total += tar2.num.Load()
		}

		if total != 0 {
			expect := max(1, (total+1)/2)
			cnt := uint64(0)
			index := 0

			for i := range defServerlimitMinUnitNum {
				if tar1 != nil {
					cnt += uint64(tar1.slot[i])
				}
				if tar2 != nil {
					cnt += uint64(tar2.slot[i])
				}
				if cnt >= expect {
					index = i
					break
				}
			}

			res := s.PoolAllocUnit * int64(index+1)

			s.mediumQuota.Store(res)
		}

		if tar1 != nil {
			tar1.RUnlock()
		}
		if tar2 != nil {
			tar2.RUnlock()
		}

		s.RUnlock()
	}

	m.tryStorePoolMediumCapacity(utimeMilli, m.poolMediumQuota())
}

func (m *MemArbitrator) tryStorePoolMediumCapacity(utimeMilli int64, capacity int64) bool {
	if capacity == 0 {
		return false
	}
	if lastState := m.lastMemState(); lastState == nil ||
		(m.poolAllocStats.lastUpdateUtimeMilli.Load()+defStorePoolMediumCapDurMilli <= utimeMilli &&
			lastState.PoolMediumCap != capacity) {
		var memState *RuntimeMemStateV1

		if lastState != nil {
			s := *lastState // copy
			s.PoolMediumCap = capacity
			memState = &s
		} else {
			memState = &RuntimeMemStateV1{
				Version:       1,
				PoolMediumCap: capacity,
			}
		}

		_ = m.recordMemState(memState, "new root pool medium cap")
		m.poolAllocStats.lastUpdateUtimeMilli.Store(utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) poolMediumQuota() int64 {
	return m.poolAllocStats.mediumQuota.Load()
}

// SuggestPoolInitCap returns the suggested initial capacity for the pool
func (m *MemArbitrator) SuggestPoolInitCap() int64 {
	return m.poolMediumQuota()
}

func (m *MemArbitrator) updateMemMagnification(utimeMilli int64) (updatedPreProf *memProfile) {
	const maxNum = int64(len(m.heapController.timedMemProfile))

	curTsAlign := utimeMilli / kilo / defUpdateMemMagnifUtimeAlign
	profs := &m.heapController.timedMemProfile
	cur := &profs[curTsAlign%maxNum]
	if cur.tsAlign < curTsAlign {
		{ // update previous record
			preTs := curTsAlign - 1
			preIdx := (maxNum + preTs) % maxNum
			pre := &profs[preIdx]

			pre.ratio = 0
			if pre.tsAlign == preTs && pre.quota > 0 {
				if pre.heap > 0 {
					pre.ratio = calcRatio(pre.heap, pre.quota)
				}
				updatedPreProf = pre
			}
		}

		v := int64(0)
		// check the memory profile in the last 60s
		for _, tsAlign := range []int64{curTsAlign - 2, curTsAlign - 1} {
			tar := &profs[(maxNum+tsAlign)%maxNum]
			if tar.tsAlign != tsAlign ||
				tar.heap >= m.oomRisk() { // calculate the magnification only when the heap is safe
				v = 0
				break
			}

			if tar.ratio <= 0 {
				break // if any record is not valid,
			}
			v = max(v, tar.ratio)
		}

		updated := false
		var oriRatio, newRatio int64

		if v != 0 && m.avoidance.memMagnif.TryLock() {
			if oriRatio = m.memMagnif(); oriRatio != 0 && v < oriRatio-10 /* 1 percent */ {
				newRatio = (oriRatio + v) / 2
				if newRatio <= kilo {
					newRatio = 0
				}
				m.doSetMemMagnif(newRatio)
				updated = true
			}
			m.avoidance.memMagnif.Unlock()
		}

		if updated {
			m.actions.Info("Update mem quota magnification ratio",
				zap.Int64("ori-ratio(‰)", oriRatio),
				zap.Int64("new-ratio(‰)", newRatio),
			)

			if lastMemState := m.lastMemState(); lastMemState != nil && newRatio < lastMemState.Magnif {
				memState := RuntimeMemStateV1{
					Version:       1,
					Magnif:        newRatio,
					PoolMediumCap: m.poolMediumQuota(),
				}
				_ = m.recordMemState(&memState, "new magnification ratio")
			}
		}

		*cur = memProfile{
			tsAlign:         curTsAlign,
			startUtimeMilli: utimeMilli,
		}
	}

	if cur.tsAlign == curTsAlign {
		if ut := m.heapController.lastGC.utime.Load(); curTsAlign == ut/int64(time.Second)/defUpdateMemMagnifUtimeAlign {
			cur.heap = max(cur.heap, m.heapController.lastGC.heapAlloc.Load())
		}
		if blockedSize, utimeSec := m.lastBlockedAt(); utimeSec/defUpdateMemMagnifUtimeAlign == curTsAlign {
			cur.quota = max(cur.quota, blockedSize)
		}
	}
	return
}

func (m *MemArbitrator) doSetMemMagnif(ratio int64) {
	m.avoidance.memMagnif.ratio.Store(ratio)
}

func (m *MemArbitrator) memMagnif() int64 {
	return m.avoidance.memMagnif.ratio.Load()
}

//go:norace
func (m *MemArbitrator) awaitFreePoolCap() int64 {
	if m.awaitFree.pool == nil {
		return 0
	}
	return m.awaitFree.pool.capacity()
}

type memPoolQuotaUsage struct{ trackedHeap, quota int64 }

//go:norace
func (m *MemArbitrator) awaitFreePoolUsed() (res memPoolQuotaUsage) {
	for i := range m.awaitFree.budget.shards {
		if d := m.awaitFree.budget.shards[i].Used.Load(); d > 0 {
			res.quota += d
		}
		if d := m.awaitFree.budget.shards[i].HeapInuse.Load(); d > 0 {
			res.trackedHeap += d
		}
	}
	m.awaitFree.lastQuotaUsage = res
	return
}

//go:norace
func (m *MemArbitrator) approxAwaitFreePoolUsed() memPoolQuotaUsage {
	return m.awaitFree.lastQuotaUsage
}

func (m *MemArbitrator) executeTick(utimeMilli int64) bool { // exec batch tasks every 1s
	if m.atMemRisk() { // skip if oom check is running because mem state is not safe
		return false
	}

	if m.tickTask.lastTickUtimeMilli.Load()+defTickDurMilli > utimeMilli {
		return false
	}
	m.tickTask.Lock()
	defer m.tickTask.Unlock()

	m.tickTask.lastTickUtimeMilli.Store(utimeMilli)

	// mem magnification
	if updatedPreProf := m.updateMemMagnification(utimeMilli); updatedPreProf != nil {
		pre := updatedPreProf
		profile := m.recordDebugProfile()
		profile.append(
			zap.Int64("last-blocked-heap", pre.heap), zap.Int64("last-blocked-quota", pre.quota),
			zap.Int64("last-magnification-ratio(‰)", pre.ratio),
			zap.Time("last-prof-start-time", time.UnixMilli(pre.startUtimeMilli)),
		)
		m.actions.Info("Mem profile timeline",
			profile.fields[:profile.n]...,
		)
	}
	// suggest pool cap
	m.updatePoolMediumCapacity(utimeMilli)
	// shrink mem profile cache
	m.shrinkDigestProfile(utimeMilli/kilo, m.digestProfileCache.limit, m.digestProfileCache.limit/2)
	return true
}

func (d *DebugFields) append(f ...zap.Field) {
	n := min(len(f), len(d.fields)-d.n)
	for i := range n {
		d.fields[d.n] = f[i]
		d.n++
	}
}

func (m *MemArbitrator) recordDebugProfile() (f DebugFields) {
	taskNumByMode := m.TaskNumByPattern()
	memMagnif := m.memMagnif()
	if memMagnif == 0 {
		memMagnif = -1
	}
	f.append(
		zap.Int64("heap-inuse", m.heapController.heapInuse.Load()),
		zap.Int64("heap-alloc", m.heapController.heapAlloc.Load()),
		zap.Int64("mem-off-heap", m.heapController.memOffHeap.Load()),
		zap.Int64("hard-limit", m.limit()),
		zap.Int64("quota-allocated", m.allocated()),
		zap.Int64("quota-softlimit", m.softLimit()),
		zap.Int64("mem-magnification-ratio(‰)", memMagnif),
		zap.Int64("root-pool-num", m.RootPoolNum()),
		zap.Int64("awaitfree-pool-cap", m.awaitFreePoolCap()),
		zap.Int64("awaitfree-pool-used", m.approxAwaitFreePoolUsed().quota),
		zap.Int64("awaitfree-pool-heapinuse", m.approxAwaitFreePoolUsed().trackedHeap),
		zap.Int64("tracked-heapinuse", m.avoidance.heapTracked.Load()),
		zap.Int64("out-of-control", m.OutOfControl()),
		zap.Int64("buffer", m.buffer.size.Load()),
		zap.Int64("task-num", m.TaskNum()),
		zap.Int64("task-priority-low", taskNumByMode[ArbitrationPriorityLow]),
		zap.Int64("task-priority-medium", taskNumByMode[ArbitrationPriorityMedium]),
		zap.Int64("task-priority-high", taskNumByMode[ArbitrationPriorityHigh]),
		zap.Int64("pending-alloc-size", m.WaitingAllocSize()),
		zap.Int64("digest-cache-num", m.digestProfileCache.num.Load()),
	)
	if t := m.heapController.memRisk.startTime.unixMilli.Load(); t != 0 {
		f.append(zap.Time("mem-risk-start", time.UnixMilli(t)))
	}
	return
}
