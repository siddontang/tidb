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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

func (m *MemArbitrator) addUnderKill(entry *rootPoolEntry, memoryUsed int64, startTime time.Time) {
	if !entry.arbitratorMu.underKill.start {
		m.underKill.add(entry)
		entry.arbitratorMu.underKill = entryKillCancelCtx{
			start:     true,
			startTime: startTime,
			reclaim:   memoryUsed,
		}
	}
}

func (m *MemArbitrator) addUnderCancel(entry *rootPoolEntry, memoryUsed int64, startTime time.Time) {
	if !entry.arbitratorMu.underCancel.start {
		m.underCancel.add(entry)
		entry.arbitratorMu.underCancel = entryKillCancelCtx{
			start:     true,
			startTime: startTime,
			reclaim:   memoryUsed,
		}
	}
}

func (m *MemArbitrator) deleteUnderKill(entry *rootPoolEntry) {
	if entry.arbitratorMu.underKill.start {
		m.underKill.delete(entry)
		entry.arbitratorMu.underKill.start = false

		m.warnKillCancel(entry, &entry.arbitratorMu.underKill, "Finish to `KILL` root pool")
	}
}

func (m *MemArbitrator) deleteUnderCancel(entry *rootPoolEntry) {
	if entry.arbitratorMu.underCancel.start {
		m.underCancel.delete(entry)
		entry.arbitratorMu.underCancel.start = false
	}
}

type memProfile struct {
	startUtimeMilli int64
	tsAlign         int64
	heap            int64 // max heap-alloc size after GC
	quota           int64 // max quota allocated when failed to arbitrate
	ratio           int64 // heap / quota
}

type heapController struct {
	memStateRecorder struct {
		RecordMemState
		lastMemState         atomic.Pointer[RuntimeMemStateV1]
		lastRecordUtimeMilli atomic.Int64
		sync.Mutex
	}
	memRisk struct {
		startTime struct {
			t         time.Time
			unixMilli atomic.Int64
		}
		lastMemStats struct {
			startTime     time.Time
			heapTotalFree int64
		}
		minHeapFreeBPS int64
		oomRisk        bool
	}
	timedMemProfile [2]memProfile
	lastGC          struct {
		heapAlloc atomic.Int64 // heap alloc size after GC
		utime     atomic.Int64 // end time of last GC
	}
	heapTotalFree atomic.Int64
	heapAlloc     atomic.Int64 // heap objects occupied bytes
	heapInuse     atomic.Int64 // heap-inuse = heap-alloc + unused
	memOffHeap    atomic.Int64 // off-heap memory: `stack` + `gc` + `other` + `meta` ...
	memInuse      atomic.Int64 // heap-inuse + off-heap: must be less than runtime-limit to avoid Heavy GC / OOM
	sync.Mutex
}

func (m *MemArbitrator) lastMemState() (res *RuntimeMemStateV1) {
	res = m.heapController.memStateRecorder.lastMemState.Load()
	return
}

// RecordMemState is an interface for recording runtime memory state
type RecordMemState interface {
	Load() (*RuntimeMemStateV1, error)
	Store(*RuntimeMemStateV1) error
}

func (m *MemArbitrator) recordMemConsumed(memConsumed, utimeSec int64) {
	m.poolAllocStats.RLock()
	defer m.poolAllocStats.RUnlock()

	const maxNum = int64(len(m.poolAllocStats.timedMap))

	tsAlign := utimeSec / defUpdateMemConsumedTimeAlignSec
	tar := &m.poolAllocStats.timedMap[tsAlign%maxNum]

	if oriTs := tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
		tar.Lock()

		if oriTs = tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.statisticsTimedMapElement = statisticsTimedMapElement{}
		}

		tar.Unlock()
	}

	cleanNext := false
	{
		tar.RLock()

		if tar.tsAlign.Load() == 0 {
			if tar.tsAlign.CompareAndSwap(0, tsAlign) {
				cleanNext = true
			}
		}

		{
			pos := min(memConsumed/m.poolAllocStats.PoolAllocUnit, defServerlimitMinUnitNum-1)
			atomic.AddUint32(&tar.slot[pos], 1)
			tar.num.Add(1)
		}

		tar.RUnlock()
	}

	if cleanNext {
		d := &m.poolAllocStats.timedMap[(tsAlign+1)%maxNum]
		d.Lock()

		if v := d.tsAlign.Load(); v < (tsAlign+1) && v != 0 {
			d.statisticsTimedMapElement = statisticsTimedMapElement{}
		}

		d.Unlock()
	}
}

func (m *MemArbitrator) tryToUpdateBuffer(memConsumed, utimeSec int64) {
	const maxNum = int64(len(m.buffer.timedMap))
	const maxDur = maxNum - defRedundancy

	tsAlign := utimeSec / defUpdateBufferTimeAlignSec
	tar := &m.buffer.timedMap[tsAlign%maxNum]

	if oriTs := tar.ts.Load(); oriTs < tsAlign && oriTs != 0 {
		tar.Lock()

		if oriTs = tar.ts.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.wrapTimeSizeQuota = wrapTimeSizeQuota{}
		}

		tar.Unlock()
	}
	cleanNext := false
	{
		tar.RLock()

		updateSize := false

		if ts := tar.ts.Load(); ts == 0 {
			if tar.ts.CompareAndSwap(0, tsAlign) {
				cleanNext = true
			}
		}

		for oldVal := tar.size.Load(); oldVal < memConsumed; oldVal = tar.size.Load() {
			if tar.size.CompareAndSwap(oldVal, memConsumed) {
				updateSize = true
				break
			}
		}

		if updateSize {
			// tsAlign-1, tsAlign
			for i := range maxDur {
				d := &m.buffer.timedMap[(maxNum+tsAlign-i)%maxNum]

				if ts := d.ts.Load(); ts > tsAlign-maxDur && ts <= tsAlign {
					memConsumed = max(memConsumed, d.size.Load())
				}
			}
			if updateSize && m.buffer.size.Load() != memConsumed {
				m.setBufferSize(memConsumed)
			}
		}

		tar.RUnlock()
	}

	if cleanNext {
		d := &m.buffer.timedMap[(tsAlign+1)%maxNum]
		d.Lock()

		if v := d.ts.Load(); v < tsAlign+1 && v != 0 {
			d.wrapTimeSizeQuota = wrapTimeSizeQuota{}
		}

		d.Unlock()
	}
}

func (m *MemArbitrator) gc() {
	m.mu.lastGC = m.mu.released
	if m.actions.GC != nil {
		m.actions.GC()
	}
	atomic.AddInt64(&m.execMetrics.Action.GC, 1)
}

func (m *MemArbitrator) reclaimHeap() {
	m.gc()
	m.refreshRuntimeMemStats() // refresh runtime mem stats after GC and record
}

func (m *MemArbitrator) setMinHeapFreeBPS(sz int64) {
	m.heapController.memRisk.minHeapFreeBPS = sz
}

func (m *MemArbitrator) minHeapFreeBPS() int64 {
	return m.heapController.memRisk.minHeapFreeBPS
}

// ResetRootPoolByID resets the root pool by ID and analyze the memory consumption info
func (m *MemArbitrator) ResetRootPoolByID(uid uint64, maxMemConsumed int64, tune bool) {
	entry := m.getRootPoolEntry(uid)
	if entry == nil {
		return
	}

	m.tryToUpdateBuffer(
		maxMemConsumed,
		m.approxUnixTimeSec())

	if tune {
		if maxMemConsumed > m.poolAllocStats.SmallPoolLimit {
			m.recordMemConsumed(
				maxMemConsumed,
				m.approxUnixTimeSec())
		}
	}

	m.resetRootPoolEntry(entry)
	m.wake()
}

func (m *MemArbitrator) resetRootPoolEntry(entry *rootPoolEntry) bool {
	{
		entry.stateMu.Lock()

		if entry.execState() == execStateIdle {
			entry.stateMu.Unlock()
			return false
		}
		entry.setExecState(execStateIdle)

		entry.stateMu.Unlock()
	}

	// aquiure the lock of root pool:
	// - wait for the alloc task to finish
	// - publish the state of entry
	if releasedSize := entry.pool.Stop(); releasedSize > 0 {
		entry.stateMu.quotaToReclaim.Add(releasedSize)
	}

	{
		m.cleanupMu.Lock()

		m.cleanupMu.fifoTasks.pushBack(entry)

		m.cleanupMu.Unlock()
	}

	return true
}

func (m *MemArbitrator) warnKillCancel(entry *rootPoolEntry, ctx *entryKillCancelCtx, reason string) {
	m.actions.Warn(
		reason,
		zap.Uint64("uid", entry.pool.uid),
		zap.String("name", entry.pool.name),
		zap.String("mem-priority", entry.ctx.memPriority.String()),
		zap.Int64("reclaimed", ctx.reclaim),
		zap.Time("start-time", ctx.startTime),
	)
}

// RemoveRootPoolByID removes & terminates the root pool by ID
func (m *MemArbitrator) RemoveRootPoolByID(uid uint64) bool {
	entry := m.getRootPoolEntry(uid)
	if entry == nil {
		return false
	}

	if m.removeRootPoolEntry(entry) {
		if ctx := entry.ctx.Load(); ctx != nil && ctx.arbitrateHelper != nil {
			ctx.arbitrateHelper.Finish()
		}
		m.wake()
		return true
	}
	return false
}

func (m *MemArbitrator) removeRootPoolEntry(entry *rootPoolEntry) bool {
	{
		entry.stateMu.Lock()

		if entry.stateMu.stop.Swap(true) {
			entry.stateMu.Unlock()
			return false
		}

		if entry.execState() != execStateIdle {
			entry.setExecState(execStateIdle)
		}

		entry.stateMu.Unlock()
	}

	// make the alloc task failed in arbitrator;
	{
		m.cleanupMu.Lock()

		m.cleanupMu.fifoTasks.pushBack(entry)

		m.cleanupMu.Unlock()
	}
	// aquiure the lock of root pool and clean up
	entry.pool.Stop()
	// any new lock of root pool must have sensed the exec state is idle

	return true
}

func (m *MemArbitrator) getRootPoolEntry(uid uint64) *rootPoolEntry {
	if e, ok := m.entryMap.getStatusShard(uid).get(uid); ok {
		return e
	}
	return nil
}

type rootPoolWrap struct {
	entry *rootPoolEntry
}

// FindRootPool finds the root pool by ID
func (m *MemArbitrator) FindRootPool(uid uint64) rootPoolWrap {
	if e := m.getRootPoolEntry(uid); e != nil {
		return rootPoolWrap{e}
	}
	return rootPoolWrap{}
}

// EmplaceRootPool emplaces a new root pool with the given uid (uid < 0 means the internal pool)
func (m *MemArbitrator) EmplaceRootPool(uid uint64) (rootPoolWrap, error) {
	if e := m.getRootPoolEntry(uid); e != nil {
		return rootPoolWrap{e}, nil
	}

	pool := &ResourcePool{
		name:           fmt.Sprintf("root-%d", uid),
		uid:            uid,
		limit:          DefMaxLimit,
		allocAlignSize: 1,
	}
	entry, err := m.addRootPool(pool)
	return rootPoolWrap{entry}, err
}

func (m *MemArbitrator) addRootPool(pool *ResourcePool) (*rootPoolEntry, error) {
	if b := pool.capacity(); b != 0 {
		return nil, fmt.Errorf("%s: has %d bytes budget left", pool.name, b)
	}
	if pool.mu.budget.pool != nil {
		return nil, fmt.Errorf("%s: already started with pool %s", pool.name, pool.mu.budget.pool.Name())
	}
	if pool.reserved != 0 {
		return nil, fmt.Errorf("%s: has %d reserved budget left", pool.name, pool.reserved)
	}

	entry, ok := m.entryMap.emplace(pool)

	if !ok {
		return nil, fmt.Errorf("%s: already exists", pool.name)
	}

	m.rootPoolNum.Add(1)
	return entry, nil
}

func (m *MemArbitrator) doAdjustSoftLimit() {
	var softLimit int64
	limit := m.limit()
	if m.mu.softLimit.mode == SoftLimitModeSpecified {
		if m.mu.softLimit.specified.size > 0 {
			softLimit = min(m.mu.softLimit.specified.size, limit)
		} else {
			softLimit = min(multiRatio(limit, m.mu.softLimit.specified.ratio), limit)
		}
	} else {
		softLimit = m.oomRisk()
	}
	m.mu.softLimit.size = softLimit
}

// SetSoftLimit sets the soft limit of the mem-arbitrator
func (m *MemArbitrator) SetSoftLimit(softLimit int64, sortLimitRatio float64, mode SoftLimitMode) {
	m.mu.Lock()

	m.mu.softLimit.mode = mode
	if mode == SoftLimitModeSpecified {
		m.mu.softLimit.specified.size = softLimit
		m.mu.softLimit.specified.ratio = intoRatio(sortLimitRatio)
	}
	m.doAdjustSoftLimit()

	m.mu.Unlock()
}

//go:norace
func (m *MemArbitrator) softLimit() int64 {
	return m.mu.softLimit.size
}

// SoftLimit returns the soft limit of the mem-arbitrator
func (m *MemArbitrator) SoftLimit() uint64 {
	if m == nil {
		return 0
	}
	return uint64(m.softLimit())
}

func (m *MemArbitrator) doSetLimit(limit int64) {
	m.mu.limit = limit
	m.mu.threshold.oomRisk = int64(float64(limit) * defOOMRiskRatio)
	m.mu.threshold.risk = int64(float64(limit) * defMemRiskRatio)
	m.doAdjustSoftLimit()
}

// SetLimit sets the limit of the mem-arbitrator and returns whether the limit has changed
func (m *MemArbitrator) SetLimit(x uint64) (changed bool) {
	newLimit := min(int64(x), DefMaxLimit)
	if newLimit <= 0 {
		return
	}

	needWake := false
	{
		m.mu.Lock()

		if limit := m.limit(); newLimit != limit {
			changed = true
			needWake = newLimit > limit // update to a greater limit
			m.doSetLimit(newLimit)
		}

		m.mu.Unlock()
	}

	if changed {
		m.resetStatistics()
	}

	if needWake {
		m.weakWake()
	}
	return
}

func (m *MemArbitrator) resetStatistics() {
	m.poolAllocStats.Lock()

	m.poolAllocStats.PoolAllocProfile = m.PoolAllocProfile()
	for i := range m.poolAllocStats.timedMap {
		m.poolAllocStats.timedMap[i].statisticsTimedMapElement = statisticsTimedMapElement{}
	}

	m.poolAllocStats.Unlock()
}

func (m *MemArbitrator) alloc(x int64) {
	m.mu.Lock()

	m.doAlloc(x)

	m.mu.Unlock()
}

func (m *MemArbitrator) doAlloc(x int64) {
	m.mu.allocated += x
}

func (m *MemArbitrator) release(x int64) {
	if x <= 0 {
		return
	}
	m.alloc(-x)
}

//go:norace
func (m *MemArbitrator) allocated() int64 {
	return m.mu.allocated
}

func (m *MemArbitrator) lastBlockedAt() (allocated, utimeSec int64) {
	return m.execMu.blockedState.allocated, m.execMu.blockedState.utimeSec
}

//go:norace
func (b *blockedState) reset() {
	*b = blockedState{}
}

//go:norace
func (m *MemArbitrator) updateBlockedAt() {
	m.execMu.blockedState = blockedState{m.allocated(), m.approxUnixTimeSec()}
}

// Allocated returns the allocated mem quota of the mem-arbitrator
func (m *MemArbitrator) Allocated() int64 {
	return m.allocated()
}

// OutOfControl returns the size of the out-of-control mem
func (m *MemArbitrator) OutOfControl() int64 {
	return m.avoidance.size.Load()
}

// WaitingAllocSize returns the pending alloc mem quota of the mem-arbitrator
func (m *MemArbitrator) WaitingAllocSize() int64 {
	return m.tasks.waitingAlloc.Load()
}

// TaskNum returns the number of pending tasks in the mem-arbitrator
func (m *MemArbitrator) TaskNum() int64 {
	return m.tasks.fifoTasks.approxSize()
}

// RootPoolNum returns the number of root pools in the mem-arbitrator
func (m *MemArbitrator) RootPoolNum() int64 {
	return m.rootPoolNum.Load()
}

//go:norace
func (m *MemArbitrator) limit() int64 {
	return m.mu.limit
}

//go:norace
func (m *MemArbitrator) available() int64 {
	return min(m.heapAvailable(), m.quotaAvailable())
}

func (m *MemArbitrator) heapAvailable() int64 {
	return m.limit() - m.reservedBuffer() - m.heapController.heapAlloc.Load()
}

func (m *MemArbitrator) quotaAvailable() int64 {
	return m.limit() - m.reservedBuffer() - m.OutOfControl() - m.allocated()
}

// Limit returns the mem quota limit of the mem-arbitrator
func (m *MemArbitrator) Limit() uint64 {
	if m == nil {
		return 0
	}
	return uint64(m.limit())
}

func (m *MemArbitrator) allocateFromArbitrator(remainBytes int64) (bool, int64) {
	reclaimedBytes := int64(0)
	ok := false
	{
		m.mu.Lock()

		available := m.quotaAvailable()

		if remainBytes <= available {
			m.doAlloc(remainBytes)
			reclaimedBytes += remainBytes
			ok = true
		} else if available > 0 {
			m.doAlloc(available)
			reclaimedBytes += available
		}

		m.mu.Unlock()
	}

	return ok, reclaimedBytes
}

func (m *MemArbitrator) doReclaimMemByPriority(target *rootPoolEntry, remainBytes int64) {
	underReclaimBytes := int64(0)

	// check under canceling pool entries
	if m.underCancel.num > 0 {
		now := m.innerTime()
		for uid, entry := range m.underCancel.entries {
			ctx := &entry.arbitratorMu.underCancel
			if ctx.fail {
				continue
			}
			if deadline := ctx.startTime.Add(defKillCancelCheckTimeout); now.Compare(deadline) >= 0 {
				m.actions.Warn("Failed to `CANCEL` root pool due to timeout",
					zap.Uint64("uid", uid),
					zap.String("name", entry.pool.name),
					zap.Int64("quota-to-reclaim", ctx.reclaim),
					zap.String("mem-priority", entry.ctx.memPriority.String()),
					zap.Time("start-time", ctx.startTime),
					zap.Time("deadline", deadline),
				)
				ctx.fail = true
				continue
			}
			underReclaimBytes += ctx.reclaim
		}
	}

	// remain-bytes <= 0
	if underReclaimBytes >= remainBytes {
		return
	}

	// task whose mode is wait_averse must have been cleaned

	for prio := minArbitrationPriority; prio < target.ctx.memPriority; prio++ {
		for pos := m.entryMap.maxQuotaShardIndex - 1; pos >= m.entryMap.minQuotaShardIndexToCheck; pos-- {
			for _, entry := range m.entryMap.quotaShards[prio][pos].entries {
				if entry.arbitratorMu.underCancel.start || entry.notRunning() {
					continue
				}
				if ctx := entry.ctx.Load(); ctx.available() {
					m.execMetrics.Cancel.PriorityMode[prio]++
					ctx.stop(ArbitratorPriorityCancel)

					if m.removeTask(entry) {
						entry.windUp(0, ArbitrateFail)
					}
					m.addUnderCancel(entry, entry.arbitratorMu.quota, m.innerTime())
					underReclaimBytes += entry.arbitratorMu.quota
					if underReclaimBytes >= remainBytes {
						return
					}
				}
			}
		}
	}
}

func (m *MemArbitrator) allocateFromPrivilegedBudget(target *rootPoolEntry, remainBytes int64) (bool, int64) {
	ok := false
	if m.privilegedEntry == target {
		ok = true
	} else if m.privilegedEntry == nil && target.ctx.preferPrivilege {
		if target.intoExecPrivileged() {
			m.privilegedEntry = target
			ok = true
		} else {
			ok = false
		}
	}

	if !ok {
		return false, 0
	}

	m.alloc(remainBytes)

	return ok, remainBytes
}

func (m *MemArbitrator) ableToGC() bool {
	return m.mu.released-m.mu.lastGC >= uint64(m.poolAllocStats.SmallPoolLimit)
}

func (m *MemArbitrator) tryRuntimeGC() bool {
	if m.ableToGC() {
		m.updateTrackedHeapStats()
		m.reclaimHeap()
		return true
	}
	return false
}

// reserved buffer for arbitrate process
func (m *MemArbitrator) reservedBuffer() int64 {
	if m.execMu.mode == ArbitratorModePriority {
		return m.buffer.size.Load()
	}
	return 0
}

func (m *MemArbitrator) arbitrate(target *rootPoolEntry) (bool, int64) {
	reclaimedBytes := int64(0)
	remainBytes := target.request.quota

	onlyPrivilegedBudget := false
	for remainBytes > m.heapAvailable() {
		if !m.tryRuntimeGC() {
			onlyPrivilegedBudget = true // only could alloc from the privileged budget
			break
		}
	}

	{
		ok := false
		reclaimed := int64(0)
		if m.execMu.mode == ArbitratorModePriority {
			ok, reclaimed = m.allocateFromPrivilegedBudget(target, remainBytes)
			reclaimedBytes += reclaimed
			remainBytes -= reclaimed
		}
		if ok {
			return true, reclaimedBytes
		} else if onlyPrivilegedBudget {
			return false, reclaimedBytes
		}
	}

	for {
		ok, reclaimed := m.allocateFromArbitrator(remainBytes)
		reclaimedBytes += reclaimed
		remainBytes -= reclaimed
		if ok {
			return true, reclaimedBytes
		}
		if !m.tryRuntimeGC() {
			break
		}
	}

	return false, reclaimedBytes
}

// NewMemArbitrator creates a new mem-arbitrator heap instance
func NewMemArbitrator(limit int64, shardNum uint64, maxQuotaShardNum int, minQuotaForReclaim int64, recorder RecordMemState) *MemArbitrator {
	if limit <= 0 {
		limit = DefMaxLimit
	}
	m := &MemArbitrator{
		mode: ArbitratorModeDisable,
	}
	m.tasks.fifoTasks.init()
	for i := range m.tasks.fifoByPriority {
		m.tasks.fifoByPriority[i].init()
	}
	shardNum = nextPow2(shardNum)
	m.tasks.fifoWaitAverse.init()
	m.notifer = NewNotifer()
	m.entryMap.init(shardNum, maxQuotaShardNum, minQuotaForReclaim)
	m.doSetLimit(limit)
	m.resetStatistics()
	m.setMinHeapFreeBPS(defMinHeapFreeBPS)
	m.cleanupMu.fifoTasks.init()
	m.underKill.init()
	m.underCancel.init()
	{
		f := func(string, ...zap.Field) {}
		m.actions.Info = f
		m.actions.Warn = f
		m.actions.Error = f
	}
	{
		m.heapController.memStateRecorder.Lock()

		m.heapController.memStateRecorder.RecordMemState = recorder
		if s, err := recorder.Load(); err == nil && s != nil {
			m.heapController.memStateRecorder.lastMemState.Store(s)
			m.doSetMemMagnif(s.Magnif)
			m.poolAllocStats.mediumQuota.Store(s.PoolMediumCap)
		}

		m.heapController.memStateRecorder.Unlock()
	}
	m.resetDigestProfileCache(shardNum)
	return m
}

func (m *MemArbitrator) resetDigestProfileCache(shardNum uint64) {
	m.digestProfileCache.shards = make([]digestProfileShard, shardNum)
	m.digestProfileCache.shardsMask = shardNum - 1
	m.digestProfileCache.num.Store(0)
	m.digestProfileCache.limit = defMaxDigestProfileCacheLimit
}

// SetDigestProfileCacheLimit sets the limit of the digest profile cache
func (m *MemArbitrator) SetDigestProfileCacheLimit(limit int64) {
	m.digestProfileCache.limit = min(max(0, limit), defMax)
}

func (m *MemArbitrator) doCancelPendingTasks(prio ArbitrationPriority, waitAverse bool) (cnt int64) {
	var entries [64]*rootPoolEntry

	fifo := &m.tasks.fifoWaitAverse
	reason := ArbitratorWaitAverseCancel
	if !waitAverse {
		fifo = &m.tasks.fifoByPriority[prio]
		reason = ArbitratorStandardCancel
	}

	for {
		size := 0
		{
			m.tasks.Lock()

			for {
				entry := fifo.front()
				if entry == nil {
					break
				}
				if m.removeTaskImpl(entry) {
					entries[size] = entry
					size++
				}
				if size == len(entries) {
					break
				}
			}

			m.tasks.Unlock()
		}

		for i := range size {
			entry := entries[i]
			if ctx := entry.ctx.Load(); ctx.available() {
				ctx.stop(reason)
			}
			entry.windUp(0, ArbitrateFail)
		}

		cnt += int64(size)

		if size != len(entries) {
			break
		}
	}

	return cnt
}

func (m *MemArbitrator) doExecuteFirstTask() (exec bool) {
	if m.tasks.fifoTasks.approxEmpty() {
		return
	}

	entry := m.extractFirstTaskEntry()

	if entry == nil {
		return
	}

	if entry.arbitratorMu.destroyed {
		if m.removeTask(entry) {
			entry.windUp(0, ArbitrateFail)
		}
		return true
	}

	{
		entry.request.taskMu.Lock()

		ok, reclaimedBytes := m.arbitrate(entry)

		if ok {
			exec = true

			if m.removeTask(entry) {
				if m.execMu.mode == ArbitratorModePriority {
					m.execMetrics.Task.SuccByPriority[entry.taskMu.fifoByPriority.priority]++
				}
				m.entryMap.addQuota(entry, reclaimedBytes)
				// wind up & publish the result
				entry.windUp(reclaimedBytes, ArbitrateOk)
			} else {
				// subscription task may have been canceled
				m.release(reclaimedBytes)
			}
		} else {
			m.release(reclaimedBytes)
			m.updateBlockedAt()
			m.doReclaimByWorkMode(entry, reclaimedBytes)
		}

		entry.request.taskMu.Unlock()
	}

	return
}

func (m *MemArbitrator) doReclaimNonBlockingTasks() {
	if m.execMu.mode == ArbitratorModeStandard {
		for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
			if m.taskNumByPriority(prio) != 0 {
				m.execMetrics.Cancel.StandardMode += m.doCancelPendingTasks(prio, false)
			}
		}
	} else if m.taskNumOfWaitAverse() != 0 {
		m.execMetrics.Cancel.WaitAverse += m.doCancelPendingTasks(maxArbitrationPriority, true)
	}
}

func (m *MemArbitrator) doReclaimByWorkMode(entry *rootPoolEntry, reclaimedBytes int64) {
	waitAverse := entry.ctx.waitAverse
	m.doReclaimNonBlockingTasks()
	// entry's ctx may have been modified
	if waitAverse {
		return
	}
	if m.execMu.mode == ArbitratorModePriority {
		m.doReclaimMemByPriority(entry, entry.request.quota-reclaimedBytes)
	}
}

func (m *MemArbitrator) doExecuteCleanupTasks() {
	for {
		var entry *rootPoolEntry
		{
			m.cleanupMu.Lock()

			entry = m.cleanupMu.fifoTasks.popFront()

			m.cleanupMu.Unlock()
		}
		if entry == nil {
			break
		}

		if m.privilegedEntry == entry {
			m.privilegedEntry = nil
		}
		m.deleteUnderCancel(entry)
		m.deleteUnderKill(entry)

		if !entry.stateMu.stop.Load() { // reset pool entry
			toRelease := entry.stateMu.quotaToReclaim.Swap(0)
			if toRelease > 0 {
				m.release(toRelease)
				atomic.AddUint64(&m.mu.released, uint64(toRelease))
				m.entryMap.addQuota(entry, -toRelease)
			}
		} else {
			if !entry.arbitratorMu.destroyed {
				if entry.arbitratorMu.quota > 0 {
					m.release(entry.arbitratorMu.quota)
					atomic.AddUint64(&m.mu.released, uint64(entry.arbitratorMu.quota))
				}
				m.entryMap.delete(entry)
				m.rootPoolNum.Add(-1)
				entry.arbitratorMu.destroyed = true
			}

			if m.removeTask(entry) {
				entry.windUp(0, ArbitrateFail)
			}
		}
	}
}

func (m *MemArbitrator) implicitRun() { // satisfy any subscription task
	if m.tasks.fifoTasks.approxEmpty() {
		return
	}

	for { // make all tasks success
		entry := m.frontTaskEntry()
		if entry == nil {
			break
		}

		if entry.arbitratorMu.destroyed {
			if m.removeTask(entry) {
				entry.windUp(0, ArbitrateFail)
			}
			continue
		}

		{
			entry.request.taskMu.Lock()

			quota := entry.request.quota
			if m.removeTask(entry) {
				m.alloc(quota)
				m.entryMap.addQuota(entry, quota)
				entry.windUp(quota, ArbitrateOk)
			}

			entry.request.taskMu.Unlock()
		}
	}
}

// -1: at ArbitratorModeDisable
// -2: mem unsafe
// >= 0: execute / cancel task num
func (m *MemArbitrator) runOneRound() (taskExecNum int) {
	m.execMu.startTime = now()
	if t := m.execMu.startTime.Unix(); t != m.approxUnixTimeSec() { // update per second duration and reduce force sharing
		m.setUnixTimeSec(t)
	}

	if mode := m.workMode(); m.execMu.mode != mode {
		m.execMu.mode = mode
		if mode == ArbitratorModeDisable { // switch to disable mode
			m.setMemSafe()
			m.execMu.blockedState.reset()
		}
	}

	if !m.cleanupMu.fifoTasks.approxEmpty() {
		m.doExecuteCleanupTasks()
	}

	if m.execMu.mode == ArbitratorModeDisable {
		m.implicitRun()
		return -1
	}

	if !m.handleMemIssues() { // mem is still unsafe
		return -2
	}

	for m.doExecuteFirstTask() {
		taskExecNum++
	}

	return taskExecNum
}

func (m *MemArbitrator) asyncRun(duration time.Duration) bool {
	if m.controlMu.running.Load() {
		return false
	}
	m.controlMu.running.Store(true)
	m.controlMu.finishCh = make(chan struct{})

	go func() {
		ticker := time.NewTicker(duration)
		for m.controlMu.running.Load() {
			select {
			case <-ticker.C:
				m.weakWake()
			case <-m.notifer.C:
				m.notifer.clear()
				m.runOneRound()
			}
		}

		ticker.Stop()
		close(m.controlMu.finishCh)
	}()
	return true
}

// RestartEntryByContext starts the root pool with the given context
func (m *MemArbitrator) RestartEntryByContext(p rootPoolWrap, ctx *ArbitrationContext) bool {
	entry := p.entry
	if entry == nil {
		return false
	}
	entry.stateMu.Lock()
	defer entry.stateMu.Unlock()

	if entry.stateMu.stop.Load() || entry.execState() != execStateIdle {
		return false
	}

	entry.pool.mu.Lock()
	defer entry.pool.mu.Unlock()

	if ctx != nil {
		if ctx.waitAverse {
			entry.ctx.preferPrivilege = false
			entry.ctx.memPriority = ArbitrationPriorityHigh
		} else {
			entry.ctx.preferPrivilege = ctx.preferPrivilege
			entry.ctx.memPriority = ctx.memPriority
		}

		entry.ctx.cancelCh = ctx.cancelCh
		entry.ctx.waitAverse = ctx.waitAverse
	} else {
		entry.ctx.cancelCh = nil
		entry.ctx.waitAverse = false
		entry.ctx.memPriority = ArbitrationPriorityMedium
		entry.ctx.preferPrivilege = false
	}
	entry.ctx.canceled.Store(false)
	entry.ctx.Store(ctx)

	if _, loaded := m.entryMap.contextCache.LoadOrStore(entry.pool.uid, entry); !loaded {
		m.entryMap.contextCache.num.Add(1)
	}

	entry.pool.doSetOutOfCapacityAction(func(s OutOfCapacityActionArgs) error {
		if m.blockingAllocate(entry, s.Request) != ArbitrateOk {
			return errArbitrateFailError
		}
		return nil
	})
	entry.pool.mu.stopped = false
	entry.setExecState(execStateRunning)

	return true
}
