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
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

type memStats struct {
	HeapAlloc, HeapInuse, TotalFree, MemOffHeap, LastGC int64
}

// HandleRuntimeStats handles the runtime memory statistics
func (m *MemArbitrator) HandleRuntimeStats(s memStats) {
	// shrink fast alloc pool
	m.tryShrinkAwaitFreePool(defPoolReservedQuota, nowUnixMilli())
	// update tracked mem stats
	m.tryUpdateTrackedMemStats(nowUnixMilli())
	// set runtime mem stats & update avoidance size
	m.trySetRuntimeMemStats(s)
	m.executeTick(nowUnixMilli())
	m.weakWake()
}

func (m *MemArbitrator) tryUpdateTrackedMemStats(utimeMilli int64) bool {
	if m.avoidance.heapTracked.lastUpdateUtimeMilli.Load()+defTrackMemStatsDurMilli <= utimeMilli {
		m.updateTrackedHeapStats()
		return true
	}
	return false
}

func (m *MemArbitrator) updateTrackedHeapStats() {
	totalTrackedHeap := int64(0)
	if m.entryMap.contextCache.num.Load() != 0 {
		maxMemUsed := int64(0)
		m.entryMap.contextCache.Range(func(_, value any) bool {
			e := value.(*rootPoolEntry)
			if e.notRunning() {
				return true
			}
			if ctx := e.ctx.Load(); ctx.available() {
				if memUsed := ctx.arbitrateHelper.HeapInuse(); memUsed > 0 {
					totalTrackedHeap += memUsed
					maxMemUsed = max(maxMemUsed, memUsed)
				}
			}
			return true
		})
		if m.buffer.size.Load() < maxMemUsed {
			m.tryToUpdateBuffer(maxMemUsed, m.approxUnixTimeSec())
		}
	}

	totalTrackedHeap += m.awaitFreePoolUsed().trackedHeap
	m.avoidance.heapTracked.Store(totalTrackedHeap)
	m.avoidance.heapTracked.lastUpdateUtimeMilli.Store(nowUnixMilli())
}

func (m *MemArbitrator) tryShrinkAwaitFreePool(minRemain int64, utimeMilli int64) bool {
	if m.awaitFree.lastShrinkUtimeMilli.Load()+defAwaitFreePoolShrinkDurMilli <= utimeMilli {
		m.shrinkAwaitFreePool(minRemain, utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) shrinkAwaitFreePool(minRemain int64, utimeMilli int64) {
	if m.awaitFree.pool.allocated() <= 0 {
		return
	}

	poolReleased := int64(0)
	reclaimed := int64(0)

	for i := range m.awaitFree.budget.shards {
		b := &m.awaitFree.budget.shards[i]

		if used := b.Used.Load(); used > 0 {
			if b.approxCapacity()-(used+minRemain) >= b.Pool.allocAlignSize && b.TryLock() {
				if used = b.Used.Load(); used > 0 {
					toReclaim := b.Capacity - (used + minRemain)

					if toReclaim >= b.Pool.allocAlignSize {
						b.Capacity -= toReclaim
						reclaimed += toReclaim
					}
				}
				b.Unlock()
			}
		} else {
			if b.approxCapacity() > 0 && b.getLastUsedTimeSec()*kilo+defAwaitFreePoolShrinkDurMilli <= utimeMilli && b.TryLock() {
				if toReclaim := b.Capacity; b.Used.Load() <= 0 && toReclaim > 0 {
					b.Capacity -= toReclaim
					reclaimed += toReclaim
				}
				b.Unlock()
			}
		}
	}

	if reclaimed > 0 {
		poolReleased = m.awaitFree.pool.releasePopBudget(reclaimed)
	}
	if poolReleased > 0 {
		m.release(poolReleased)
		atomic.AddUint64(&m.mu.released, uint64(poolReleased))
		atomic.AddInt64(&m.execMetrics.AwaitFree.Shrink, 1)
		m.weakWake()
	}

	m.awaitFree.lastShrinkUtimeMilli.Store(utimeMilli)
}

func (m *MemArbitrator) isMemSafe() bool {
	return m.heapController.memInuse.Load() < m.oomRisk()
}

func (m *MemArbitrator) isMemNoRisk() bool {
	return m.isMemSafe() && m.heapController.heapAlloc.Load() < m.memRisk()
}

func (m *MemArbitrator) calcMemRisk() *RuntimeMemStateV1 {
	if m.mu.softLimit.mode != SoftLimitModeAuto {
		return nil
	}

	memState := RuntimeMemStateV1{
		Version: 1,
		LastRisk: LastRisk{
			HeapAlloc:  m.heapController.heapAlloc.Load(),
			QuotaAlloc: m.allocated(),
		},

		PoolMediumCap: m.poolMediumQuota(),
	}

	if memState.LastRisk.QuotaAlloc == 0 || memState.LastRisk.HeapAlloc <= memState.LastRisk.QuotaAlloc {
		return nil
	}
	memState.Magnif = calcRatio(memState.LastRisk.HeapAlloc, memState.LastRisk.QuotaAlloc) + 100 /* 10 percent */
	if p := m.lastMemState(); p != nil {
		memState.Magnif = max(memState.Magnif, p.Magnif)
	}

	return &memState
}

// return `true` is memory state is safe
func (m *MemArbitrator) handleMemIssues() (isSafe bool) {
	if m.atMemRisk() {
		gcExecuted := m.tryRuntimeGC()
		if !gcExecuted {
			m.refreshRuntimeMemStats()
		}

		if m.isMemNoRisk() {
			m.updateTrackedHeapStats()
			m.updateAvoidSize() // no need to refresh runtime mem stats

			{ // warning
				profile := m.recordDebugProfile()
				m.actions.Info("Memory is safe", profile.fields[:profile.n]...)
			}
			m.setMemSafe()
			return true
		}

		m.doReclaimNonBlockingTasks()
		m.handleMemRisk(gcExecuted)
		return false
	} else if !m.isMemSafe() {
		m.doReclaimNonBlockingTasks()
		m.intoMemRisk()
		return false
	}
	return true
}

func (*MemArbitrator) innerTime() time.Time {
	if intest.InTest {
		if mockNow != nil {
			return mockNow()
		}
	}
	return now()
}

func (m *MemArbitrator) handleMemRisk(gcExecuted bool) {
	now := m.innerTime()
	oomRisk := m.heapController.memInuse.Load() > m.limit()
	dur := now.Sub(m.heapController.memRisk.lastMemStats.startTime)
	if !oomRisk && dur < defHeapReclaimCheckDuration {
		return
	}
	heapUseBPS := int64(0)

	if dur > 0 {
		heapFrees := m.heapController.heapTotalFree.Load() - m.heapController.memRisk.lastMemStats.heapTotalFree
		heapUseBPS = int64(float64(heapFrees) / dur.Seconds())
	}
	if oomRisk || memHangRisk(heapUseBPS, m.minHeapFreeBPS(), now, m.heapController.memRisk.startTime.t) {
		m.intoOOMRisk()

		memToReclaim := m.heapController.memInuse.Load() - m.memRisk()

		{ // warning
			profile := m.recordDebugProfile()
			profile.append(
				zap.Float64("heap-use-speed(MiB/s)", float64(heapUseBPS*100/byteSizeMB)/100),
				zap.Float64("required-speed(MiB/s)", float64(m.minHeapFreeBPS())/float64(byteSizeMB)),
				zap.Int64("quota-to-reclaim", max(0, memToReclaim)),
			)
			m.actions.Warn("`OOM RISK`: try to `KILL` running root pool", profile.fields[:profile.n]...)
		}

		if newKillNum, reclaiming := m.killTopnEntry(memToReclaim); newKillNum != 0 {
			m.heapController.memRisk.startTime.t = m.innerTime() // restart oom check
			m.heapController.memRisk.startTime.unixMilli.Store(m.heapController.memRisk.startTime.t.UnixMilli())

			{ // warning
				profile := m.recordDebugProfile()
				profile.append(
					zap.Int64("pool-under-kill-num", m.underKill.num),
					zap.Int("new-kill-num", newKillNum),
					zap.Int64("quota-under-reclaim", reclaiming),
					zap.Int64("rest-quota-to-reclaim", max(0, memToReclaim-reclaiming)),
				)
				m.actions.Warn("Restart runtime memory check", profile.fields[:profile.n]...)
			}
		} else {
			underKillNum := 0
			for _, entry := range m.underKill.entries {
				if !entry.arbitratorMu.underKill.fail {
					underKillNum++
				}
			}
			if underKillNum == 0 {
				forceKill := 0
				for { // make all tasks success
					entry := m.frontTaskEntry()
					if entry == nil {
						break
					}
					// force kill
					if ctx := entry.ctx.Load(); ctx.available() {
						ctx.stop(ArbitratorOOMRiskKill)
						m.execMetrics.Risk.OOMKill[entry.ctx.memPriority]++
						forceKill++
						if m.removeTask(entry) {
							entry.windUp(0, ArbitrateFail)
						}
					}
				}
				if forceKill != 0 {
					profile := m.recordDebugProfile()
					profile.append(
						zap.Int("kill-awaiting-num", forceKill),
						zap.Int64("pool-under-kill-num", m.underKill.num),
						zap.Int64("quota-under-reclaim", reclaiming),
						zap.Int64("rest-quota-to-reclaim", max(0, memToReclaim-reclaiming)),
					)
					m.actions.Warn("No more running root pool can be killed to resolve `OOM RISK`; KILL all awaiting tasks;",
						profile.fields[:profile.n]...,
					)
				} else {
					profile := m.recordDebugProfile()
					profile.append(
						zap.Int64("pool-under-kill-num", m.underKill.num),
						zap.Int64("quota-under-reclaim", reclaiming),
						zap.Int64("rest-quota-to-reclaim", max(0, memToReclaim-reclaiming)),
					)
					m.actions.Warn("No more running root pool or awaiting task can be terminated to resolve `OOM RISK`",
						profile.fields[:profile.n]...,
					)
				}
			}
		}
	} else {
		{ // warning
			profile := m.recordDebugProfile()
			profile.append(zap.Float64("heap-use-speed(MiB/s)",
				float64(heapUseBPS*100/byteSizeMB)/100),
				zap.Float64("required-speed(MiB/s)", float64(m.minHeapFreeBPS())/float64(byteSizeMB)))
			m.actions.Warn("Runtime memory free speed meets require, start re-check", profile.fields[:profile.n]...)
		}
	}

	if dur >= defHeapReclaimCheckDuration {
		m.heapController.memRisk.lastMemStats.heapTotalFree = m.heapController.heapTotalFree.Load()
		m.heapController.memRisk.lastMemStats.startTime = m.innerTime()
	}

	if !gcExecuted {
		m.gc()
	}
}

func memHangRisk(freeSpeedBPS, minHeapFreeSpeedBPS int64, now, startTime time.Time) bool {
	return freeSpeedBPS < minHeapFreeSpeedBPS || now.Sub(startTime) > defHeapReclaimCheckMaxDuration
}

func (m *MemArbitrator) killTopnEntry(required int64) (newKillNum int, reclaimed int64) {
	if m.underKill.num > 0 {
		now := m.innerTime()
		for uid, entry := range m.underKill.entries {
			ctx := &entry.arbitratorMu.underKill
			if ctx.fail {
				continue
			}
			if deadline := ctx.startTime.Add(defKillCancelCheckTimeout); now.Compare(deadline) >= 0 {
				m.actions.Error("Failed to `KILL` root pool due to timeout",
					zap.Uint64("uid", uid),
					zap.String("name", entry.pool.name),
					zap.Int64("mem-to-reclaim", ctx.reclaim),
					zap.String("mem-priority", entry.ctx.memPriority.String()),
					zap.Time("start-time", ctx.startTime),
					zap.Time("deadline", deadline),
				)
				ctx.fail = true
				continue
			}
			reclaimed += ctx.reclaim
		}
	}

	if reclaimed >= required {
		return
	}

	for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
		for pos := m.entryMap.maxQuotaShardIndex - 1; pos >= m.entryMap.minQuotaShardIndexToCheck; pos-- {
			for uid, entry := range m.entryMap.quotaShards[prio][pos].entries {
				if entry.arbitratorMu.underKill.start || entry.notRunning() {
					continue
				}

				if ctx := entry.ctx.Load(); ctx.available() {
					memoryUsed := ctx.arbitrateHelper.HeapInuse()

					if memoryUsed <= 0 {
						continue
					}

					m.addUnderKill(entry, memoryUsed, m.innerTime())
					reclaimed += memoryUsed
					ctx.stop(ArbitratorOOMRiskKill)
					newKillNum++
					m.execMetrics.Risk.OOMKill[prio]++

					{ // warning
						m.actions.Warn("Start to `KILL` root pool",
							zap.Uint64("uid", uid),
							zap.String("name", entry.pool.name),
							zap.Int64("mem-used", memoryUsed),
							zap.String("mem-priority", ctx.memPriority.String()),
							zap.Int64("rest-to-reclaim", max(0, required-reclaimed)))
					}
					if m.removeTask(entry) {
						{ // warning
							m.actions.Warn("Make the mem quota subscription failed",
								zap.Uint64("uid", uid), zap.String("name", entry.pool.name))
						}
						entry.windUp(0, ArbitrateFail)
					}

					if reclaimed >= required {
						return
					}
				}
			}
		}
	}

	return
}

// LastRisk represents the last risk state of memory
type LastRisk struct {
	HeapAlloc  int64 `json:"heap"`
	QuotaAlloc int64 `json:"quota"`
}

// RuntimeMemStateV1 represents the runtime memory state
type RuntimeMemStateV1 struct {
	Version  int64    `json:"version"`
	LastRisk LastRisk `json:"last-risk"`
	// magnification ratio of heap-alloc/quota
	Magnif int64 `json:"magnif"`
	// medium quota usage of root pools
	PoolMediumCap int64 `json:"pool-medium-cap"`

	// TODO: top-n profiles by digest
	// topNProfiles [3][2]int64 `json:"top-n-profiles"`
}

func (m *MemArbitrator) recordMemState(s *RuntimeMemStateV1, reason string) error {
	m.heapController.memStateRecorder.Lock()
	defer m.heapController.memStateRecorder.Unlock()
	m.heapController.memStateRecorder.lastMemState.Store(s)

	if err := m.heapController.memStateRecorder.Store(s); err != nil {
		m.execMetrics.Action.RecordMemState.Fail++
		return err
	}
	m.execMetrics.Action.RecordMemState.Succ++
	m.actions.Info("Record mem state",
		zap.String("reason", reason),
		zap.String("data", fmt.Sprintf("%+v", s)),
	)
	return nil
}

// GetAwaitFreeBudgets returns the concurrent budget shard by the given uid
func (m *MemArbitrator) GetAwaitFreeBudgets(uid uint64) *TrackedConcurrentBudget {
	index := shardIndexByUID(uid, m.awaitFree.budget.sizeMask)
	return &m.awaitFree.budget.shards[index]
}

func (m *MemArbitrator) initAwaitFreePool(allocAlignSize, shardNum int64) {
	if allocAlignSize <= 0 {
		allocAlignSize = defAwaitFreePoolAllocAlignSize
	}

	p := &ResourcePool{
		name:           "awaitfree-pool",
		uid:            0,
		limit:          DefMaxLimit,
		allocAlignSize: allocAlignSize,

		maxUnusedBlocks: 0,
	}

	p.SetOutOfCapacityAction(func(s OutOfCapacityActionArgs) error {
		if m.heapController.heapAlloc.Load() > m.oomRisk()-s.Request ||
			m.allocated() > m.limit()-m.OutOfControl()-s.Request {
			m.updateBlockedAt()
			m.execMetrics.AwaitFree.Fail++
			return errArbitrateFailError
		}

		m.alloc(s.Request)
		p.forceAddCap(s.Request)
		m.execMetrics.AwaitFree.Succ++

		return nil
	})

	m.awaitFree.pool = p

	{
		cnt := nextPow2(uint64(shardNum))
		m.awaitFree.budget.shards = make([]TrackedConcurrentBudget, cnt)
		m.awaitFree.budget.sizeMask = cnt - 1
		for i := range m.awaitFree.budget.shards {
			m.awaitFree.budget.shards[i].Pool = p
		}
	}
}

// ArbitratorStopReason represents the reason why the arbitrate helper will be stopped
type ArbitratorStopReason int

// ArbitrateHelperReason values
const (
	ArbitratorOOMRiskKill ArbitratorStopReason = iota
	ArbitratorWaitAverseCancel
	ArbitratorStandardCancel
	ArbitratorPriorityCancel
)

// String returns the string representation of the ArbitratorStopReason
func (r ArbitratorStopReason) String() (desc string) {
	switch r {
	case ArbitratorOOMRiskKill:
		desc = "KILL(out-of-memory)"
	case ArbitratorWaitAverseCancel:
		desc = "CANCEL(out-of-quota & wait-averse)"
	case ArbitratorStandardCancel:
		desc = "CANCEL(out-of-quota & standard-mode)"
	case ArbitratorPriorityCancel:
		desc = "CANCEL(out-of-quota & priority-mode)"
	default:
		desc = "UNKNOWN"
	}
	return
}

// ArbitrateHelper is an interface for the arbitrate helper
type ArbitrateHelper interface {
	Stop(ArbitratorStopReason) bool // kill by arbitrator only when meeting oom risk; cancel by arbitrator;
	HeapInuse() int64               // track heap usage
	Finish()
}

// ArbitrationContext represents the context & properties of the root pool which is accessible for the global mem-arbitrator
type ArbitrationContext struct {
	arbitrateHelper ArbitrateHelper
	cancelCh        <-chan struct{}
	memPriority     ArbitrationPriority
	stopped         atomic.Bool
	waitAverse      bool
	preferPrivilege bool
}

func (ctx *ArbitrationContext) available() bool {
	if ctx != nil && ctx.arbitrateHelper != nil && !ctx.stopped.Load() {
		return true
	}
	return false
}

func (ctx *ArbitrationContext) stop(reason ArbitratorStopReason) {
	if ctx.stopped.Swap(true) {
		return
	}
	ctx.arbitrateHelper.Stop(reason)
}

// NewArbitrationContext creates a new arbitration context
func NewArbitrationContext(
	cancelCh <-chan struct{},
	arbitrateHelper ArbitrateHelper,
	memPriority ArbitrationPriority,
	waitAverse bool,
	preferPrivilege bool,
) *ArbitrationContext {
	return &ArbitrationContext{
		cancelCh:        cancelCh,
		arbitrateHelper: arbitrateHelper,
		memPriority:     memPriority,
		waitAverse:      waitAverse,
		preferPrivilege: preferPrivilege,
	}
}

// NumByPattern represents the number of tasks by 4 pattern: priority(low, medium, high), wait-averse
type NumByPattern [maxArbitrateMode]int64

// TaskNumByPattern returns the number of tasks by pattern and there may be overlap
func (m *MemArbitrator) TaskNumByPattern() (res NumByPattern) {
	for i := minArbitrationPriority; i < maxArbitrationPriority; i++ {
		res[i] = m.taskNumByPriority(i)
	}
	res[ArbitrationWaitAverse] = m.taskNumOfWaitAverse()
	return
}

// ConsumeQuotaFromAwaitFreePool consumes quota from the awaitfree-pool by the given uid
func (m *MemArbitrator) ConsumeQuotaFromAwaitFreePool(uid uint64, req int64) bool {
	return m.GetAwaitFreeBudgets(uid).ConsumeQuota(m.approxUnixTimeSec(), req) == nil
}

// ConsumeQuota consumes quota from the concurrent budget
// req > 0: alloc quota; try to pull from upstream;
// req <= 0: release quota
func (b *ConcurrentBudget) ConsumeQuota(utimeSec int64, req int64) error {
	if req > 0 {
		if b.getLastUsedTimeSec() != utimeSec {
			b.setLastUsedTimeSec(utimeSec)
		}
		if b.Used.Add(req) > b.approxCapacity() {
			if err := b.PullFromUpstream(); err != nil {
				return err
			}
		}
	} else {
		b.Used.Add(req)
	}
	return nil
}

// ReportHeapInuseToAwaitFreePool reports the heap inuse to the awaitfree-pool by the given uid
func (m *MemArbitrator) ReportHeapInuseToAwaitFreePool(uid uint64, req int64) {
	m.GetAwaitFreeBudgets(uid).ReportHeapInuse(req)
}

// ReportHeapInuse reports the heap inuse to the concurrent budget
// req > 0: consume
// req < 0: release
func (b *TrackedConcurrentBudget) ReportHeapInuse(req int64) {
	b.HeapInuse.Add(req)
}

func (m *MemArbitrator) stop() bool {
	m.controlMu.Lock()
	defer m.controlMu.Unlock()

	if !m.controlMu.running.Load() {
		return false
	}

	m.controlMu.running.Store(false)
	m.wake()

	<-m.controlMu.finishCh

	m.runOneRound()

	return true
}

// AtMemRisk checks if the memory is under risk
func (m *MemArbitrator) AtMemRisk() bool {
	return m.atMemRisk()
}

// AtOOMRisk checks if the memory is under risk
//
//go:norace
func (m *MemArbitrator) AtOOMRisk() bool {
	return m.heapController.memRisk.oomRisk
}

func (m *MemArbitrator) atMemRisk() bool {
	return m.heapController.memRisk.startTime.unixMilli.Load() != 0
}

func (m *MemArbitrator) intoOOMRisk() {
	m.heapController.memRisk.oomRisk = true
	m.execMetrics.Risk.OOM++
}

//go:norace
func (m *MemArbitrator) oomRisk() int64 {
	return m.mu.threshold.oomRisk
}

//go:norace
func (m *MemArbitrator) memRisk() int64 {
	return m.mu.threshold.risk
}

func (m *MemArbitrator) intoMemRisk() {
	now := m.innerTime()
	m.heapController.memRisk.startTime.t = now
	m.heapController.memRisk.startTime.unixMilli.Store(now.UnixMilli())
	m.heapController.memRisk.lastMemStats.heapTotalFree = m.heapController.heapTotalFree.Load()
	m.heapController.memRisk.lastMemStats.startTime = now
	m.execMetrics.Risk.Mem++

	{
		profile := m.recordDebugProfile()
		profile.append(zap.Int64("threshold", m.mu.threshold.oomRisk))
		m.actions.Warn("Memory inuse reach threshold", profile.fields[:profile.n]...)
	}

	{ // GC
		m.reclaimHeap()
	}

	if memState := m.calcMemRisk(); memState != nil {
		if memState.Magnif > defMaxMagnif {
			// There may be extreme memory leak issues. It's recommended to set soft limit manually.
			m.actions.Warn("Memory pressure is abnormally high",
				zap.Int64("mem-magnification-ratio(‰)", memState.Magnif),
				zap.Int64("upper-limit-ratio(‰)", defMaxMagnif))
			memState.Magnif = defMaxMagnif
		}
		{
			m.avoidance.memMagnif.Lock()

			m.doSetMemMagnif(memState.Magnif)

			m.avoidance.memMagnif.Unlock()
		}

		if err := m.recordMemState(memState, "oom risk"); err != nil {
			m.actions.Error("Failed to save mem-risk", zap.Error(err))
		}
	}

	if m.isMemNoRisk() {
		m.wake()
	}
}

func (m *MemArbitrator) setMemSafe() {
	m.heapController.memRisk.startTime.unixMilli.Store(0)
	m.heapController.memRisk.oomRisk = false
}

//go:norace
func (m *MemArbitrator) setUnixTimeSec(s int64) {
	m.UnixTimeSec = s
}

func (m *MemArbitrator) approxUnixTimeSec() int64 {
	return m.UnixTimeSec
}
