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

package ddl

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	tikv "github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	pdHttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (w *worker) executeDistTask(jobCtx *jobContext, t table.Table, reorgInfo *reorgInfo) error {
	stepCtx := jobCtx.stepCtx
	taskType := proto.Backfill
	tkBuilder := NewTaskKeyBuilder().
		SetMultiSchema(reorgInfo.Job.MultiSchemaInfo).
		SetMergeTempIndex(reorgInfo.mergingTmpIdx)
	taskKey := tkBuilder.Build(reorgInfo.Job.ID)
	g, ctx := errgroup.WithContext(w.workCtx)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)

	done := make(chan struct{})
	// For resuming add index task.
	// Need to fetch task by taskKey in tidb_global_task and tidb_global_task_history tables.
	// When pausing the related ddl job, it is possible that the task with taskKey is succeed and in tidb_global_task_history.
	// As a result, when resuming the related ddl job,
	// it is necessary to check task exits in tidb_global_task and tidb_global_task_history tables.
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return err
	}

	var (
		taskID                                              int64
		lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed int
	)
	if task != nil {
		// It's possible that the task state is succeed but the ddl job is paused.
		// When task in succeed state, we can skip the dist task execution/scheduling process.
		if task.State == proto.TaskStateSucceed {
			w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			logutil.DDLLogger().Info(
				"task succeed, start to resume the ddl job",
				zap.String("task-key", taskKey))
			return nil
		}
		taskMeta := &BackfillTaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
			return errors.Trace(err)
		}
		taskID = task.ID
		lastRequiredSlots = task.RequiredSlots
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()
		g.Go(func() error {
			defer close(done)
			backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
			err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logutil.DDLLogger(),
				func(context.Context) (bool, error) {
					return true, handle.ResumeTask(w.workCtx, taskKey)
				},
			)
			if err != nil {
				return err
			}
			err = handle.WaitTaskDoneOrPaused(ctx, task.ID)
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	} else {
		job := reorgInfo.Job
		workerCntLimit := job.ReorgMeta.GetConcurrency()
		requiredSlots, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			return err
		}
		logutil.DDLLogger().Info("adjusted add-index task required slots",
			zap.Int("worker-cnt", workerCntLimit), zap.Int("required-slots", requiredSlots),
			zap.String("task-key", taskKey))
		rowSize := estimateTableRowSize(w.workCtx, w.store, w.sess.GetRestrictedSQLExecutor(), t)
		taskMeta := &BackfillTaskMeta{
			Job:             *job.Clone(),
			EleIDs:          extractElemIDs(reorgInfo),
			EleTypeKey:      reorgInfo.currElement.TypeKey,
			CloudStorageURI: w.jobContext(job.ID, job.ReorgMeta).cloudStorageURI,
			MergeTempIndex:  reorgInfo.mergingTmpIdx,
			EstimateRowSize: rowSize,
			Version:         BackfillTaskMetaVersion1,
		}

		metaData, err := json.Marshal(taskMeta)
		if err != nil {
			return err
		}

		targetScope := reorgInfo.ReorgMeta.TargetScope
		maxNodeCnt := reorgInfo.ReorgMeta.MaxNodeCount
		task, err := handle.SubmitTask(ctx, taskKey, taskType, w.store.GetKeyspace(), requiredSlots, targetScope, maxNodeCnt, metaData)
		if err != nil {
			return err
		}

		taskID = task.ID
		lastRequiredSlots = requiredSlots
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()

		g.Go(func() error {
			defer close(done)
			err := handle.WaitTaskDoneOrPaused(ctx, task.ID)
			failpoint.InjectCall("pauseAfterDistTaskFinished")
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	}

	g.Go(func() error {
		checkFinishTk := time.NewTicker(CheckBackfillJobFinishInterval)
		defer checkFinishTk.Stop()
		updateRowCntTk := time.NewTicker(UpdateBackfillJobRowCountInterval)
		defer updateRowCntTk.Stop()
		for {
			select {
			case <-done:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
				err := w.checkRunnableOrHandlePauseOrCanceled(stepCtx, taskKey)
				return errors.Trace(err)
			case <-checkFinishTk.C:
				err := w.checkRunnableOrHandlePauseOrCanceled(stepCtx, taskKey)
				if err != nil {
					return errors.Trace(err)
				}
			case <-updateRowCntTk.C:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			}
		}
	})

	g.Go(func() error {
		modifyTaskParamLoop(ctx, jobCtx.sysTblMgr, taskManager, done,
			reorgInfo.Job.ID, taskID, lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed)
		return nil
	})

	err = g.Wait()
	return err
}

func (w *worker) checkRunnableOrHandlePauseOrCanceled(stepCtx context.Context, taskKey string) (err error) {
	if err = w.isReorgRunnable(stepCtx, true); err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			if err = handle.PauseTask(w.workCtx, taskKey); err != nil {
				logutil.DDLLogger().Warn("pause task error", zap.String("task_key", taskKey), zap.Error(err))
				return nil
			}
			failpoint.InjectCall("syncDDLTaskPause")
		}
		if !dbterror.ErrCancelledDDLJob.Equal(err) {
			return errors.Trace(err)
		}
		if err = handle.CancelTask(w.workCtx, taskKey); err != nil {
			logutil.DDLLogger().Warn("cancel task error", zap.String("task_key", taskKey), zap.Error(err))
			return nil
		}
	}
	return nil
}

// Note: we can achieve the same effect by calling ModifyTaskByID directly inside
// the process of 'ADMIN ALTER DDL JOB xxx', so we can eliminate the goroutine,
// but if the task hasn't been created we need to make sure the task is created
// with config after ALTER DDL JOB is executed. A possible solution is to make
// the DXF task submission and 'ADMIN ALTER DDL JOB xxx' txn conflict with each
// other when they overlap in time, by modify the job at the same time when submit
// task, as we are using optimistic txn. But this will cause WRITE CONFLICT with
// outer txn in transitOneJobStep.
func modifyTaskParamLoop(
	ctx context.Context,
	sysTblMgr systable.Manager,
	taskManager storage.Manager,
	done chan struct{},
	jobID, taskID int64,
	lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed int,
) {
	logger := logutil.DDLLogger().With(zap.Int64("jobID", jobID), zap.Int64("taskID", taskID))
	ticker := time.NewTicker(UpdateDDLJobReorgCfgInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}

		latestJob, err := sysTblMgr.GetJobByID(ctx, jobID)
		if err != nil {
			if goerrors.Is(err, systable.ErrNotFound) {
				logger.Info("job not found, might already finished")
				return
			}
			logger.Error("get job failed, will retry later", zap.Error(err))
			continue
		}

		modifies := make([]proto.Modification, 0, 3)
		workerCntLimit := latestJob.ReorgMeta.GetConcurrency()
		requiredSlots, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			logger.Error("adjust required slots failed", zap.Error(err))
			continue
		}
		if requiredSlots != lastRequiredSlots {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyRequiredSlots,
				To:   int64(requiredSlots),
			})
		}
		batchSize := latestJob.ReorgMeta.GetBatchSize()
		if batchSize != lastBatchSize {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyBatchSize,
				To:   int64(batchSize),
			})
		}
		maxWriteSpeed := latestJob.ReorgMeta.GetMaxWriteSpeed()
		if maxWriteSpeed != lastMaxWriteSpeed {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyMaxWriteSpeed,
				To:   int64(maxWriteSpeed),
			})
		}
		if len(modifies) == 0 {
			continue
		}
		currTask, err := taskManager.GetTaskByID(ctx, taskID)
		if err != nil {
			if goerrors.Is(err, storage.ErrTaskNotFound) {
				logger.Info("task not found, might already finished")
				return
			}
			logger.Error("get task failed, will retry later", zap.Error(err))
			continue
		}
		if !currTask.State.CanMoveToModifying() {
			// user might modify param again while another modify is ongoing.
			logger.Info("task state is not suitable for modifying, will retry later",
				zap.String("state", currTask.State.String()))
			continue
		}
		if err = taskManager.ModifyTaskByID(ctx, taskID, &proto.ModifyParam{
			PrevState:     currTask.State,
			Modifications: modifies,
		}); err != nil {
			logger.Error("modify task failed", zap.Error(err))
			continue
		}
		logger.Info("modify task success",
			zap.Int("oldRequiredSlots", lastRequiredSlots), zap.Int("newRequiredSlots", requiredSlots),
			zap.Int("oldBatchSize", lastBatchSize), zap.Int("newBatchSize", batchSize),
			zap.String("oldMaxWriteSpeed", units.HumanSize(float64(lastMaxWriteSpeed))),
			zap.String("newMaxWriteSpeed", units.HumanSize(float64(maxWriteSpeed))),
		)
		lastRequiredSlots = requiredSlots
		lastBatchSize = batchSize
		lastMaxWriteSpeed = maxWriteSpeed
	}
}

func adjustConcurrency(ctx context.Context, taskMgr storage.Manager, workerCnt int) (int, error) {
	cpuCount, err := taskMgr.GetCPUCountOfNode(ctx)
	if err != nil {
		return 0, err
	}
	return min(workerCnt, cpuCount), nil
}

// EstimateTableRowSizeForTest is used for test.
var EstimateTableRowSizeForTest = estimateTableRowSize

// estimateTableRowSize estimates the row size in bytes of a table.
// This function tries to retrieve row size in following orders:
//  1. AVG_ROW_LENGTH column from information_schema.tables.
//  2. region info's approximate key size / key number.
func estimateTableRowSize(
	ctx context.Context,
	store kv.Storage,
	exec sqlexec.RestrictedSQLExecutor,
	tbl table.Table,
) (sizeInBytes int) {
	defer util.Recover(metrics.LabelDDL, "estimateTableRowSize", nil, false)
	var gErr error
	defer func() {
		tidblogutil.Logger(ctx).Info("estimate row size",
			zap.Int64("tableID", tbl.Meta().ID), zap.Int("size", sizeInBytes), zap.Error(gErr))
	}()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		"select AVG_ROW_LENGTH from information_schema.tables where TIDB_TABLE_ID = %?", tbl.Meta().ID)
	if err != nil {
		gErr = err
		return 0
	}
	if len(rows) == 0 {
		gErr = errors.New("no average row data")
		return 0
	}
	avgRowSize := rows[0].GetInt64(0)
	if avgRowSize != 0 {
		return int(avgRowSize)
	}
	regionRowSize, err := estimateRowSizeFromRegion(ctx, store, tbl)
	if err != nil {
		gErr = err
		return 0
	}
	return regionRowSize
}

func estimateRowSizeFromRegion(ctx context.Context, store kv.Storage, tbl table.Table) (int, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return 0, fmt.Errorf("not a helper.Storage")
	}
	h := &helper.Helper{
		Store:       hStore,
		RegionCache: hStore.GetRegionCache(),
	}
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return 0, err
	}
	pid := tbl.Meta().ID
	sk, ek := tablecodec.GetTableHandleKeyRange(pid)
	start, end := hStore.GetCodec().EncodeRegionRange(sk, ek)
	// We use the second region to prevent the influence of the front and back tables.
	regionLimit := 3
	regionInfos, err := pdCli.GetRegionsByKeyRange(ctx, pdHttp.NewKeyRange(start, end), regionLimit)
	if err != nil {
		return 0, err
	}
	if len(regionInfos.Regions) != regionLimit {
		return 0, fmt.Errorf("less than 3 regions")
	}
	sample := regionInfos.Regions[1]
	if sample.ApproximateKeys == 0 || sample.ApproximateSize == 0 {
		return 0, fmt.Errorf("zero approximate size")
	}
	return int(uint64(sample.ApproximateSize)*size.MB) / int(sample.ApproximateKeys), nil
}

func (w *worker) updateDistTaskRowCount(taskKey string, jobID int64) {
	taskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task manager", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	task, err := taskMgr.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	rowCount, err := taskMgr.GetSubtaskRowCount(w.workCtx, task.ID, proto.BackfillStepReadIndex)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get subtask row count", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	w.getReorgCtx(jobID).setRowCount(rowCount)
}

func getNextPartitionInfo(reorg *reorgInfo, t table.PartitionedTable, currPhysicalTableID int64) (pid int64, startKey, endKey kv.Key, err error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return 0, nil, nil, nil
	}

	// This will be used in multiple different scenarios/ALTER TABLE:
	// ADD INDEX - no change in partitions, just use pi.Definitions (1)
	// REORGANIZE PARTITION - copy data from partitions to be dropped (2)
	// REORGANIZE PARTITION - (re)create indexes on partitions to be added (3)
	// REORGANIZE PARTITION - Update new Global indexes with data from non-touched partitions (4)
	// (i.e. pi.Definitions - pi.DroppingDefinitions)
	// MODIFY COLUMN - no change in partitions, just use pi.Definitions (5)
	if bytes.Equal(reorg.currElement.TypeKey, meta.IndexElementKey) {
		// case 1, 3 or 4
		if len(pi.AddingDefinitions) == 0 {
			// case 1
			// Simply AddIndex, without any partitions added or dropped!
			if reorg.mergingTmpIdx && currPhysicalTableID == t.Meta().ID {
				// If the current Physical id is the table id,
				// 1. All indexes are global index, the next Physical id should be the first partition id.
				// 2. Not all indexes are global index, return 0.
				allGlobal := true
				for _, element := range reorg.elements {
					if !bytes.Equal(element.TypeKey, meta.IndexElementKey) {
						allGlobal = false
						break
					}
					idxInfo := model.FindIndexInfoByID(t.Meta().Indices, element.ID)
					if !idxInfo.Global {
						allGlobal = false
						break
					}
				}
				if allGlobal {
					pid = 0
				} else {
					pid = pi.Definitions[0].ID
				}
			} else {
				pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
			}
		} else {
			// case 3 (or if not found AddingDefinitions; 4)
			// check if recreating Global Index (during Reorg Partition)
			pid, err = findNextPartitionID(currPhysicalTableID, pi.AddingDefinitions)
			if err != nil {
				// case 4
				// Not a partition in the AddingDefinitions, so it must be an existing
				// non-touched partition, i.e. recreating Global Index for the non-touched partitions
				pid, err = findNextNonTouchedPartitionID(currPhysicalTableID, pi)
			}
		}
	} else if len(pi.DroppingDefinitions) == 0 {
		// case 5
		pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
	} else {
		// case 2
		pid, err = findNextPartitionID(currPhysicalTableID, pi.DroppingDefinitions)
	}
	if err != nil {
		// Fatal error, should not run here.
		logutil.DDLLogger().Error("find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return 0, nil, nil, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return 0, nil, nil, nil
	}

	failpoint.Inject("mockUpdateCachedSafePoint", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			ts := oracle.GoTimeToTS(time.Now())
			//nolint:forcetypeassert
			s := reorg.jobCtx.store.(tikv.Storage)
			s.UpdateTxnSafePointCache(ts, time.Now())
			time.Sleep(time.Second * 3)
		}
	})

	if reorg.mergingTmpIdx {
		elements := reorg.elements
		firstElemTempID := tablecodec.TempIndexPrefix | elements[0].ID
		lastElemTempID := tablecodec.TempIndexPrefix | elements[len(elements)-1].ID
		startKey = tablecodec.EncodeIndexSeekKey(pid, firstElemTempID, nil)
		endKey = tablecodec.EncodeIndexSeekKey(pid, lastElemTempID, []byte{255})
	} else {
		currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		startKey, endKey, err = getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
	}
	return pid, startKey, endKey, nil
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func updateReorgInfo(sessPool *sess.Pool, t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pid, startKey, endKey, err := getNextPartitionInfo(reorg, t, reorg.PhysicalTableID)
	if err != nil {
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}
	reorg.PhysicalTableID, reorg.StartKey, reorg.EndKey = pid, startKey, endKey

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, sessPool)
	logutil.DDLLogger().Info("job update reorgInfo",
		zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partitionTableID", pid),
		zap.String("startKey", hex.EncodeToString(reorg.StartKey)),
		zap.String("endKey", hex.EncodeToString(reorg.EndKey)), zap.Error(err))
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func findNextNonTouchedPartitionID(currPartitionID int64, pi *model.PartitionInfo) (int64, error) {
	pid, err := findNextPartitionID(currPartitionID, pi.Definitions)
	if err != nil {
		return 0, err
	}
	if pid == 0 {
		return 0, nil
	}
	for _, notFoundErr := findNextPartitionID(pid, pi.DroppingDefinitions); notFoundErr == nil; {
		// This can be optimized, but it is not frequently called, so keeping as-is
		pid, err = findNextPartitionID(pid, pi.Definitions)
		if pid == 0 {
			break
		}
	}
	return pid, err
}

// AllocateIndexID allocates an index ID from TableInfo.
func AllocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

func getIndexInfoByNameAndColumn(oldTableInfo *model.TableInfo, newOne *model.IndexInfo) *model.IndexInfo {
	for _, oldOne := range oldTableInfo.Indices {
		if newOne.Name.L == oldOne.Name.L && indexColumnSliceEqual(newOne.Columns, oldOne.Columns) {
			return oldOne
		}
	}
	return nil
}

func indexColumnSliceEqual(a, b []*model.IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		logutil.DDLLogger().Warn("admin repair table : index's columns length equal to 0")
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v.Name.L != b[i].Name.L {
			return false
		}
	}
	return true
}

type cleanUpIndexWorker struct {
	baseIndexWorker
}

func newCleanUpIndexWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*cleanUpIndexWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblCleanupIdxRate, false)
	if err != nil {
		return nil, err
	}

	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().IsColumnarIndex() {
			continue
		}
		if index.Meta().Global {
			indexes = append(indexes, index)
		}
	}
	return &cleanUpIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bCtx,
			indexes:     indexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *cleanUpIndexWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)

		lockCtx := new(kv.LockCtx)
		evalCtx := w.exprCtx.GetEvalCtx()
		loc, ec := evalCtx.Location(), evalCtx.ErrCtx()
		n := len(w.indexes)
		globalIndexKeys := make([]kv.Key, 0, len(idxRecords))
		allKeys := make([]kv.Key, 0, len(idxRecords))
		for i, idxRecord := range idxRecords {
			key, distinct, err := w.indexes[i%n].GenIndexKey(ec, loc, idxRecord.vals, idxRecord.handle, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if distinct {
				globalIndexKeys = append(globalIndexKeys, key)
			}
			allKeys = append(allKeys, key)
		}

		var found map[string][]byte
		found, err = kv.BatchGetValue(ctx, txn, globalIndexKeys)
		if err != nil {
			return errors.Trace(err)
		}

		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			if val, ok := found[string(allKeys[i])]; ok {
				// Only delete if it is from the partition it was read from.
				handle, errPart := tablecodec.DecodeHandleInIndexValue(val)
				if errPart != nil {
					return errors.Trace(errPart)
				}
				if partHandle, ok := handle.(kv.PartitionHandle); ok {
					if partHandle.PartitionID != handleRange.physicalTable.GetPhysicalID() {
						continue
					}
				}
				// Lock the global index entry, to prevent deleting something that is concurrently added.
				err = txn.LockKeys(ctx, lockCtx, allKeys[i])
				if err != nil {
					return errors.Trace(err)
				}
			}
			// we fetch records row by row, so records will belong to
			// index[0], index[1] ... index[n-1], index[0], index[1] ...
			// respectively. So indexes[i%n] is the index of idxRecords[i].
			err = w.indexes[i%n].Delete(w.tblCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "cleanUpIndexBackfillDataInTxn", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})

	return
}

// cleanupPhysicalTableIndex handles the drop partition reorganization state for a non-partitioned table or a partition.
func (w *worker) cleanupPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	logutil.DDLLogger().Info("start to clean up index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	return w.writePhysicalTableRecord(w.workCtx, w.sessPool, t, typeCleanUpIndexWorker, reorgInfo)
}

// cleanupGlobalIndex handles the drop partition reorganization state to clean up index entries of partitions.
func (w *worker) cleanupGlobalIndexes(tbl table.PartitionedTable, partitionIDs []int64, reorgInfo *reorgInfo) error {
	var err error
	var finish bool
	for !finish {
		p := tbl.GetPartition(reorgInfo.PhysicalTableID)
		if p == nil {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, tbl.Meta().ID)
		}
		err = w.cleanupPhysicalTableIndex(p, reorgInfo)
		if err != nil {
			break
		}
		finish, err = w.updateReorgInfoForPartitions(tbl, reorgInfo, partitionIDs)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(err)
}

// updateReorgInfoForPartitions will find the next partition in partitionIDs according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfoForPartitions(t table.PartitionedTable, reorg *reorgInfo, partitionIDs []int64) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	var pid int64
	for i, pi := range partitionIDs {
		if pi == reorg.PhysicalTableID {
			if i == len(partitionIDs)-1 {
				return true, nil
			}
			pid = partitionIDs[i+1]
			break
		}
	}

	currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.DDLLogger().Info("job update reorg info", zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partition table ID", pid), zap.String("start key", hex.EncodeToString(start)),
		zap.String("end key", hex.EncodeToString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

// changingIndex is used to store the index that need to be changed during modifying column.
type changingIndex struct {
	IndexInfo *model.IndexInfo
	// Column offset in idxInfo.Columns.
	Offset int
	// When the modifying column is contained in the index, a temp index is created.
	// isTemp indicates whether the indexInfo is a temp index created by a previous modify column job.
	isTemp bool
}

// FindRelatedIndexesToChange finds the indexes that covering the given column.
// The normal one will be overwritten by the temp one.
func FindRelatedIndexesToChange(tblInfo *model.TableInfo, colName ast.CIStr) []changingIndex {
	// In multi-schema change jobs that contains several "modify column" sub-jobs, there may be temp indexes for another temp index.
	// To prevent reorganizing too many indexes, we should create the temp indexes that are really necessary.
	var normalIdxInfos, tempIdxInfos []changingIndex
	for _, idxInfo := range tblInfo.Indices {
		if pos := findIdxCol(idxInfo, colName); pos != -1 {
			isTemp := isTempIndex(idxInfo, tblInfo)
			r := changingIndex{IndexInfo: idxInfo, Offset: pos, isTemp: isTemp}
			if isTemp {
				tempIdxInfos = append(tempIdxInfos, r)
			} else {
				normalIdxInfos = append(normalIdxInfos, r)
			}
		}
	}
	// Overwrite if the index has the corresponding temp index. For example,
	// we try to find the indexes that contain the column `b` and there are two indexes, `i(a, b)` and `$i($a, b)`.
	// Note that the symbol `$` means temporary. The index `$i($a, b)` is temporarily created by the previous "modify a" statement.
	// In this case, we would create a temporary index like $$i($a, $b), so the latter should be chosen.
	result := normalIdxInfos
	for _, tmpIdx := range tempIdxInfos {
		origName := tmpIdx.IndexInfo.GetChangingOriginName()
		for i, normIdx := range normalIdxInfos {
			if normIdx.IndexInfo.Name.O == origName {
				result[i] = tmpIdx
			}
		}
	}
	return result
}

// isColumnarIndexColumn checks if any index contains the given column is a columnar index.
func isColumnarIndexColumn(tblInfo *model.TableInfo, col *model.ColumnInfo) bool {
	indexesToChange := FindRelatedIndexesToChange(tblInfo, col.Name)
	for _, idx := range indexesToChange {
		if idx.IndexInfo.IsColumnarIndex() {
			return true
		}
	}
	return false
}

// isTempIndex checks whether the index is a temp index created by modify column.
// There are two types of temp index:
// 1. The index contains a temp column that is newly added, indicated by ChangeStateInfo
// 2. The index contains a old column changing its type in place, indicated by UsingChangingType
func isTempIndex(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) bool {
	for _, idxCol := range idxInfo.Columns {
		if idxCol.UseChangingType || tblInfo.Columns[idxCol.Offset].ChangeStateInfo != nil {
			return true
		}
	}
	return false
}

func findIdxCol(idxInfo *model.IndexInfo, colName ast.CIStr) int {
	for offset, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == colName.L {
			return offset
		}
	}
	return -1
}

func renameIndexes(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == from.L {
			idx.Name = to
		} else if isTempIndex(idx, tblInfo) &&
			(idx.GetChangingOriginName() == from.O ||
				idx.GetRemovingOriginName() == from.O) {
			idx.Name.L = strings.Replace(idx.Name.L, from.L, to.L, 1)
			idx.Name.O = strings.Replace(idx.Name.O, from.O, to.O, 1)
		}
		for _, col := range idx.Columns {
			originalCol := tblInfo.Columns[col.Offset]
			if originalCol.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
				col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
				col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
			}
		}
	}
}

func renameHiddenColumns(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, col := range tblInfo.Columns {
		if col.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
			col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
			col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
		}
	}
}

// CheckAndBuildIndexConditionString validates whether the given expression is compatible with
// the table schema and returns a string representation of the expression.
func CheckAndBuildIndexConditionString(tblInfo *model.TableInfo, indexConditionExpr ast.ExprNode) (string, error) {
	if indexConditionExpr == nil {
		return "", nil
	}

	// Be careful, in `CREATE TABLE` statement, the `tblInfo.Partition` is always nil here. We have to
	// check it in `buildTablePartitionInfo` again.
	if tblInfo.Partition != nil {
		return "", dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"partial index on partitioned table is not supported")
	}

	// check partial index condition expression
	err := checkIndexCondition(tblInfo, indexConditionExpr)
	if err != nil {
		return "", errors.Trace(err)
	}

	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	sb.Reset()
	err = indexConditionExpr.Restore(restoreCtx)
	if err != nil {
		return "", errors.Trace(err)
	}

	return sb.String(), nil
}

func checkIndexCondition(tblInfo *model.TableInfo, indexCondition ast.ExprNode) error {
	// Only the following expressions are supported:
	// 1. column IS NULL
	// 2. column IS NOT NULL
	// 3. column = / != / > / < / >= / <= const
	// The column must be a visible column in the table, and the const must be a literal value with
	// the same type as the column.
	// The column must **NOT** be a generated column. We can loosen this restriction in the future.
	//
	// TODO: support more expressions in the future.
	if indexCondition == nil {
		return nil
	}

	switch cond := indexCondition.(type) {
	case *ast.IsNullExpr:
		// `IS NULL` and `IS NOT NULL` are both in this branch.
		columnName, ok := cond.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a column name in the IS NULL expression")
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name %s referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		return nil
	case *ast.BinaryOperationExpr:
		if cond.Op != opcode.EQ && cond.Op != opcode.NE && cond.Op != opcode.GT &&
			cond.Op != opcode.LT && cond.Op != opcode.GE && cond.Op != opcode.LE {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("binary operation %s is not supported", cond.Op.String()))
		}

		var columnName *ast.ColumnNameExpr
		var anotherSide ast.ExprNode
		columnName, ok := cond.L.(*ast.ColumnNameExpr)
		if !ok {
			// maybe the right side is a column name
			columnName, ok = cond.R.(*ast.ColumnNameExpr)
			if !ok {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					"partial index condition must include a column name in the binary operation")
			}

			anotherSide = cond.L
		} else {
			anotherSide = cond.R
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name `%s` referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		// The another side must be a literal value, and it must have the same type as the column.
		constantExpr, ok := anotherSide.(ast.ValueExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a literal value on the other side of the binary operation")
		}
		// Reference `types.DefaultTypeForValue`, they are all possible types for literal values.
		// However, this switch-case still includes more types than the ones we have in that function
		// to avoid breaking in the future.
		//
		// Accept tiny type conversion as the type of the literal value is too limited. We shouldn't
		// force the user to use such a limited range of types.
		//
		// It'll allow precision / length difference in most of the cases.
		switch constantExpr.GetType().GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeInt24, mysql.TypeBit, mysql.TypeYear:
			// the target column must be an integer type or enum or set
			if columnInfo.GetType() != mysql.TypeTiny &&
				columnInfo.GetType() != mysql.TypeShort &&
				columnInfo.GetType() != mysql.TypeLong &&
				columnInfo.GetType() != mysql.TypeLonglong &&
				columnInfo.GetType() != mysql.TypeInt24 &&
				columnInfo.GetType() != mysql.TypeBit &&
				columnInfo.GetType() != mysql.TypeYear &&
				columnInfo.GetType() != mysql.TypeEnum &&
				columnInfo.GetType() != mysql.TypeSet {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			// the target column must be either a float or double type
			// TODO: consider whether need to support decimal type in this branch
			if columnInfo.GetType() != mysql.TypeFloat &&
				columnInfo.GetType() != mysql.TypeDouble &&
				columnInfo.GetType() != mysql.TypeNewDecimal {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			if types.IsString(columnInfo.GetType()) {
				// check the collation of the column and the literal value
				if columnInfo.FieldType.GetCharset() != constantExpr.GetType().GetCharset() {
					return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
						fmt.Sprintf("the charset %s of the column `%s` in partial index condition is not compatible with the literal value charset %s",
							columnInfo.FieldType.GetCharset(), columnName.Name.Name.L, constantExpr.GetType().GetCharset()))
				}

				return nil
			}

			// Allow to compare a datetime type column with a string literal, because we don't have a datetime literal.
			// This branch will allow users to use datetime columns in index condition.
			if columnInfo.GetType() == mysql.TypeTimestamp ||
				columnInfo.GetType() == mysql.TypeDate ||
				columnInfo.GetType() == mysql.TypeDuration ||
				columnInfo.GetType() == mysql.TypeNewDate ||
				columnInfo.GetType() == mysql.TypeDatetime {
				return nil
			}

			// ENUM and SET are also allowed for string literal.
			if columnInfo.GetType() == mysql.TypeEnum || columnInfo.GetType() == mysql.TypeSet {
				return nil
			}

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
					columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
		case mysql.TypeNull:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"= NULL is not supported in partial index condition because it is always false")
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration, mysql.TypeNewDate,
			mysql.TypeDatetime, mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet:
			// The `DATE '2025-07-28'` is actually a `cast` function, so they are also not supported yet.
			intest.Assert(false, "should never generate literal values of these types")

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		default:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		}
	default:
		return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"the kind of partial index condition is not supported")
	}
}

func buildAffectColumn(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]*model.IndexColumn, error) {
	ectx := exprstatic.NewExprContext()

	// Build affect column for partial index.
	if idxInfo.HasCondition() {
		cols, err := tables.ExtractColumnsFromCondition(ectx, idxInfo, tblInfo, true)
		if err != nil {
			return nil, err
		}
		return tables.DedupIndexColumns(cols), nil
	}

	return nil, nil
}

// buildIndexConditionChecker builds an expression for evaluating the index condition based on
// the given columns.
func buildIndexConditionChecker(copCtx copr.CopContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (func(row chunk.Row) (bool, error), error) {
	schema, names := copCtx.GetBase().GetSchemaAndNames()

	exprCtx := copCtx.GetBase().ExprCtx
	expr, err := expression.ParseSimpleExpr(exprCtx, idxInfo.ConditionExprString, expression.WithInputSchemaAndNames(schema, names, tblInfo))
	if err != nil {
		return nil, err
	}

	return func(row chunk.Row) (bool, error) {
		datum, isNull, err := expr.EvalInt(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return false, err
		}
		// If the result is NULL, it usually means the original column itself is NULL.
		// In this case, we should refuse to consider the index for partial index condition.
		return datum > 0 && !isNull, nil
	}, nil
}
