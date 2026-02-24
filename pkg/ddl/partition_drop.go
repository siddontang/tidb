// Copyright 2018 PingCAP, Inc.
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
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// CheckDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func CheckDropTablePartition(meta *model.TableInfo, partLowerNames []string) error {
	pi := meta.Partition
	if pi.Type != ast.PartitionTypeRange && pi.Type != ast.PartitionTypeList {
		return dbterror.ErrOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
	}

	if meta.Affinity != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("DROP PARTITION of a table with AFFINITY option")
	}

	// To be error compatible with MySQL, we need to do this first!
	// see https://github.com/pingcap/tidb/issues/31681#issuecomment-1015536214
	oldDefs := pi.Definitions
	if len(oldDefs) <= len(partLowerNames) {
		return errors.Trace(dbterror.ErrDropLastPartition)
	}

	dupCheck := make(map[string]bool)
	for _, pn := range partLowerNames {
		found := false
		for _, def := range oldDefs {
			if def.Name.L == pn {
				if _, ok := dupCheck[pn]; ok {
					return errors.Trace(dbterror.ErrDropPartitionNonExistent.GenWithStackByArgs("DROP"))
				}
				dupCheck[pn] = true
				found = true
				break
			}
		}
		if !found {
			return errors.Trace(dbterror.ErrDropPartitionNonExistent.GenWithStackByArgs("DROP"))
		}
	}
	return nil
}

// updateDroppingPartitionInfo move dropping partitions to DroppingDefinitions
func updateDroppingPartitionInfo(tblInfo *model.TableInfo, partLowerNames []string) {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-len(partLowerNames))
	droppingDefs := make([]model.PartitionDefinition, 0, len(partLowerNames))

	// consider using a map to probe partLowerNames if too many partLowerNames
	for i := range oldDefs {
		found := slices.Contains(partLowerNames, oldDefs[i].Name.L)
		if found {
			droppingDefs = append(droppingDefs, oldDefs[i])
		} else {
			newDefs = append(newDefs, oldDefs[i])
		}
	}

	tblInfo.Partition.Definitions = newDefs
	tblInfo.Partition.DroppingDefinitions = droppingDefs
}

func getPartitionDef(tblInfo *model.TableInfo, partName string) (index int, def *model.PartitionDefinition, _ error) {
	defs := tblInfo.Partition.Definitions
	for i := range defs {
		if strings.EqualFold(defs[i].Name.L, strings.ToLower(partName)) {
			return i, &(defs[i]), nil
		}
	}
	return index, nil, table.ErrUnknownPartition.GenWithStackByArgs(partName, tblInfo.Name.O)
}

func getPartitionIDsFromDefinitions(defs []model.PartitionDefinition) []int64 {
	pids := make([]int64, 0, len(defs))
	for _, def := range defs {
		pids = append(pids, def.ID)
	}
	return pids
}

func hasGlobalIndex(tblInfo *model.TableInfo) bool {
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Global {
			return true
		}
	}
	return false
}

// getTableInfoWithDroppingPartitions builds oldTableInfo including dropping partitions, only used by onDropTablePartition.
func getTableInfoWithDroppingPartitions(t *model.TableInfo) *model.TableInfo {
	p := t.Partition
	nt := t.Clone()
	np := *p
	npd := make([]model.PartitionDefinition, 0, len(p.Definitions)+len(p.DroppingDefinitions))
	npd = append(npd, p.Definitions...)
	npd = append(npd, p.DroppingDefinitions...)
	np.Definitions = npd
	np.DroppingDefinitions = nil
	nt.Partition = &np
	return nt
}

func dropLabelRules(ctx context.Context, schemaName, tableName string, partNames []string) error {
	deleteRules := make([]string, 0, len(partNames))
	for _, partName := range partNames {
		deleteRules = append(deleteRules, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, schemaName, tableName, partName))
	}
	// delete batch rules
	patch := label.NewRulePatch([]*label.Rule{}, deleteRules)
	return infosync.UpdateLabelRules(ctx, patch)
}

// rollbackLikeDropPartition does rollback for Reorganize partition and Add partition.
// It will drop newly created partitions that has not yet been used, including cleaning
// up label rules and bundles as well as changed indexes due to global flag.
func (w *worker) rollbackLikeDropPartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	partInfo := args.PartInfo
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	tblInfo.Partition.DroppingDefinitions = nil
	// Collect table/partition ids to clean up, through args.OldPhysicalTblIDs
	// GC will later also drop matching Placement bundles.
	// If we delete them now, it could lead to non-compliant placement or failure during flashback
	physicalTableIDs, pNames := removePartitionAddingDefinitionsFromTableInfo(tblInfo)
	// TODO: Will this drop LabelRules for existing partitions, if the new partitions have the same name?
	err = dropLabelRules(w.ctx, job.SchemaName, tblInfo.Name.L, pNames)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
		job.State = model.JobStateCancelled
		return ver, err
	}
	if partInfo.Type != ast.PartitionTypeNone {
		// ALTER TABLE ... PARTITION BY
		// Also remove anything with the new table id
		if partInfo.NewTableID != 0 {
			physicalTableIDs = append(physicalTableIDs, partInfo.NewTableID)
		}
		// Reset if it was normal table before
		if tblInfo.Partition.Type == ast.PartitionTypeNone ||
			tblInfo.Partition.DDLType == ast.PartitionTypeNone {
			tblInfo.Partition = nil
		}
	}

	var dropIndices []*model.IndexInfo
	for _, indexInfo := range tblInfo.Indices {
		if indexInfo.State == model.StateWriteOnly {
			dropIndices = append(dropIndices, indexInfo)
		}
	}
	var deleteIndices []model.TableIDIndexID
	for _, indexInfo := range dropIndices {
		DropIndexColumnFlag(tblInfo, indexInfo)
		RemoveDependentHiddenColumns(tblInfo, indexInfo)
		removeIndexInfo(tblInfo, indexInfo)
		if indexInfo.Global {
			deleteIndices = append(deleteIndices, model.TableIDIndexID{TableID: tblInfo.ID, IndexID: indexInfo.ID})
		}
		// All other indexes has only been applied to new partitions, that is deleted in whole,
		// including indexes.
	}
	if tblInfo.Partition != nil {
		tblInfo.Partition.ClearReorgIntermediateInfo()
	}

	_, err = alterTablePartitionBundles(metaMut, tblInfo, nil)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	args.OldPhysicalTblIDs = physicalTableIDs
	args.OldGlobalIndexes = deleteIndices
	job.FillFinishedArgs(args)
	return ver, nil
}

// onDropTablePartition deletes old partition meta.
// States in reverse order:
// StateNone
//
//	Old partitions are queued to be deleted (delete_range), global index up-to-date
//
// StateDeleteReorganization
//
//	Old partitions are not accessible/used by any sessions.
//	Inserts/updates of global index which still have entries pointing to old partitions
//	will overwrite those entries.
//	In the background we are reading all old partitions and deleting their entries from
//	the global indexes.
//
// StateDeleteOnly
//
//	Old partitions are no longer visible, but if there is inserts/updates to the global indexes,
//	duplicate key errors will be given, even if the entries are from dropped partitions.
//
// StateWriteOnly
//
//	Old partitions are blocked for read and write. But for read we are allowing
//	"overlapping" partition to be read instead. Which means that write can only
//	happen in the 'overlapping' partitions original range, not into the extended
//	range open by the dropped partitions.
//
// StatePublic
//
//	Original state, unaware of DDL
func (w *worker) onDropTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	partNames := args.PartNames
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch job.SchemaState {
	case model.StatePublic:
		// Here we mark the partitions to be dropped, so they are not read or written
		err = CheckDropTablePartition(tblInfo, partNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Reason, see https://github.com/pingcap/tidb/issues/55888
		// Only mark the partitions as to be dropped, so they are not used, but not yet removed.
		originalDefs := tblInfo.Partition.Definitions
		updateDroppingPartitionInfo(tblInfo, partNames)
		tblInfo.Partition.Definitions = originalDefs
		job.SchemaState = model.StateWriteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		tblInfo.Partition.DDLAction = job.Type

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// Since the previous state do not use the dropping partitions,
		// we can now actually remove them, allowing to write into the overlapping range
		// of the higher range partition or LIST default partition.
		updateDroppingPartitionInfo(tblInfo, partNames)
		err = dropLabelRules(jobCtx.stepCtx, job.SchemaName, tblInfo.Name.L, partNames)
		if err != nil {
			// TODO: Add failpoint error/cancel injection and test failure/rollback and cancellation!
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the label rules")
		}

		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		var bundles []*placement.Bundle
		// create placement groups for each dropped partition to keep the data's placement before GC
		// These placements groups will be deleted after GC
		bundles, err = droppedPartitionBundles(metaMut, tblInfo, tblInfo.Partition.DroppingDefinitions)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		var tableBundle *placement.Bundle
		// Recompute table bundle to remove dropped partitions rules from its group
		tableBundle, err = placement.NewTableBundle(metaMut, tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		if tableBundle != nil {
			bundles = append(bundles, tableBundle)
		}

		if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		job.SchemaState = model.StateDeleteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteOnly:
		// This state is not a real 'DeleteOnly' state, because tidb does not maintain the state check in partitionDefinition.
		// Insert this state to confirm all servers can not see the old partitions when reorg is running,
		// so that no new data will be inserted into old partitions when reorganizing.
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteReorganization:
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		if hasGlobalIndex(tblInfo) {
			oldTblInfo := getTableInfoWithDroppingPartitions(tblInfo)
			var done bool
			done, err = w.cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx, job, oldTblInfo, physicalTableIDs)
			if err != nil || !done {
				return ver, errors.Trace(err)
			}
		}
		removeTiFlashAvailablePartitionIDs(tblInfo, physicalTableIDs)
		droppedDefs := tblInfo.Partition.DroppingDefinitions
		tblInfo.Partition.DroppingDefinitions = nil
		job.SchemaState = model.StateNone
		tblInfo.Partition.DDLState = job.SchemaState
		tblInfo.Partition.DDLAction = model.ActionNone
		// used by ApplyDiff in updateSchemaVersion
		args.OldPhysicalTblIDs = physicalTableIDs
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		dropPartitionEvent := notifier.NewDropPartitionEvent(
			tblInfo,
			&model.PartitionInfo{Definitions: droppedDefs},
		)
		err = asyncNotifyEvent(jobCtx, dropPartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.SchemaState = model.StateNone
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(args)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}
	return ver, errors.Trace(err)
}

func removeTiFlashAvailablePartitionIDs(tblInfo *model.TableInfo, pids []int64) {
	if tblInfo.TiFlashReplica == nil {
		return
	}
	// Remove the partitions
	ids := tblInfo.TiFlashReplica.AvailablePartitionIDs
	// Rarely called, so OK to take some time, to make it easy
	for _, id := range pids {
		for i, avail := range ids {
			if id == avail {
				tmp := ids[:i]
				tmp = append(tmp, ids[i+1:]...)
				ids = tmp
				break
			}
		}
	}
	tblInfo.TiFlashReplica.AvailablePartitionIDs = ids
}

func replaceTruncatePartitions(job *model.Job, t *meta.Mutator, tblInfo *model.TableInfo, oldIDs, newIDs []int64) (oldDefinitions, newDefinitions []model.PartitionDefinition, err error) {
	oldDefinitions = make([]model.PartitionDefinition, 0, len(oldIDs))
	newDefinitions = make([]model.PartitionDefinition, 0, len(oldIDs))
	pi := tblInfo.Partition
	for i, id := range oldIDs {
		for defIdx := range pi.Definitions {
			// use a reference to actually set the new ID!
			def := &pi.Definitions[defIdx]
			if id == def.ID {
				oldDefinitions = append(oldDefinitions, def.Clone())
				def.ID = newIDs[i]
				// Shallow copy, since we do not need to replace them.
				newDefinitions = append(newDefinitions, *def)
				break
			}
		}
	}

	if err := clearTruncatePartitionTiflashStatus(tblInfo, newDefinitions, oldIDs); err != nil {
		return nil, nil, err
	}

	if err := updateTruncatePartitionLabelRules(job, t, oldDefinitions, newDefinitions, tblInfo, oldIDs); err != nil {
		return nil, nil, err
	}
	return oldDefinitions, newDefinitions, nil
}

func (w *worker) cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo, oldIDs []int64) (bool, error) {
	tbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, errors.Trace(err)
	}
	pt, ok := tbl.(table.PartitionedTable)
	if !ok {
		return false, dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	elements := make([]*meta.Element, 0, len(tblInfo.Indices))
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Global {
			elements = append(elements, &meta.Element{ID: idxInfo.ID, TypeKey: meta.IndexElementKey})
		}
	}
	if len(elements) == 0 {
		return true, nil
	}
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		return false, err1
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	reorgInfo, err := getReorgInfoFromPartitions(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, pt, oldIDs, elements)

	if err != nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, errors.Trace(err)
	}
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (dropIndexErr error) {
		defer tidbutil.Recover(metrics.LabelDDL, "onDropTablePartition",
			func() {
				dropIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("drop partition panic")
			}, false)
		return w.cleanupGlobalIndexes(pt, oldIDs, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, nil
		}
		if dbterror.ErrPausedDDLJob.Equal(err) {
			// if ErrPausedDDLJob, we should return, check for the owner and re-wait job done.
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// onTruncateTablePartition truncates old partition meta.
//
// # StateNone
//
// Unaware of DDL.
//
// # StateWriteOnly
//
// Still sees and uses the old partition, but should filter out index reads of
// global index which has ids from pi.NewPartitionIDs.
// Allow duplicate key errors even if one cannot access the global index entry by reading!
// This state is not really needed if there are no global indexes, but used for consistency.
//
// # StateDeleteOnly
//
// Sees new partition, but should filter out index reads of global index which
// has ids from pi.DroppingDefinitions.
// Allow duplicate key errors even if one cannot access the global index entry by reading!
//
// # StateDeleteReorganization
//
// Now no other session has access to the old partition,
// but there are global index entries left pointing to the old partition,
// so they should be filtered out (see pi.DroppingDefinitions) and on write (insert/update)
// the old partition's row should be deleted and the global index key allowed
// to be overwritten.
// During this time the old partition is read and removing matching entries in
// smaller batches.
// This state is not really needed if there are no global indexes, but used for consistency.
//
// # StatePublic
//
// DDL done.
func (w *worker) onTruncateTablePartition(jobCtx *jobContext, job *model.Job) (int64, error) {
	var ver int64
	canCancel := false
	if job.SchemaState == model.StatePublic {
		canCancel = true
	}
	args, err := model.GetTruncateTableArgs(job)
	if err != nil {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	oldIDs, newIDs := args.OldPartitionIDs, args.NewPartitionIDs
	if len(oldIDs) != len(newIDs) {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(errors.New("len(oldIDs) must be the same as len(newIDs)"))
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	if job.IsRollingback() {
		return convertTruncateTablePartitionJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob, tblInfo)
	}

	failpoint.Inject("truncatePartCancel1", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			err = errors.New("Injected error by truncatePartCancel1")
			failpoint.Return(ver, err)
		}
	})

	var oldDefinitions []model.PartitionDefinition
	var newDefinitions []model.PartitionDefinition

	switch job.SchemaState {
	case model.StatePublic:
		// This work as a flag to ignore Global Index entries from the new partitions!
		// Used in IDsInDDLToIgnore() for filtering new partitions from
		// the global index
		pi.NewPartitionIDs = newIDs[:]
		pi.DDLAction = model.ActionTruncateTablePartition

		job.SchemaState = model.StateWriteOnly
		pi.DDLState = job.SchemaState
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// We can still rollback here, since we have not yet started to write to the new partitions!
		oldDefinitions, newDefinitions, err = replaceTruncatePartitions(job, jobCtx.metaMut, tblInfo, oldIDs, newIDs)
		if err != nil {
			return ver, errors.Trace(err)
		}
		var scatterScope string
		if val, ok := job.GetSystemVars(vardef.TiDBScatterRegion); ok {
			scatterScope = val
		}
		preSplitAndScatter(w.sess.Context, jobCtx.store, tblInfo, newDefinitions, scatterScope)
		failpoint.Inject("truncatePartFail1", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				err = errors.New("Injected error by truncatePartFail1")
				failpoint.Return(ver, err)
			}
		})
		// This work as a flag to ignore Global Index entries from the old partitions!
		// Used in IDsInDDLToIgnore() for filtering old partitions from
		// the global index
		pi.DroppingDefinitions = oldDefinitions
		// And we don't need to filter for new partitions any longer
		job.SchemaState = model.StateDeleteOnly
		pi.DDLState = job.SchemaState
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteOnly:
		// Now we don't see the old partitions, but other sessions may still use them.
		// So to keep the Global Index consistent, we will still keep it up-to-date with
		// the old partitions, as well as the new partitions.
		// Also ensures that no writes will happen after GC in DeleteRanges.

		job.SchemaState = model.StateDeleteReorganization
		pi.DDLState = job.SchemaState
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteReorganization:
		// Now the old partitions are no longer accessible, but they are still referenced in
		// the global indexes (although allowed to be overwritten).
		// So time to clear them.

		var done bool
		done, err = w.cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx, job, tblInfo, oldIDs)
		if err != nil || !done {
			return ver, errors.Trace(err)
		}
		failpoint.Inject("truncatePartFail2", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				err = errors.New("Injected error by truncatePartFail2")
				failpoint.Return(ver, err)
			}
		})
		// For the truncatePartitionEvent
		oldDefinitions = pi.DroppingDefinitions
		newDefinitions = make([]model.PartitionDefinition, 0, len(oldIDs))
		for i, def := range oldDefinitions {
			newDef := def.Clone()
			newDef.ID = newIDs[i]
			newDefinitions = append(newDefinitions, newDef)
		}

		pi.DroppingDefinitions = nil
		pi.NewPartitionIDs = nil
		pi.DDLState = model.StateNone
		pi.DDLAction = model.ActionNone

		// Create new affinity groups first (critical operation - must succeed)
		if tblInfo.Affinity != nil {
			if err = createTableAffinityGroupsInPD(jobCtx, tblInfo); err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}

		// Delete old affinity groups (best-effort cleanup - ignore errors)
		if tblInfo.Affinity != nil {
			if err := deleteTableAffinityGroupsInPD(jobCtx, tblInfo, oldDefinitions); err != nil {
				logutil.DDLLogger().Error("failed to delete old partition affinity groups from PD", zap.Error(err), zap.Int64("tableID", tblInfo.ID))
			}
		}

		failpoint.Inject("truncatePartFail3", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				err = errors.New("Injected error by truncatePartFail3")
				failpoint.Return(ver, err)
			}
		})
		// used by ApplyDiff in updateSchemaVersion
		args.ShouldUpdateAffectedPartitions = true
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		truncatePartitionEvent := notifier.NewTruncatePartitionEvent(
			tblInfo,
			&model.PartitionInfo{Definitions: newDefinitions},
			&model.PartitionInfo{Definitions: oldDefinitions},
		)
		err = asyncNotifyEvent(jobCtx, truncatePartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(&model.TruncateTableArgs{
			OldPartitionIDs: oldIDs,
		})
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}
	return ver, errors.Trace(err)
}

func clearTruncatePartitionTiflashStatus(tblInfo *model.TableInfo, newPartitions []model.PartitionDefinition, oldIDs []int64) error {
	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
		e := infosync.ConfigureTiFlashPDForPartitions(true, &newPartitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID)
		failpoint.Inject("FailTiFlashTruncatePartition", func() {
			e = errors.New("enforced error")
		})
		if e != nil {
			logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(e))
			return e
		}
		tblInfo.TiFlashReplica.Available = false
		// Set partition replica become unavailable.
		removeTiFlashAvailablePartitionIDs(tblInfo, oldIDs)
	}
	return nil
}

func updateTruncatePartitionLabelRules(job *model.Job, t *meta.Mutator, oldPartitions, newPartitions []model.PartitionDefinition, tblInfo *model.TableInfo, oldIDs []int64) error {
	bundles, err := placement.NewPartitionListBundles(t, newPartitions)
	if err != nil {
		return errors.Trace(err)
	}

	tableBundle, err := placement.NewTableBundle(t, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	if tableBundle != nil {
		bundles = append(bundles, tableBundle)
	}

	// create placement groups for each dropped partition to keep the data's placement before GC
	// These placements groups will be deleted after GC
	keepDroppedBundles, err := droppedPartitionBundles(t, tblInfo, oldPartitions)
	if err != nil {
		return errors.Trace(err)
	}
	bundles = append(bundles, keepDroppedBundles...)

	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		return errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	tableID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L)
	oldPartRules := make([]string, 0, len(oldIDs))
	for _, newPartition := range newPartitions {
		oldPartRuleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L, newPartition.Name.L)
		oldPartRules = append(oldPartRules, oldPartRuleID)
	}

	rules, err := infosync.GetLabelRules(context.TODO(), append(oldPartRules, tableID))
	if err != nil {
		return errors.Wrapf(err, "failed to get label rules from PD")
	}

	newPartIDs := getPartitionIDs(tblInfo)
	newRules := make([]*label.Rule, 0, len(oldIDs)+1)
	if tr, ok := rules[tableID]; ok {
		newRules = append(newRules, tr.Clone().Reset(job.SchemaName, tblInfo.Name.L, "", append(newPartIDs, tblInfo.ID)...))
	}

	for idx, newPartition := range newPartitions {
		if pr, ok := rules[oldPartRules[idx]]; ok {
			newRules = append(newRules, pr.Clone().Reset(job.SchemaName, tblInfo.Name.L, newPartition.Name.L, newPartition.ID))
		}
	}

	patch := label.NewRulePatch(newRules, []string{})
	err = infosync.UpdateLabelRules(context.TODO(), patch)
	if err != nil {
		return errors.Wrapf(err, "failed to notify PD the label rules")
	}

	return nil
}
