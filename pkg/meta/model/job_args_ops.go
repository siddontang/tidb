// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	pdhttp "github.com/tikv/pd/client/http"
)

// ModifyTableAutoIDCacheArgs is the arguments for Modify Table AutoID Cache ddl job.
type ModifyTableAutoIDCacheArgs struct {
	NewCache int64 `json:"new_cache,omitempty"`
}

func (a *ModifyTableAutoIDCacheArgs) getArgsV1(*Job) []any {
	return []any{a.NewCache}
}

func (a *ModifyTableAutoIDCacheArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.NewCache))
}

// GetModifyTableAutoIDCacheArgs gets the args for modify table autoID cache ddl job.
func GetModifyTableAutoIDCacheArgs(job *Job) (*ModifyTableAutoIDCacheArgs, error) {
	return getOrDecodeArgs[*ModifyTableAutoIDCacheArgs](&ModifyTableAutoIDCacheArgs{}, job)
}

// ShardRowIDArgs is the arguments for shard row ID ddl job.
type ShardRowIDArgs struct {
	ShardRowIDBits uint64 `json:"shard_row_id_bits,omitempty"`
}

func (a *ShardRowIDArgs) getArgsV1(*Job) []any {
	return []any{a.ShardRowIDBits}
}

func (a *ShardRowIDArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.ShardRowIDBits))
}

// GetShardRowIDArgs gets the args for shard row ID ddl job.
func GetShardRowIDArgs(job *Job) (*ShardRowIDArgs, error) {
	return getOrDecodeArgs[*ShardRowIDArgs](&ShardRowIDArgs{}, job)
}

// AlterTTLInfoArgs is the arguments for alter ttl info job.
type AlterTTLInfoArgs struct {
	TTLInfo            *TTLInfo `json:"ttl_info,omitempty"`
	TTLEnable          *bool    `json:"ttl_enable,omitempty"`
	TTLCronJobSchedule *string  `json:"ttl_cron_job_schedule,omitempty"`
}

func (a *AlterTTLInfoArgs) getArgsV1(*Job) []any {
	return []any{a.TTLInfo, a.TTLEnable, a.TTLCronJobSchedule}
}

func (a *AlterTTLInfoArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TTLInfo, &a.TTLEnable, &a.TTLCronJobSchedule))
}

// GetAlterTTLInfoArgs gets the args for alter ttl info job.
func GetAlterTTLInfoArgs(job *Job) (*AlterTTLInfoArgs, error) {
	return getOrDecodeArgs[*AlterTTLInfoArgs](&AlterTTLInfoArgs{}, job)
}

// CheckConstraintArgs is the arguments for both AlterCheckConstraint and DropCheckConstraint job.
type CheckConstraintArgs struct {
	ConstraintName ast.CIStr `json:"constraint_name,omitempty"`
	Enforced       bool      `json:"enforced,omitempty"`
}

func (a *CheckConstraintArgs) getArgsV1(*Job) []any {
	return []any{a.ConstraintName, a.Enforced}
}

func (a *CheckConstraintArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.ConstraintName, &a.Enforced))
}

// GetCheckConstraintArgs gets the AlterCheckConstraint args.
func GetCheckConstraintArgs(job *Job) (*CheckConstraintArgs, error) {
	return getOrDecodeArgs[*CheckConstraintArgs](&CheckConstraintArgs{}, job)
}

// AddCheckConstraintArgs is the args for add check constraint
type AddCheckConstraintArgs struct {
	Constraint *ConstraintInfo `json:"constraint_info"`
}

func (a *AddCheckConstraintArgs) getArgsV1(*Job) []any {
	return []any{a.Constraint}
}

func (a *AddCheckConstraintArgs) decodeV1(job *Job) error {
	a.Constraint = &ConstraintInfo{}
	return errors.Trace(job.decodeArgs(&a.Constraint))
}

// GetAddCheckConstraintArgs gets the AddCheckConstraint args.
func GetAddCheckConstraintArgs(job *Job) (*AddCheckConstraintArgs, error) {
	return getOrDecodeArgs[*AddCheckConstraintArgs](&AddCheckConstraintArgs{}, job)
}

// AlterTablePlacementArgs is the arguments for alter table placements ddl job.
type AlterTablePlacementArgs struct {
	PlacementPolicyRef *PolicyRefInfo `json:"placement_policy_ref,omitempty"`
}

func (a *AlterTablePlacementArgs) getArgsV1(*Job) []any {
	return []any{a.PlacementPolicyRef}
}

func (a *AlterTablePlacementArgs) decodeV1(job *Job) error {
	// when the target policy is 'default', policy info is nil
	return errors.Trace(job.decodeArgs(&a.PlacementPolicyRef))
}

// GetAlterTablePlacementArgs gets the args for alter table placements ddl job.
func GetAlterTablePlacementArgs(job *Job) (*AlterTablePlacementArgs, error) {
	return getOrDecodeArgs[*AlterTablePlacementArgs](&AlterTablePlacementArgs{}, job)
}

// SetTiFlashReplicaArgs is the arguments for setting TiFlash replica ddl.
type SetTiFlashReplicaArgs struct {
	TiflashReplica ast.TiFlashReplicaSpec `json:"tiflash_replica,omitempty"`
	// Note that ResetAvailable is only used in v2 job.
	ResetAvailable bool `json:"reset_available,omitempty"`
}

func (a *SetTiFlashReplicaArgs) getArgsV1(*Job) []any {
	return []any{a.TiflashReplica}
}

func (a *SetTiFlashReplicaArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TiflashReplica))
}

// GetSetTiFlashReplicaArgs gets the args for setting TiFlash replica ddl.
func GetSetTiFlashReplicaArgs(job *Job) (*SetTiFlashReplicaArgs, error) {
	return getOrDecodeArgs[*SetTiFlashReplicaArgs](&SetTiFlashReplicaArgs{}, job)
}

// UpdateTiFlashReplicaStatusArgs is the arguments for updating TiFlash replica status ddl.
type UpdateTiFlashReplicaStatusArgs struct {
	Available  bool  `json:"available,omitempty"`
	PhysicalID int64 `json:"physical_id,omitempty"`
}

func (a *UpdateTiFlashReplicaStatusArgs) getArgsV1(*Job) []any {
	return []any{a.Available, a.PhysicalID}
}

func (a *UpdateTiFlashReplicaStatusArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.Available, &a.PhysicalID))
}

// GetUpdateTiFlashReplicaStatusArgs gets the args for updating TiFlash replica status ddl.
func GetUpdateTiFlashReplicaStatusArgs(job *Job) (*UpdateTiFlashReplicaStatusArgs, error) {
	return getOrDecodeArgs[*UpdateTiFlashReplicaStatusArgs](&UpdateTiFlashReplicaStatusArgs{}, job)
}

// LockTablesArgs is the argument for LockTables.
type LockTablesArgs struct {
	LockTables    []TableLockTpInfo `json:"lock_tables,omitempty"`
	IndexOfLock   int               `json:"index_of_lock,omitempty"`
	UnlockTables  []TableLockTpInfo `json:"unlock_tables,omitempty"`
	IndexOfUnlock int               `json:"index_of_unlock,omitempty"`
	SessionInfo   SessionInfo       `json:"session_info,omitempty"`
	IsCleanup     bool              `json:"is_cleanup:omitempty"`
}

func (a *LockTablesArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *LockTablesArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetLockTablesArgs get the LockTablesArgs argument.
func GetLockTablesArgs(job *Job) (*LockTablesArgs, error) {
	return getOrDecodeArgs[*LockTablesArgs](&LockTablesArgs{}, job)
}

// AlterTableModeArgs is the argument for AlterTableMode.
type AlterTableModeArgs struct {
	TableMode TableMode `json:"table_mode,omitempty"`
	SchemaID  int64     `json:"schema_id,omitempty"`
	TableID   int64     `json:"table_id,omitempty"`
}

func (a *AlterTableModeArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *AlterTableModeArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetAlterTableModeArgs get the AlterTableModeArgs argument.
func GetAlterTableModeArgs(job *Job) (*AlterTableModeArgs, error) {
	return getOrDecodeArgs[*AlterTableModeArgs](&AlterTableModeArgs{}, job)
}

// RepairTableArgs is the argument for repair table
type RepairTableArgs struct {
	TableInfo *TableInfo `json:"table_info"`
}

func (a *RepairTableArgs) getArgsV1(*Job) []any {
	return []any{a.TableInfo}
}

func (a *RepairTableArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TableInfo))
}

// GetRepairTableArgs get the repair table args.
func GetRepairTableArgs(job *Job) (*RepairTableArgs, error) {
	return getOrDecodeArgs[*RepairTableArgs](&RepairTableArgs{}, job)
}

// AlterTableAttributesArgs is the argument for alter table attributes
type AlterTableAttributesArgs struct {
	LabelRule *pdhttp.LabelRule `json:"label_rule,omitempty"`
}

func (a *AlterTableAttributesArgs) getArgsV1(*Job) []any {
	return []any{a.LabelRule}
}

func (a *AlterTableAttributesArgs) decodeV1(job *Job) error {
	a.LabelRule = &pdhttp.LabelRule{}
	return errors.Trace(job.decodeArgs(a.LabelRule))
}

// GetAlterTableAttributesArgs get alter table attribute args from job.
func GetAlterTableAttributesArgs(job *Job) (*AlterTableAttributesArgs, error) {
	return getOrDecodeArgs[*AlterTableAttributesArgs](&AlterTableAttributesArgs{}, job)
}

// RecoverArgs is the argument for recover table/schema.
type RecoverArgs struct {
	RecoverInfo *RecoverSchemaInfo `json:"recover_info,omitempty"`
	CheckFlag   int64              `json:"check_flag,omitempty"`

	// used during runtime
	AffectedPhysicalIDs []int64 `json:"-"`
}

func (a *RecoverArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionRecoverTable {
		return []any{a.RecoverTableInfos()[0], a.CheckFlag}
	}
	return []any{a.RecoverInfo, a.CheckFlag}
}

func (a *RecoverArgs) decodeV1(job *Job) error {
	var (
		recoverTableInfo  *RecoverTableInfo
		recoverSchemaInfo = &RecoverSchemaInfo{}
		recoverCheckFlag  int64
	)
	if job.Type == ActionRecoverTable {
		err := job.decodeArgs(&recoverTableInfo, &recoverCheckFlag)
		if err != nil {
			return errors.Trace(err)
		}
		recoverSchemaInfo.RecoverTableInfos = []*RecoverTableInfo{recoverTableInfo}
	} else {
		err := job.decodeArgs(recoverSchemaInfo, &recoverCheckFlag)
		if err != nil {
			return errors.Trace(err)
		}
	}
	a.RecoverInfo = recoverSchemaInfo
	a.CheckFlag = recoverCheckFlag
	return nil
}

// RecoverTableInfos get all the recover infos.
func (a *RecoverArgs) RecoverTableInfos() []*RecoverTableInfo {
	return a.RecoverInfo.RecoverTableInfos
}

// GetRecoverArgs get the recover table/schema args.
func GetRecoverArgs(job *Job) (*RecoverArgs, error) {
	return getOrDecodeArgs[*RecoverArgs](&RecoverArgs{}, job)
}

// PlacementPolicyArgs is the argument for create/alter/drop placement policy
type PlacementPolicyArgs struct {
	Policy         *PolicyInfo `json:"policy,omitempty"`
	ReplaceOnExist bool        `json:"replace_on_exist,omitempty"`
	PolicyName     ast.CIStr   `json:"policy_name,omitempty"`

	// it's set for alter/drop policy in v2
	PolicyID int64 `json:"policy_id"`
}

func (a *PlacementPolicyArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionCreatePlacementPolicy {
		return []any{a.Policy, a.ReplaceOnExist}
	} else if job.Type == ActionAlterPlacementPolicy {
		return []any{a.Policy}
	}
	return []any{a.PolicyName}
}

func (a *PlacementPolicyArgs) decodeV1(job *Job) error {
	a.PolicyID = job.SchemaID

	if job.Type == ActionCreatePlacementPolicy {
		return errors.Trace(job.decodeArgs(&a.Policy, &a.ReplaceOnExist))
	} else if job.Type == ActionAlterPlacementPolicy {
		return errors.Trace(job.decodeArgs(&a.Policy))
	}
	return errors.Trace(job.decodeArgs(&a.PolicyName))
}

// GetPlacementPolicyArgs gets the placement policy args.
func GetPlacementPolicyArgs(job *Job) (*PlacementPolicyArgs, error) {
	return getOrDecodeArgs[*PlacementPolicyArgs](&PlacementPolicyArgs{}, job)
}

// SetDefaultValueArgs is the argument for setting default value ddl.
type SetDefaultValueArgs struct {
	Col *ColumnInfo `json:"column_info,omitempty"`
}

func (a *SetDefaultValueArgs) getArgsV1(*Job) []any {
	return []any{a.Col}
}

func (a *SetDefaultValueArgs) decodeV1(job *Job) error {
	a.Col = &ColumnInfo{}
	return errors.Trace(job.decodeArgs(a.Col))
}

// GetSetDefaultValueArgs get the args for setting default value ddl.
func GetSetDefaultValueArgs(job *Job) (*SetDefaultValueArgs, error) {
	return getOrDecodeArgs[*SetDefaultValueArgs](&SetDefaultValueArgs{}, job)
}

// KeyRange is copied from kv.KeyRange to avoid cycle import.
// Unused fields are removed.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// FlashbackClusterArgs is the argument for flashback cluster.
type FlashbackClusterArgs struct {
	FlashbackTS        uint64         `json:"flashback_ts,omitempty"`
	PDScheduleValue    map[string]any `json:"pd_schedule_value,omitempty"`
	EnableGC           bool           `json:"enable_gc,omitempty"`
	EnableAutoAnalyze  bool           `json:"enable_auto_analyze,omitempty"`
	EnableTTLJob       bool           `json:"enable_ttl_job,omitempty"`
	SuperReadOnly      bool           `json:"super_read_only,omitempty"`
	LockedRegionCnt    uint64         `json:"locked_region_cnt,omitempty"`
	StartTS            uint64         `json:"start_ts,omitempty"`
	CommitTS           uint64         `json:"commit_ts,omitempty"`
	FlashbackKeyRanges []KeyRange     `json:"key_ranges,omitempty"`
}

func (a *FlashbackClusterArgs) getArgsV1(*Job) []any {
	enableAutoAnalyze := "ON"
	superReadOnly := "ON"
	enableTTLJob := "ON"
	if !a.EnableAutoAnalyze {
		enableAutoAnalyze = "OFF"
	}
	if !a.SuperReadOnly {
		superReadOnly = "OFF"
	}
	if !a.EnableTTLJob {
		enableTTLJob = "OFF"
	}

	return []any{
		a.FlashbackTS, a.PDScheduleValue, a.EnableGC,
		enableAutoAnalyze, superReadOnly, a.LockedRegionCnt,
		a.StartTS, a.CommitTS, enableTTLJob, a.FlashbackKeyRanges,
	}
}

func (a *FlashbackClusterArgs) decodeV1(job *Job) error {
	var autoAnalyzeValue, readOnlyValue, ttlJobEnableValue string

	if err := job.decodeArgs(
		&a.FlashbackTS, &a.PDScheduleValue, &a.EnableGC,
		&autoAnalyzeValue, &readOnlyValue, &a.LockedRegionCnt,
		&a.StartTS, &a.CommitTS, &ttlJobEnableValue, &a.FlashbackKeyRanges,
	); err != nil {
		return errors.Trace(err)
	}

	if autoAnalyzeValue == "ON" {
		a.EnableAutoAnalyze = true
	}
	if readOnlyValue == "ON" {
		a.SuperReadOnly = true
	}
	if ttlJobEnableValue == "ON" {
		a.EnableTTLJob = true
	}

	return nil
}

// GetFlashbackClusterArgs get the flashback cluster argument from job.
func GetFlashbackClusterArgs(job *Job) (*FlashbackClusterArgs, error) {
	return getOrDecodeArgs[*FlashbackClusterArgs](&FlashbackClusterArgs{}, job)
}

// IndexOp is used to identify arguemnt type, which is only used for v1 index args.
// TODO(joechenrh): remove this type after totally switched to v2
type IndexOp byte

// List op types.
const (
	OpAddIndex = iota
	OpDropIndex
	OpRollbackAddIndex
)

// IndexArg is the argument for single add/drop/rename index operation.
// Different types of job use different fields.
// Below lists used fields for each type (listed in order of the layout in v1)
//
//	Adding NonPK: Unique, IndexName, IndexPartSpecifications, IndexOption, SQLMode, Warning(not stored, always nil), Global
//	Adding PK: Unique, IndexName, IndexPartSpecifications, IndexOptions, HiddelCols, Global
//	Adding vector index: IndexName, IndexPartSpecifications, IndexOption, FuncExpr
//	Drop index: IndexName, IfExist, IndexID
//	Rollback add index: IndexName, IfExist, IsColumnar
//	Rename index: IndexName
type IndexArg struct {
	// Global is never used, we only use Global in IndexOption. Can be deprecated later.
	Global                  bool                          `json:"-"`
	Unique                  bool                          `json:"unique,omitempty"`
	IndexName               ast.CIStr                     `json:"index_name,omitempty"`
	IndexPartSpecifications []*ast.IndexPartSpecification `json:"index_part_specifications"`
	IndexOption             *ast.IndexOption              `json:"index_option,omitempty"`
	HiddenCols              []*ColumnInfo                 `json:"hidden_cols,omitempty"`

	// For vector index
	FuncExpr string `json:"func_expr,omitempty"`
	// IsColumnar is used to distinguish columnar index and normal index.
	// It used to be `IsVector`, after adding columnar index, we extend it to `IsColumnar`.
	// But we keep the json field name as `is_vector` for compatibility.
	IsColumnar bool `json:"is_vector,omitempty"`

	// ColumnarIndexType is used to distinguish different columnar index types.
	// Note: 1. when you want to read it, always calling `GetColumnarIndexType`` rather than using it directly.
	//       2. when you set it, make sure IsColumnar = ColumnarIndexType != ColumnarIndexTypeNA.
	ColumnarIndexType ColumnarIndexType `json:"columnar_index_type,omitempty"`

	// For PK
	IsPK    bool          `json:"is_pk,omitempty"`
	SQLMode mysql.SQLMode `json:"sql_mode,omitempty"`

	// IfExist will be used in onDropIndex.
	IndexID  int64 `json:"index_id,omitempty"`
	IfExist  bool  `json:"if_exist,omitempty"`
	IsGlobal bool  `json:"is_global,omitempty"`

	// Only used for job args v2.
	SplitOpt *IndexArgSplitOpt `json:"split_opt,omitempty"`

	// ConditionString is used to store the partial index condition string for the index.
	ConditionString string `json:"condition_string,omitempty"`
}

// GetColumnarIndexType gets the real columnar index type in a backward compatibility way.
func (a *IndexArg) GetColumnarIndexType() ColumnarIndexType {
	// For compatibility, if columnar index type is not set, and it's a columnar index, it's a vector index.

	// If the columnar index type is NA and it's not a columnar index, it's a general index.
	if a.ColumnarIndexType == ColumnarIndexTypeNA && !a.IsColumnar {
		return ColumnarIndexTypeNA
	}
	// If the columnar index type is NA and it's a columnar index, it's a vector index.
	if a.ColumnarIndexType == ColumnarIndexTypeNA && a.IsColumnar {
		return ColumnarIndexTypeVector
	}
	return a.ColumnarIndexType
}

// IndexArgSplitOpt is a field of IndexArg used by index presplit.
type IndexArgSplitOpt struct {
	Lower      []string   `json:"lower,omitempty"`
	Upper      []string   `json:"upper,omitempty"`
	Num        int64      `json:"num,omitempty"`
	ValueLists [][]string `json:"value_lists,omitempty"`
}

// ModifyIndexArgs is the argument for add/drop/rename index jobs,
// which includes PK and vector index.
type ModifyIndexArgs struct {
	IndexArgs []*IndexArg `json:"index_args,omitempty"`

	// Belows is used for finished args.
	PartitionIDs []int64 `json:"partition_ids,omitempty"`

	// This is only used for getFinishedArgsV1 to distinguish different type of job in v1,
	// since they need different arguments layout.
	// TODO(joechenrh): remove this flag after totally switched to v2
	OpType IndexOp `json:"-"`
}

func (a *ModifyIndexArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionRenameIndex {
		return []any{a.IndexArgs[0].IndexName, a.IndexArgs[1].IndexName}
	}

	// Drop index
	if job.Type == ActionDropIndex || job.Type == ActionDropPrimaryKey {
		if len(a.IndexArgs) == 1 {
			return []any{a.IndexArgs[0].IndexName, a.IndexArgs[0].IfExist}
		}
		indexNames := make([]ast.CIStr, len(a.IndexArgs))
		ifExists := make([]bool, len(a.IndexArgs))
		for i, idxArg := range a.IndexArgs {
			indexNames[i] = idxArg.IndexName
			ifExists[i] = idxArg.IfExist
		}
		return []any{indexNames, ifExists}
	}

	// Add columnar index
	if job.Type == ActionAddColumnarIndex {
		arg := a.IndexArgs[0]
		return []any{arg.IndexName, arg.IndexPartSpecifications[0], arg.IndexOption, arg.FuncExpr, arg.ColumnarIndexType}
	}

	// Add primary key
	if job.Type == ActionAddPrimaryKey {
		arg := a.IndexArgs[0]

		// The sixth argument is set and never used.
		// Leave it as nil to make it compatible with history job.
		return []any{
			arg.Unique, arg.IndexName, arg.IndexPartSpecifications,
			arg.IndexOption, arg.SQLMode, nil, arg.Global,
		}
	}

	// Add index
	n := len(a.IndexArgs)
	unique := make([]bool, n)
	indexName := make([]ast.CIStr, n)
	indexPartSpecification := make([][]*ast.IndexPartSpecification, n)
	indexOption := make([]*ast.IndexOption, n)
	hiddenCols := make([][]*ColumnInfo, n)
	global := make([]bool, n)

	for i, arg := range a.IndexArgs {
		unique[i] = arg.Unique
		indexName[i] = arg.IndexName
		indexPartSpecification[i] = arg.IndexPartSpecifications
		indexOption[i] = arg.IndexOption
		hiddenCols[i] = arg.HiddenCols
		global[i] = arg.Global
	}

	// This is to make the args compatible with old logic
	if n == 1 {
		return []any{unique[0], indexName[0], indexPartSpecification[0], indexOption[0], hiddenCols[0], global[0]}
	}

	return []any{unique, indexName, indexPartSpecification, indexOption, hiddenCols, global}
}

func (a *ModifyIndexArgs) decodeV1(job *Job) error {
	var err error
	switch job.Type {
	case ActionRenameIndex:
		err = a.decodeRenameIndexV1(job)
	case ActionAddIndex:
		err = a.decodeAddIndexV1(job)
	case ActionAddColumnarIndex:
		err = a.decodeAddColumnarIndexV1(job)
	case ActionAddPrimaryKey:
		err = a.decodeAddPrimaryKeyV1(job)
	default:
		err = errors.Errorf("Invalid job type for decoding %d", job.Type)
	}
	return errors.Trace(err)
}

func (a *ModifyIndexArgs) decodeRenameIndexV1(job *Job) error {
	var from, to ast.CIStr
	if err := job.decodeArgs(&from, &to); err != nil {
		return errors.Trace(err)
	}
	a.IndexArgs = []*IndexArg{
		{IndexName: from},
		{IndexName: to},
	}
	return nil
}

func (a *ModifyIndexArgs) decodeDropIndexV1(job *Job) error {
	indexNames := make([]ast.CIStr, 1)
	ifExists := make([]bool, 1)
	if err := job.decodeArgs(&indexNames[0], &ifExists[0]); err != nil {
		if err = job.decodeArgs(&indexNames, &ifExists); err != nil {
			return errors.Trace(err)
		}
	}

	a.IndexArgs = make([]*IndexArg, len(indexNames))
	for i, indexName := range indexNames {
		a.IndexArgs[i] = &IndexArg{
			IndexName: indexName,
			IfExist:   ifExists[i],
		}
	}
	return nil
}

func (a *ModifyIndexArgs) decodeAddIndexV1(job *Job) error {
	uniques := make([]bool, 1)
	indexNames := make([]ast.CIStr, 1)
	indexPartSpecifications := make([][]*ast.IndexPartSpecification, 1)
	indexOptions := make([]*ast.IndexOption, 1)
	hiddenCols := make([][]*ColumnInfo, 1)
	globals := make([]bool, 1)

	if err := job.decodeArgs(
		&uniques, &indexNames, &indexPartSpecifications,
		&indexOptions, &hiddenCols, &globals); err != nil {
		if err = job.decodeArgs(
			&uniques[0], &indexNames[0], &indexPartSpecifications[0],
			&indexOptions[0], &hiddenCols[0], &globals[0]); err != nil {
			return errors.Trace(err)
		}
	}

	for i, unique := range uniques {
		a.IndexArgs = append(a.IndexArgs, &IndexArg{
			Unique:                  unique,
			IndexName:               indexNames[i],
			IndexPartSpecifications: indexPartSpecifications[i],
			IndexOption:             indexOptions[i],
			HiddenCols:              hiddenCols[i],
			Global:                  globals[i],
		})
	}
	return nil
}

func (a *ModifyIndexArgs) decodeAddPrimaryKeyV1(job *Job) error {
	a.IndexArgs = []*IndexArg{{IsPK: true}}
	var unused any
	if err := job.decodeArgs(
		&a.IndexArgs[0].Unique, &a.IndexArgs[0].IndexName, &a.IndexArgs[0].IndexPartSpecifications,
		&a.IndexArgs[0].IndexOption, &a.IndexArgs[0].SQLMode,
		&unused, &a.IndexArgs[0].Global); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (a *ModifyIndexArgs) decodeAddColumnarIndexV1(job *Job) error {
	var (
		indexName              ast.CIStr
		indexPartSpecification *ast.IndexPartSpecification
		indexOption            *ast.IndexOption
		funcExpr               string
		columnarIndexType      ColumnarIndexType
	)

	if err := job.decodeArgs(
		&indexName, &indexPartSpecification, &indexOption, &funcExpr, &columnarIndexType); err != nil {
		return errors.Trace(err)
	}

	a.IndexArgs = []*IndexArg{{
		IndexName:               indexName,
		IndexPartSpecifications: []*ast.IndexPartSpecification{indexPartSpecification},
		IndexOption:             indexOption,
		FuncExpr:                funcExpr,
		IsColumnar:              true,
		ColumnarIndexType:       columnarIndexType,
	}}
	return nil
}

func (a *ModifyIndexArgs) getFinishedArgsV1(job *Job) []any {
	// Add index
	if a.OpType == OpAddIndex {
		if job.Type == ActionAddColumnarIndex {
			return []any{a.IndexArgs[0].IndexID, a.IndexArgs[0].IfExist, a.PartitionIDs, a.IndexArgs[0].IsGlobal}
		}

		n := len(a.IndexArgs)
		indexIDs := make([]int64, n)
		ifExists := make([]bool, n)
		isGlobals := make([]bool, n)
		for i, arg := range a.IndexArgs {
			indexIDs[i] = arg.IndexID
			ifExists[i] = arg.IfExist
			isGlobals[i] = arg.Global
		}
		return []any{indexIDs, ifExists, a.PartitionIDs, isGlobals}
	}

	// Below is to make the args compatible with old logic:
	// 1. For drop index, arguments are [CIStr, bool, int64, []int64, bool].
	// 3. For rollback add index, arguments are [[]CIStr, []bool, []int64].
	if a.OpType == OpRollbackAddIndex {
		indexNames := make([]ast.CIStr, len(a.IndexArgs))
		ifExists := make([]bool, len(a.IndexArgs))
		for i, idxArg := range a.IndexArgs {
			indexNames[i] = idxArg.IndexName
			ifExists[i] = idxArg.IfExist
		}
		return []any{indexNames, ifExists, a.PartitionIDs}
	}

	idxArg := a.IndexArgs[0]
	return []any{idxArg.IndexName, idxArg.IfExist, idxArg.IndexID, a.PartitionIDs, idxArg.IsColumnar}
}

// GetRenameIndexes get name of renamed index.
func (a *ModifyIndexArgs) GetRenameIndexes() (from, to ast.CIStr) {
	from, to = a.IndexArgs[0].IndexName, a.IndexArgs[1].IndexName
	return
}

// GetModifyIndexArgs gets the add/rename index args.
func GetModifyIndexArgs(job *Job) (*ModifyIndexArgs, error) {
	return getOrDecodeArgs(&ModifyIndexArgs{}, job)
}

// GetDropIndexArgs is only used to get drop index arg.
// The logic is separated from ModifyIndexArgs.decodeV1.
// TODO(joechenrh): replace this function with GetModifyIndexArgs after totally switched to v2.
func GetDropIndexArgs(job *Job) (*ModifyIndexArgs, error) {
	if job.Version == JobVersion2 {
		return getOrDecodeArgsV2[*ModifyIndexArgs](job)
	}

	// For add index jobs(ActionAddIndex, etc.) in v1, it can store both drop index arguments and add index arguments.
	// The logic in ModifyIndexArgs.decodeV1 maybe:
	//		Decode rename index args if type == ActionRenameIndex
	//		Try decode drop index args
	//		Try decode add index args if failed
	// So we separate this from decodeV1 to avoid unnecessary "try decode" logic.
	a := &ModifyIndexArgs{}
	err := a.decodeDropIndexV1(job)
	return a, errors.Trace(err)
}

// GetFinishedModifyIndexArgs gets the add/drop index args.
func GetFinishedModifyIndexArgs(job *Job) (*ModifyIndexArgs, error) {
	if job.Version == JobVersion2 {
		return getOrDecodeArgsV2[*ModifyIndexArgs](job)
	}

	if job.IsRollingback() || job.Type == ActionDropIndex || job.Type == ActionDropPrimaryKey {
		indexNames := make([]ast.CIStr, 1)
		ifExists := make([]bool, 1)
		indexIDs := make([]int64, 1)
		var partitionIDs []int64
		isColumnar := false
		var err error

		if job.IsRollingback() {
			// Rollback add indexes
			err = job.decodeArgs(&indexNames, &ifExists, &partitionIDs, &isColumnar)
		} else {
			// Finish drop index
			err = job.decodeArgs(&indexNames[0], &ifExists[0], &indexIDs[0], &partitionIDs, &isColumnar)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		a := &ModifyIndexArgs{
			PartitionIDs: partitionIDs,
		}
		a.IndexArgs = make([]*IndexArg, len(indexNames))
		for i, indexName := range indexNames {
			a.IndexArgs[i] = &IndexArg{
				IndexName:  indexName,
				IfExist:    ifExists[i],
				IsColumnar: isColumnar,
			}
		}
		// For drop index, store index id in IndexArgs, no impact on other situations.
		// Currently, there is only one indexID since drop index is not supported in multi schema change.
		// TODO(joechenrh): modify this and corresponding logic if we need support drop multi indexes in V1.
		a.IndexArgs[0].IndexID = indexIDs[0]

		return a, nil
	}

	// Add index/vector index/PK
	addIndexIDs := make([]int64, 1)
	ifExists := make([]bool, 1)
	isGlobals := make([]bool, 1)
	var partitionIDs []int64

	// add vector index args doesn't store slice.
	if err := job.decodeArgs(&addIndexIDs[0], &ifExists[0], &partitionIDs, &isGlobals[0]); err != nil {
		if err = job.decodeArgs(&addIndexIDs, &ifExists, &partitionIDs, &isGlobals); err != nil {
			return nil, errors.Errorf("Failed to decode finished arguments from job version 1")
		}
	}
	a := &ModifyIndexArgs{PartitionIDs: partitionIDs}
	for i, indexID := range addIndexIDs {
		a.IndexArgs = append(a.IndexArgs, &IndexArg{
			IndexID:  indexID,
			IfExist:  ifExists[i],
			IsGlobal: isGlobals[i],
		})
	}
	return a, nil
}

// ModifyColumnArgs is the argument for modify column.
type ModifyColumnArgs struct {
	Column           *ColumnInfo         `json:"column,omitempty"`
	OldColumnID      int64               `json:"old_column_id,omitempty"`
	OldColumnName    ast.CIStr           `json:"old_column_name,omitempty"`
	Position         *ast.ColumnPosition `json:"position,omitempty"`
	ModifyColumnType byte                `json:"modify_column_type,omitempty"`
	NewShardBits     uint64              `json:"new_shard_bits,omitempty"`
	// ChangingColumn is the temporary column derived from OldColumn
	ChangingColumn *ColumnInfo `json:"changing_column,omitempty"`
	// ChangingIdxs is only used in test, so don't persist it
	ChangingIdxs []*IndexInfo `json:"-"`
	// RedundantIdxs stores newly-created temp indexes which can be overwritten by other temp indexes.
	// These idxs will be added to finished args after job done.
	RedundantIdxs []int64 `json:"removed_idxs,omitempty"`

	// Finished args
	// IndexIDs stores index ids to be added to gc table.
	IndexIDs     []int64 `json:"index_ids,omitempty"`
	NewIndexIDs  []int64 `json:"new_index_ids,omitempty"`
	PartitionIDs []int64 `json:"partition_ids,omitempty"`
}

func (a *ModifyColumnArgs) getArgsV1(*Job) []any {
	// during upgrade, if https://github.com/pingcap/tidb/issues/54689 triggered,
	// older node might run the job submitted by new version, but it expects 5
	// args initially, and append the later 3 at runtime.
	if a.ChangingColumn == nil {
		return []any{a.Column, a.OldColumnName, a.Position, a.ModifyColumnType, a.NewShardBits}
	}
	return []any{
		a.Column, a.OldColumnName, a.Position, a.ModifyColumnType,
		a.NewShardBits, a.ChangingColumn, a.ChangingIdxs, a.RedundantIdxs, a.OldColumnID,
	}
}

func (a *ModifyColumnArgs) decodeV1(job *Job) error {
	return job.decodeArgs(
		&a.Column, &a.OldColumnName, &a.Position, &a.ModifyColumnType,
		&a.NewShardBits, &a.ChangingColumn, &a.ChangingIdxs, &a.RedundantIdxs, &a.OldColumnID,
	)
}

func (a *ModifyColumnArgs) getFinishedArgsV1(*Job) []any {
	return []any{a.IndexIDs, a.PartitionIDs, a.NewIndexIDs}
}

// GetModifyColumnArgs get the modify column argument from job.
func GetModifyColumnArgs(job *Job) (*ModifyColumnArgs, error) {
	return getOrDecodeArgs(&ModifyColumnArgs{}, job)
}

// GetFinishedModifyColumnArgs get the finished modify column argument from job.
func GetFinishedModifyColumnArgs(job *Job) (*ModifyColumnArgs, error) {
	if job.Version == JobVersion1 {
		var (
			indexIDs     []int64
			partitionIDs []int64
			newIndexIDs  []int64
		)
		if err := job.decodeArgs(&indexIDs, &partitionIDs, &newIndexIDs); err != nil {
			return nil, errors.Trace(err)
		}
		return &ModifyColumnArgs{
			IndexIDs:     indexIDs,
			PartitionIDs: partitionIDs,
			NewIndexIDs:  newIndexIDs,
		}, nil
	}
	return getOrDecodeArgsV2[*ModifyColumnArgs](job)
}

// RefreshMetaArgs is the argument for RefreshMeta.
// InvolvedDB/InvolvedTable used for setting InvolvingSchemaInfo to
// indicates the schema info involved in the DDL job.
type RefreshMetaArgs struct {
	SchemaID      int64  `json:"schema_id,omitempty"`
	TableID       int64  `json:"table_id,omitempty"`
	InvolvedDB    string `json:"involved_db,omitempty"`
	InvolvedTable string `json:"involved_table,omitempty"`
}

func (a *RefreshMetaArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *RefreshMetaArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetRefreshMetaArgs get the refresh meta argument.
func GetRefreshMetaArgs(job *Job) (*RefreshMetaArgs, error) {
	return getOrDecodeArgs[*RefreshMetaArgs](&RefreshMetaArgs{}, job)
}

// AlterTableAffinityArgs is the argument for AlterTableAffinity
type AlterTableAffinityArgs struct {
	Affinity *TableAffinityInfo `json:"affinity,omitempty"`
}

func (a *AlterTableAffinityArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *AlterTableAffinityArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetAlterTableAffinityArgs get the alter table affinity argument.
func GetAlterTableAffinityArgs(job *Job) (*AlterTableAffinityArgs, error) {
	return getOrDecodeArgs[*AlterTableAffinityArgs](&AlterTableAffinityArgs{}, job)
}
