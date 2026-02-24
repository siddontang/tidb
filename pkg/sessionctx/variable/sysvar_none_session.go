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

package variable

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	tikvcliutil "github.com/tikv/client-go/v2/util"
)

// defaultSysVarsNoneSession contains system variables with NONE or SESSION scope.
var defaultSysVarsNoneSession = []*SysVar{
	/* The system variables below have NONE scope  */
	{Scope: vardef.ScopeNone, Name: vardef.SystemTimeZone, Value: "CST"},
	{Scope: vardef.ScopeNone, Name: vardef.Hostname, Value: vardef.DefHostname},
	{Scope: vardef.ScopeNone, Name: vardef.Port, Value: "4000", Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: vardef.ScopeNone, Name: vardef.VersionComment, Value: "TiDB Server (Apache License 2.0) " + versioninfo.TiDBEdition + " Edition, MySQL 8.0 compatible"},
	{Scope: vardef.ScopeNone, Name: vardef.Version, Value: mysql.ServerVersion},
	{Scope: vardef.ScopeNone, Name: vardef.DataDir, Value: "/usr/local/mysql/data/"},
	{Scope: vardef.ScopeNone, Name: vardef.Socket, Value: ""},
	{Scope: vardef.ScopeNone, Name: "license", Value: "Apache License 2.0"},
	{Scope: vardef.ScopeNone, Name: "have_ssl", Value: "DISABLED", Type: vardef.TypeBool},
	{Scope: vardef.ScopeNone, Name: "have_openssl", Value: "DISABLED", Type: vardef.TypeBool},
	{Scope: vardef.ScopeNone, Name: "ssl_ca", Value: ""},
	{Scope: vardef.ScopeNone, Name: "ssl_cert", Value: ""},
	{Scope: vardef.ScopeNone, Name: "ssl_key", Value: ""},
	{Scope: vardef.ScopeNone, Name: "version_compile_os", Value: runtime.GOOS},
	{Scope: vardef.ScopeNone, Name: "version_compile_machine", Value: runtime.GOARCH},
	/* TiDB specific variables */
	{Scope: vardef.ScopeNone, Name: vardef.TiDBEnableEnhancedSecurity, Value: vardef.Off, Type: vardef.TypeStr},
	{Scope: vardef.ScopeNone, Name: vardef.TiDBAllowFunctionForExpressionIndex, ReadOnly: true, Value: collectAllowFuncName4ExpressionIndex()},

	/* The system variables below have SESSION scope  */
	{Scope: vardef.ScopeSession, Name: vardef.Timestamp, Value: vardef.DefTimestamp, MinValue: 0, MaxValue: math.MaxInt32, Type: vardef.TypeFloat, GetSession: func(s *SessionVars) (string, error) {
		if timestamp, ok := s.systems[vardef.Timestamp]; ok && timestamp != vardef.DefTimestamp {
			return timestamp, nil
		}
		timestamp := s.StmtCtx.GetOrStoreStmtCache(stmtctx.StmtNowTsCacheKey, time.Now()).(time.Time)
		return types.ToString(float64(timestamp.UnixNano()) / float64(time.Second))
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		timestamp, ok := s.systems[vardef.Timestamp]
		return timestamp, ok && timestamp != vardef.DefTimestamp, nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		val := tidbOptFloat64(originalValue, vardef.DefTimestampFloat)
		if val > math.MaxInt32 {
			return originalValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.Timestamp, originalValue)
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.WarningCount, Value: "0", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(s.SysWarningCount), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.ErrorCount, Value: "0", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.Itoa(int(s.SysErrorCount)), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.LastInsertID, Value: "0", Type: vardef.TypeUnsigned, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxUint64, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return "", false, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.Identity, Value: "0", Type: vardef.TypeUnsigned, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxUint64, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.StmtCtx.PrevLastInsertID, 10), nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return "", false, nil
	}},
	/* TiDB specific variables */
	{Scope: vardef.ScopeSession, Name: vardef.TiDBTxnReadTS, Value: "", Hidden: true, SetSession: func(s *SessionVars, val string) error {
		return setTxnReadTS(s, val)
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBReadStaleness, Value: strconv.Itoa(vardef.DefTiDBReadStaleness), Type: vardef.TypeInt, MinValue: math.MinInt32, MaxValue: 0, AllowEmpty: true, Hidden: false, SetSession: func(s *SessionVars, val string) error {
		return setReadStaleness(s, val)
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBEnforceMPPExecution, Type: vardef.TypeBool, Value: BoolToOnOff(config.GetGlobalConfig().Performance.EnforceMPP), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) && !vars.allowMPPExecution {
			return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs("tidb_enforce_mpp", "1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first.")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.enforceMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxTiFlashThreads, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxThreads), MinValue: -1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxThreads = TidbOptInt64(val, vardef.DefTiFlashMaxThreads)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxBytesBeforeTiFlashExternalJoin, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxBytesBeforeExternalJoin), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalJoin = TidbOptInt64(val, vardef.DefTiFlashMaxBytesBeforeExternalJoin)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxBytesBeforeExternalGroupBy), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalGroupBy = TidbOptInt64(val, vardef.DefTiFlashMaxBytesBeforeExternalGroupBy)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxBytesBeforeTiFlashExternalSort, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMaxBytesBeforeExternalSort), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxBytesBeforeExternalSort = TidbOptInt64(val, vardef.DefTiFlashMaxBytesBeforeExternalSort)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashMemQuotaQueryPerNode, Type: vardef.TypeInt, Value: strconv.Itoa(vardef.DefTiFlashMemQuotaQueryPerNode), MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashMaxQueryMemoryPerNode = TidbOptInt64(val, vardef.DefTiFlashMemQuotaQueryPerNode)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashQuerySpillRatio, Type: vardef.TypeFloat, Value: strconv.FormatFloat(vardef.DefTiFlashQuerySpillRatio, 'f', -1, 64), MinValue: 0, MaxValue: 1, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, flag vardef.ScopeFlag) (string, error) {
		val, err := strconv.ParseFloat(normalizedValue, 64)
		if err != nil {
			return "", err
		}
		if val > 0.85 || val < 0 {
			return "", errors.New("The valid value of tidb_tiflash_auto_spill_ratio is between 0 and 0.85")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.TiFlashQuerySpillRatio = tidbOptFloat64(val, vardef.DefTiFlashQuerySpillRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashHashJoinVersion, Value: vardef.DefTiFlashHashJoinVersion, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			lowerValue := strings.ToLower(normalizedValue)
			if lowerValue != joinversion.HashJoinVersionLegacy && lowerValue != joinversion.HashJoinVersionOptimized {
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s", originalValue, vardef.TiFlashHashJoinVersion, joinversion.HashJoinVersionLegacy+", "+joinversion.HashJoinVersionOptimized)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashHashJoinVersion = val
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTiFlashPipelineMode, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBEnableTiFlashPipelineMode), SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.TiFlashEnablePipelineMode.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.TiFlashEnablePipelineMode.Load()), nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGenByArgs(vardef.TiDBEnableTiFlashPipelineMode))
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSnapshot, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		err := setSnapshotTS(s, val)
		if err != nil {
			return err
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptProjectionPushDown, Value: BoolToOnOff(vardef.DefOptEnableProjectionPushDown), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowProjectionPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptDeriveTopN, Value: BoolToOnOff(vardef.DefOptDeriveTopN), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDeriveTopN = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptAggPushDown, Value: BoolToOnOff(vardef.DefOptAggPushDown), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptDistinctAggPushDown, Value: BoolToOnOff(config.GetGlobalConfig().Performance.DistinctAggPushDown), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowDistinctAggPushDown = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSkewDistinctAgg, Value: BoolToOnOff(vardef.DefTiDBSkewDistinctAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableSkewDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOpt3StageDistinctAgg, Value: BoolToOnOff(vardef.DefTiDB3StageDistinctAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable3StageDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnable3StageMultiDistinctAgg, Value: BoolToOnOff(vardef.DefTiDB3StageMultiDistinctAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable3StageMultiDistinctAgg = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptExplainNoEvaledSubQuery, Value: BoolToOnOff(vardef.DefTiDBOptExplainEvaledSubquery), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ExplainNonEvaledSubQuery = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptWriteRowID, Value: BoolToOnOff(vardef.DefOptWriteRowID), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowWriteRowID = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBChecksumTableConcurrency, Value: strconv.Itoa(vardef.DefChecksumTableConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBBatchInsert, Value: BoolToOnOff(vardef.DefBatchInsert), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBBatchDelete, Value: BoolToOnOff(vardef.DefBatchDelete), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchDelete = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBBatchCommit, Value: BoolToOnOff(vardef.DefBatchCommit), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.BatchCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBCurrentTS, Value: strconv.Itoa(vardef.DefCurretTS), Type: vardef.TypeInt, AllowEmpty: true, MinValue: 0, MaxValue: math.MaxInt64, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return strconv.FormatUint(s.TxnCtx.StartTS, 10), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastTxnInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return s.LastTxnInfo, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastQueryInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastQueryInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBEnableChunkRPC, Value: BoolToOnOff(config.GetGlobalConfig().TiKVClient.EnableChunkRPC), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableChunkRPC = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TxnIsolationOneShot, Value: "", skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		s.txnIsolationLevelOneShot.state = oneShotSet
		s.txnIsolationLevelOneShot.value = val
		return nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		if s.txnIsolationLevelOneShot.state != oneShotDef {
			return s.txnIsolationLevelOneShot.value, true, nil
		}
		return "", false, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptimizerSelectivityLevel, Value: strconv.Itoa(vardef.DefTiDBOptimizerSelectivityLevel), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerSelectivityLevel = tidbOptPositiveInt32(val, vardef.DefTiDBOptimizerSelectivityLevel)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexPruneThreshold, Value: strconv.Itoa(vardef.DefTiDBOptIndexPruneThreshold), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.OptIndexPruneThreshold = TidbOptInt(val, vardef.DefTiDBOptIndexPruneThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptimizerEnableOuterJoinReorder, Value: BoolToOnOff(vardef.DefTiDBEnableOuterJoinReorder), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableOuterJoinReorder = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptimizerEnableNAAJ, Value: BoolToOnOff(vardef.DefTiDBEnableNAAJ), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNAAJ = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableSemiJoinRewrite, Value: BoolToOnOff(vardef.DefOptEnableSemiJoinRewrite), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableSemiJoinRewrite = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBDDLReorgPriority, Value: "PRIORITY_LOW", Type: vardef.TypeEnum, skipInit: true, PossibleValues: []string{"PRIORITY_LOW", "PRIORITY_NORMAL", "PRIORITY_HIGH"}, SetSession: func(s *SessionVars, val string) error {
		s.setDDLReorgPriority(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSlowQueryFile, Value: "", skipInit: true, SetSession: func(s *SessionVars, val string) error {
		s.SlowQueryFile = val
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBWaitSplitRegionFinish, Value: BoolToOnOff(vardef.DefTiDBWaitSplitRegionFinish), skipInit: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionFinish = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBWaitSplitRegionTimeout, Value: strconv.Itoa(vardef.DefWaitSplitRegionTimeout), skipInit: true, Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.WaitSplitRegionTimeout = uint64(tidbOptPositiveInt32(val, vardef.DefWaitSplitRegionTimeout))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBLowResolutionTSO, Value: vardef.Off, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.lowResolutionTSO = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBAllowRemoveAutoInc, Value: BoolToOnOff(vardef.DefTiDBAllowRemoveAutoInc), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowRemoveAutoInc = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBIsolationReadEngines, Value: strings.Join(config.GetGlobalConfig().IsolationRead.Engines, ","), Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		engines := strings.Split(normalizedValue, ",")
		var formatVal string
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			if i != 0 {
				formatVal += ","
			}
			switch {
			case strings.EqualFold(engine, kv.TiKV.Name()):
				formatVal += kv.TiKV.Name()
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				formatVal += kv.TiFlash.Name()
			case strings.EqualFold(engine, kv.TiDB.Name()):
				formatVal += kv.TiDB.Name()
			default:
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBIsolationReadEngines, normalizedValue)
			}
		}
		return formatVal, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.IsolationReadEngines = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			switch engine {
			case kv.TiKV.Name():
				s.IsolationReadEngines[kv.TiKV] = struct{}{}
			case kv.TiFlash.Name():
				// If the tiflash is removed by the strict SQL mode. The hint should also not take effect.
				if !s.StmtCtx.TiFlashEngineRemovedDueToStrictSQLMode {
					s.IsolationReadEngines[kv.TiFlash] = struct{}{}
				}
			case kv.TiDB.Name():
				s.IsolationReadEngines[kv.TiDB] = struct{}{}
			}
		}
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMetricSchemaStep, Value: strconv.Itoa(vardef.DefTiDBMetricSchemaStep), Type: vardef.TypeUnsigned, skipInit: true, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaStep = TidbOptInt64(val, vardef.DefTiDBMetricSchemaStep)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBCDCWriteSource, Value: "0", Type: vardef.TypeInt, MinValue: 0, MaxValue: 15, SetSession: func(s *SessionVars, val string) error {
		s.CDCWriteSource = uint64(TidbOptInt(val, 0))
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMetricSchemaRangeDuration, Value: strconv.Itoa(vardef.DefTiDBMetricSchemaRangeDuration), skipInit: true, Type: vardef.TypeUnsigned, MinValue: 10, MaxValue: 60 * 60 * 60, SetSession: func(s *SessionVars, val string) error {
		s.MetricSchemaRangeDuration = TidbOptInt64(val, vardef.DefTiDBMetricSchemaRangeDuration)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBFoundInPlanCache, Value: BoolToOnOff(vardef.DefTiDBFoundInPlanCache), Type: vardef.TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInPlanCache), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBFoundInBinding, Value: BoolToOnOff(vardef.DefTiDBFoundInBinding), Type: vardef.TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.PrevFoundInBinding), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.RandSeed1, Type: vardef.TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed1(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return strconv.FormatUint(uint64(s.Rng.GetSeed1()), 10), true, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.RandSeed2, Type: vardef.TypeInt, Value: "0", skipInit: true, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.Rng.SetSeed2(uint32(tidbOptPositiveInt32(val, 0)))
		return nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return "0", nil
	}, GetStateValue: func(s *SessionVars) (string, bool, error) {
		return strconv.FormatUint(uint64(s.Rng.GetSeed2()), 10), true, nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBReadConsistency, Value: string(ReadConsistencyStrict), Type: vardef.TypeStr, Hidden: true,
		Validation: func(_ *SessionVars, normalized string, _ string, _ vardef.ScopeFlag) (string, error) {
			return normalized, validateReadConsistencyLevel(normalized)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.ReadConsistency = ReadConsistencyLevel(val)
			return nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastDDLInfo, Value: "", ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		info, err := json.Marshal(s.LastDDLInfo)
		if err != nil {
			return "", err
		}
		return string(info), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBLastPlanReplayerToken, Value: "", ReadOnly: true,
		GetSession: func(s *SessionVars) (string, error) {
			return s.LastPlanReplayerToken, nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBUseAlloc, Value: BoolToOnOff(vardef.DefTiDBUseAlloc), Type: vardef.TypeBool, ReadOnly: true, GetSession: func(s *SessionVars) (string, error) {
		return BoolToOnOff(s.preUseChunkAlloc), nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBExplicitRequestSourceType, Value: "", Type: vardef.TypeEnum, PossibleValues: tikvcliutil.ExplicitTypeList, GetSession: func(s *SessionVars) (string, error) {
		return s.ExplicitRequestSourceType, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.ExplicitRequestSourceType = val
		return nil
	}},
}
