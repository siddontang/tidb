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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/pingcap/tidb/pkg/util/tikvutil"
	"github.com/pingcap/tidb/pkg/util/tls"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	tikvcfg "github.com/tikv/client-go/v2/config"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle/oracles"
)

// defaultSysVarsInstanceGlobal contains system variables with INSTANCE or GLOBAL scope.
var defaultSysVarsInstanceGlobal = []*SysVar{
	/* The system variables below have INSTANCE scope  */
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBLogFileMaxDays, Value: strconv.Itoa(config.GetGlobalConfig().Log.File.MaxDays), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		maxAge, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return err
		}
		vardef.GlobalLogMaxDays.Store(int32(maxAge))
		cfg := config.GetGlobalConfig().Log.ToLogConfig()
		cfg.Config.File.MaxDays = int(maxAge)

		err = logutil.ReplaceLogger(cfg, keyspace.WrapZapcoreWithKeyspace())
		if err != nil {
			return err
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(int64(vardef.GlobalLogMaxDays.Load()), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBConfig, Value: "", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetJSONConfig()
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBGeneralLog, Value: BoolToOnOff(vardef.DefTiDBGeneralLog), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.ProcessGeneralLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.ProcessGeneralLog.Load()), nil
	}},
	// NOTE: The trace-event switch is experimental. It is subject to changes.
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBTraceEvent, Hidden: kerneltype.IsClassic(), Value: vardef.DefTiDBTraceEvent, Type: vardef.TypeStr,
		SetGlobal: func(_ context.Context, _ *SessionVars, val string) error {
			if kerneltype.IsClassic() {
				return errors.New("can only be set for TiDB X kernel")
			}
			if val == "" {
				// Reset the flight recorder
				recorder := traceevent.GetFlightRecorder()
				if recorder != nil {
					recorder.Close()
				}
				return nil
			}
			var config traceevent.FlightRecorderConfig
			err := json.Unmarshal([]byte(val), &config)
			if err != nil {
				return errors.Trace(err)
			}
			err = traceevent.StartLogFlightRecorder(&config)
			return err
		},
		GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
			recorder := traceevent.GetFlightRecorder()
			if recorder != nil {
				if recorder.Config != nil {
					data, err := json.Marshal(recorder.Config)
					if err != nil {
						return "", errors.Trace(err)
					}
					return string(data), nil
				}
			}
			return "", nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSlowTxnLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowTxnThreshold),
		Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
			s.SlowTxnThreshold = TidbOptUint64(val, logutil.DefaultSlowTxnThreshold)
			return nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBSlowLogThreshold, Value: strconv.Itoa(logutil.DefaultSlowThreshold), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			atomic.StoreUint64(&config.GetGlobalConfig().Instance.SlowThreshold, uint64(TidbOptInt64(val, logutil.DefaultSlowThreshold)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Instance.SlowThreshold), 10), nil
		}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBRecordPlanInSlowLog, Value: int32ToBoolStr(logutil.DefaultRecordPlanInSlowLog), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog, uint32(TidbOptInt64(val, logutil.DefaultRecordPlanInSlowLog)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		enabled := atomic.LoadUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog) == 1
		return BoolToOnOff(enabled), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableSlowLog, Value: BoolToOnOff(logutil.DefaultTiDBEnableSlowLog), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.EnableSlowLog.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableSlowLog.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBCheckMb4ValueInUTF8, Value: BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBPProfSQLCPU, Value: strconv.Itoa(vardef.DefTiDBPProfSQLCPU), Type: vardef.TypeInt, MinValue: 0, MaxValue: 1, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnablePProfSQLCPU.Store(uint32(tidbOptPositiveInt32(val, vardef.DefTiDBPProfSQLCPU)) > 0)
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		val := "0"
		if vardef.EnablePProfSQLCPU.Load() {
			val = "1"
		}
		return val, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBDDLSlowOprThreshold, Value: strconv.Itoa(vardef.DefTiDBDDLSlowOprThreshold), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint32(&vardef.DDLSlowOprThreshold, uint32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLSlowOprThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(atomic.LoadUint32(&vardef.DDLSlowOprThreshold)), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBForcePriority, Value: mysql.Priority2Str[vardef.DefTiDBForcePriority], Type: vardef.TypeEnum, PossibleValues: []string{"NO_PRIORITY", "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED"}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreInt32(&vardef.ForcePriority, int32(mysql.Str2Priority(val)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&vardef.ForcePriority))], nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBExpensiveQueryTimeThreshold, Value: strconv.Itoa(vardef.DefTiDBExpensiveQueryTimeThreshold), Type: vardef.TypeUnsigned, MinValue: int64(vardef.MinExpensiveQueryTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&vardef.ExpensiveQueryTimeThreshold, uint64(tidbOptPositiveInt32(val, vardef.DefTiDBExpensiveQueryTimeThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&vardef.ExpensiveQueryTimeThreshold), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBExpensiveTxnTimeThreshold, Value: strconv.Itoa(vardef.DefTiDBExpensiveTxnTimeThreshold), Type: vardef.TypeUnsigned, MinValue: int64(vardef.MinExpensiveTxnTimeThreshold), MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		atomic.StoreUint64(&vardef.ExpensiveTxnTimeThreshold, uint64(tidbOptPositiveInt32(val, vardef.DefTiDBExpensiveTxnTimeThreshold)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(atomic.LoadUint64(&vardef.ExpensiveTxnTimeThreshold), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableCollectExecutionInfo, Value: BoolToOnOff(vardef.DefTiDBEnableCollectExecutionInfo), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		oldConfig := config.GetGlobalConfig()
		newValue := TiDBOptOn(val)
		if oldConfig.Instance.EnableCollectExecutionInfo.Load() != newValue {
			newConfig := *oldConfig
			newConfig.Instance.EnableCollectExecutionInfo.Store(newValue)
			config.StoreGlobalConfig(&newConfig)
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.PluginLoad, Value: "", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginLoad, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.PluginDir, Value: "/data/deploy/plugin", ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.PluginDir, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.PluginAuditLogBufferSize, Value: strconv.Itoa(config.GetGlobalConfig().Instance.PluginAuditLogFlushInterval), ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.PluginAuditLogBufferSize), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.PluginAuditLogFlushInterval, Value: strconv.Itoa(config.GetGlobalConfig().Instance.PluginAuditLogFlushInterval), ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.PluginAuditLogFlushInterval), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.MaxConnections, Value: strconv.FormatUint(uint64(config.GetGlobalConfig().Instance.MaxConnections), 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 100000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		config.GetGlobalConfig().Instance.MaxConnections = uint32(TidbOptInt64(val, 0))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatUint(uint64(config.GetGlobalConfig().Instance.MaxConnections), 10), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableDDL, Value: BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			oldVal, newVal := config.GetGlobalConfig().Instance.TiDBEnableDDL.Load(), TiDBOptOn(val)
			if oldVal != newVal {
				err := switchDDL(newVal)
				if err != nil {
					return err
				}
				config.GetGlobalConfig().Instance.TiDBEnableDDL.Store(newVal)
			}
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableDDL.Load()), nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBEnableStatsOwner, Value: BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load()), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			oldVal, newVal := config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load(), TiDBOptOn(val)
			if oldVal != newVal {
				err := switchStats(newVal)
				if err != nil {
					return err
				}
				config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Store(newVal)
			}
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load()), nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBRCReadCheckTS, Value: BoolToOnOff(vardef.DefRCReadCheckTS), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableRCReadCheckTS.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableRCReadCheckTS.Load()), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryEnablePersistent, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return BoolToOnOff(config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFilename, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return config.GetGlobalConfig().Instance.StmtSummaryFilename, nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFileMaxDays, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxDays), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFileMaxSize, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxSize), nil
	}},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryFileMaxBackups, ReadOnly: true, GetGlobal: func(_ context.Context, _ *SessionVars) (string, error) {
		return strconv.Itoa(config.GetGlobalConfig().Instance.StmtSummaryFileMaxBackups), nil
	}},

	/* The system variables below have GLOBAL scope  */
	{Scope: vardef.ScopeGlobal, Name: vardef.MaxPreparedStmtCount, Value: strconv.FormatInt(vardef.DefMaxPreparedStmtCount, 10), Type: vardef.TypeInt, MinValue: -1, MaxValue: 1048576,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return errors.Trace(err)
			}
			vardef.MaxPreparedStmtCountValue.Store(num)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.InitConnect, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		p := parser.New()
		p.SetSQLMode(vars.SQLMode)
		p.SetParserConfig(vars.BuildParserConfig())
		_, _, err := p.ParseSQL(normalizedValue)
		if err != nil {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(vardef.InitConnect)
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordEnable, Value: vardef.Off, Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordPolicy, Value: "MEDIUM", Type: vardef.TypeEnum, PossibleValues: []string{"LOW", "MEDIUM", "STRONG"}},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordCheckUserName, Value: vardef.On, Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordLength, Value: "8", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			numberCount, specialCharCount, mixedCaseCount := vardef.PasswordValidtaionNumberCount.Load(), vardef.PasswordValidationSpecialCharCount.Load(), vardef.PasswordValidationMixedCaseCount.Load()
			length, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := numberCount + specialCharCount + 2*mixedCaseCount; int32(length) < minLength {
				return strconv.FormatInt(int64(minLength), 10), nil
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidationLength.Store(int32(TidbOptInt64(val, 8)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidationLength.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordMixedCaseCount, Value: "1", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			length, numberCount, specialCharCount := vardef.PasswordValidationLength.Load(), vardef.PasswordValidtaionNumberCount.Load(), vardef.PasswordValidationSpecialCharCount.Load()
			mixedCaseCount, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := numberCount + specialCharCount + 2*int32(mixedCaseCount); length < minLength {
				err = updatePasswordValidationLength(vars, minLength)
				if err != nil {
					return "", err
				}
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidationMixedCaseCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidationMixedCaseCount.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordNumberCount, Value: "1", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			length, specialCharCount, mixedCaseCount := vardef.PasswordValidationLength.Load(), vardef.PasswordValidationSpecialCharCount.Load(), vardef.PasswordValidationMixedCaseCount.Load()
			numberCount, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := int32(numberCount) + specialCharCount + 2*mixedCaseCount; length < minLength {
				err = updatePasswordValidationLength(vars, minLength)
				if err != nil {
					return "", err
				}
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidtaionNumberCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidtaionNumberCount.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordSpecialCharCount, Value: "1", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			length, numberCount, mixedCaseCount := vardef.PasswordValidationLength.Load(), vardef.PasswordValidtaionNumberCount.Load(), vardef.PasswordValidationMixedCaseCount.Load()
			specialCharCount, err := strconv.ParseInt(normalizedValue, 10, 32)
			if err != nil {
				return "", err
			}
			if minLength := numberCount + int32(specialCharCount) + 2*mixedCaseCount; length < minLength {
				err = updatePasswordValidationLength(vars, minLength)
				if err != nil {
					return "", err
				}
			}
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.PasswordValidationSpecialCharCount.Store(int32(TidbOptInt64(val, 1)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.PasswordValidationSpecialCharCount.Load()), 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.ValidatePasswordDictionary, Value: "", Type: vardef.TypeStr},
	{Scope: vardef.ScopeGlobal, Name: vardef.DefaultPasswordLifetime, Value: "0", Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxUint16},
	{Scope: vardef.ScopeGlobal, Name: vardef.DisconnectOnExpiredPassword, Value: vardef.On, Type: vardef.TypeBool, ReadOnly: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(!vardef.IsSandBoxModeEnabled.Load()), nil
	}},

	/* TiDB specific variables */
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTSOClientBatchMaxWaitTime, Value: strconv.FormatFloat(vardef.DefTiDBTSOClientBatchMaxWaitTime, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 10,
		GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.MaxTSOBatchWaitInterval.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return (*SetPDClientDynamicOption.Load())(vardef.TiDBTSOClientBatchMaxWaitTime, val)
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTSOFollowerProxy, Value: BoolToOnOff(vardef.DefTiDBEnableTSOFollowerProxy), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableTSOFollowerProxy.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(vardef.TiDBEnableTSOFollowerProxy, val)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.PDEnableFollowerHandleRegion, Value: BoolToOnOff(vardef.DefPDEnableFollowerHandleRegion), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnablePDFollowerHandleRegion.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(vardef.PDEnableFollowerHandleRegion, val)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableBatchQueryRegion, Value: BoolToOnOff(vardef.DefTiDBEnableBatchQueryRegion), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableBatchQueryRegion.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return (*SetPDClientDynamicOption.Load())(vardef.TiDBEnableBatchQueryRegion, val)
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableLocalTxn, Value: BoolToOnOff(vardef.DefTiDBEnableLocalTxn), Hidden: true, Type: vardef.TypeBool, Depended: true, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableLocalTxn.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		oldVal := vardef.EnableLocalTxn.Load()
		newVal := TiDBOptOn(val)
		// Make sure the TxnScope is always Global when disable the Local Txn.
		// ON -> OFF
		if oldVal && !newVal {
			s.TxnScope = kv.NewGlobalTxnScopeVar()
		}
		vardef.EnableLocalTxn.Store(newVal)
		return nil
	}},
	{
		Scope:    vardef.ScopeGlobal,
		Name:     vardef.TiDBAutoAnalyzeRatio,
		Value:    strconv.FormatFloat(vardef.DefAutoAnalyzeRatio, 'f', -1, 64),
		Type:     vardef.TypeFloat,
		MinValue: 0,
		MaxValue: math.MaxUint64,
		// The value of TiDBAutoAnalyzeRatio should be greater than 0.00001 or equal to 0.00001.
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			ratio, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				return "", err
			}
			const minRatio = 0.00001
			const tolerance = 1e-9
			if ratio < minRatio && math.Abs(ratio-minRatio) > tolerance {
				return "", errors.Errorf("the value of %s should be greater than or equal to %f", vardef.TiDBAutoAnalyzeRatio, minRatio)
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzeStartTime, Value: vardef.DefAutoAnalyzeStartTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzeEndTime, Value: vardef.DefAutoAnalyzeEndTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal | vardef.ScopeInstance, Name: vardef.TiDBMemQuotaBindingCache, Value: strconv.FormatInt(vardef.DefTiDBMemQuotaBindingCache, 10), Type: vardef.TypeUnsigned, MaxValue: math.MaxInt32, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.MemQuotaBindingCache.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.MemQuotaBindingCache.Store(TidbOptInt64(val, vardef.DefTiDBMemQuotaBindingCache))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLFlashbackConcurrency, Value: strconv.Itoa(vardef.DefTiDBDDLFlashbackConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLFlashbackConcurrency(int32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLFlashbackConcurrency)))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDDLReorgWorkerCount, Value: strconv.Itoa(vardef.DefTiDBDDLReorgWorkerCount), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLReorgWorkerCounter(int32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLReorgWorkerCount)))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDDLReorgBatchSize, Value: strconv.Itoa(vardef.DefTiDBDDLReorgBatchSize), Type: vardef.TypeUnsigned, MinValue: int64(vardef.MinDDLReorgBatchSize), MaxValue: uint64(vardef.MaxDDLReorgBatchSize), SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLReorgBatchSize(int32(tidbOptPositiveInt32(val, vardef.DefTiDBDDLReorgBatchSize)))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLReorgMaxWriteSpeed, Value: strconv.Itoa(vardef.DefTiDBDDLReorgMaxWriteSpeed), Type: vardef.TypeStr,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			i64, err := units.RAMInBytes(val)
			if err != nil {
				return errors.Trace(err)
			}
			if i64 < 0 || i64 > units.PiB {
				// Here we limit the max value to 1 PiB instead of math.MaxInt64, since:
				// 1. it is large enough
				// 2. units.RAMInBytes would first cast the size to a float, and may lose precision when the size is too large
				return fmt.Errorf("invalid value for '%d', it should be within [%d, %d]", i64, 0, units.PiB)
			}
			vardef.DDLReorgMaxWriteSpeed.Store(i64)
			return nil
		}, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.DDLReorgMaxWriteSpeed.Load(), 10), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLErrorCountLimit, Value: strconv.Itoa(vardef.DefTiDBDDLErrorCountLimit), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLErrorCountLimit(TidbOptInt64(val, vardef.DefTiDBDDLErrorCountLimit))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMaxDeltaSchemaCount, Value: strconv.Itoa(vardef.DefTiDBMaxDeltaSchemaCount), Type: vardef.TypeUnsigned, MinValue: 100, MaxValue: 16384, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		// It's a global variable, but it also wants to be cached in server.
		vardef.SetMaxDeltaSchemaCount(TidbOptInt64(val, vardef.DefTiDBMaxDeltaSchemaCount))
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBEnablePointGetCache, Value: BoolToOnOff(vardef.DefTiDBPointGetCache), Hidden: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePointGetCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBScatterRegion, Value: vardef.DefTiDBScatterRegion, PossibleValues: []string{vardef.ScatterOff, vardef.ScatterTable, vardef.ScatterGlobal}, Type: vardef.TypeStr,
		SetSession: func(vars *SessionVars, val string) error {
			vars.ScatterRegion = val
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return vars.ScatterRegion, nil
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			lowerVal := strings.ToLower(normalizedValue)
			if lowerVal != vardef.ScatterOff && lowerVal != vardef.ScatterTable && lowerVal != vardef.ScatterGlobal {
				return "", fmt.Errorf("invalid value for '%s', it should be either '%s', '%s' or '%s'", lowerVal, vardef.ScatterOff, vardef.ScatterTable, vardef.ScatterGlobal)
			}
			return lowerVal, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableStmtSummary, Value: BoolToOnOff(vardef.DefTiDBEnableStmtSummary), Type: vardef.TypeBool, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetEnabled(TiDBOptOn(val))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryInternalQuery, Value: BoolToOnOff(vardef.DefTiDBStmtSummaryInternalQuery), Type: vardef.TypeBool, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetEnableInternalQuery(TiDBOptOn(val))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryRefreshInterval, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryRefreshInterval), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			// convert val to int64
			return stmtsummaryv2.SetRefreshInterval(TidbOptInt64(val, vardef.DefTiDBStmtSummaryRefreshInterval))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryHistorySize, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryHistorySize), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetHistorySize(TidbOptInt(val, vardef.DefTiDBStmtSummaryHistorySize))
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeInstance, Name: vardef.TiDBStmtSummaryMaxStmtCount, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryMaxStmtCount), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxInt16, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetMaxStmtCount(TidbOptInt(val, vardef.DefTiDBStmtSummaryMaxStmtCount))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStmtSummaryMaxSQLLength, Value: strconv.Itoa(vardef.DefTiDBStmtSummaryMaxSQLLength), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return stmtsummaryv2.SetMaxSQLLength(TidbOptInt(val, vardef.DefTiDBStmtSummaryMaxSQLLength))
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCapturePlanBaseline, Value: vardef.DefTiDBCapturePlanBaseline, Type: vardef.TypeBool, AllowEmptyAll: true},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEvolvePlanTaskMaxTime, Value: strconv.Itoa(vardef.DefTiDBEvolvePlanTaskMaxTime), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEvolvePlanTaskStartTime, Value: vardef.DefTiDBEvolvePlanTaskStartTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEvolvePlanTaskEndTime, Value: vardef.DefTiDBEvolvePlanTaskEndTime, Type: vardef.TypeTime},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStoreLimit, Value: strconv.FormatInt(atomic.LoadInt64(&config.GetGlobalConfig().TiKVClient.StoreLimit), 10), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(tikvstore.StoreLimit.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		tikvstore.StoreLimit.Store(TidbOptInt64(val, vardef.DefTiDBStoreLimit))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTxnCommitBatchSize, Value: strconv.FormatUint(tikvstore.DefTxnCommitBatchSize, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 1 << 30,
		GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
			return strconv.FormatUint(tikvstore.TxnCommitBatchSize.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			tikvstore.TxnCommitBatchSize.Store(uint64(TidbOptInt64(val, int64(tikvstore.DefTxnCommitBatchSize))))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBRestrictedReadOnly, Value: BoolToOnOff(vardef.DefTiDBRestrictedReadOnly), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		// For user initiated SET GLOBAL, also change the value of TiDBSuperReadOnly
		if on && s.StmtCtx.StmtType == "Set" {
			err := s.GlobalVarsAccessor.SetGlobalSysVarOnly(context.Background(), vardef.TiDBSuperReadOnly, "ON", false)
			if err != nil {
				return err
			}
			err = GetSysVar(vardef.TiDBSuperReadOnly).SetGlobal(context.Background(), s, "ON")
			if err != nil {
				return err
			}
		}
		vardef.RestrictedReadOnly.Store(on)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSuperReadOnly, Value: BoolToOnOff(vardef.DefTiDBSuperReadOnly), Type: vardef.TypeBool, Validation: func(s *SessionVars, normalizedValue string, _ string, _ vardef.ScopeFlag) (string, error) {
		on := TiDBOptOn(normalizedValue)
		if !on && s.StmtCtx.StmtType == "Set" {
			result, err := s.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBRestrictedReadOnly)
			if err != nil {
				return normalizedValue, err
			}
			if TiDBOptOn(result) {
				return normalizedValue, fmt.Errorf("can't turn off %s when %s is on", vardef.TiDBSuperReadOnly, vardef.TiDBRestrictedReadOnly)
			}
		}
		return normalizedValue, nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.VarTiDBSuperReadOnly.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableGOGCTuner, Value: BoolToOnOff(vardef.DefTiDBEnableGOGCTuner), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		on := TiDBOptOn(val)
		gctuner.EnableGOGCTuner.Store(on)
		if !on {
			gctuner.SetDefaultGOGC()
		}
		gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGOGCTunerMaxValue, Value: strconv.Itoa(vardef.DefTiDBGOGCMaxValue),
		Type: vardef.TypeInt, MinValue: 10, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			maxValue := TidbOptInt64(val, vardef.DefTiDBGOGCMaxValue)
			gctuner.SetMaxGCPercent(uint32(maxValue))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(int64(gctuner.MaxGCPercent()), 10), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, origin string, scope vardef.ScopeFlag) (string, error) {
			maxValue := TidbOptInt64(origin, vardef.DefTiDBGOGCMaxValue)
			if maxValue <= int64(gctuner.MinGCPercent()) {
				return "", errors.New("tidb_gogc_tuner_max_value should be more than tidb_gogc_tuner_min_value")
			}
			return origin, nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGOGCTunerMinValue, Value: strconv.Itoa(vardef.DefTiDBGOGCMinValue),
		Type: vardef.TypeInt, MinValue: 10, MaxValue: math.MaxInt32, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			minValue := TidbOptInt64(val, vardef.DefTiDBGOGCMinValue)
			gctuner.SetMinGCPercent(uint32(minValue))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(int64(gctuner.MinGCPercent()), 10), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, origin string, scope vardef.ScopeFlag) (string, error) {
			minValue := TidbOptInt64(origin, vardef.DefTiDBGOGCMinValue)
			if minValue >= int64(gctuner.MaxGCPercent()) {
				return "", errors.New("tidb_gogc_tuner_min_value should be less than tidb_gogc_tuner_max_value")
			}
			return origin, nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTelemetry, Value: BoolToOnOff(vardef.DefTiDBEnableTelemetry), Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableHistoricalStats, Value: vardef.Off, Type: vardef.TypeBool, Depended: true},
	/* tikv gc metrics */
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCEnable, Value: vardef.On, Type: vardef.TypeBool, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_enable", vardef.On)
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_enable", val, "Current GC enable status")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCRunInterval, Value: "10m0s", Type: vardef.TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_run_interval", "10m0s")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_run_interval", val, "GC run interval, at least 10m, in Go format.")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCLifetime, Value: "10m0s", Type: vardef.TypeDuration, MinValue: int64(time.Minute * 10), MaxValue: uint64(time.Hour * 24 * 365), GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_life_time", "10m0s")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_life_time", val, "All versions within life time will not be collected by GC, at least 10m, in Go format.")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCConcurrency, Value: "-1", Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		autoConcurrencyVal, err := getTiDBTableValue(s, "tikv_gc_auto_concurrency", vardef.On)
		if err == nil && autoConcurrencyVal == vardef.On {
			return "-1", nil // convention for "AUTO"
		}
		return getTiDBTableValue(s, "tikv_gc_concurrency", "-1")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		autoConcurrency := vardef.Off
		if val == "-1" {
			autoConcurrency = vardef.On
		}
		// Update both autoconcurrency and concurrency.
		if err := setTiDBTableValue(s, "tikv_gc_auto_concurrency", autoConcurrency, "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used"); err != nil {
			return err
		}
		return setTiDBTableValue(s, "tikv_gc_concurrency", val, "How many goroutines used to do GC parallel, [1, 256], default 2")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCScanLockMode, Value: "LEGACY", Type: vardef.TypeEnum, PossibleValues: []string{"PHYSICAL", "LEGACY"}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return getTiDBTableValue(s, "tikv_gc_scan_lock_mode", "LEGACY")
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		return setTiDBTableValue(s, "tikv_gc_scan_lock_mode", val, "Mode of scanning locks, \"physical\" or \"legacy\"")
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGCMaxWaitTime, Value: strconv.Itoa(vardef.DefTiDBGCMaxWaitTime), Type: vardef.TypeInt, MinValue: 600, MaxValue: 31536000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.GCMaxWaitTime.Store(TidbOptInt64(val, vardef.DefTiDBGCMaxWaitTime))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTableCacheLease, Value: strconv.Itoa(vardef.DefTiDBTableCacheLease), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 10, SetGlobal: func(_ context.Context, s *SessionVars, sVal string) error {
		var val int64
		val, err := strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		vardef.TableCacheLease.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzePartitionBatchSize,
		Value: strconv.Itoa(vardef.DefTiDBAutoAnalyzePartitionBatchSize),
		Type:  vardef.TypeUnsigned, MinValue: 1, MaxValue: mysql.PartitionCountLimit,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			var val int64
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return errors.Trace(err)
			}
			vardef.AutoAnalyzePartitionBatchSize.Store(val)
			return nil
		}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxNoReplacement.FastGenByArgs(vardef.TiDBAutoAnalyzePartitionBatchSize))
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.MaxUserConnections, Value: strconv.FormatUint(vardef.DefMaxUserConnections, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 100000,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.MaxUserConnectionsValue.Store(uint32(TidbOptInt64(val, vardef.DefMaxUserConnections)))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatUint(uint64(vardef.MaxUserConnectionsValue.Load()), 10), nil
		},
	},
	// variable for top SQL feature.
	// TopSQL enable only be controlled by TopSQL pub/sub sinker.
	// This global variable only uses to update the global config which store in PD(ETCD).
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTopSQL, Value: BoolToOnOff(topsqlstate.DefTiDBTopSQLEnable), Type: vardef.TypeBool, AllowEmpty: true, GlobalConfigName: vardef.GlobalConfigEnableTopSQL},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSourceID, Value: "1", Type: vardef.TypeInt, MinValue: 1, MaxValue: 15, GlobalConfigName: vardef.GlobalConfigSourceID},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTopSQLMaxTimeSeriesCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxTimeSeriesCount), Type: vardef.TypeInt, MinValue: 1, MaxValue: 5000, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxStatementCount.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxStatementCount.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTopSQLMaxMetaCount, Value: strconv.Itoa(topsqlstate.DefTiDBTopSQLMaxMetaCount), Type: vardef.TypeInt, MinValue: 1, MaxValue: 10000, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(topsqlstate.GlobalState.MaxCollect.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		topsqlstate.GlobalState.MaxCollect.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.SkipNameResolve, Value: vardef.Off, Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal, Name: vardef.DefaultAuthPlugin, Value: mysql.AuthNativePassword, Type: vardef.TypeEnum, PossibleValues: []string{mysql.AuthNativePassword, mysql.AuthCachingSha2Password, mysql.AuthTiDBSM3Password, mysql.AuthLDAPSASL, mysql.AuthLDAPSimple}},
	{
		Scope: vardef.ScopeGlobal,
		Name:  vardef.TiDBPersistAnalyzeOptions,
		Value: BoolToOnOff(vardef.DefTiDBPersistAnalyzeOptions),
		Type:  vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.PersistAnalyzeOptions.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			persist := TiDBOptOn(val)
			vardef.PersistAnalyzeOptions.Store(persist)
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableAutoAnalyze, Value: BoolToOnOff(vardef.DefTiDBEnableAutoAnalyze), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.RunAutoAnalyze.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.RunAutoAnalyze.Store(TiDBOptOn(val))
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal,
		Name:  vardef.TiDBAnalyzeColumnOptions,
		Value: vardef.DefTiDBAnalyzeColumnOptions,
		Type:  vardef.TypeStr,
		GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
			return vardef.AnalyzeColumnOptions.Load(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.AnalyzeColumnOptions.Store(strings.ToUpper(val))
			return nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			choice := strings.ToUpper(normalizedValue)
			if choice != ast.AllColumns.String() && choice != ast.PredicateColumns.String() {
				return "", errors.Errorf(
					"invalid value for %s, it should be either '%s' or '%s'",
					vardef.TiDBAnalyzeColumnOptions,
					ast.AllColumns.String(),
					ast.PredicateColumns.String(),
				)
			}
			return normalizedValue, nil
		},
	},
	{
		Scope: vardef.ScopeGlobal,
		Name:  vardef.TiDBEnableAutoAnalyzePriorityQueue,
		Value: BoolToOnOff(vardef.DefTiDBEnableAutoAnalyzePriorityQueue),
		Type:  vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableAutoAnalyzePriorityQueue.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.EnableAutoAnalyzePriorityQueue.Store(TiDBOptOn(val))
			return nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if !TiDBOptOn(normalizedValue) {
				return "", errors.New("tidb_enable_auto_analyze_priority_queue has been deprecated and TiDB will always use priority queue to schedule auto analyze")
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGOGCTunerThreshold, Value: strconv.FormatFloat(vardef.DefTiDBGOGCTunerThreshold, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.GOGCTunerThreshold.Load(), 'f', -1, 64), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			floatValue := tidbOptFloat64(normalizedValue, vardef.DefTiDBGOGCTunerThreshold)
			globalMemoryLimitTuner := gctuner.GlobalMemoryLimitTuner.GetPercentage()
			if floatValue < 0 && floatValue > 0.9 {
				return "", ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBGOGCTunerThreshold, normalizedValue)
			}
			// globalMemoryLimitTuner must not be 0. it will be 0 when tidb_server_memory_limit_gc_trigger is not set during startup.
			if globalMemoryLimitTuner != 0 && globalMemoryLimitTuner < floatValue+0.05 {
				return "", errors.New("tidb_gogc_tuner_threshold should be less than tidb_server_memory_limit_gc_trigger - 0.05")
			}
			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) (err error) {
			factor := tidbOptFloat64(val, vardef.DefTiDBGOGCTunerThreshold)
			vardef.GOGCTunerThreshold.Store(factor)
			memTotal := memory.ServerMemoryLimit.Load()
			if memTotal == 0 {
				memTotal, err = memory.MemTotal()
				if err != nil {
					return err
				}
			}
			if factor > 0 {
				threshold := float64(memTotal) * factor
				gctuner.Tuning(uint64(threshold))
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeInstance, Name: vardef.TiDBServerMemoryLimit, Value: vardef.DefTiDBServerMemoryLimit, Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.ServerMemoryLimitOriginText.Load(), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			_, str, err := parseMemoryLimit(s, normalizedValue, originalValue)
			if err != nil {
				return "", err
			}
			return str, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			bt, str, err := parseMemoryLimit(s, val, val)
			if err != nil {
				return err
			}
			memory.ServerMemoryLimitOriginText.Store(str)
			memory.ServerMemoryLimit.Store(bt)
			threshold := float64(bt) * vardef.GOGCTunerThreshold.Load()
			gctuner.Tuning(uint64(threshold))
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			memory.AjustGlobalMemArbitratorLimit()
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemArbitratorSoftLimit, Value: vardef.DefTiDBMemArbitratorSoftLimitText, Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if normalizedValue == memory.ArbitratorSoftLimitModDisableName {
				return normalizedValue, nil
			}
			if strings.ToLower(normalizedValue) == memory.ArbitratorSoftLimitModeAutoName {
				return memory.ArbitratorSoftLimitModeAutoName, nil
			}
			if v, err := strconv.ParseInt(normalizedValue, 10, 64); err == nil {
				if v > 1 {
					return normalizedValue, nil
				} else if v <= 0 {
					return "", ErrTiDBMemArbitratorSoftLimit
				}
				// v == 1 is legal
			}
			floatValue, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				return "", err
			}
			if floatValue > 0 && floatValue <= 1 {
				return normalizedValue, nil
			}
			return "", ErrTiDBMemArbitratorSoftLimit
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.GetGlobalMemArbitratorSoftLimitText(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, str string) error {
			memory.SetGlobalMemArbitratorSoftLimit(str)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemArbitratorMode, Value: vardef.DefTiDBMemArbitratorModeText, Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			normalizedValue = strings.ToLower(normalizedValue)
			switch normalizedValue {
			case memory.ArbitratorModeDisableName, memory.ArbitratorModeStandardName, memory.ArbitratorModePriorityName:
				return normalizedValue, nil
			default:
				return "", fmt.Errorf("%s: %s; %s; %s;",
					vardef.TiDBMemArbitratorMode,
					memory.ArbitratorModeDisableName,
					memory.ArbitratorModeStandardName,
					memory.ArbitratorModePriorityName)
			}
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.GetGlobalMemArbitratorWorkModeText(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, str string) error {
			memory.SetGlobalMemArbitratorWorkMode(str)
			return nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMemArbitratorWaitAverse, Value: vardef.DefTiDBMemArbitratorWaitAverse, Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if normalizedValue == "0" || normalizedValue == "1" || normalizedValue == "nolimit" {
				return normalizedValue, nil
			}
			return "", ErrTiDBMemArbitratorWaitAverse
		},
		SetSession: func(s *SessionVars, val string) error {
			switch val {
			case "0":
				s.MemArbitrator.WaitAverse = MemArbitratorWaitAverseDisable
			case "1":
				s.MemArbitrator.WaitAverse = MemArbitratorWaitAverseEnable
			default:
				s.MemArbitrator.WaitAverse = MemArbitratorNolimit
			}
			return nil
		},
	},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMemArbitratorQueryReserved, Value: vardef.DefTiDBMemArbitratorQueryReservedText, Type: vardef.TypeStr,
		IsHintUpdatableVerified: true,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if normalizedValue == "0" {
				return normalizedValue, nil
			}
			if v, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil && int64(v) > 1 {
				return normalizedValue, nil
			}
			return "", ErrTiDBMemArbitratorQueryReserved
		},
		SetSession: func(s *SessionVars, val string) error {
			intValue, err := strconv.ParseUint(val, 10, 64)
			if err == nil {
				s.MemArbitrator.QueryReserved = int64(intValue)
				return nil
			}
			return err
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBServerMemoryLimitSessMinSize, Value: strconv.FormatUint(vardef.DefTiDBServerMemoryLimitSessMinSize, 10), Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return memory.ServerMemoryLimitSessMinSize.String(), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			intVal, err := strconv.ParseUint(normalizedValue, 10, 64)
			if err != nil {
				bt, str := parseByteSize(normalizedValue)
				if str == "" {
					return "", err
				}
				intVal = bt
			}
			if intVal > 0 && intVal < 128 { // 128 Bytes
				s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBServerMemoryLimitSessMinSize, originalValue))
				intVal = 128
			}
			return strconv.FormatUint(intVal, 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			intVal, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return err
			}
			memory.ServerMemoryLimitSessMinSize.Store(intVal)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeInstance, Name: vardef.TiDBServerMemoryLimitGCTrigger, Value: strconv.FormatFloat(vardef.DefTiDBServerMemoryLimitGCTrigger, 'f', -1, 64), Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(gctuner.GlobalMemoryLimitTuner.GetPercentage(), 'f', -1, 64), nil
		},
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			floatValue, err := strconv.ParseFloat(normalizedValue, 64)
			if err != nil {
				perc, str := parsePercentage(normalizedValue)
				if len(str) == 0 {
					return "", err
				}
				floatValue = float64(perc) / 100
			}
			gogcTunerThreshold := vardef.GOGCTunerThreshold.Load()
			if floatValue < 0.51 || floatValue > 1 { // 51% ~ 100%
				return "", ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBServerMemoryLimitGCTrigger, normalizedValue)
			}
			// gogcTunerThreshold must not be 0. it will be 0 when tidb_gogc_tuner_threshold is not set during startup.
			if gogcTunerThreshold != 0 && floatValue < gogcTunerThreshold+0.05 {
				return "", errors.New("tidb_server_memory_limit_gc_trigger should be greater than tidb_gogc_tuner_threshold + 0.05")
			}

			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			floatValue, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return err
			}
			gctuner.GlobalMemoryLimitTuner.SetPercentage(floatValue)
			gctuner.GlobalMemoryLimitTuner.UpdateMemoryLimit()
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableColumnTracking,
		Value: BoolToOnOff(true),
		Type:  vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(true), nil
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			// This variable is deprecated and will be removed in the future.
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("The 'tidb_enable_column_tracking' variable is deprecated and will be removed in future versions of TiDB. It is always set to 'ON' now."))
			return normalizedValue, nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.RequireSecureTransport, Value: BoolToOnOff(vardef.DefRequireSecureTransport), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(tls.RequireSecureTransport.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			tls.RequireSecureTransport.Store(TiDBOptOn(val))
			return nil
		}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if vars.StmtCtx.StmtType == "Set" && TiDBOptOn(normalizedValue) {
				// On tidbcloud dedicated cluster with the default configuration, if an user modify
				// @@global.require_secure_transport=on, he can not login the cluster anymore!
				// A workaround for this is making require_secure_transport read-only for that case.
				// SEM(security enhanced mode) is enabled by default with only that settings.
				cfg := config.GetGlobalConfig()
				if cfg.Security.EnableSEM {
					return "", errors.New("require_secure_transport can not be set to ON with SEM(security enhanced mode) enabled")
				}
				// Refuse to set RequireSecureTransport to ON if the connection
				// issuing the change is not secure. This helps reduce the chance of users being locked out.
				if vars.TLSConnectionState == nil {
					return "", errors.New("require_secure_transport can only be set to ON if the connection issuing the change is secure")
				}
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBStatsLoadPseudoTimeout, Value: BoolToOnOff(vardef.DefTiDBStatsLoadPseudoTimeout), Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.StatsLoadPseudoTimeout.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.StatsLoadPseudoTimeout.Store(TiDBOptOn(val))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableBatchDML, Value: BoolToOnOff(vardef.DefTiDBEnableBatchDML), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableBatchDML.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableBatchDML.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeInstance, Name: vardef.TiDBStatsCacheMemQuota, Value: strconv.Itoa(vardef.DefTiDBStatsCacheMemQuota),
		MinValue: 0, MaxValue: vardef.MaxTiDBStatsCacheMemQuota, Type: vardef.TypeInt,
		GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.StatsCacheMemQuota.Load(), 10), nil
		}, SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			v := TidbOptInt64(s, vardef.DefTiDBStatsCacheMemQuota)
			oldv := vardef.StatsCacheMemQuota.Load()
			if v != oldv {
				vardef.StatsCacheMemQuota.Store(v)
				SetStatsCacheCapacityFunc := SetStatsCacheCapacity.Load()
				(*SetStatsCacheCapacityFunc)(v)
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBQueryLogMaxLen, Value: strconv.Itoa(vardef.DefTiDBQueryLogMaxLen), Type: vardef.TypeInt, MinValue: 0, MaxValue: 1073741824, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.QueryLogMaxLen.Store(int32(TidbOptInt64(val, vardef.DefTiDBQueryLogMaxLen)))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprint(vardef.QueryLogMaxLen.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCommitterConcurrency, Value: strconv.Itoa(vardef.DefTiDBCommitterConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		tikvutil.CommitterConcurrency.Store(int32(TidbOptInt64(val, vardef.DefTiDBCommitterConcurrency)))
		cfg := config.GetGlobalConfig().GetTiKVConfig()
		tikvcfg.StoreGlobalConfig(cfg)
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprint(tikvutil.CommitterConcurrency.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemQuotaAnalyze, Value: strconv.Itoa(vardef.DefTiDBMemQuotaAnalyze), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(GetMemQuotaAnalyze(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			SetMemQuotaAnalyze(TidbOptInt64(val, vardef.DefTiDBMemQuotaAnalyze))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePrepPlanCache, Value: BoolToOnOff(vardef.DefTiDBEnablePrepPlanCache), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePreparedPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPrepPlanCacheSize, Aliases: []string{vardef.TiDBSessionPlanCacheSize}, Value: strconv.FormatUint(uint64(vardef.DefTiDBPrepPlanCacheSize), 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.PreparedPlanCacheSize = uVal
		}
		return err
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBPrepPlanCacheSize, vardef.TiDBSessionPlanCacheSize)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePrepPlanCacheMemoryMonitor, Value: BoolToOnOff(vardef.DefTiDBEnablePrepPlanCacheMemoryMonitor), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePreparedPlanCacheMemoryMonitor = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBPrepPlanCacheMemoryGuardRatio, Value: strconv.FormatFloat(vardef.DefTiDBPrepPlanCacheMemoryGuardRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		f, err := strconv.ParseFloat(val, 64)
		if err == nil {
			vardef.PreparedPlanCacheMemoryGuardRatio.Store(f)
		}
		return err
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatFloat(vardef.PreparedPlanCacheMemoryGuardRatio.Load(), 'f', -1, 64), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNonPreparedPlanCache, Value: BoolToOnOff(vardef.DefTiDBEnableNonPreparedPlanCache), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNonPreparedPlanCache = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNonPreparedPlanCacheForDML, Value: BoolToOnOff(vardef.DefTiDBEnableNonPreparedPlanCacheForDML), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNonPreparedPlanCacheForDML = TiDBOptOn(val)
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiDBOptEnableFuzzyBinding,
		Value:                   BoolToOnOff(false),
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableFuzzyBinding = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBNonPreparedPlanCacheSize, Value: strconv.FormatUint(uint64(vardef.DefTiDBNonPreparedPlanCacheSize), 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.NonPreparedPlanCacheSize = uVal
		}
		return err
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		appendDeprecationWarning(vars, vardef.TiDBNonPreparedPlanCacheSize, vardef.TiDBSessionPlanCacheSize)
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPlanCacheMaxPlanSize, Value: strconv.FormatUint(vardef.DefTiDBPlanCacheMaxPlanSize, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.PlanCacheMaxPlanSize = uVal
		}
		return err
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSessionPlanCacheSize, Aliases: []string{vardef.TiDBPrepPlanCacheSize}, Value: strconv.FormatUint(uint64(vardef.DefTiDBSessionPlanCacheSize), 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 100000, SetSession: func(s *SessionVars, val string) error {
		uVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil {
			s.SessionPlanCacheSize = uVal
		}
		return err
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableInstancePlanCache, Value: vardef.Off, Type: vardef.TypeBool,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableInstancePlanCache.Load()), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.EnableInstancePlanCache.Store(TiDBOptOn(val))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBInstancePlanCacheReservedPercentage,
		Value: strconv.FormatFloat(vardef.DefTiDBInstancePlanCacheReservedPercentage, 'f', -1, 64),
		Type:  vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.InstancePlanCacheReservedPercentage.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			v := tidbOptFloat64(val, vardef.DefTiDBInstancePlanCacheReservedPercentage)
			if v < 0 || v > 1 {
				return errors.Errorf("invalid tidb_instance_plan_cache_reserved_percentage value %s", val)
			}
			vardef.InstancePlanCacheReservedPercentage.Store(v)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBInstancePlanCacheMaxMemSize, Value: strconv.Itoa(int(vardef.DefTiDBInstancePlanCacheMaxMemSize)), Type: vardef.TypeStr,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.InstancePlanCacheMaxMemSize.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			v, str := parseByteSize(val)
			if str == "" || v < 0 {
				return errors.Errorf("invalid tidb_instance_plan_cache_max_mem_size value %s", val)
			}
			if v < vardef.MinTiDBInstancePlanCacheMemSize {
				return errors.Errorf("tidb_instance_plan_cache_max_mem_size should be at least 100MiB")
			}
			vardef.InstancePlanCacheMaxMemSize.Store(int64(v))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemOOMAction, Value: vardef.DefTiDBMemOOMAction, PossibleValues: []string{"CANCEL", "LOG"}, Type: vardef.TypeEnum,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return vardef.OOMAction.Load(), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.OOMAction.Store(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMaxAutoAnalyzeTime, Value: strconv.Itoa(vardef.DefTiDBMaxAutoAnalyzeTime), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.MaxAutoAnalyzeTime.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				vardef.MaxAutoAnalyzeTime.Store(num)
			}
			return err
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoAnalyzeConcurrency,
		Value:    strconv.Itoa(vardef.DefTiDBAutoAnalyzeConcurrency),
		Type:     vardef.TypeInt,
		MinValue: 0, MaxValue: math.MaxInt32,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(int64(vardef.AutoAnalyzeConcurrency.Load()), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			num, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				vardef.AutoAnalyzeConcurrency.Store(int32(num))
			}
			return err
		},
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			// Check if auto-analyze and auto-analyze priority queue are enabled
			enableAutoAnalyze := vardef.RunAutoAnalyze.Load()
			enableAutoAnalyzePriorityQueue := vardef.EnableAutoAnalyzePriorityQueue.Load()

			// Validate that both required settings are enabled
			if !enableAutoAnalyze || !enableAutoAnalyzePriorityQueue {
				return originalValue, errors.Errorf(
					"cannot set %s: requires both tidb_enable_auto_analyze and tidb_enable_auto_analyze_priority_queue to be true. Current values: tidb_enable_auto_analyze=%v, tidb_enable_auto_analyze_priority_queue=%v",
					vardef.TiDBAutoAnalyzeConcurrency,
					enableAutoAnalyze,
					enableAutoAnalyzePriorityQueue,
				)
			}

			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableMDL, Value: BoolToOnOff(vardef.DefTiDBEnableMDL), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, vars *SessionVars, val string) error {
		if vardef.IsMDLEnabled() != TiDBOptOn(val) {
			err := SwitchMDL(TiDBOptOn(val))
			if err != nil {
				return err
			}
		}
		return nil
	}, GetGlobal: func(_ context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.IsMDLEnabled()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableDistTask, Value: BoolToOnOff(vardef.DefTiDBEnableDistTask), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		if vardef.EnableDistTask.Load() != TiDBOptOn(val) {
			vardef.EnableDistTask.Store(TiDBOptOn(val))
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableDistTask.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableFastCreateTable, Value: BoolToOnOff(vardef.DefTiDBEnableFastCreateTable), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		if vardef.EnableFastCreateTable.Load() != TiDBOptOn(val) {
			vardef.EnableFastCreateTable.Store(TiDBOptOn(val))
		}
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableFastCreateTable.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableNoopVariables, Value: BoolToOnOff(vardef.DefTiDBEnableNoopVariables), Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableNoopVariables.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableNoopVariables.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableGCAwareMemoryTrack, Value: BoolToOnOff(vardef.DefEnableTiDBGCAwareMemoryTrack), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		memory.EnableGCAwareMemoryTrack.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(memory.EnableGCAwareMemoryTrack.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableTmpStorageOnOOM, Value: BoolToOnOff(vardef.DefTiDBEnableTmpStorageOnOOM), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableTmpStorageOnOOM.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableTmpStorageOnOOM.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAutoBuildStatsConcurrency, Value: strconv.Itoa(vardef.DefTiDBAutoBuildStatsConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSysProcScanConcurrency, Value: strconv.Itoa(vardef.DefTiDBSysProcScanConcurrency), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemoryUsageAlarmRatio, Value: strconv.FormatFloat(vardef.DefMemoryUsageAlarmRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0.0, MaxValue: 1.0, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.MemoryUsageAlarmRatio.Store(tidbOptFloat64(val, vardef.DefMemoryUsageAlarmRatio))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return fmt.Sprintf("%g", vardef.MemoryUsageAlarmRatio.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBMemoryUsageAlarmKeepRecordNum, Value: strconv.Itoa(vardef.DefMemoryUsageAlarmKeepRecordNum), Type: vardef.TypeInt, MinValue: 1, MaxValue: 10000, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.MemoryUsageAlarmKeepRecordNum.Store(TidbOptInt64(val, vardef.DefMemoryUsageAlarmKeepRecordNum))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.MemoryUsageAlarmKeepRecordNum.Load(), 10), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.PasswordReuseHistory, Value: strconv.Itoa(vardef.DefPasswordReuseHistory), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.PasswordHistory.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.PasswordHistory.Store(TidbOptInt64(val, vardef.DefPasswordReuseHistory))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.PasswordReuseTime, Value: strconv.Itoa(vardef.DefPasswordReuseTime), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint32, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return strconv.FormatInt(vardef.PasswordReuseInterval.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.PasswordReuseInterval.Store(TidbOptInt64(val, vardef.DefPasswordReuseTime))
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableHistoricalStatsForCapture, Value: BoolToOnOff(vardef.DefTiDBEnableHistoricalStatsForCapture), Type: vardef.TypeBool,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			vardef.EnableHistoricalStatsForCapture.Store(TiDBOptOn(s))
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableHistoricalStatsForCapture.Load()), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBHistoricalStatsDuration, Value: vardef.DefTiDBHistoricalStatsDuration.String(), Type: vardef.TypeDuration, MinValue: int64(time.Second), MaxValue: uint64(time.Hour * 24 * 365),
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.HistoricalStatsDuration.Load().String(), nil
		}, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			vardef.HistoricalStatsDuration.Store(d)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBLowResolutionTSOUpdateInterval, Value: strconv.Itoa(vardef.DefTiDBLowResolutionTSOUpdateInterval), Type: vardef.TypeInt, MinValue: 10, MaxValue: 60000,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.LowResolutionTSOUpdateInterval.Store(uint32(TidbOptInt64(val, vardef.DefTiDBLowResolutionTSOUpdateInterval)))
			if SetLowResolutionTSOUpdateInterval != nil {
				interval := time.Duration(vardef.LowResolutionTSOUpdateInterval.Load()) * time.Millisecond
				return SetLowResolutionTSOUpdateInterval(interval)
			}
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal,
		Name:  vardef.TiDBEnableTSValidation,
		Value: BoolToOnOff(vardef.DefTiDBEnableTSValidation),
		Type:  vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			oracles.EnableTSValidation.Store(TiDBOptOn(val))
			return nil
		},
	},
}
