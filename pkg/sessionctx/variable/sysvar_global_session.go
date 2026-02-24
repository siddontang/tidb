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
	goerr "errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/executor/join/joinversion"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/privilege/privileges/ldap"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/naming"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// defaultSysVarsGlobalSession contains system variables with both GLOBAL and SESSION scope.
var defaultSysVarsGlobalSession = []*SysVar{
	/* The system variables below have GLOBAL and SESSION scope  */
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanReplayerContinuousCapture, Value: BoolToOnOff(false), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			historicalStatsEnabled, err := s.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableHistoricalStats)
			if err != nil {
				return err
			}
			if !TiDBOptOn(historicalStatsEnabled) && TiDBOptOn(val) {
				return errors.Errorf("%v should be enabled before enabling %v", vardef.TiDBEnableHistoricalStats, vardef.TiDBEnablePlanReplayerContinuousCapture)
			}
			s.EnablePlanReplayedContinuesCapture = TiDBOptOn(val)
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return BoolToOnOff(vars.EnablePlanReplayedContinuesCapture), nil
		},
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			historicalStatsEnabled, err := vars.GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableHistoricalStats)
			if err != nil {
				return "", err
			}
			if !TiDBOptOn(historicalStatsEnabled) && TiDBOptOn(s) {
				return "", errors.Errorf("%v should be enabled before enabling %v", vardef.TiDBEnableHistoricalStats, vardef.TiDBEnablePlanReplayerContinuousCapture)
			}
			return s, nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanReplayerCapture, Value: BoolToOnOff(vardef.DefTiDBEnablePlanReplayerCapture), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnablePlanReplayerCapture = TiDBOptOn(val)
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			return BoolToOnOff(vars.EnablePlanReplayerCapture), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRowFormatVersion, Value: strconv.Itoa(vardef.DefTiDBRowFormatV1), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 2, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.SetDDLReorgRowFormat(TidbOptInt64(val, vardef.DefTiDBRowFormatV2))
		return nil
	}, SetSession: func(s *SessionVars, val string) error {
		formatVersion := TidbOptInt64(val, vardef.DefTiDBRowFormatV1)
		if formatVersion == vardef.DefTiDBRowFormatV1 {
			s.RowEncoder.Enable = false
		} else if formatVersion == vardef.DefTiDBRowFormatV2 {
			s.RowEncoder.Enable = true
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableRowLevelChecksum, Value: BoolToOnOff(vardef.DefTiDBEnableRowLevelChecksum), Type: vardef.TypeBool,
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableRowLevelChecksum.Load()), nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			vardef.EnableRowLevelChecksum.Store(TiDBOptOn(s))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.SQLSelectLimit, Value: "18446744073709551615", Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.DefaultWeekFormat, Value: vardef.DefDefaultWeekFormat, Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 7},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.SQLModeVar,
		Value:                   mysql.DefaultSQLMode,
		IsHintUpdatableVerified: true,
		Validation: func(
			vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag,
		) (string, error) {
			// Ensure the SQL mode parses
			normalizedValue = mysql.FormatSQLModeStr(normalizedValue)
			if _, err := mysql.GetSQLMode(normalizedValue); err != nil {
				return originalValue, err
			}
			return normalizedValue, nil
		}, SetSession: func(s *SessionVars, val string) error {
			val = mysql.FormatSQLModeStr(val)
			// Modes is a list of different modes separated by commas.
			sqlMode, err := mysql.GetSQLMode(val)
			if err != nil {
				return errors.Trace(err)
			}
			s.SQLMode = sqlMode
			s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
			return nil
		}},
	{
		Scope:                   vardef.ScopeGlobal,
		Name:                    vardef.TiDBLoadBindingTimeout,
		Value:                   strconv.Itoa(vardef.DefTiDBLoadBindingTimeout),
		Type:                    vardef.TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: false,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			timeoutMS := tidbOptPositiveInt32(s, vardef.DefTiDBLoadBindingTimeout)
			vars.LoadBindingTimeout = uint64(timeoutMS)
			return nil
		}},
	{
		Scope:                   vardef.ScopeGlobal,
		Name:                    vardef.TiDBEnableBindingUsage,
		Value:                   BoolToOnOff(vardef.DefTiDBEnableBindingUsage),
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: false,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.EnableBindingUsage.Store(TiDBOptOn(val))
			return nil
		}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return BoolToOnOff(vardef.EnableBindingUsage.Load()), nil
		}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.MaxExecutionTime,
		Value:                   "0",
		Type:                    vardef.TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			timeoutMS := tidbOptPositiveInt32(val, 0)
			s.MaxExecutionTime = uint64(timeoutMS)
			return nil
		}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiKVClientReadTimeout,
		Value:                   "0",
		Type:                    vardef.TypeUnsigned,
		MinValue:                0,
		MaxValue:                math.MaxInt32,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			timeoutMS := tidbOptPositiveInt32(val, 0)
			s.TiKVClientReadTimeout = uint64(timeoutMS)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CollationServer, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[vardef.CharacterSetServer] = coll.CharsetName
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.DefaultCollationForUTF8MB4, Value: mysql.DefaultCollationName, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		coll, err := checkDefaultCollationForUTF8MB4(vars, normalizedValue, originalValue, scope)
		if err == nil {
			vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxNoReplacement.FastGenByArgs(vardef.DefaultCollationForUTF8MB4))
		}
		return coll, err
	}, SetSession: func(s *SessionVars, val string) error {
		s.DefaultCollationForUTF8MB4 = val
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TimeZone,
		Value:                   "SYSTEM",
		IsHintUpdatableVerified: true,
		Validation: func(
			varErrFunctionsNoopImpls *SessionVars, normalizedValue string, originalValue string,
			scope vardef.ScopeFlag,
		) (string, error) {
			if strings.EqualFold(normalizedValue, "SYSTEM") {
				return "SYSTEM", nil
			}
			_, err := timeutil.ParseTimeZone(normalizedValue)
			return normalizedValue, err
		}, SetSession: func(s *SessionVars, val string) error {
			tz, err := timeutil.ParseTimeZone(val)
			if err != nil {
				return err
			}
			s.TimeZone = tz
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.ForeignKeyChecks, Value: BoolToOnOff(vardef.DefTiDBForeignKeyChecks), Type: vardef.TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if TiDBOptOn(normalizedValue) {
			vars.ForeignKeyChecks = true
			return vardef.On, nil
		} else if !TiDBOptOn(normalizedValue) {
			vars.ForeignKeyChecks = false
			return vardef.Off, nil
		}
		return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.ForeignKeyChecks, originalValue)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBForeignKeyCheckInSharedLock, Value: BoolToOnOff(vardef.DefTiDBForeignKeyCheckInSharedLock), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ForeignKeyCheckInSharedLock = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableForeignKey, Value: BoolToOnOff(true), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableForeignKey.Store(TiDBOptOn(val))
		return nil
	}, GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableForeignKey.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CollationDatabase, Value: mysql.DefaultCollationName, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[vardef.CharsetDatabase] = coll.CharsetName
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.AutoIncrementIncrement, Value: strconv.FormatInt(vardef.DefAutoIncrementIncrement, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementIncrement is valid in [1, 65535].
		s.AutoIncrementIncrement = tidbOptPositiveInt32(val, vardef.DefAutoIncrementIncrement)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.AutoIncrementOffset, Value: strconv.FormatInt(vardef.DefAutoIncrementOffset, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint16, SetSession: func(s *SessionVars, val string) error {
		// AutoIncrementOffset is valid in [1, 65535].
		s.AutoIncrementOffset = tidbOptPositiveInt32(val, vardef.DefAutoIncrementOffset)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetClient, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharacterSetClient)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetResults, Value: mysql.DefaultCharset, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return normalizedValue, nil
		}
		return checkCharacterSet(normalizedValue, "")
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TxnIsolation, Value: "REPEATABLE-READ", Type: vardef.TypeEnum, Aliases: []string{vardef.TransactionIsolation}, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		// MySQL appends a warning here for tx_isolation is deprecated
		// TiDB doesn't currently, but may in future. It is still commonly used by applications
		// So it might be noisy to do so.
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TransactionIsolation, Value: "REPEATABLE-READ", Type: vardef.TypeEnum, Aliases: []string{vardef.TxnIsolation}, PossibleValues: []string{"READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkIsolationLevel(vars, normalizedValue, originalValue, scope)
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CollationConnection, Value: mysql.DefaultCollationName, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCollation(vars, normalizedValue, originalValue, scope)
	}, SetSession: func(s *SessionVars, val string) error {
		if coll, err := collate.GetCollationByName(val); err == nil {
			s.systems[vardef.CharacterSetConnection] = coll.CharsetName
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.AutoCommit, Value: vardef.On, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		isAutocommit := TiDBOptOn(val)
		// Implicitly commit the possible ongoing transaction if mode is changed from off to on.
		if !s.IsAutocommit() && isAutocommit {
			s.SetInTxn(false)
		}
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharsetDatabase, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharsetDatabase)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[vardef.CollationDatabase] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.WaitTimeout, Value: strconv.FormatInt(vardef.DefWaitTimeout, 10), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.InteractiveTimeout, Value: "28800", Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: secondsPerYear},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.InnodbLockWaitTimeout, Value: strconv.FormatInt(vardef.DefInnodbLockWaitTimeout, 10), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 1073741824, SetSession: func(s *SessionVars, val string) error {
		lockWaitSec := TidbOptInt64(val, vardef.DefInnodbLockWaitTimeout)
		s.LockWaitTimeout = lockWaitSec * 1000
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.GroupConcatMaxLen,
		Value:                   strconv.FormatUint(vardef.DefGroupConcatMaxLen, 10),
		IsHintUpdatableVerified: true,
		Type:                    vardef.TypeUnsigned,
		MinValue:                4,
		MaxValue:                math.MaxUint64,
		Validation: func(
			vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag,
		) (string, error) {
			// https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_group_concat_max_len
			// Minimum Value 4
			// Maximum Value (64-bit platforms) 18446744073709551615
			// Maximum Value (32-bit platforms) 4294967295
			if mathutil.IntBits == 32 {
				if val, err := strconv.ParseUint(normalizedValue, 10, 64); err == nil {
					if val > uint64(math.MaxUint32) {
						vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.GroupConcatMaxLen, originalValue))
						return strconv.FormatInt(int64(math.MaxUint32), 10), nil
					}
				}
			}
			return normalizedValue, nil
		},
		SetSession: func(sv *SessionVars, s string) error {
			var err error
			if sv.GroupConcatMaxLen, err = strconv.ParseUint(s, 10, 64); err != nil {
				return err
			}
			return nil
		},
		GetSession: func(sv *SessionVars) (string, error) {
			return strconv.FormatUint(sv.GroupConcatMaxLen, 10), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetConnection, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharacterSetConnection)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[vardef.CollationConnection] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CharacterSetServer, Value: mysql.DefaultCharset, skipInit: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		return checkCharacterSet(normalizedValue, vardef.CharacterSetServer)
	}, SetSession: func(s *SessionVars, val string) error {
		if cs, err := charset.GetCharsetInfo(val); err == nil {
			s.systems[vardef.CollationServer] = cs.DefaultCollation
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.MaxAllowedPacket, Value: strconv.FormatUint(vardef.DefMaxAllowedPacket, 10), Type: vardef.TypeUnsigned, MinValue: 1024, MaxValue: vardef.MaxOfMaxAllowedPacket,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if vars.StmtCtx.StmtType == "Set" && scope == vardef.ScopeSession {
				err := ErrReadOnly.GenWithStackByArgs("SESSION", vardef.MaxAllowedPacket, "GLOBAL")
				return normalizedValue, err
			}
			// Truncate the value of max_allowed_packet to be a multiple of 1024,
			// nonmultiples are rounded down to the nearest multiple.
			u, err := strconv.ParseUint(normalizedValue, 10, 64)
			if err != nil {
				return normalizedValue, err
			}
			remainder := u % 1024
			if remainder != 0 {
				vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.MaxAllowedPacket, normalizedValue))
				u -= remainder
			}
			return strconv.FormatUint(u, 10), nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return strconv.FormatUint(s.MaxAllowedPacket, 10), nil
		},
		SetSession: func(s *SessionVars, val string) error {
			var err error
			if s.MaxAllowedPacket, err = strconv.ParseUint(val, 10, 64); err != nil {
				return err
			}
			return nil
		},
	},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.WindowingUseHighPrecision,
		Value:                   vardef.On,
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.WindowingUseHighPrecision = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.BlockEncryptionMode, Value: vardef.DefBlockEncryptionMode, Type: vardef.TypeEnum, PossibleValues: []string{"aes-128-ecb", "aes-192-ecb", "aes-256-ecb", "aes-128-cbc", "aes-192-cbc", "aes-256-cbc", "aes-128-ofb", "aes-192-ofb", "aes-256-ofb", "aes-128-cfb", "aes-192-cfb", "aes-256-cfb"}},
	/* TiDB specific variables */
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowMPPExecution, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBAllowMPPExecution), Depended: true, SetSession: func(s *SessionVars, val string) error {
		s.allowMPPExecution = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowTiFlashCop, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBAllowTiFlashCop), SetSession: func(s *SessionVars, val string) error {
		s.allowTiFlashCop = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashFastScan, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiFlashFastScan), SetSession: func(s *SessionVars, val string) error {
		s.TiFlashFastScan = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMPPStoreFailTTL, Type: vardef.TypeStr, Value: vardef.DefTiDBMPPStoreFailTTL, SetSession: func(s *SessionVars, val string) error {
		s.MPPStoreFailTTL = val
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_mpp_store_fail_ttl is always 0s. This variable has been deprecated and will be removed in the future releases"))
		return vardef.DefTiDBMPPStoreFailTTL, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashExchangeWithNewCollation, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBHashExchangeWithNewCollation), SetSession: func(s *SessionVars, val string) error {
		s.HashExchangeWithNewCollation = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBCJThresholdCount, Value: strconv.Itoa(vardef.DefBroadcastJoinThresholdCount), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdCount = TidbOptInt64(val, vardef.DefBroadcastJoinThresholdCount)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBCJThresholdSize, Value: strconv.Itoa(vardef.DefBroadcastJoinThresholdSize), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.BroadcastJoinThresholdSize = TidbOptInt64(val, vardef.DefBroadcastJoinThresholdSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPreferBCJByExchangeDataSize, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefPreferBCJByExchangeDataSize), SetSession: func(s *SessionVars, val string) error {
		s.PreferBCJByExchangeDataSize = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBuildStatsConcurrency, Value: strconv.Itoa(vardef.DefBuildStatsConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBuildSamplingStatsConcurrency, Value: strconv.Itoa(vardef.DefBuildSamplingStatsConcurrency), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCartesianBCJ, Value: strconv.Itoa(vardef.DefOptCartesianBCJ), Type: vardef.TypeInt, MinValue: 0, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AllowCartesianBCJ = TidbOptInt(val, vardef.DefOptCartesianBCJ)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptMPPOuterJoinFixedBuildSide, Value: BoolToOnOff(vardef.DefOptMPPOuterJoinFixedBuildSide), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.MPPOuterJoinFixedBuildSide = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBExecutorConcurrency, Value: strconv.Itoa(vardef.DefExecutorConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.ExecutorConcurrency = tidbOptPositiveInt32(val, vardef.DefExecutorConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDistSQLScanConcurrency, Value: strconv.Itoa(vardef.DefDistSQLScanConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		s.distSQLScanConcurrency = tidbOptPositiveInt32(val, vardef.DefDistSQLScanConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAnalyzeDistSQLScanConcurrency, Value: strconv.Itoa(vardef.DefAnalyzeDistSQLScanConcurrency), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.analyzeDistSQLScanConcurrency = tidbOptPositiveInt32(val, vardef.DefAnalyzeDistSQLScanConcurrency)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptInSubqToJoinAndAgg, Value: BoolToOnOff(vardef.DefOptInSubqToJoinAndAgg), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetAllowInSubqToJoinAndAgg(TiDBOptOn(val))
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiDBOptPreferRangeScan,
		Value:                   BoolToOnOff(vardef.DefOptPreferRangeScan),
		Type:                    vardef.TypeBool,
		IsHintUpdatableVerified: true,
		SetSession: func(s *SessionVars, val string) error {
			s.SetAllowPreferRangeScan(TiDBOptOn(val))
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptLimitPushDownThreshold, Value: strconv.Itoa(vardef.DefOptLimitPushDownThreshold), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.LimitPushDownThreshold = TidbOptInt64(val, vardef.DefOptLimitPushDownThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCorrelationThreshold, Value: strconv.FormatFloat(vardef.DefOptCorrelationThreshold, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationThreshold = tidbOptFloat64(val, vardef.DefOptCorrelationThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableCorrelationAdjustment, Value: BoolToOnOff(vardef.DefOptEnableCorrelationAdjustment), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableCorrelationAdjustment = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCorrelationExpFactor, Value: strconv.Itoa(vardef.DefOptCorrelationExpFactor), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.CorrelationExpFactor = int(TidbOptInt64(val, vardef.DefOptCorrelationExpFactor))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRiskScaleNDVSkewRatio, Value: strconv.FormatFloat(vardef.DefOptRiskScaleNDVSkewRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.RiskScaleNDVSkewRatio = tidbOptFloat64(val, vardef.DefOptRiskScaleNDVSkewRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCartesianJoinOrderThreshold, Value: strconv.FormatFloat(vardef.DefOptCartesianJoinOrderThreshold, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CartesianJoinOrderThreshold = tidbOptFloat64(val, vardef.DefOptCartesianJoinOrderThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRiskEqSkewRatio, Value: strconv.FormatFloat(vardef.DefOptRiskEqSkewRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.RiskEqSkewRatio = tidbOptFloat64(val, vardef.DefOptRiskEqSkewRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRiskRangeSkewRatio, Value: strconv.FormatFloat(vardef.DefOptRiskRangeSkewRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.RiskRangeSkewRatio = tidbOptFloat64(val, vardef.DefOptRiskRangeSkewRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRiskGroupNDVSkewRatio, Value: strconv.FormatFloat(vardef.DefOptRiskGroupNDVSkewRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.RiskGroupNDVSkewRatio = tidbOptFloat64(val, vardef.DefOptRiskGroupNDVSkewRatio)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptAlwaysKeepJoinKey, Value: BoolToOnOff(vardef.DefOptAlwaysKeepJoinKey), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AlwaysKeepJoinKey = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCPUFactor, Value: strconv.FormatFloat(vardef.DefOptCPUFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.cpuFactor = tidbOptFloat64(val, vardef.DefOptCPUFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTiFlashConcurrencyFactor, Value: strconv.FormatFloat(vardef.DefOptTiFlashConcurrencyFactor, 'f', -1, 64), skipInit: true, Type: vardef.TypeFloat, MinValue: 1, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.CopTiFlashConcurrencyFactor = tidbOptFloat64(val, vardef.DefOptTiFlashConcurrencyFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptCopCPUFactor, Value: strconv.FormatFloat(vardef.DefOptCopCPUFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.copCPUFactor = tidbOptFloat64(val, vardef.DefOptCopCPUFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptNetworkFactor, Value: strconv.FormatFloat(vardef.DefOptNetworkFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.networkFactor = tidbOptFloat64(val, vardef.DefOptNetworkFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptScanFactor, Value: strconv.FormatFloat(vardef.DefOptScanFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.scanFactor = tidbOptFloat64(val, vardef.DefOptScanFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptDescScanFactor, Value: strconv.FormatFloat(vardef.DefOptDescScanFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.descScanFactor = tidbOptFloat64(val, vardef.DefOptDescScanFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSeekFactor, Value: strconv.FormatFloat(vardef.DefOptSeekFactor, 'f', -1, 64), skipInit: true, Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.seekFactor = tidbOptFloat64(val, vardef.DefOptSeekFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptMemoryFactor, Value: strconv.FormatFloat(vardef.DefOptMemoryFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.memoryFactor = tidbOptFloat64(val, vardef.DefOptMemoryFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptDiskFactor, Value: strconv.FormatFloat(vardef.DefOptDiskFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.diskFactor = tidbOptFloat64(val, vardef.DefOptDiskFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexScanCostFactor = tidbOptFloat64(val, vardef.DefOptIndexScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexReaderCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexReaderCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexReaderCostFactor = tidbOptFloat64(val, vardef.DefOptIndexReaderCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableReaderCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableReaderCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableReaderCostFactor = tidbOptFloat64(val, vardef.DefOptTableReaderCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableFullScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableFullScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableFullScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableFullScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableRangeScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableRangeScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableRangeScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableRangeScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableRowIDScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableRowIDScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableRowIDScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableRowIDScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTableTiFlashScanCostFactor, Value: strconv.FormatFloat(vardef.DefOptTableTiFlashScanCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TableTiFlashScanCostFactor = tidbOptFloat64(val, vardef.DefOptTableTiFlashScanCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexLookupCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexLookupCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexLookupCostFactor = tidbOptFloat64(val, vardef.DefOptIndexLookupCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexMergeCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexMergeCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexMergeCostFactor = tidbOptFloat64(val, vardef.DefOptIndexMergeCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSortCostFactor, Value: strconv.FormatFloat(vardef.DefOptSortCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.SortCostFactor = tidbOptFloat64(val, vardef.DefOptSortCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptTopNCostFactor, Value: strconv.FormatFloat(vardef.DefOptTopNCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.TopNCostFactor = tidbOptFloat64(val, vardef.DefOptTopNCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptLimitCostFactor, Value: strconv.FormatFloat(vardef.DefOptLimitCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.LimitCostFactor = tidbOptFloat64(val, vardef.DefOptLimitCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptStreamAggCostFactor, Value: strconv.FormatFloat(vardef.DefOptStreamAggCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.StreamAggCostFactor = tidbOptFloat64(val, vardef.DefOptStreamAggCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptHashAggCostFactor, Value: strconv.FormatFloat(vardef.DefOptHashAggCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.HashAggCostFactor = tidbOptFloat64(val, vardef.DefOptHashAggCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptMergeJoinCostFactor, Value: strconv.FormatFloat(vardef.DefOptMergeJoinCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.MergeJoinCostFactor = tidbOptFloat64(val, vardef.DefOptMergeJoinCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptHashJoinCostFactor, Value: strconv.FormatFloat(vardef.DefOptHashJoinCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.HashJoinCostFactor = tidbOptFloat64(val, vardef.DefOptHashJoinCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexJoinCostFactor, Value: strconv.FormatFloat(vardef.DefOptIndexJoinCostFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.IndexJoinCostFactor = tidbOptFloat64(val, vardef.DefOptIndexJoinCostFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptSelectivityFactor, Value: strconv.FormatFloat(vardef.DefOptSelectivityFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1, SetSession: func(s *SessionVars, val string) error {
		s.SelectivityFactor = tidbOptFloat64(val, vardef.DefOptSelectivityFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptimizerEnableNewOnlyFullGroupByCheck, Value: BoolToOnOff(vardef.DefTiDBOptimizerEnableNewOFGB), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerEnableNewOnlyFullGroupByCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptConcurrencyFactor, Value: strconv.FormatFloat(vardef.DefOptConcurrencyFactor, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64, SetSession: func(s *SessionVars, val string) error {
		s.concurrencyFactor = tidbOptFloat64(val, vardef.DefOptConcurrencyFactor)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptForceInlineCTE, Value: BoolToOnOff(vardef.DefOptForceInlineCTE), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.enableForceInlineCTE = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexJoinBatchSize, Value: strconv.Itoa(vardef.DefIndexJoinBatchSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.IndexJoinBatchSize = tidbOptPositiveInt32(val, vardef.DefIndexJoinBatchSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexLookupSize, Value: strconv.Itoa(vardef.DefIndexLookupSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.IndexLookupSize = tidbOptPositiveInt32(val, vardef.DefIndexLookupSize)
		return nil
	}},
	newExecConcurrencySysVar(vardef.TiDBIndexLookupConcurrency, vardef.DefIndexLookupConcurrency, func(s *SessionVars, v int) { s.indexLookupConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBIndexLookupJoinConcurrency, vardef.DefIndexLookupJoinConcurrency, func(s *SessionVars, v int) { s.indexLookupJoinConcurrency = v }),
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexSerialScanConcurrency, Value: strconv.Itoa(vardef.DefIndexSerialScanConcurrency), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, SetSession: func(s *SessionVars, val string) error {
		// NOTE: do nothing here because this variable is deprecated.
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGen("The 'tidb_index_serial_scan_concurrency' variable is deprecated. Sequential scans follow 'tidb_executor_concurrency', and index statistics collection uses 'tidb_analyze_distsql_scan_concurrency'."))
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipUTF8Check, Value: BoolToOnOff(vardef.DefSkipUTF8Check), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipUTF8Check = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipASCIICheck, Value: BoolToOnOff(vardef.DefSkipASCIICheck), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipASCIICheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDMLBatchSize, Value: strconv.Itoa(vardef.DefDMLBatchSize), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.DMLBatchSize = int(TidbOptInt64(val, vardef.DefDMLBatchSize))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxChunkSize, Value: strconv.Itoa(vardef.DefMaxChunkSize), Type: vardef.TypeUnsigned, MinValue: maxChunkSizeLowerBound, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.MaxChunkSize = tidbOptPositiveInt32(val, vardef.DefMaxChunkSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowBatchCop, Value: strconv.Itoa(vardef.DefTiDBAllowBatchCop), Type: vardef.TypeInt, MinValue: 0, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AllowBatchCop = int(TidbOptInt64(val, vardef.DefTiDBAllowBatchCop))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBShardRowIDBits, Value: strconv.Itoa(vardef.DefShardRowIDBits), Type: vardef.TypeInt, MinValue: 0, MaxValue: vardef.MaxShardRowIDBits, SetSession: func(s *SessionVars, val string) error {
		s.ShardRowIDBits = TidbOptUint64(val, vardef.DefShardRowIDBits)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPreSplitRegions, Value: strconv.Itoa(vardef.DefPreSplitRegions), Type: vardef.TypeInt, MinValue: 0, MaxValue: vardef.MaxPreSplitRegions, SetSession: func(s *SessionVars, val string) error {
		s.PreSplitRegions = TidbOptUint64(val, vardef.DefPreSplitRegions)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBInitChunkSize, Value: strconv.Itoa(vardef.DefInitChunkSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: initChunkSizeUpperBound, SetSession: func(s *SessionVars, val string) error {
		s.InitChunkSize = tidbOptPositiveInt32(val, vardef.DefInitChunkSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableCascadesPlanner, Value: vardef.Off, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableCascadesPlanner(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableIndexMerge, Value: BoolToOnOff(vardef.DefTiDBEnableIndexMerge), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SetEnableIndexMerge(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableTablePartition, Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, "AUTO"}, Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
		if s == vardef.Off {
			vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_table_partition is always turned on. This variable has been deprecated and will be removed in the future releases"))
		}
		return vardef.On, nil
	}},
	// Keeping tidb_enable_list_partition here, to give errors if setting it to anything other than ON
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableListTablePartition, Value: vardef.On, Type: vardef.TypeBool, Validation: func(vars *SessionVars, normalizedValue, _ string, _ vardef.ScopeFlag) (string, error) {
		vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGenByArgs(vardef.TiDBEnableListTablePartition))
		if !TiDBOptOn(normalizedValue) {
			return normalizedValue, errors.Errorf("tidb_enable_list_partition is now always on, and cannot be turned off")
		}
		return normalizedValue, nil
	}},
	newExecConcurrencySysVar(vardef.TiDBHashJoinConcurrency, vardef.DefTiDBHashJoinConcurrency, func(s *SessionVars, v int) { s.hashJoinConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBProjectionConcurrency, vardef.DefTiDBProjectionConcurrency, func(s *SessionVars, v int) { s.projectionConcurrency = v }, withMinValue(-1), withAllowAutoValue(false)),
	newExecConcurrencySysVar(vardef.TiDBHashAggPartialConcurrency, vardef.DefTiDBHashAggPartialConcurrency, func(s *SessionVars, v int) { s.hashAggPartialConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBHashAggFinalConcurrency, vardef.DefTiDBHashAggFinalConcurrency, func(s *SessionVars, v int) { s.hashAggFinalConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBWindowConcurrency, vardef.DefTiDBWindowConcurrency, func(s *SessionVars, v int) { s.windowConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBMergeJoinConcurrency, vardef.DefTiDBMergeJoinConcurrency, func(s *SessionVars, v int) { s.mergeJoinConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBStreamAggConcurrency, vardef.DefTiDBStreamAggConcurrency, func(s *SessionVars, v int) { s.streamAggConcurrency = v }),
	newExecConcurrencySysVar(vardef.TiDBIndexMergeIntersectionConcurrency, vardef.DefTiDBIndexMergeIntersectionConcurrency, func(s *SessionVars, v int) { s.indexMergeIntersectionConcurrency = v }),
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableParallelApply, Value: BoolToOnOff(vardef.DefTiDBEnableParallelApply), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableParallelApply = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMemQuotaApplyCache, Value: strconv.Itoa(vardef.DefTiDBMemQuotaApplyCache), Type: vardef.TypeUnsigned, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaApplyCache = TidbOptInt64(val, vardef.DefTiDBMemQuotaApplyCache)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBackoffLockFast, Value: strconv.Itoa(tikvstore.DefBackoffLockFast), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackoffLockFast = tidbOptPositiveInt32(val, tikvstore.DefBackoffLockFast)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBackOffWeight, Value: strconv.Itoa(tikvstore.DefBackOffWeight), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt32, SetSession: func(s *SessionVars, val string) error {
		s.KVVars.BackOffWeight = tidbOptPositiveInt32(val, tikvstore.DefBackOffWeight)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTxnEntrySizeLimit, Value: strconv.Itoa(vardef.DefTiDBTxnEntrySizeLimit), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: config.MaxTxnEntrySizeLimit, SetSession: func(s *SessionVars, val string) error {
		s.TxnEntrySizeLimit = TidbOptUint64(val, vardef.DefTiDBTxnEntrySizeLimit)
		return nil
	}, SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		vardef.TxnEntrySizeLimit.Store(TidbOptUint64(val, vardef.DefTiDBTxnEntrySizeLimit))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRetryLimit, Value: strconv.Itoa(vardef.DefTiDBRetryLimit), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RetryLimit = TidbOptInt64(val, vardef.DefTiDBRetryLimit)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDisableTxnAutoRetry, Value: BoolToOnOff(vardef.DefTiDBDisableTxnAutoRetry), Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if normalizedValue == vardef.Off {
				vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(vardef.Off, vardef.On))
			}
			return vardef.On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.DisableTxnAutoRetry = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBConstraintCheckInPlace, Value: BoolToOnOff(vardef.DefTiDBConstraintCheckInPlace), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.ConstraintCheckInPlace = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTxnMode, Value: vardef.DefTiDBTxnMode, AllowEmptyAll: true, Type: vardef.TypeEnum, PossibleValues: []string{vardef.PessimisticTxnMode, vardef.OptimisticTxnMode}, SetSession: func(s *SessionVars, val string) error {
		s.TxnMode = strings.ToUpper(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableWindowFunction, Value: BoolToOnOff(vardef.DefEnableWindowFunction), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableWindowFunction = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePipelinedWindowFunction, Value: BoolToOnOff(vardef.DefEnablePipelinedWindowFunction), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePipelinedWindowExec = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableNoDecorrelateInSelect, Value: BoolToOnOff(vardef.DefOptEnableNoDecorrelateInSelect), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableNoDecorrelateInSelect = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableStrictDoubleTypeCheck, Value: BoolToOnOff(vardef.DefEnableStrictDoubleTypeCheck), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStrictDoubleTypeCheck = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableVectorizedExpression, Value: BoolToOnOff(vardef.DefEnableVectorizedExpression), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableVectorizedExpression = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableFastAnalyze, Value: BoolToOnOff(vardef.DefTiDBUseFastAnalyze), Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			if TiDBOptOn(normalizedValue) {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("the fast analyze feature has already been removed in TiDB v7.5.0, so this will have no effect"))
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableFastAnalyze = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipIsolationLevelCheck, Value: BoolToOnOff(vardef.DefTiDBSkipIsolationLevelCheck), Type: vardef.TypeBool},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableRateLimitAction, Value: BoolToOnOff(vardef.DefTiDBEnableRateLimitAction), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnabledRateLimitAction = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowFallbackToTiKV, Value: "", Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == "" {
			return "", nil
		}
		engines := strings.Split(normalizedValue, ",")
		var formatVal string
		storeTypes := make(map[kv.StoreType]struct{})
		for i, engine := range engines {
			engine = strings.TrimSpace(engine)
			switch {
			case strings.EqualFold(engine, kv.TiFlash.Name()):
				if _, ok := storeTypes[kv.TiFlash]; !ok {
					if i != 0 {
						formatVal += ","
					}
					formatVal += kv.TiFlash.Name()
					storeTypes[kv.TiFlash] = struct{}{}
				}
			default:
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBAllowFallbackToTiKV, normalizedValue)
			}
		}
		return formatVal, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.AllowFallbackToTiKV = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(val, ",") {
			if engine == kv.TiFlash.Name() {
				s.AllowFallbackToTiKV[kv.TiFlash] = struct{}{}
			}
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAutoIncrementInGenerated, Value: BoolToOnOff(vardef.DefTiDBEnableAutoIncrementInGenerated), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAutoIncrementInGenerated = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPlacementMode, Value: vardef.DefTiDBPlacementMode, Type: vardef.TypeEnum, PossibleValues: []string{vardef.PlacementModeStrict, vardef.PlacementModeIgnore}, SetSession: func(s *SessionVars, val string) error {
		s.PlacementMode = val
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptJoinReorderThreshold, Value: strconv.Itoa(vardef.DefTiDBOptJoinReorderThreshold), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 63, SetSession: func(s *SessionVars, val string) error {
		s.TiDBOptJoinReorderThreshold = tidbOptPositiveInt32(val, vardef.DefTiDBOptJoinReorderThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptJoinReorderThroughSel, Value: BoolToOnOff(vardef.DefTiDBOptJoinReorderThroughSel), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TiDBOptJoinReorderThroughSel = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNoopFuncs, Value: vardef.DefTiDBEnableNoopFuncs, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.Warn}, Depended: true, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		// The behavior is very weird if someone can turn TiDBEnableNoopFuncs OFF, but keep any of the following on:
		// TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, serverReadOnly, SQLAutoIsNull
		// To prevent this strange position, prevent setting to OFF when any of these sysVars are ON of the same scope.
		if normalizedValue == vardef.Off {
			for _, potentialIncompatibleSysVar := range []string{vardef.TxReadOnly, vardef.TransactionReadOnly, vardef.OfflineMode, vardef.SuperReadOnly, vardef.ReadOnly, vardef.SQLAutoIsNull} {
				val, _ := vars.GetSystemVar(potentialIncompatibleSysVar) // session scope
				if scope == vardef.ScopeGlobal {                         // global scope
					var err error
					val, err = vars.GlobalVarsAccessor.GetGlobalSysVar(potentialIncompatibleSysVar)
					if err != nil {
						return originalValue, errUnknownSystemVariable.GenWithStackByArgs(potentialIncompatibleSysVar)
					}
				}
				if TiDBOptOn(val) {
					return originalValue, errValueNotSupportedWhen.GenWithStackByArgs(vardef.TiDBEnableNoopFuncs, potentialIncompatibleSysVar)
				}
			}
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.NoopFuncsMode = TiDBOptOnOffWarn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBReplicaRead, Value: "leader", Type: vardef.TypeEnum, PossibleValues: []string{"leader", "prefer-leader", "follower", "leader-and-follower", "closest-replicas", "closest-adaptive", "learner"}, SetSession: func(s *SessionVars, val string) error {
		if strings.EqualFold(val, "follower") {
			s.SetReplicaRead(kv.ReplicaReadFollower)
		} else if strings.EqualFold(val, "leader-and-follower") {
			s.SetReplicaRead(kv.ReplicaReadMixed)
		} else if strings.EqualFold(val, "leader") || len(val) == 0 {
			s.SetReplicaRead(kv.ReplicaReadLeader)
		} else if strings.EqualFold(val, "closest-replicas") {
			s.SetReplicaRead(kv.ReplicaReadClosest)
		} else if strings.EqualFold(val, "closest-adaptive") {
			s.SetReplicaRead(kv.ReplicaReadClosestAdaptive)
		} else if strings.EqualFold(val, "learner") {
			s.SetReplicaRead(kv.ReplicaReadLearner)
		} else if strings.EqualFold(val, "prefer-leader") {
			s.SetReplicaRead(kv.ReplicaReadPreferLeader)
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAdaptiveClosestReadThreshold, Value: strconv.Itoa(vardef.DefAdaptiveClosestReadThreshold), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.ReplicaClosestReadThreshold = TidbOptInt64(val, vardef.DefAdaptiveClosestReadThreshold)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBUsePlanBaselines, Value: BoolToOnOff(vardef.DefTiDBUsePlanBaselines), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.UsePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEvolvePlanBaselines, Value: BoolToOnOff(vardef.DefTiDBEvolvePlanBaselines), Type: vardef.TypeBool, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == "ON" && !config.CheckTableBeforeDrop {
			return normalizedValue, errors.Errorf("Cannot enable baseline evolution feature, it is not generally available now")
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EvolvePlanBaselines = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableExtendedStats, Value: BoolToOnOff(false), Hidden: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExtendedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.CTEMaxRecursionDepth, Value: strconv.Itoa(vardef.DefCTEMaxRecursionDepth), Type: vardef.TypeInt, MinValue: 0, MaxValue: 4294967295, SetSession: func(s *SessionVars, val string) error {
		s.CTEMaxRecursionDepth = TidbOptInt(val, vardef.DefCTEMaxRecursionDepth)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAllowAutoRandExplicitInsert, Value: BoolToOnOff(vardef.DefTiDBAllowAutoRandExplicitInsert), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.AllowAutoRandExplicitInsert = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableClusteredIndex, Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.IntOnly}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		if normalizedValue == vardef.IntOnly {
			vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(normalizedValue, fmt.Sprintf("'%s' or '%s'", vardef.On, vardef.Off)))
		}
		return normalizedValue, nil
	}, SetSession: func(s *SessionVars, val string) error {
		s.EnableClusteredIndex = vardef.TiDBOptEnableClustered(val)
		return nil
	}},
	// Keeping tidb_enable_global_index here, to give error if setting it to anything other than ON
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableGlobalIndex, Type: vardef.TypeBool, Value: vardef.On, Validation: func(vars *SessionVars, normalizedValue, _ string, _ vardef.ScopeFlag) (string, error) {
		if !TiDBOptOn(normalizedValue) {
			vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_global_index is always turned on. This variable has been deprecated and will be removed in the future releases"))
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPartitionPruneMode, Value: vardef.DefTiDBPartitionPruneMode, Type: vardef.TypeEnum, PossibleValues: []string{"static", "dynamic", "static-only", "dynamic-only"}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		mode := PartitionPruneMode(normalizedValue).Update()
		if !mode.Valid() {
			return normalizedValue, ErrWrongTypeForVar.GenWithStackByArgs(vardef.TiDBPartitionPruneMode)
		}
		return string(mode), nil
	}, GetSession: func(s *SessionVars) (string, error) {
		return s.PartitionPruneMode.Load(), nil
	}, SetSession: func(s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(newMode) == Static {
			s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("static prune mode is deprecated and will be removed in the future release."))
		}
		if PartitionPruneMode(s.PartitionPruneMode.Load()) == Static && PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please analyze all partition tables again for consistency between partition and global stats"))
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please avoid setting partition prune mode to dynamic at session level and set partition prune mode to dynamic at global level"))
		}
		s.PartitionPruneMode.Store(newMode)
		return nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		newMode := strings.ToLower(strings.TrimSpace(val))
		if PartitionPruneMode(newMode) == Static {
			s.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("static prune mode is deprecated and will be removed in the future release."))
		}
		if PartitionPruneMode(newMode) == Dynamic {
			s.StmtCtx.AppendWarning(errors.NewNoStackError("Please analyze all partition tables again for consistency between partition and global stats"))
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRedactLog, Value: vardef.DefTiDBRedactLog, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.Marker}, InternalSessionVariable: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			s.EnableRedactLog = val
			// NOTE: switch of errors is a singleton, thus we can not set it for different sessions
			errors.RedactLogEnabled.Store(val)
			return nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableRedactLog = val
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBShardAllocateStep, Value: strconv.Itoa(vardef.DefTiDBShardAllocateStep), Type: vardef.TypeInt, MinValue: 1, MaxValue: uint64(math.MaxInt64), SetSession: func(s *SessionVars, val string) error {
		s.ShardAllocateStep = TidbOptInt64(val, vardef.DefTiDBShardAllocateStep)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAsyncCommit, Value: BoolToOnOff(vardef.DefTiDBEnableAsyncCommit), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAsyncCommit = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnable1PC, Value: BoolToOnOff(vardef.DefTiDBEnable1PC), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.Enable1PC = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBGuaranteeLinearizability, Value: BoolToOnOff(vardef.DefTiDBGuaranteeLinearizability), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.GuaranteeLinearizability = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAnalyzeVersion, Value: strconv.Itoa(vardef.DefTiDBAnalyzeVersion), Type: vardef.TypeInt, MinValue: 1, MaxValue: 2, SetSession: func(s *SessionVars, val string) error {
		s.AnalyzeVersion = tidbOptPositiveInt32(val, vardef.DefTiDBAnalyzeVersion)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptIndexJoinBuild, Value: BoolToOnOff(vardef.DefTiDBOptIndexJoinBuild), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnhanceIndexJoinBuildV2 = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBHashJoinVersion, Value: vardef.DefTiDBHashJoinVersion, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			lowerValue := strings.ToLower(normalizedValue)
			if lowerValue != joinversion.HashJoinVersionLegacy && lowerValue != joinversion.HashJoinVersionOptimized {
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s", originalValue, vardef.TiDBHashJoinVersion, joinversion.HashJoinVersionLegacy+", "+joinversion.HashJoinVersionOptimized)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.UseHashJoinV2 = joinversion.IsOptimizedVersion(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableHashJoin, Value: BoolToOnOff(vardef.DefTiDBOptEnableHashJoin), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.DisableHashJoin = !TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableIndexMergeJoin, Value: BoolToOnOff(vardef.DefTiDBEnableIndexMergeJoin), Hidden: true, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableIndexMergeJoin = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTrackAggregateMemoryUsage, Value: BoolToOnOff(vardef.DefTiDBTrackAggregateMemoryUsage), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.TrackAggregateMemoryUsage = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMultiStatementMode, Value: vardef.Off, Type: vardef.TypeEnum, PossibleValues: []string{vardef.Off, vardef.On, vardef.Warn}, SetSession: func(s *SessionVars, val string) error {
		s.MultiStatementMode = TiDBOptOnOffWarn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableExchangePartition, Value: vardef.On, Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			if s == vardef.Off {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_exchange_partition is always turned on. This variable has been deprecated and will be removed in the future releases"))
			}
			return vardef.On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiDBEnableExchangePartition = true
			return nil
		}},
	// It's different from tmp_table_size or max_heap_table_size. See https://github.com/pingcap/tidb/issues/28691.
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTmpTableMaxSize, Value: strconv.Itoa(vardef.DefTiDBTmpTableMaxSize), Type: vardef.TypeUnsigned, MinValue: 1 << 20, MaxValue: 1 << 37, SetSession: func(s *SessionVars, val string) error {
		s.TMPTableSize = TidbOptInt64(val, vardef.DefTiDBTmpTableMaxSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableOrderedResultMode, Value: BoolToOnOff(vardef.DefTiDBEnableOrderedResultMode), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableStableResultMode = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePseudoForOutdatedStats, Value: BoolToOnOff(vardef.DefTiDBEnablePseudoForOutdatedStats), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePseudoForOutdatedStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRegardNULLAsPoint, Value: BoolToOnOff(vardef.DefTiDBRegardNULLAsPoint), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RegardNULLAsPoint = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePaging, Value: BoolToOnOff(vardef.DefTiDBEnablePaging), Type: vardef.TypeBool, Hidden: true, SetSession: func(s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		s.EnablePaging = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableLegacyInstanceScope, Value: BoolToOnOff(vardef.DefEnableLegacyInstanceScope), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLegacyInstanceScope = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBStatsLoadSyncWait, Value: strconv.Itoa(vardef.DefTiDBStatsLoadSyncWait), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt32,
		SetSession: func(s *SessionVars, val string) error {
			s.StatsLoadSyncWait.Store(TidbOptInt64(val, vardef.DefTiDBStatsLoadSyncWait))
			return nil
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatInt(vardef.StatsLoadSyncWait.Load(), 10), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.StatsLoadSyncWait.Store(TidbOptInt64(val, vardef.DefTiDBStatsLoadSyncWait))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSysdateIsNow, Value: BoolToOnOff(vardef.DefSysdateIsNow), Type: vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.SysdateIsNow = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableParallelHashaggSpill, Value: BoolToOnOff(vardef.DefTiDBEnableParallelHashaggSpill), Type: vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.EnableParallelHashaggSpill = TiDBOptOn(s)
			if !vars.EnableParallelHashaggSpill {
				vars.StmtCtx.AppendWarning(ErrWarnDeprecatedSyntaxSimpleMsg.FastGen("tidb_enable_parallel_hashagg_spill will be removed in the future and hash aggregate spill will be enabled by default."))
			}
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableMutationChecker, Hidden: true,
		Value: BoolToOnOff(vardef.DefTiDBEnableMutationChecker), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableMutationChecker = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBTxnAssertionLevel, Value: vardef.GetDefaultTxnAssertionLevel(), PossibleValues: []string{vardef.AssertionOffStr, vardef.AssertionFastStr, vardef.AssertionStrictStr}, Hidden: true, Type: vardef.TypeEnum, SetSession: func(s *SessionVars, val string) error {
		s.AssertionLevel = tidbOptAssertionLevel(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBBatchPendingTiFlashCount, Value: strconv.Itoa(vardef.DefTiDBBatchPendingTiFlashCount), MinValue: 0, MaxValue: math.MaxUint32, Hidden: false, Type: vardef.TypeUnsigned, SetSession: func(s *SessionVars, val string) error {
		b, e := strconv.Atoi(val)
		if e != nil {
			b = vardef.DefTiDBBatchPendingTiFlashCount
		}
		s.BatchPendingTiFlashCount = b
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIgnorePreparedCacheCloseStmt, Value: BoolToOnOff(vardef.DefTiDBIgnorePreparedCacheCloseStmt), Type: vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IgnorePreparedCacheCloseStmt = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableNewCostInterface, Value: BoolToOnOff(true), Hidden: false, Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			if s == vardef.Off {
				vars.StmtCtx.AppendWarning(errWarnDeprecatedSyntax.FastGenByArgs(vardef.Off, vardef.On))
			}
			return vardef.On, nil
		},
		SetSession: func(*SessionVars, string) error {
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBCostModelVersion, Value: strconv.Itoa(vardef.DefTiDBCostModelVer), Hidden: false, Type: vardef.TypeInt, MinValue: 1, MaxValue: 2,
		SetSession: func(vars *SessionVars, s string) error {
			vars.CostModelVersion = int(TidbOptInt64(s, 1))
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIndexJoinDoubleReadPenaltyCostRate, Value: strconv.Itoa(0), Hidden: false, Type: vardef.TypeFloat, MinValue: 0, MaxValue: math.MaxUint64,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IndexJoinDoubleReadPenaltyCostRate = tidbOptFloat64(s, 0)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRCWriteCheckTs, Type: vardef.TypeBool, Value: BoolToOnOff(vardef.DefTiDBRcWriteCheckTs), SetSession: func(s *SessionVars, val string) error {
		s.RcWriteCheckTS = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRemoveOrderbyInSubquery, Value: BoolToOnOff(vardef.DefTiDBRemoveOrderbyInSubquery), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.RemoveOrderbyInSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMemQuotaQuery, Value: strconv.Itoa(vardef.DefTiDBMemQuotaQuery), Type: vardef.TypeInt, MinValue: -1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemQuotaQuery = TidbOptInt64(val, vardef.DefTiDBMemQuotaQuery)
		s.MemTracker.SetBytesLimit(s.MemQuotaQuery)
		return nil
	}, Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
		intVal := TidbOptInt64(normalizedValue, vardef.DefTiDBMemQuotaQuery)
		if intVal > 0 && intVal < 128 {
			vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBMemQuotaQuery, originalValue))
			normalizedValue = "128"
		}
		return normalizedValue, nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBNonTransactionalIgnoreError, Value: BoolToOnOff(vardef.DefTiDBBatchDMLIgnoreError), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.NonTransactionalIgnoreError = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashFineGrainedShuffleStreamCount, Value: strconv.Itoa(vardef.DefTiFlashFineGrainedShuffleStreamCount), Type: vardef.TypeInt, MinValue: -1, MaxValue: 1024,
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashFineGrainedShuffleStreamCount = TidbOptInt64(val, vardef.DefTiFlashFineGrainedShuffleStreamCount)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashFineGrainedShuffleBatchSize, Value: strconv.Itoa(vardef.DefTiFlashFineGrainedShuffleBatchSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxUint64,
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashFineGrainedShuffleBatchSize = uint64(TidbOptInt64(val, vardef.DefTiFlashFineGrainedShuffleBatchSize))
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSimplifiedMetrics, Value: BoolToOnOff(vardef.DefTiDBSimplifiedMetrics), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, vars *SessionVars, s string) error {
			metrics.ToggleSimplifiedMode(TiDBOptOn(s))
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMinPagingSize, Value: strconv.Itoa(vardef.DefMinPagingSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MinPagingSize = tidbOptPositiveInt32(val, vardef.DefMinPagingSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxPagingSize, Value: strconv.Itoa(vardef.DefMaxPagingSize), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MaxPagingSize = tidbOptPositiveInt32(val, vardef.DefMaxPagingSize)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMemoryDebugModeMinHeapInUse, Value: strconv.Itoa(0), Type: vardef.TypeInt, MinValue: math.MinInt64, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryDebugModeMinHeapInUse = TidbOptInt64(val, 0)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBMemoryDebugModeAlarmRatio, Value: strconv.Itoa(0), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.MemoryDebugModeAlarmRatio = TidbOptInt64(val, 0)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.SQLRequirePrimaryKey, Value: vardef.Off, Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PrimaryKeyRequired = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableAnalyzeSnapshot, Value: BoolToOnOff(vardef.DefTiDBEnableAnalyzeSnapshot), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAnalyzeSnapshot = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBGenerateBinaryPlan, Value: BoolToOnOff(vardef.DefTiDBGenerateBinaryPlan), Type: vardef.TypeBool, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		GenerateBinaryPlan.Store(TiDBOptOn(val))
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableDDLAnalyze, Value: BoolToOnOff(vardef.DefTiDBEnableDDLAnalyze), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			if TiDBOptOn(val) && s.AnalyzeVersion == 1 {
				return errors.New("tidb_stats_update_during_ddl can only be enabled with tidb_analyze_version 2")
			}
			s.EnableDDLAnalyze = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBDefaultStrMatchSelectivity, Value: strconv.FormatFloat(vardef.DefTiDBDefaultStrMatchSelectivity, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.DefaultStrMatchSelectivity = tidbOptFloat64(val, vardef.DefTiDBDefaultStrMatchSelectivity)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLEnableFastReorg, Value: BoolToOnOff(vardef.DefTiDBEnableFastReorg), Type: vardef.TypeBool, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableFastReorg.Load()), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.EnableFastReorg.Store(TiDBOptOn(val))
		return nil
	}},
	// This system var is set disk quota for lightning sort dir, from 100 GB to 1PB.
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBDDLDiskQuota, Value: strconv.Itoa(vardef.DefTiDBDDLDiskQuota), Type: vardef.TypeInt, MinValue: vardef.DefTiDBDDLDiskQuota, MaxValue: 1024 * 1024 * vardef.DefTiDBDDLDiskQuota / 100, GetGlobal: func(_ context.Context, sv *SessionVars) (string, error) {
		return strconv.FormatUint(vardef.DDLDiskQuota.Load(), 10), nil
	}, SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
		vardef.DDLDiskQuota.Store(TidbOptUint64(val, vardef.DefTiDBDDLDiskQuota))
		return nil
	}},
	// can't assign validate function here. Because validation function will run after GetGlobal function
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCloudStorageURI, Value: "", Type: vardef.TypeStr, GetGlobal: func(ctx context.Context, sv *SessionVars) (string, error) {
		cloudStorageURI := vardef.CloudStorageURI.Load()
		if len(cloudStorageURI) > 0 {
			cloudStorageURI = ast.RedactURL(cloudStorageURI)
		}
		return cloudStorageURI, nil
	}, SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		if len(val) > 0 && val != vardef.CloudStorageURI.Load() {
			if err := ValidateCloudStorageURI(ctx, val); err != nil {
				// convert annotations (second-level message) to message so clientConn.writeError
				// will print friendly error.
				if goerr.As(err, new(*errors.Error)) {
					err = errors.New(err.Error())
				}
				return err
			}
		}
		vardef.CloudStorageURI.Store(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBConstraintCheckInPlacePessimistic, Value: BoolToOnOff(config.GetGlobalConfig().PessimisticTxn.ConstraintCheckInPlacePessimistic), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.ConstraintCheckInPlacePessimistic = TiDBOptOn(val)
			if !s.ConstraintCheckInPlacePessimistic {
				metrics.LazyPessimisticUniqueCheckSetCount.Inc()
			}
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableTiFlashReadForWriteStmt, Value: vardef.On, Type: vardef.TypeBool,
		Validation: func(vars *SessionVars, s string, s2 string, flag vardef.ScopeFlag) (string, error) {
			if s == vardef.Off {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError("tidb_enable_tiflash_read_for_write_stmt is always turned on. This variable has been deprecated and will be removed in the future releases"))
			}
			return vardef.On, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableTiFlashReadForWriteStmt = true
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableUnsafeSubstitute, Value: BoolToOnOff(false), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableUnsafeSubstitute = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptRangeMaxSize, Value: strconv.FormatInt(vardef.DefTiDBOptRangeMaxSize, 10), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetSession: func(s *SessionVars, val string) error {
		s.RangeMaxSize = TidbOptInt64(val, vardef.DefTiDBOptRangeMaxSize)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptAdvancedJoinHint, Value: BoolToOnOff(vardef.DefTiDBOptAdvancedJoinHint), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableAdvancedJoinHint = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBOptUseInvisibleIndexes, Value: BoolToOnOff(false), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptimizerUseInvisibleIndexes = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:     vardef.TiDBAnalyzePartitionConcurrency,
		Value:    strconv.FormatInt(vardef.DefTiDBAnalyzePartitionConcurrency, 10),
		Type:     vardef.TypeInt,
		MinValue: 1,
		MaxValue: 128,
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzePartitionConcurrency = int(TidbOptInt64(val, vardef.DefTiDBAnalyzePartitionConcurrency))
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMergePartitionStatsConcurrency, Value: strconv.FormatInt(vardef.DefTiDBMergePartitionStatsConcurrency, 10), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency,
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzePartitionMergeConcurrency = TidbOptInt(val, vardef.DefTiDBMergePartitionStatsConcurrency)
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:  vardef.TiDBEnableAsyncMergeGlobalStats,
		Value: BoolToOnOff(vardef.DefTiDBEnableAsyncMergeGlobalStats),
		Type:  vardef.TypeBool,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			vars.StmtCtx.AppendWarning(errors.NewNoStackError("The 'tidb_enable_async_merge_global_stats' variable will always be enabled in a future release; changing it is discouraged."))
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.EnableAsyncMergeGlobalStats = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptPrefixIndexSingleScan, Value: BoolToOnOff(vardef.DefTiDBOptPrefixIndexSingleScan), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.OptPrefixIndexSingleScan = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptPartialOrderedIndexForTopN, Value: vardef.DefTiDBOptPartialOrderedIndexForTopN, Type: vardef.TypeEnum, PossibleValues: []string{"DISABLE", "COST"}, IsHintUpdatableVerified: true,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			// Allow DISABLE, COST (case-insensitive)
			upperValue := strings.ToUpper(strings.TrimSpace(originalValue))
			if upperValue != "DISABLE" && upperValue != "COST" {
				return normalizedValue, ErrWrongValueForVar.GenWithStackByArgs(vardef.TiDBOptPartialOrderedIndexForTopN, originalValue)
			}
			return upperValue, nil
		}, SetSession: func(s *SessionVars, val string) error {
			s.OptPartialOrderedIndexForTopN = strings.ToUpper(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBExternalTS, Value: strconv.FormatInt(vardef.DefTiDBExternalTS, 10), SetGlobal: func(ctx context.Context, s *SessionVars, val string) error {
		ts, err := parseTSFromNumberOrTime(s, val)
		if err != nil {
			return err
		}
		return SetExternalTimestamp(ctx, ts)
	}, GetGlobal: func(ctx context.Context, s *SessionVars) (string, error) {
		ts, err := GetExternalTimestamp(ctx)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(ts)), err
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableExternalTSRead, Value: BoolToOnOff(vardef.DefTiDBEnableExternalTSRead), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableExternalTSRead = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableReusechunk, Value: BoolToOnOff(vardef.DefTiDBEnableReusechunk), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableReuseChunk = TiDBOptOn(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBIgnoreInlistPlanDigest, Value: BoolToOnOff(vardef.DefTiDBIgnoreInlistPlanDigest), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.IgnoreInlistPlanDigest.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.IgnoreInlistPlanDigest.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLJobEnable, Value: BoolToOnOff(vardef.DefTiDBTTLJobEnable), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.EnableTTLJob.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableTTLJob.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLScanBatchSize, Value: strconv.Itoa(vardef.DefTiDBTTLScanBatchSize), Type: vardef.TypeInt, MinValue: vardef.DefTiDBTTLScanBatchMinSize, MaxValue: vardef.DefTiDBTTLScanBatchMaxSize, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLScanBatchSize.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := vardef.TTLScanBatchSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLDeleteBatchSize, Value: strconv.Itoa(vardef.DefTiDBTTLDeleteBatchSize), Type: vardef.TypeInt, MinValue: vardef.DefTiDBTTLDeleteBatchMinSize, MaxValue: vardef.DefTiDBTTLDeleteBatchMaxSize, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLDeleteBatchSize.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := vardef.TTLDeleteBatchSize.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLDeleteRateLimit, Value: strconv.Itoa(vardef.DefTiDBTTLDeleteRateLimit), Type: vardef.TypeInt, MinValue: 0, MaxValue: math.MaxInt64, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLDeleteRateLimit.Store(val)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		val := vardef.TTLDeleteRateLimit.Load()
		return strconv.FormatInt(val, 10), nil
	}},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBStoreBatchSize, Value: strconv.FormatInt(vardef.DefTiDBStoreBatchSize, 10),
		Type: vardef.TypeInt, MinValue: 0, MaxValue: 25000, SetSession: func(s *SessionVars, val string) error {
			s.StoreBatchSize = TidbOptInt(val, vardef.DefTiDBStoreBatchSize)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.MppExchangeCompressionMode, Type: vardef.TypeStr, Value: vardef.DefaultExchangeCompressionMode.Name(),
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := vardef.ToExchangeCompressionMode(normalizedValue)
			if !ok {
				var msg string
				for m := vardef.ExchangeCompressionModeNONE; m <= vardef.ExchangeCompressionModeUnspecified; m += 1 {
					if m == 0 {
						msg = m.Name()
					} else {
						msg = fmt.Sprintf("%s, %s", msg, m.Name())
					}
				}
				err := fmt.Errorf("incorrect value: `%s`. %s options: %s",
					originalValue,
					vardef.MppExchangeCompressionMode, msg)
				return normalizedValue, err
			}
			return normalizedValue, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.mppExchangeCompressionMode, _ = vardef.ToExchangeCompressionMode(val)
			if s.ChooseMppVersion() == kv.MppVersionV0 && s.mppExchangeCompressionMode != vardef.ExchangeCompressionModeUnspecified {
				s.StmtCtx.AppendWarning(fmt.Errorf("mpp exchange compression won't work under current mpp version %d", kv.MppVersionV0))
			}

			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.MppVersion, Type: vardef.TypeStr, Value: kv.MppVersionUnspecifiedName,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := kv.ToMppVersion(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s options: %d (unspecified)",
				originalValue, vardef.MppVersion, kv.MppVersionUnspecified)
			for i := kv.MppVersionV0; i <= kv.GetNewestMppVersion(); i += 1 {
				errMsg = fmt.Sprintf("%s, %d", errMsg, i)
			}

			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			version, _ := kv.ToMppVersion(val)
			s.mppVersion = version
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLJobScheduleWindowStartTime, Value: vardef.DefTiDBTTLJobScheduleWindowStartTime, Type: vardef.TypeTime, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			startTime, err := time.ParseInLocation(vardef.FullDayTimeFormat, s, time.UTC)
			if err != nil {
				return err
			}
			vardef.TTLJobScheduleWindowStartTime.Store(startTime)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			startTime := vardef.TTLJobScheduleWindowStartTime.Load()
			return startTime.Format(vardef.FullDayTimeFormat), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLJobScheduleWindowEndTime, Value: vardef.DefTiDBTTLJobScheduleWindowEndTime, Type: vardef.TypeTime, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			endTime, err := time.ParseInLocation(vardef.FullDayTimeFormat, s, time.UTC)
			if err != nil {
				return err
			}
			vardef.TTLJobScheduleWindowEndTime.Store(endTime)
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			endTime := vardef.TTLJobScheduleWindowEndTime.Load()
			return endTime.Format(vardef.FullDayTimeFormat), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLScanWorkerCount, Value: strconv.Itoa(vardef.DefTiDBTTLScanWorkerCount), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 256, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			vardef.TTLScanWorkerCount.Store(int32(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.Itoa(int(vardef.TTLScanWorkerCount.Load())), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLDeleteWorkerCount, Value: strconv.Itoa(vardef.DefTiDBTTLDeleteWorkerCount), Type: vardef.TypeUnsigned, MinValue: 1, MaxValue: 256, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			val, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return err
			}
			vardef.TTLDeleteWorkerCount.Store(int32(val))
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return strconv.Itoa(int(vardef.TTLDeleteWorkerCount.Load())), nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableResourceControl, Value: BoolToOnOff(vardef.DefTiDBEnableResourceControl), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		if TiDBOptOn(s) != vardef.EnableResourceControl.Load() {
			vardef.EnableResourceControl.Store(TiDBOptOn(s))
			(*SetGlobalResourceControl.Load())(TiDBOptOn(s))
			logutil.BgLogger().Info("set resource control", zap.Bool("enable", TiDBOptOn(s)))
		}
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableResourceControl.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBResourceControlStrictMode, Value: BoolToOnOff(vardef.DefTiDBResourceControlStrictMode), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		opOn := TiDBOptOn(s)
		if opOn != vardef.EnableResourceControlStrictMode.Load() {
			vardef.EnableResourceControlStrictMode.Store(opOn)
			logutil.BgLogger().Info("change resource control strict mode", zap.Bool("enable", TiDBOptOn(s)))
		}
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableResourceControlStrictMode.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPessimisticTransactionFairLocking, Value: BoolToOnOff(vardef.DefTiDBPessimisticTransactionFairLocking), Type: vardef.TypeBool,
		Validation: func(_ *SessionVars, val string, _ string, _ vardef.ScopeFlag) (string, error) {
			if kerneltype.IsNextGen() && TiDBOptOn(val) {
				return vardef.Off, ErrNotSupportedInNextGen.FastGenByArgs(vardef.TiDBPessimisticTransactionFairLocking)
			}
			return val, nil
		},
		SetSession: func(s *SessionVars, val string) error {
			s.PessimisticTransactionFairLocking = TiDBOptOn(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanCacheForParamLimit, Value: BoolToOnOff(vardef.DefTiDBEnablePlanCacheForParamLimit), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePlanCacheForParamLimit = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableINLJoinInnerMultiPattern, Value: BoolToOnOff(vardef.DefTiDBEnableINLJoinMultiPattern), Type: vardef.TypeBool,
		SetSession: func(s *SessionVars, val string) error {
			s.EnableINLJoinInnerMultiPattern = TiDBOptOn(val)
			return nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return BoolToOnOff(s.EnableINLJoinInnerMultiPattern), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashComputeDispatchPolicy, Value: string(vardef.DefTiFlashComputeDispatchPolicy), Type: vardef.TypeStr, SetSession: setTiFlashComputeDispatchPolicy,
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			return setTiFlashComputeDispatchPolicy(vars, s)
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnablePlanCacheForSubquery, Value: BoolToOnOff(vardef.DefTiDBEnablePlanCacheForSubquery), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnablePlanCacheForSubquery = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableLateMaterialization, Value: BoolToOnOff(vardef.DefTiDBOptEnableLateMaterialization), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLateMaterialization = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBLoadBasedReplicaReadThreshold, Value: vardef.DefTiDBLoadBasedReplicaReadThreshold.String(), Type: vardef.TypeDuration, MaxValue: uint64(time.Hour), SetSession: func(s *SessionVars, val string) error {
		d, err := time.ParseDuration(val)
		if err != nil {
			return err
		}
		s.LoadBasedReplicaReadThreshold = d
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTTLRunningTasks, Value: strconv.Itoa(vardef.DefTiDBTTLRunningTasks), Type: vardef.TypeInt, MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency, AllowAutoValue: true, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		vardef.TTLRunningTasks.Store(int32(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(int(vardef.TTLRunningTasks.Load())), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptOrderingIdxSelThresh, Value: strconv.FormatFloat(vardef.DefTiDBOptOrderingIdxSelThresh, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.OptOrderingIdxSelThresh = tidbOptFloat64(val, vardef.DefTiDBOptOrderingIdxSelThresh)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptOrderingIdxSelRatio, Value: strconv.FormatFloat(vardef.DefTiDBOptOrderingIdxSelRatio, 'f', -1, 64), Type: vardef.TypeFloat, MinValue: -1, MaxValue: 1,
		SetSession: func(s *SessionVars, val string) error {
			s.OptOrderingIdxSelRatio = tidbOptFloat64(val, vardef.DefTiDBOptOrderingIdxSelRatio)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBOptEnableMPPSharedCTEExecution, Value: BoolToOnOff(vardef.DefTiDBOptEnableMPPSharedCTEExecution), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableMPPSharedCTEExecution = TiDBOptOn(val)
		return nil
	}},
	{
		Scope:                   vardef.ScopeGlobal | vardef.ScopeSession,
		Name:                    vardef.TiDBOptFixControl,
		Value:                   "",
		Type:                    vardef.TypeStr,
		IsHintUpdatableVerified: true,
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			// validation logic for setting global
			// we don't put this in Validation to avoid repeating the checking logic for setting session.
			_, warnings, err := fixcontrol.ParseToMap(val)
			if err != nil {
				return err
			}
			for _, warning := range warnings {
				vars.StmtCtx.AppendWarning(errors.NewNoStackError(warning))
			}
			return nil
		},
		SetSession: func(s *SessionVars, val string) error {
			newMap, warnings, err := fixcontrol.ParseToMap(val)
			if err != nil {
				return err
			}
			for _, warning := range warnings {
				s.StmtCtx.AppendWarning(errors.NewNoStackError(warning))
			}
			s.OptimizerFixControl = newMap
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBAnalyzeSkipColumnTypes, Value: "json,blob,mediumblob,longblob,mediumtext,longtext", Type: vardef.TypeStr,
		Validation: func(vars *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			return ValidAnalyzeSkipColumnTypes(normalizedValue)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.AnalyzeSkipColumnTypes = ParseAnalyzeSkipColumnTypes(val)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBPlanCacheInvalidationOnFreshStats, Value: BoolToOnOff(vardef.DefTiDBPlanCacheInvalidationOnFreshStats), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.PlanCacheInvalidationOnFreshStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashReplicaRead, Value: vardef.DefTiFlashReplicaRead, Type: vardef.TypeEnum, PossibleValues: []string{vardef.DefTiFlashReplicaRead, vardef.ClosestAdaptiveStr, vardef.ClosestReplicasStr},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashReplicaRead = tiflash.GetTiFlashReplicaReadByStr(val)
			return nil
		},
		GetSession: func(s *SessionVars) (string, error) {
			return tiflash.GetTiFlashReplicaRead(s.TiFlashReplicaRead), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBFastCheckTable, Value: BoolToOnOff(vardef.DefTiDBEnableFastCheckTable), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.FastCheckTable = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSkipMissingPartitionStats, Value: BoolToOnOff(vardef.DefTiDBSkipMissingPartitionStats), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.SkipMissingPartitionStats = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLAuthMethodName, Value: vardef.DefAuthenticationLDAPSASLAuthMethodName, Type: vardef.TypeEnum, PossibleValues: []string{ldap.SASLAuthMethodSCRAMSHA1, ldap.SASLAuthMethodSCRAMSHA256, ldap.SASLAuthMethodGSSAPI}, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetSASLAuthMethod(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetSASLAuthMethod(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLServerHost, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSASLAuthImpl.SetLDAPServerHost(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetLDAPServerHost(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLServerPort, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSASLServerPort), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxUint16, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetLDAPServerPort(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetLDAPServerPort()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLTLS, Value: BoolToOnOff(vardef.DefAuthenticationLDAPSASLTLS), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetEnableTLS(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(ldap.LDAPSASLAuthImpl.GetEnableTLS()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLCAPath, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		return ldap.LDAPSASLAuthImpl.SetCAPath(s)
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetCAPath(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLUserSearchAttr, Value: vardef.DefAuthenticationLDAPSASLUserSearchAttr, Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSASLAuthImpl.SetSearchAttr(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetSearchAttr(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLBindBaseDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindBaseDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetBindBaseDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLBindRootDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindRootDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSASLAuthImpl.GetBindRootDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLBindRootPWD, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSASLAuthImpl.SetBindRootPW(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		if ldap.LDAPSASLAuthImpl.GetBindRootPW() == "" {
			return "", nil
		}
		return vardef.MaskPwd, nil
	}},
	// TODO: allow setting init_pool_size to 0 to disable pooling
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLInitPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSASLInitPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetInitCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetInitCapacity()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSASLMaxPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSASLMaxPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSASLAuthImpl.SetMaxCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSASLAuthImpl.GetMaxCapacity()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleAuthMethodName, Value: vardef.DefAuthenticationLDAPSimpleAuthMethodName, Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		s = strings.ToUpper(s)
		// Only "SIMPLE" is supported
		if s != "SIMPLE" {
			return errors.Errorf("auth method %s is not supported", s)
		}
		return nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleServerHost, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSimpleAuthImpl.SetLDAPServerHost(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetLDAPServerHost(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleServerPort, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSimpleServerPort), Type: vardef.TypeInt, MinValue: 1, MaxValue: math.MaxUint16, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetLDAPServerPort(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetLDAPServerPort()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleTLS, Value: BoolToOnOff(vardef.DefAuthenticationLDAPSimpleTLS), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetEnableTLS(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(ldap.LDAPSimpleAuthImpl.GetEnableTLS()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleCAPath, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		return ldap.LDAPSimpleAuthImpl.SetCAPath(s)
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetCAPath(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleUserSearchAttr, Value: vardef.DefAuthenticationLDAPSimpleUserSearchAttr, Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		// TODO: validate the ip/hostname
		ldap.LDAPSimpleAuthImpl.SetSearchAttr(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetSearchAttr(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleBindBaseDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindBaseDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetBindBaseDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleBindRootDN, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindRootDN(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return ldap.LDAPSimpleAuthImpl.GetBindRootDN(), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleBindRootPWD, Value: "", Type: vardef.TypeStr, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		ldap.LDAPSimpleAuthImpl.SetBindRootPW(s)
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		if ldap.LDAPSimpleAuthImpl.GetBindRootPW() == "" {
			return "", nil
		}
		return vardef.MaskPwd, nil
	}},
	// TODO: allow setting init_pool_size to 0 to disable pooling
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleInitPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSimpleInitPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetInitCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetInitCapacity()), nil
	}},
	{Scope: vardef.ScopeGlobal, Name: vardef.AuthenticationLDAPSimpleMaxPoolSize, Value: strconv.Itoa(vardef.DefAuthenticationLDAPSimpleMaxPoolSize), Type: vardef.TypeInt, MinValue: 1, MaxValue: 32767, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ldap.LDAPSimpleAuthImpl.SetMaxCapacity(int(val))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return strconv.Itoa(ldap.LDAPSimpleAuthImpl.GetMaxCapacity()), nil
	}},
	// runtime filter variables group
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRuntimeFilterTypeName, Value: vardef.DefRuntimeFilterType, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := ToRuntimeFilterType(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s should be sepreated by , such as %s, also we only support IN and MIN_MAX now. ",
				originalValue, vardef.TiDBRuntimeFilterTypeName, vardef.DefRuntimeFilterType)
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.runtimeFilterTypes, _ = ToRuntimeFilterType(val)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBRuntimeFilterModeName, Value: vardef.DefRuntimeFilterMode, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			_, ok := RuntimeFilterModeStringToMode(normalizedValue)
			if ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: %s. %s options: %s ",
				originalValue, vardef.TiDBRuntimeFilterModeName, vardef.DefRuntimeFilterMode)
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.runtimeFilterMode, _ = RuntimeFilterModeStringToMode(val)
			return nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:  vardef.TiDBLockUnchangedKeys,
		Value: BoolToOnOff(vardef.DefTiDBLockUnchangedKeys),
		Type:  vardef.TypeBool,
		SetSession: func(vars *SessionVars, s string) error {
			vars.LockUnchangedKeys = TiDBOptOn(s)
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBEnableCheckConstraint, Value: BoolToOnOff(vardef.DefTiDBEnableCheckConstraint), Type: vardef.TypeBool, SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
		vardef.EnableCheckConstraint.Store(TiDBOptOn(s))
		return nil
	}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
		return BoolToOnOff(vardef.EnableCheckConstraint.Load()), nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeInstance, Name: vardef.TiDBSchemaCacheSize, Value: strconv.Itoa(vardef.DefTiDBSchemaCacheSize), Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			_, str, err := parseSchemaCacheSize(s, normalizedValue, originalValue)
			if err != nil {
				return "", err
			}
			return str, nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, val string) error {
			// It does not take effect immediately, but within a ddl lease, infoschema reload would cause the v2 to be used.
			bt, str, err := parseSchemaCacheSize(vars, val, val)
			if err != nil {
				return err
			}
			if vardef.SchemaCacheSize.Load() != bt && ChangeSchemaCacheSize != nil {
				if err := ChangeSchemaCacheSize(ctx, bt); err != nil {
					return err
				}
			}
			vardef.SchemaCacheSize.Store(bt)
			vardef.SchemaCacheSizeOriginText.Store(str)
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.SchemaCacheSizeOriginText.Load(), nil
		}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBSessionAlias, Value: "", Type: vardef.TypeStr,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			chars := []rune(normalizedValue)
			warningAdded := false
			if len(chars) > 64 {
				s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBSessionAlias, originalValue))
				warningAdded = true
				chars = chars[:64]
				normalizedValue = string(chars)
			}

			// truncate to a valid identifier
			for normalizedValue != "" && util.IsInCorrectIdentifierName(normalizedValue) {
				if !warningAdded {
					s.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(vardef.TiDBSessionAlias, originalValue))
					warningAdded = true
				}
				chars = chars[:len(chars)-1]
				normalizedValue = string(chars)
			}

			return normalizedValue, nil
		},
		SetSession: func(vars *SessionVars, s string) error {
			vars.SessionAlias = s
			return nil
		}, GetSession: func(vars *SessionVars) (string, error) {
			return vars.SessionAlias, nil
		}},
	{
		Scope:          vardef.ScopeGlobal | vardef.ScopeSession,
		Name:           vardef.TiDBOptObjective,
		Value:          vardef.DefTiDBOptObjective,
		Type:           vardef.TypeEnum,
		PossibleValues: []string{vardef.OptObjectiveModerate, vardef.OptObjectiveDeterminate},
		SetSession: func(vars *SessionVars, s string) error {
			vars.OptObjective = s
			return nil
		},
	},
	{Scope: vardef.ScopeInstance, Name: vardef.TiDBServiceScope, Value: "", Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			return normalizedValue, naming.Check(originalValue)
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			newValue := strings.ToLower(s)
			vardef.ServiceScope.Store(newValue)
			oldConfig := config.GetGlobalConfig()
			if oldConfig.Instance.TiDBServiceScope != newValue {
				newConfig := *oldConfig
				newConfig.Instance.TiDBServiceScope = newValue
				config.StoreGlobalConfig(&newConfig)
			}
			return nil
		}, GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.ServiceScope.Load(), nil
		}},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBSchemaVersionCacheLimit, Value: strconv.Itoa(vardef.DefTiDBSchemaVersionCacheLimit), Type: vardef.TypeInt, MinValue: 2, MaxValue: math.MaxUint8, AllowEmpty: true,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.SchemaVersionCacheLimit.Store(TidbOptInt64(val, vardef.DefTiDBSchemaVersionCacheLimit))
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBIdleTransactionTimeout, Value: strconv.Itoa(vardef.DefTiDBIdleTransactionTimeout), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: secondsPerYear,
		SetSession: func(s *SessionVars, val string) error {
			s.IdleTransactionTimeout = tidbOptPositiveInt32(val, vardef.DefTiDBIdleTransactionTimeout)
			return nil
		}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.DivPrecisionIncrement, Value: strconv.Itoa(vardef.DefDivPrecisionIncrement), Type: vardef.TypeUnsigned, MinValue: 0, MaxValue: 30,
		SetSession: func(s *SessionVars, val string) error {
			s.DivPrecisionIncrement = tidbOptPositiveInt32(val, vardef.DefDivPrecisionIncrement)
			return nil
		}},
	{Scope: vardef.ScopeSession, Name: vardef.TiDBDMLType, Value: vardef.DefTiDBDMLType, Type: vardef.TypeStr,
		SetSession: func(s *SessionVars, val string) error {
			lowerVal := strings.ToLower(val)
			if strings.EqualFold(lowerVal, "standard") {
				s.BulkDMLEnabled = false
				return nil
			}
			if strings.EqualFold(lowerVal, "bulk") {
				s.BulkDMLEnabled = true
				return nil
			}
			return errors.Errorf("unsupport DML type: %s", val)
		},
		IsHintUpdatableVerified: true,
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiFlashHashAggPreAggMode, Value: vardef.DefTiFlashPreAggMode, Type: vardef.TypeStr,
		Validation: func(_ *SessionVars, normalizedValue string, originalValue string, _ vardef.ScopeFlag) (string, error) {
			if _, ok := ToTiPBTiFlashPreAggMode(normalizedValue); ok {
				return normalizedValue, nil
			}
			errMsg := fmt.Sprintf("incorrect value: `%s`. %s options: %s",
				originalValue, vardef.TiFlashHashAggPreAggMode, ValidTiFlashPreAggMode())
			return normalizedValue, errors.New(errMsg)
		},
		SetSession: func(s *SessionVars, val string) error {
			s.TiFlashPreAggMode = val
			return nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableLazyCursorFetch, Value: BoolToOnOff(vardef.DefTiDBEnableLazyCursorFetch), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		s.EnableLazyCursorFetch = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBEnableSharedLockPromotion, Value: BoolToOnOff(vardef.DefTiDBEnableSharedLockPromotion), Type: vardef.TypeBool, SetSession: func(s *SessionVars, val string) error {
		if s.NoopFuncsMode != OffInt && TiDBOptOn(val) {
			logutil.BgLogger().Warn("tidb_enable_shared_lock_promotion set to on would override tidb_enable_noop_functions on")
		}
		s.SharedLockPromotion = TiDBOptOn(val)
		return nil
	}},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBMaxDistTaskNodes, Value: strconv.Itoa(vardef.DefTiDBMaxDistTaskNodes), Type: vardef.TypeInt, MinValue: -1, MaxValue: 128,
		Validation: func(s *SessionVars, normalizedValue string, originalValue string, scope vardef.ScopeFlag) (string, error) {
			maxNodes := TidbOptInt(normalizedValue, vardef.DefTiDBMaxDistTaskNodes)
			if maxNodes == 0 {
				return normalizedValue, errors.New("max_dist_task_nodes should be -1 or [1, 128]")
			}
			return normalizedValue, nil
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBTSOClientRPCMode, Value: vardef.DefTiDBTSOClientRPCMode, Type: vardef.TypeEnum, PossibleValues: []string{vardef.TSOClientRPCModeDefault, vardef.TSOClientRPCModeParallel, vardef.TSOClientRPCModeParallelFast},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return (*SetPDClientDynamicOption.Load())(vardef.TiDBTSOClientRPCMode, val)
		},
	},
	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBCircuitBreakerPDMetadataErrorRateThresholdRatio, Value: strconv.FormatFloat(vardef.DefTiDBCircuitBreakerPDMetaErrorRateRatio, 'f', -1, 64),
		Type: vardef.TypeFloat, MinValue: 0, MaxValue: 1,
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return strconv.FormatFloat(vardef.CircuitBreakerPDMetadataErrorRateThresholdRatio.Load(), 'f', -1, 64), nil
		},
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			v := tidbOptFloat64(val, vardef.DefTiDBCircuitBreakerPDMetaErrorRateRatio)
			if v < 0 || v > 1 {
				return errors.Errorf("invalid tidb_cb_pd_metadata_error_rate_threshold_ratio value %s", val)
			}
			vardef.CircuitBreakerPDMetadataErrorRateThresholdRatio.Store(v)
			if ChangePDMetadataCircuitBreakerErrorRateThresholdRatio != nil {
				ChangePDMetadataCircuitBreakerErrorRateThresholdRatio(uint32(v * 100))
			}
			return nil
		},
	},

	{Scope: vardef.ScopeGlobal, Name: vardef.TiDBAccelerateUserCreationUpdate, Value: BoolToOnOff(vardef.DefTiDBAccelerateUserCreationUpdate), Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			vardef.AccelerateUserCreationUpdate.Store(TiDBOptOn(val))
			return nil
		},
	},
	{
		Scope:      vardef.ScopeGlobal | vardef.ScopeSession,
		Name:       vardef.TiDBPipelinedDmlResourcePolicy,
		Value:      vardef.DefTiDBPipelinedDmlResourcePolicy,
		Type:       vardef.TypeStr,
		SetSession: setPipelinedDmlResourcePolicy,
		// because the special character in custom syntax cannot be correctly handled in set_var hint
		IsHintUpdatableVerified: true,
	},
	{
		Scope:    vardef.ScopeGlobal,
		Name:     vardef.TiDBAdvancerCheckPointLagLimit,
		Value:    vardef.DefTiDBAdvancerCheckPointLagLimit.String(),
		Type:     vardef.TypeDuration,
		MinValue: int64(time.Second), MaxValue: uint64(time.Hour * 24 * 365),
		SetGlobal: func(_ context.Context, sv *SessionVars, s string) error {
			d, err := time.ParseDuration(s)
			if err != nil {
				return err
			}
			vardef.AdvancerCheckPointLagLimit.Store(d)
			return nil
		},
		GetGlobal: func(ctx context.Context, sv *SessionVars) (string, error) {
			return vardef.AdvancerCheckPointLagLimit.Load().String(), nil
		},
	},
	{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: vardef.TiDBSlowLogRules, Value: "", Type: vardef.TypeStr,
		SetSession: func(s *SessionVars, val string) error {
			slowLogRules, err := ParseSessionSlowLogRules(val)
			if err != nil {
				return err
			}
			s.SlowLogRules.SlowLogRules = slowLogRules
			s.SlowLogRules.NeedUpdateEffectiveFields = true
			return nil
		},
		GetSession: func(vars *SessionVars) (string, error) {
			if vars.SlowLogRules.SlowLogRules != nil {
				return vars.SlowLogRules.RawRules, nil
			}
			return "", nil
		},
		SetGlobal: func(ctx context.Context, vars *SessionVars, s string) error {
			gRules, err := ParseGlobalSlowLogRules(s)
			if err != nil {
				return err
			}
			vardef.GlobalSlowLogRules.Store(gRules)
			return nil
		},
		GetGlobal: func(ctx context.Context, vars *SessionVars) (string, error) {
			return vardef.GlobalSlowLogRules.Load().RawRules, nil
		},
	},
	{
		Scope:    vardef.ScopeGlobal,
		Name:     vardef.TiDBSlowLogMaxPerSec,
		Value:    "0",
		Type:     vardef.TypeInt,
		MinValue: 0, MaxValue: 1000000,
		SetGlobal: func(_ context.Context, sv *SessionVars, s string) error {
			d := TidbOptInt(s, 0)
			if d == int(vardef.GlobalSlowLogRateLimiter.Limit()) {
				return nil
			}

			if d == 0 {
				vardef.GlobalSlowLogRateLimiter.SetLimit(rate.Inf)
				return nil
			}
			vardef.GlobalSlowLogRateLimiter.SetLimit(rate.Limit(d))
			vardef.GlobalSlowLogRateLimiter.SetBurst(d)
			return nil
		},
		GetGlobal: func(ctx context.Context, sv *SessionVars) (string, error) {
			return strconv.Itoa(int(vardef.GlobalSlowLogRateLimiter.Limit())), nil
		},
	},
	{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:  vardef.TiDBIndexLookUpPushDownPolicy,
		Value: vardef.DefTiDBIndexLookUpPushDownPolicy,
		Type:  vardef.TypeEnum,
		PossibleValues: []string{
			vardef.IndexLookUpPushDownPolicyHintOnly,
			vardef.IndexLookUpPushDownPolicyAffinityForce,
			vardef.IndexLookUpPushDownPolicyForce,
		},
		IsHintUpdatableVerified: true,
		SetSession: func(vars *SessionVars, s string) error {
			vars.IndexLookUpPushDownPolicy = s
			return nil
		},
	},
}
