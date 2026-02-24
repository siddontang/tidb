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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"go.uber.org/zap"
)

type concurrencySetter func(s *SessionVars, v int)
type execConcurrencySysVarOption func(sv *SysVar)

func withAllowAutoValue(allow bool) execConcurrencySysVarOption {
	return func(sv *SysVar) { sv.AllowAutoValue = allow }
}

func withMinValue(minVal int64) execConcurrencySysVarOption {
	return func(sv *SysVar) { sv.MinValue = minVal }
}

// newExecConcurrencySysVar creates a session/global SysVar for executor concurrency settings.
func newExecConcurrencySysVar(name string, defValue int, setter concurrencySetter, opts ...execConcurrencySysVarOption) *SysVar {
	sv := &SysVar{
		Scope: vardef.ScopeGlobal | vardef.ScopeSession,
		Name:  name,
		Value: strconv.Itoa(defValue), Type: vardef.TypeInt,
		MinValue: 1, MaxValue: vardef.MaxConfigurableConcurrency,
		AllowAutoValue: true,
		SetSession: func(s *SessionVars, val string) error {
			setter(s, tidbOptPositiveInt32(val, vardef.ConcurrencyUnset))
			return nil
		},
		Validation: func(vars *SessionVars, normalizedValue, originalValue string, scope vardef.ScopeFlag) (string, error) {
			appendDeprecationWarning(vars, name, vardef.TiDBExecutorConcurrency)
			return normalizedValue, nil
		},
	}
	for _, opt := range opts {
		opt(sv)
	}

	return sv
}

// All system variables are ordered by their scopes, which follow the order of scopes below:
//
//	[NONE, SESSION, INSTANCE, GLOBAL, GLOBAL & SESSION]
//
// If you are adding a new system variable, please put it in the corresponding scope file:
//   - sysvar_none_session.go: NONE and SESSION scope
//   - sysvar_instance_global.go: INSTANCE and GLOBAL scope
//   - sysvar_global_session.go: GLOBAL & SESSION scope
var defaultSysVars = func() []*SysVar {
	vars := make([]*SysVar, 0, len(defaultSysVarsNoneSession)+len(defaultSysVarsInstanceGlobal)+len(defaultSysVarsGlobalSession))
	vars = append(vars, defaultSysVarsNoneSession...)
	vars = append(vars, defaultSysVarsInstanceGlobal...)
	vars = append(vars, defaultSysVarsGlobalSession...)
	return vars
}()

// GlobalSystemVariableInitialValue gets the default value for a system variable including ones that are dynamically set (e.g. based on the store)
func GlobalSystemVariableInitialValue(varName, varVal string) string {
	switch varName {
	case vardef.TiDBEnableAsyncCommit, vardef.TiDBEnable1PC:
		if config.GetGlobalConfig().Store == config.StoreTypeTiKV {
			varVal = vardef.On
		}
	case vardef.TiDBMemOOMAction:
		if intest.InTest {
			varVal = vardef.OOMActionLog
		}
	case vardef.TiDBEnableAutoAnalyze:
		if intest.InTest {
			varVal = vardef.Off
		}
	// For the following sysvars, we change the default
	// FOR NEW INSTALLS ONLY. In most cases you don't want to do this.
	// It is better to change the value in the Sysvar struct, so that
	// all installs will have the same value.
	case vardef.TiDBRowFormatVersion:
		varVal = strconv.Itoa(vardef.DefTiDBRowFormatV2)
	case vardef.TiDBTxnAssertionLevel:
		if kerneltype.IsNextGen() {
			varVal = vardef.GetDefaultTxnAssertionLevel()
		} else {
			varVal = vardef.AssertionFastStr
		}
	case vardef.TiDBEnableMutationChecker:
		varVal = vardef.On
	case vardef.TiDBPessimisticTransactionFairLocking:
		if kerneltype.IsNextGen() {
			varVal = vardef.Off
		} else {
			varVal = vardef.On
		}
	}
	return varVal
}

func setTiFlashComputeDispatchPolicy(s *SessionVars, val string) error {
	p, err := tiflashcompute.GetDispatchPolicyByStr(val)
	if err != nil {
		return err
	}
	s.TiFlashComputeDispatchPolicy = p
	return nil
}

func setPipelinedDmlResourcePolicy(s *SessionVars, val string) error {
	// ensure the value is trimmed and lowercased
	val = strings.TrimSpace(val)
	lowVal := strings.ToLower(val)
	switch lowVal {
	case vardef.StrategyStandard:
		s.PipelinedDMLConfig.PipelinedFlushConcurrency = vardef.DefaultFlushConcurrency
		s.PipelinedDMLConfig.PipelinedResolveLockConcurrency = vardef.DefaultResolveConcurrency
		s.PipelinedDMLConfig.PipelinedWriteThrottleRatio = 0
	case vardef.StrategyConservative:
		s.PipelinedDMLConfig.PipelinedFlushConcurrency = vardef.ConservativeFlushConcurrency
		s.PipelinedDMLConfig.PipelinedResolveLockConcurrency = vardef.ConservativeResolveConcurrency
		s.PipelinedDMLConfig.PipelinedWriteThrottleRatio = 0
	default:
		// Create a temporary config to hold new values to avoid partial application
		newConfig := PipelinedDMLConfig{
			PipelinedFlushConcurrency:       vardef.DefaultFlushConcurrency,
			PipelinedResolveLockConcurrency: vardef.DefaultResolveConcurrency,
			PipelinedWriteThrottleRatio:     0,
		}

		// More flexible custom format validation
		if !strings.HasPrefix(lowVal, vardef.StrategyCustom) {
			return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
		}

		// Extract everything after "custom"
		remaining := strings.TrimSpace(lowVal[len(vardef.StrategyCustom):])
		if len(remaining) < 2 || !strings.HasPrefix(remaining, "{") || !strings.HasSuffix(remaining, "}") {
			return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
		}

		// Extract and trim content between brackets
		content := strings.TrimSpace(remaining[1 : len(remaining)-1])
		if content == "" {
			return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
		}

		// Split parameters
		rawParams := strings.Split(content, ",")
		for _, rawParam := range rawParams {
			param := strings.TrimSpace(rawParam)
			if param == "" {
				return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
			}

			// Split key-values
			parts := strings.FieldsFunc(param, func(r rune) bool {
				return r == '=' || r == ':'
			})

			if len(parts) != 2 {
				return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
			}

			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			switch key {
			case "concurrency":
				concurrency, err := strconv.ParseInt(value, 10, 64)
				if err != nil || concurrency < vardef.MinPipelinedDMLConcurrency || concurrency > vardef.MaxPipelinedDMLConcurrency {
					logutil.BgLogger().Warn(
						"invalid concurrency value in pipelined DML resource policy",
						zap.String("value", val),
						zap.String("concurrency", value),
						zap.Error(err),
					)
					return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
				}
				newConfig.PipelinedFlushConcurrency = int(concurrency)
			case "resolve_concurrency":
				concurrency, err := strconv.ParseInt(value, 10, 64)
				if err != nil || concurrency < vardef.MinPipelinedDMLConcurrency || concurrency > vardef.MaxPipelinedDMLConcurrency {
					logutil.BgLogger().Warn(
						"invalid resolve_concurrency value in pipelined DML resource policy",
						zap.String("value", val),
						zap.String("resolve_concurrency", value),
						zap.Error(err),
					)
					return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
				}
				newConfig.PipelinedResolveLockConcurrency = int(concurrency)
			case "write_throttle_ratio":
				ratio, err := strconv.ParseFloat(value, 64)
				if err != nil || ratio < 0 || ratio >= 1 {
					logutil.BgLogger().Warn(
						"invalid write_throttle_ratio value in pipelined DML resource policy",
						zap.String("value", val),
						zap.String("write_throttle_ratio", value),
						zap.Error(err),
					)
					return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
				}
				newConfig.PipelinedWriteThrottleRatio = ratio
			default:
				return ErrWrongValueForVar.FastGenByArgs(vardef.TiDBPipelinedDmlResourcePolicy, val)
			}
		}

		// Only apply changes after all validation passed
		s.PipelinedDMLConfig = newConfig
	}
	return nil
}
