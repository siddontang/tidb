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

package expression

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type timeFormatFunctionClass struct {
	baseFunctionClass
}

func (c *timeFormatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDuration, types.ETString)
	if err != nil {
		return nil, err
	}
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.SetFlen((args[1].GetType(ctx.GetEvalCtx()).GetFlen() + 1) / 2 * 11)
	sig := &builtinTimeFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimeFormat)
	return sig, nil
}

type builtinTimeFormatSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTimeFormatSig) Clone() builtinFunc {
	newSig := &builtinTimeFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	// if err != nil, then dur is ZeroDuration, outputs 00:00:00 in this case which follows the behavior of mysql.
	if err != nil {
		logutil.BgLogger().Warn("time_format.args[0].EvalDuration failed", zap.Error(err))
	}
	if isNull {
		return "", isNull, err
	}
	formatMask, isNull, err := b.args[1].EvalString(ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	res, err := b.formatTime(dur, formatMask)
	return res, isNull, err
}

// formatTime see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) formatTime(t types.Duration, formatMask string) (res string, err error) {
	return t.DurationFormat(formatMask)
}

type timeToSecFunctionClass struct {
	baseFunctionClass
}

func (c *timeToSecFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinTimeToSecSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimeToSec)
	return sig, nil
}

type builtinTimeToSecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTimeToSecSig) Clone() builtinFunc {
	newSig := &builtinTimeToSecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	duration, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	var sign int
	if duration.Duration >= 0 {
		sign = 1
	} else {
		sign = -1
	}
	return int64(sign * (duration.Hour()*3600 + duration.Minute()*60 + duration.Second())), false, nil
}

type timestampAddFunctionClass struct {
	baseFunctionClass
}

func (c *timestampAddFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETReal, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	flen := mysql.MaxDatetimeWidthNoFsp
	con, ok := args[0].(*Constant)
	if !ok {
		return nil, errors.New("should not happened")
	}
	unit, null, err := con.EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if null || err != nil {
		return nil, errors.New("should not happened")
	}
	if unit == ast.TimeUnitMicrosecond.String() {
		flen = mysql.MaxDatetimeWidthWithFsp
	}

	bf.tp.SetFlen(flen)
	sig := &builtinTimestampAddSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimestampAdd)
	return sig, nil
}

type builtinTimestampAddSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTimestampAddSig) Clone() builtinFunc {
	newSig := &builtinTimestampAddSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

var (
	minDatetimeInGoTime, _ = types.MinDatetime.GoTime(time.Local)
	minDatetimeNanos       = float64(minDatetimeInGoTime.Unix())*1e9 + float64(minDatetimeInGoTime.Nanosecond())
	maxDatetimeInGoTime, _ = types.MaxDatetime.GoTime(time.Local)
	maxDatetimeNanos       = float64(maxDatetimeInGoTime.Unix())*1e9 + float64(maxDatetimeInGoTime.Nanosecond())
	minDatetimeMonths      = float64(types.MinDatetime.Year()*12 + types.MinDatetime.Month() - 1) // 0001-01-01 00:00:00
	maxDatetimeMonths      = float64(types.MaxDatetime.Year()*12 + types.MaxDatetime.Month() - 1) // 9999-12-31 00:00:00
)

func validAddTime(nano1 float64, nano2 float64) bool {
	return nano1+nano2 >= minDatetimeNanos && nano1+nano2 <= maxDatetimeNanos
}

func validAddMonth(month1 float64, year, month int) bool {
	tmp := month1 + float64(year)*12 + float64(month-1)
	return tmp >= minDatetimeMonths && tmp <= maxDatetimeMonths
}

func addUnitToTime(unit string, t time.Time, v float64) (time.Time, bool, error) {
	s := math.Trunc(v * 1000000)
	// round to the nearest int
	v = math.Round(v)
	var tb time.Time
	nano := float64(t.Unix())*1e9 + float64(t.Nanosecond())
	switch unit {
	case "MICROSECOND":
		if !validAddTime(v*float64(time.Microsecond), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(v) * time.Microsecond)
	case "SECOND":
		if !validAddTime(s*float64(time.Microsecond), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(s) * time.Microsecond)
	case "MINUTE":
		if !validAddTime(v*float64(time.Minute), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(v) * time.Minute)
	case "HOUR":
		if !validAddTime(v*float64(time.Hour), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(v) * time.Hour)
	case "DAY":
		if !validAddTime(v*24*float64(time.Hour), nano) {
			return tb, true, nil
		}
		tb = t.AddDate(0, 0, int(v))
	case "WEEK":
		if !validAddTime(v*24*7*float64(time.Hour), nano) {
			return tb, true, nil
		}
		tb = t.AddDate(0, 0, 7*int(v))
	case "MONTH":
		if !validAddMonth(v, t.Year(), int(t.Month())) {
			return tb, true, nil
		}

		var err error
		tb, err = types.AddDate(0, int64(v), 0, t)
		if err != nil {
			return tb, false, err
		}
	case "QUARTER":
		if !validAddMonth(v*3, t.Year(), int(t.Month())) {
			return tb, true, nil
		}
		tb = t.AddDate(0, 3*int(v), 0)
	case "YEAR":
		if !validAddMonth(v*12, t.Year(), int(t.Month())) {
			return tb, true, nil
		}
		tb = t.AddDate(int(v), 0, 0)
	default:
		return tb, false, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, unit)
	}
	return tb, false, nil
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	v, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg, isNull, err := b.args[2].EvalTime(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tm1, err := arg.GoTime(time.Local)
	if err != nil {
		tc := typeCtx(ctx)
		tc.AppendWarning(err)
		return "", true, nil
	}
	tb, overflow, err := addUnitToTime(unit, tm1, v)
	if err != nil {
		return "", true, err
	}
	if overflow {
		return "", true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	fsp := types.DefaultFsp
	// use MaxFsp when microsecond is not zero
	if tb.Nanosecond()/1000 != 0 {
		fsp = types.MaxFsp
	}
	r := types.NewTime(types.FromGoTime(tb), b.resolveType(arg.Type(), unit), fsp)
	if err = r.Check(typeCtx(ctx)); err != nil {
		return "", true, handleInvalidTimeError(ctx, err)
	}
	return r.String(), false, nil
}

func (b *builtinTimestampAddSig) resolveType(typ uint8, unit string) uint8 {
	// The approach below is from MySQL.
	// The field type for the result of an Item_date function is defined as
	// follows:
	//
	//- If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
	//- If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
	//	minutes, seconds or microsecond then type is MYSQL_TYPE_DATETIME.
	//- Otherwise the result is MYSQL_TYPE_STRING
	//	(This is because you can't know if the string contains a DATE, MYSQL_TIME
	//	or DATETIME argument)
	if typ == mysql.TypeDate && (unit == "HOUR" || unit == "MINUTE" || unit == "SECOND" || unit == "MICROSECOND") {
		return mysql.TypeDatetime
	}
	return typ
}

type toDaysFunctionClass struct {
	baseFunctionClass
}

func (c *toDaysFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinToDaysSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_ToDays)
	return sig, nil
}

type builtinToDaysSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinToDaysSig) Clone() builtinFunc {
	newSig := &builtinToDaysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	return ret, false, nil
}

type toSecondsFunctionClass struct {
	baseFunctionClass
}

func (c *toSecondsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinToSecondsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_ToSeconds)
	return sig, nil
}

type builtinToSecondsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinToSecondsSig) Clone() builtinFunc {
	newSig := &builtinToSecondsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	return ret, false, nil
}

type utcTimeFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, argTps...)
	if err != nil {
		return nil, err
	}
	fsp, err := getFspByIntArg(ctx, args, c.funcName)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)
	// 1. no sign.
	// 2. hour is in the 2-digit range.
	bf.tp.SetFlen(bf.tp.GetFlen() - 2)

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimeWithArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimeWithArg)
	} else {
		sig = &builtinUTCTimeWithoutArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimeWithoutArg)
	}
	return sig, nil
}

type builtinUTCTimeWithoutArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUTCTimeWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimeWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, _, err := types.ParseDuration(typeCtx(ctx), nowTs.UTC().Format(types.TimeFormat), 0)
	return v, false, err
}

type builtinUTCTimeWithArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUTCTimeWithArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimeWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}

	if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
		return types.Duration{}, true, types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
	} else if fsp > int64(types.MaxFsp) {
		return types.Duration{}, true, types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "utc_time", types.MaxFsp)
	}

	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, _, err := types.ParseDuration(typeCtx(ctx), nowTs.UTC().Format(types.TimeFSPFormat), int(fsp))
	return v, false, err
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
	sig := &builtinLastDaySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LastDay)
	return sig, nil
}

type builtinLastDaySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLastDaySig) Clone() builtinFunc {
	newSig := &builtinLastDaySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	tm := arg
	year, month := tm.Year(), tm.Month()
	if tm.Month() == 0 || (tm.Day() == 0 && sqlMode(ctx).HasNoZeroDateMode()) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	lastDay := types.GetLastDay(year, month)
	ret := types.NewTime(types.FromDate(year, month, lastDay, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)
	return ret, false, nil
}

// getExpressionFsp calculates the fsp from given expression.
// This function must by called before calling newBaseBuiltinFuncWithTp.
func getExpressionFsp(ctx BuildContext, expression Expression) (int, error) {
	constExp, isConstant := expression.(*Constant)
	if isConstant {
		str, isNil, err := constExp.EvalString(ctx.GetEvalCtx(), chunk.Row{})
		if isNil || err != nil {
			return 0, err
		}
		return types.GetFsp(str), nil
	}
	warpExpr := WrapWithCastAsTime(ctx, expression, types.NewFieldType(mysql.TypeDatetime))
	return min(warpExpr.GetType(ctx.GetEvalCtx()).GetDecimal(), types.MaxFsp), nil
}

// tidbParseTsoFunctionClass extracts physical time from a tso
type tidbParseTsoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbParseTsoFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, types.ETInt)
	if err != nil {
		return nil, err
	}

	bf.tp.SetType(mysql.TypeDatetime)
	bf.tp.SetFlen(mysql.MaxDateWidth)
	bf.tp.SetDecimal(types.DefaultFsp)
	sig := &builtinTidbParseTsoSig{bf}
	return sig, nil
}

type builtinTidbParseTsoSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTidbParseTsoSig) Clone() builtinFunc {
	newSig := &builtinTidbParseTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTidbParseTsoSig.
func (b *builtinTidbParseTsoSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil || arg <= 0 {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	t := oracle.GetTimeFromTS(uint64(arg))
	result := types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.MaxFsp)
	err = result.ConvertTimeZone(time.Local, location(ctx))
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

// tidbParseTsoFunctionClass extracts logical time from a tso
type tidbParseTsoLogicalFunctionClass struct {
	baseFunctionClass
}

func (c *tidbParseTsoLogicalFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}

	sig := &builtinTidbParseTsoLogicalSig{bf}
	return sig, nil
}

type builtinTidbParseTsoLogicalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTidbParseTsoLogicalSig) Clone() builtinFunc {
	newSig := &builtinTidbParseTsoLogicalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTidbParseTsoLogicalSig.
func (b *builtinTidbParseTsoLogicalSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil || arg <= 0 {
		return 0, true, err
	}

	t := oracle.ExtractLogical(uint64(arg))
	return t, false, nil
}

// tidbBoundedStalenessFunctionClass reads a time window [a, b] and compares it with the latest SafeTS
// to determine which TS to use in a read only transaction.
type tidbBoundedStalenessFunctionClass struct {
	baseFunctionClass
}

func (c *tidbBoundedStalenessFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(3)
	sig := &builtinTiDBBoundedStalenessSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBBoundedStalenessSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
	expropt.KVStorePropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBBoundedStalenessSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.KVStorePropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBBoundedStalenessSig) Clone() builtinFunc {
	newSig := &builtinTidbParseTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBBoundedStalenessSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	store, err := b.GetKVStore(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	leftTime, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	rightTime, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if invalidLeftTime, invalidRightTime := leftTime.InvalidZero(), rightTime.InvalidZero(); invalidLeftTime || invalidRightTime {
		if invalidLeftTime {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, leftTime.String()))
		}
		if invalidRightTime {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rightTime.String()))
		}
		return types.ZeroTime, true, err
	}
	timeZone := getTimeZone(ctx)
	minTime, err := leftTime.GoTime(timeZone)
	if err != nil {
		return types.ZeroTime, true, err
	}
	maxTime, err := rightTime.GoTime(timeZone)
	if err != nil {
		return types.ZeroTime, true, err
	}
	if minTime.After(maxTime) {
		return types.ZeroTime, true, nil
	}
	// Because the minimum unit of a TSO is millisecond, so we only need fsp to be 3.
	return types.NewTime(types.FromGoTime(calAppropriateTime(minTime, maxTime, GetStmtMinSafeTime(vars.StmtCtx, store, timeZone))), mysql.TypeDatetime, 3), false, nil
}

// GetStmtMinSafeTime get minSafeTime
func GetStmtMinSafeTime(sc *stmtctx.StatementContext, store kv.Storage, timeZone *time.Location) time.Time {
	var minSafeTS uint64
	txnScope := config.GetTxnScopeFromConfig()
	if store != nil {
		minSafeTS = store.GetMinSafeTS(txnScope)
	}
	// Inject mocked SafeTS for test.
	failpoint.Inject("injectSafeTS", func(val failpoint.Value) {
		injectTS := val.(int)
		minSafeTS = uint64(injectTS)
	})
	// Try to get from the stmt cache to make sure this function is deterministic.
	minSafeTS = sc.GetOrStoreStmtCache(stmtctx.StmtSafeTSCacheKey, minSafeTS).(uint64)
	return oracle.GetTimeFromTS(minSafeTS).In(timeZone)
}

// CalAppropriateTime directly calls calAppropriateTime
func CalAppropriateTime(minTime, maxTime, minSafeTime time.Time) time.Time {
	return calAppropriateTime(minTime, maxTime, minSafeTime)
}

// For a SafeTS t and a time range [t1, t2]:
//  1. If t < t1, we will use t1 as the result,
//     and with it, a read request may fail because it's an unreached SafeTS.
//  2. If t1 <= t <= t2, we will use t as the result, and with it,
//     a read request won't fail.
//  2. If t2 < t, we will use t2 as the result,
//     and with it, a read request won't fail because it's bigger than the latest SafeTS.
func calAppropriateTime(minTime, maxTime, minSafeTime time.Time) time.Time {
	if minSafeTime.Before(minTime) || minSafeTime.After(maxTime) {
		logutil.BgLogger().Debug("calAppropriateTime",
			zap.Time("minTime", minTime),
			zap.Time("maxTime", maxTime),
			zap.Time("minSafeTime", minSafeTime))
		if minSafeTime.Before(minTime) {
			return minTime
		} else if minSafeTime.After(maxTime) {
			return maxTime
		}
	}
	logutil.BgLogger().Debug("calAppropriateTime",
		zap.Time("minTime", minTime),
		zap.Time("maxTime", maxTime),
		zap.Time("minSafeTime", minSafeTime))
	return minSafeTime
}

// getFspByIntArg is used by some time functions to get the result fsp. If len(expr) == 0, then the fsp is not explicit set, use 0 as default.
func getFspByIntArg(ctx BuildContext, exps []Expression, funcName string) (int, error) {
	if len(exps) == 0 {
		return 0, nil
	}
	if len(exps) != 1 {
		return 0, errors.Errorf("Should not happen, the num of argument should be 1, but got %d", len(exps))
	}
	_, ok := exps[0].(*Constant)
	if ok {
		fsp, isNuLL, err := exps[0].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil || isNuLL {
			// If isNULL, it may be a bug of parser. Return 0 to be compatible with old version.
			return 0, err
		}

		if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
			return 0, types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
		} else if fsp > int64(types.MaxFsp) {
			return 0, types.ErrTooBigPrecision.GenWithStackByArgs(fsp, funcName, types.MaxFsp)
		}
		return int(fsp), nil
	}
	// Should no happen. But our tests may generate non-constant input.
	return 0, nil
}

type tidbCurrentTsoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbCurrentTsoFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBCurrentTsoSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBCurrentTsoSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinTiDBCurrentTsoSig) Clone() builtinFunc {
	newSig := &builtinTiDBCurrentTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBCurrentTsoSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals currentTSO().
func (b *builtinTiDBCurrentTsoSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	tso, _ := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), "tidb_current_ts")
	itso, _ := strconv.ParseInt(tso, 10, 64)
	return itso, false, nil
}
