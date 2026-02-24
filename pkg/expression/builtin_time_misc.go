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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type convertTzFunctionClass struct {
	baseFunctionClass
}

func (c *convertTzFunctionClass) getDecimal(ctx BuildContext, arg Expression) int {
	decimal := types.MaxFsp
	if dt, isConstant := arg.(*Constant); isConstant {
		switch arg.GetType(ctx.GetEvalCtx()).EvalType() {
		case types.ETInt:
			decimal = 0
		case types.ETReal, types.ETDecimal:
			decimal = arg.GetType(ctx.GetEvalCtx()).GetDecimal()
		case types.ETString:
			str, isNull, err := dt.EvalString(ctx.GetEvalCtx(), chunk.Row{})
			if err == nil && !isNull {
				decimal = types.DateFSP(str)
			}
		}
	}
	if decimal > types.MaxFsp {
		return types.MaxFsp
	}
	if decimal < types.MinFsp {
		return types.MinFsp
	}
	return decimal
}

func (c *convertTzFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// tzRegex holds the regex to check whether a string is a time zone.
	tzRegex, err := regexp.Compile(`(^[-+](0?[0-9]|1[0-3]):[0-5]?\d$)|(^\+14:00?$)`)
	if err != nil {
		return nil, err
	}

	decimal := c.getDecimal(ctx, args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(decimal)
	sig := &builtinConvertTzSig{
		baseBuiltinFunc: bf,
		timezoneRegex:   tzRegex,
	}
	sig.setPbCode(tipb.ScalarFuncSig_ConvertTz)
	return sig, nil
}

type builtinConvertTzSig struct {
	baseBuiltinFunc
	timezoneRegex *regexp.Regexp
}

func (b *builtinConvertTzSig) Clone() builtinFunc {
	newSig := &builtinConvertTzSig{timezoneRegex: b.timezoneRegex}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals CONVERT_TZ(dt,from_tz,to_tz).
// `CONVERT_TZ` function returns NULL if the arguments are invalid.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	dt, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}
	if dt.InvalidZero() {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, dt.String()))
	}
	fromTzStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}

	toTzStr, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}

	return b.convertTz(dt, fromTzStr, toTzStr)
}

func (b *builtinConvertTzSig) convertTz(dt types.Time, fromTzStr, toTzStr string) (types.Time, bool, error) {
	if fromTzStr == "" || toTzStr == "" {
		return types.ZeroTime, true, nil
	}
	fromTzMatched := b.timezoneRegex.MatchString(fromTzStr)
	toTzMatched := b.timezoneRegex.MatchString(toTzStr)

	var fromTz, toTz *time.Location
	var err error

	if fromTzMatched {
		fromTz = time.FixedZone(fromTzStr, timeZone2int(fromTzStr))
	} else {
		if strings.EqualFold(fromTzStr, "SYSTEM") {
			fromTzStr = "Local"
		}
		fromTz, err = time.LoadLocation(fromTzStr)
		if err != nil {
			return types.ZeroTime, true, nil
		}
	}

	t, err := dt.AdjustedGoTime(fromTz)
	if err != nil {
		return types.ZeroTime, true, nil
	}
	t = t.In(time.UTC)

	if toTzMatched {
		toTz = time.FixedZone(toTzStr, timeZone2int(toTzStr))
	} else {
		if strings.EqualFold(toTzStr, "SYSTEM") {
			toTzStr = "Local"
		}
		toTz, err = time.LoadLocation(toTzStr)
		if err != nil {
			return types.ZeroTime, true, nil
		}
	}

	return types.NewTime(types.FromGoTime(t.In(toTz)), mysql.TypeDatetime, b.tp.GetDecimal()), false, nil
}

type makeDateFunctionClass struct {
	baseFunctionClass
}

func (c *makeDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
	sig := &builtinMakeDateSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MakeDate)
	return sig, nil
}

type builtinMakeDateSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMakeDateSig) Clone() builtinFunc {
	newSig := &builtinMakeDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evaluates a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) evalTime(ctx EvalContext, row chunk.Row) (d types.Time, isNull bool, err error) {
	args := b.getArgs()
	var year, dayOfYear int64
	year, isNull, err = args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	dayOfYear, isNull, err = args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	if dayOfYear <= 0 || year < 0 || year > 9999 {
		return d, true, nil
	}
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}
	startTime := types.NewTime(types.FromDate(int(year), 1, 1, 0, 0, 0, 0), mysql.TypeDate, 0)
	retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
	if retTimestamp == 0 {
		return d, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, startTime.String()))
	}
	ret := types.TimeFromDays(retTimestamp + dayOfYear - 1)
	if ret.IsZero() || ret.Year() > 9999 {
		return d, true, nil
	}
	return ret, false, nil
}

type makeTimeFunctionClass struct {
	baseFunctionClass
}

func (c *makeTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp, decimal := args[2].GetType(ctx.GetEvalCtx()).EvalType(), 0
	switch tp {
	case types.ETInt:
	case types.ETReal, types.ETDecimal:
		decimal = args[2].GetType(ctx.GetEvalCtx()).GetDecimal()
		if decimal > 6 || decimal == types.UnspecifiedLength {
			decimal = 6
		}
	default:
		decimal = 6
	}
	// MySQL will cast the first and second arguments to INT, and the third argument to DECIMAL.
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETInt, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(decimal)
	sig := &builtinMakeTimeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MakeTime)
	return sig, nil
}

type builtinMakeTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMakeTimeSig) Clone() builtinFunc {
	newSig := &builtinMakeTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMakeTimeSig) makeTime(ctx types.Context, hour int64, minute int64, second float64, hourUnsignedFlag bool) (types.Duration, error) {
	var overflow bool
	// MySQL TIME datatype: https://dev.mysql.com/doc/refman/5.7/en/time.html
	// ranges from '-838:59:59.000000' to '838:59:59.000000'
	if hour < 0 && hourUnsignedFlag {
		hour = 838
		overflow = true
	}
	if hour < -838 {
		hour = -838
		overflow = true
	} else if hour > 838 {
		hour = 838
		overflow = true
	}
	if (hour == -838 || hour == 838) && minute == 59 && second > 59 {
		overflow = true
	}
	if overflow {
		minute = 59
		second = 59
	}
	fsp := b.tp.GetDecimal()
	d, _, err := types.ParseDuration(ctx, fmt.Sprintf("%02d:%02d:%v", hour, minute, second), fsp)
	return d, err
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	dur := types.ZeroDuration
	dur.Fsp = types.MaxFsp
	hour, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	minute, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if minute < 0 || minute >= 60 {
		return dur, true, nil
	}
	second, isNull, err := b.args[2].EvalReal(ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if second < 0 || second >= 60 {
		return dur, true, nil
	}
	dur, err = b.makeTime(typeCtx(ctx), hour, minute, second, mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()))
	if err != nil {
		return dur, true, err
	}
	return dur, false, nil
}

type periodAddFunctionClass struct {
	baseFunctionClass
}

func (c *periodAddFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(6)
	sig := &builtinPeriodAddSig{bf}
	return sig, nil
}

// validPeriod checks if this period is valid, it comes from MySQL 8.0+.
func validPeriod(p int64) bool {
	return !(p < 0 || p%100 == 0 || p%100 > 12)
}

// period2Month converts a period to months, in which period is represented in the format of YYMM or YYYYMM.
// Note that the period argument is not a date value.
func period2Month(period uint64) uint64 {
	if period == 0 {
		return 0
	}

	year, month := period/100, period%100
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}

	return year*12 + month - 1
}

// month2Period converts a month to a period.
func month2Period(month uint64) uint64 {
	if month == 0 {
		return 0
	}

	year := month / 12
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}

	return year*100 + month%12 + 1
}

type builtinPeriodAddSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPeriodAddSig) Clone() builtinFunc {
	newSig := &builtinPeriodAddSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	p, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	n, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	// in MySQL, if p is invalid but n is NULL, the result is NULL, so we have to check if n is NULL first.
	if !validPeriod(p) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_add")
	}

	sumMonth := int64(period2Month(uint64(p))) + n
	return int64(month2Period(uint64(sumMonth))), false, nil
}

type periodDiffFunctionClass struct {
	baseFunctionClass
}

func (c *periodDiffFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(6)
	sig := &builtinPeriodDiffSig{bf}
	return sig, nil
}

type builtinPeriodDiffSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPeriodDiffSig) Clone() builtinFunc {
	newSig := &builtinPeriodDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	p1, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	p2, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if !validPeriod(p1) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	if !validPeriod(p2) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	return int64(period2Month(uint64(p1)) - period2Month(uint64(p2))), false, nil
}

type quarterFunctionClass struct {
	baseFunctionClass
}

func (c *quarterFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)

	sig := &builtinQuarterSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Quarter)
	return sig, nil
}

type builtinQuarterSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinQuarterSig) Clone() builtinFunc {
	newSig := &builtinQuarterSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	return int64((date.Month() + 2) / 3), false, nil
}

type secToTimeFunctionClass struct {
	baseFunctionClass
}

func (c *secToTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var retFsp int
	argType := args[0].GetType(ctx.GetEvalCtx())
	argEvalTp := argType.EvalType()

	// to match MySQL behavior more check issue #59428
	if IsBinaryLiteral(args[0]) {
		retFsp = types.MinFsp
	} else if argEvalTp == types.ETString {
		retFsp = types.UnspecifiedLength
	} else {
		retFsp = argType.GetDecimal()
	}
	if retFsp > types.MaxFsp || retFsp == types.UnspecifiedFsp {
		retFsp = types.MaxFsp
	} else if retFsp < types.MinFsp {
		retFsp = types.MinFsp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(retFsp)
	sig := &builtinSecToTimeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SecToTime)
	return sig, nil
}

type builtinSecToTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSecToTimeSig) Clone() builtinFunc {
	newSig := &builtinSecToTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals SEC_TO_TIME(seconds).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	secondsFloat, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var (
		hour          uint64
		minute        uint64
		second        uint64
		demical       float64
		secondDemical float64
		negative      string
	)

	if secondsFloat < 0 {
		negative = "-"
		secondsFloat = math.Abs(secondsFloat)
	}
	seconds := uint64(secondsFloat)
	demical = secondsFloat - float64(seconds)

	hour = seconds / 3600
	if hour > 838 {
		hour = 838
		minute = 59
		second = 59
		demical = 0
		tc := typeCtx(ctx)
		err = tc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("time", strconv.FormatFloat(secondsFloat, 'f', -1, 64)))
		if err != nil {
			return types.Duration{}, err != nil, err
		}
	} else {
		minute = seconds % 3600 / 60
		second = seconds % 60
	}
	secondDemical = float64(second) + demical

	var dur types.Duration
	dur, _, err = types.ParseDuration(typeCtx(ctx), fmt.Sprintf("%s%02d:%02d:%s", negative, hour, minute, strconv.FormatFloat(secondDemical, 'f', -1, 64)), b.tp.GetDecimal())
	if err != nil {
		return types.Duration{}, err != nil, err
	}
	return dur, false, nil
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, c.funcName, args)
	if err != nil {
		return nil, err
	}
	switch tp1.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.GetType() {
		case mysql.TypeDuration:
			sig = &builtinSubDatetimeAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDatetimeAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDateTimeNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeDateTimeNull)
		default:
			sig = &builtinSubDatetimeAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDatetimeAndString)
		}
	case mysql.TypeDate:
		charset, collate := ctx.GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)
		switch tp2.GetType() {
		case mysql.TypeDuration:
			sig = &builtinSubDateAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDateAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeStringNull)
		default:
			sig = &builtinSubDateAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDateAndString)
		}
	case mysql.TypeDuration:
		switch tp2.GetType() {
		case mysql.TypeDuration:
			sig = &builtinSubDurationAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDurationAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDurationNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeDurationNull)
		default:
			sig = &builtinSubDurationAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDurationAndString)
		}
	default:
		switch tp2.GetType() {
		case mysql.TypeDuration:
			sig = &builtinSubStringAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubStringAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeStringNull)
		default:
			sig = &builtinSubStringAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubStringAndString)
		}
	}
	return sig, nil
}

type builtinSubDatetimeAndDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubDatetimeAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubDatetimeAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndDurationSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}

	if arg0.IsZero() {
		return types.ZeroDatetime, true, nil
	}

	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	tc := typeCtx(ctx)
	result, err := arg0.Add(tc, arg1.Neg())
	if err != nil {
		return types.ZeroDatetime, true, err
	}

	return result, err != nil, err
}

type builtinSubDatetimeAndStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubDatetimeAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndStringSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}

	if arg0.IsZero() {
		return types.ZeroDatetime, true, nil
	}

	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	result, err := arg0.Add(tc, arg1.Neg())
	if err != nil {
		return types.ZeroDatetime, true, err
	}

	return result, err != nil, err
}

type builtinSubTimeDateTimeNullSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubTimeDateTimeNullSig) Clone() builtinFunc {
	newSig := &builtinSubTimeDateTimeNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDateTimeNullSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinSubStringAndDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubStringAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubStringAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndDurationSig) evalString(ctx EvalContext, row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tc := typeCtx(ctx)
	if isDuration(arg0) {
		result, err = strDurationSubDuration(tc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				tc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, isNull, err = strDatetimeSubDuration(tc, arg0, arg1)
	return result, isNull, err
}

type builtinSubStringAndStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubStringAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubStringAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) evalString(ctx EvalContext, row chunk.Row) (result string, isNull bool, err error) {
	var (
		s, arg0 string
		arg1    types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.args[1].GetType(ctx)
	if mysql.HasBinaryFlag(arg1Type.GetFlag()) {
		return "", true, nil
	}
	s, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tc := typeCtx(ctx)
	arg1, _, err = types.ParseDuration(tc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	if isDuration(arg0) {
		result, err = strDurationSubDuration(tc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				tc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, isNull, err = strDatetimeSubDuration(tc, arg0, arg1)
	return result, isNull, err
}

type builtinSubTimeStringNullSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubTimeStringNullSig) Clone() builtinFunc {
	newSig := &builtinSubTimeStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubTimeStringNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeStringNullSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinSubDurationAndDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubDurationAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubDurationAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinSubDurationAndStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubDurationAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDurationAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndStringSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return types.ZeroDuration, true, nil
		}
		return types.ZeroDuration, true, err
	}
	result, err := arg0.Sub(arg1)
	return result, err != nil, err
}

type builtinSubTimeDurationNullSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubTimeDurationNullSig) Clone() builtinFunc {
	newSig := &builtinSubTimeDurationNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDurationNullSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinSubDateAndDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubDateAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubDateAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndDurationSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	if arg0.IsZero() {
		return "", true, nil
	}

	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	arg0.SetType(mysql.TypeDatetime)
	result, err := arg0.Add(typeCtx(ctx), arg1.Neg())
	if err != nil {
		return "", true, err
	}

	return result.String(), err != nil, err
}

type builtinSubDateAndStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubDateAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDateAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	if arg0.IsZero() {
		return "", true, nil
	}

	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}

	arg0.SetType(mysql.TypeDatetime)
	result, err := arg0.Add(tc, arg1.Neg())
	if err != nil {
		return "", true, err
	}

	return result.String(), err != nil, err
}

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
