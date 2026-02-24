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
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type fromUnixTimeFunctionClass struct {
	baseFunctionClass
}

func (c *fromUnixTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTps := types.ETDatetime, make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETDecimal)
	if len(args) == 2 {
		retTp = types.ETString
		argTps = append(argTps, types.ETString)
	}

	arg0Tp := args[0].GetType(ctx.GetEvalCtx())
	isArg0Str := arg0Tp.EvalType() == types.ETString
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTps...)
	if err != nil {
		return nil, err
	}

	if fieldString(arg0Tp.GetType()) {
		//Improve string cast Unix Time precision
		x, ok := (bf.getArgs()[0]).(*ScalarFunction)
		if ok {
			//used to adjust FromUnixTime precision #Fixbug35184
			if x.FuncName.L == ast.Cast {
				if x.RetType.GetDecimal() == 0 && (x.RetType.GetType() == mysql.TypeNewDecimal) {
					x.RetType.SetDecimal(6)
					fieldLen := min(x.RetType.GetFlen()+6, mysql.MaxDecimalWidth)
					x.RetType.SetFlen(fieldLen)
				}
			}
		}
	}

	if len(args) > 1 {
		sig = &builtinFromUnixTime2ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FromUnixTime2Arg)
		return sig, nil
	}

	// Calculate the time fsp.
	fsp := types.MaxFsp
	if !isArg0Str {
		if arg0Tp.GetDecimal() != types.UnspecifiedLength {
			fsp = min(bf.tp.GetDecimal(), arg0Tp.GetDecimal())
		}
	}
	bf.setDecimalAndFlenForDatetime(fsp)

	sig = &builtinFromUnixTime1ArgSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FromUnixTime1Arg)
	return sig, nil
}

func evalFromUnixTime(ctx EvalContext, fsp int, unixTimeStamp *types.MyDecimal) (res types.Time, isNull bool, err error) {
	// 0 <= unixTimeStamp <= 32536771199.999999
	if unixTimeStamp.IsNegative() {
		return res, true, nil
	}
	integralPart, err := unixTimeStamp.ToInt()
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) && !terror.ErrorEqual(err, types.ErrOverflow) {
		return res, true, err
	}
	// The max integralPart should not be larger than 32536771199.
	// Refer to https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-28.html
	if integralPart > 32536771199 {
		return res, true, nil
	}
	// Split the integral part and fractional part of a decimal timestamp.
	// e.g. for timestamp 12345.678,
	// first get the integral part 12345,
	// then (12345.678 - 12345) * (10^9) to get the decimal part and convert it to nanosecond precision.
	integerDecimalTp := new(types.MyDecimal).FromInt(integralPart)
	fracDecimalTp := new(types.MyDecimal)
	err = types.DecimalSub(unixTimeStamp, integerDecimalTp, fracDecimalTp)
	if err != nil {
		return res, true, err
	}
	nano := new(types.MyDecimal).FromInt(int64(time.Second))
	x := new(types.MyDecimal)
	err = types.DecimalMul(fracDecimalTp, nano, x)
	if err != nil {
		return res, true, err
	}
	fractionalPart, err := x.ToInt() // here fractionalPart is result multiplying the original fractional part by 10^9.
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, err
	}
	if fsp < 0 {
		fsp = types.MaxFsp
	}

	tc := typeCtx(ctx)
	tmp := time.Unix(integralPart, fractionalPart).In(tc.Location())
	t, err := convertTimeToMysqlTime(tmp, fsp, types.ModeHalfUp)
	if err != nil {
		return res, true, err
	}
	return t, false, nil
}

// fieldString returns true if precision cannot be determined
func fieldString(fieldType byte) bool {
	switch fieldType {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		return true
	default:
		return false
	}
}

type builtinFromUnixTime1ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFromUnixTime1ArgSig) Clone() builtinFunc {
	newSig := &builtinFromUnixTime1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinFromUnixTime1ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime1ArgSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	unixTimeStamp, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil || isNull {
		return res, isNull, err
	}
	return evalFromUnixTime(ctx, b.tp.GetDecimal(), unixTimeStamp)
}

type builtinFromUnixTime2ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFromUnixTime2ArgSig) Clone() builtinFunc {
	newSig := &builtinFromUnixTime2ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinFromUnixTime2ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime2ArgSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	unixTimeStamp, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	t, isNull, err := evalFromUnixTime(ctx, b.tp.GetDecimal(), unixTimeStamp)
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = t.DateFormat(format)
	return res, err != nil, err
}

type getFormatFunctionClass struct {
	baseFunctionClass
}

func (c *getFormatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(17)
	sig := &builtinGetFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_GetFormat)
	return sig, nil
}

type builtinGetFormatSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinGetFormatSig) Clone() builtinFunc {
	newSig := &builtinGetFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	t, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	l, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	res := b.getFormat(t, l)
	return res, false, nil
}

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(ctx BuildContext, arg Expression) (tp byte, fsp int) {
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		return tp, types.MaxFsp
	}
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil || isNull {
		return
	}

	isDuration, isDate := types.GetFormatType(format)
	if isDuration && !isDate {
		tp = mysql.TypeDuration
	} else if !isDuration && isDate {
		tp = mysql.TypeDate
	}
	if strings.Contains(format, "%f") {
		fsp = types.MaxFsp
	}
	return
}

// getFunction see https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (c *strToDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp, fsp := c.getRetTp(ctx, args[1])
	switch retTp {
	case mysql.TypeDate:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.setDecimalAndFlenForDate()
		sig = &builtinStrToDateDateSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDate)
	case mysql.TypeDatetime:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.setDecimalAndFlenForDatetime(fsp)
		sig = &builtinStrToDateDatetimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDatetime)
	case mysql.TypeDuration:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.setDecimalAndFlenForTime(fsp)
		sig = &builtinStrToDateDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDuration)
	}
	return sig, nil
}

type builtinStrToDateDateSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinStrToDateDateSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDateSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	tc := typeCtx(ctx)
	succ := t.StrToDate(tc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValueForType.GenWithStackByArgs(types.DateTimeStr, date, ast.StrToDate))
	}
	t.SetType(mysql.TypeDate)
	t.SetFsp(types.MinFsp)
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinStrToDateDatetimeSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDatetimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	tc := typeCtx(ctx)
	succ := t.StrToDate(tc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetType(mysql.TypeDatetime)
	t.SetFsp(b.tp.GetDecimal())
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinStrToDateDurationSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration
// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	date, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var t types.Time
	tc := typeCtx(ctx)
	succ := t.StrToDate(tc, date, format)
	if !succ {
		return types.Duration{}, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetFsp(b.tp.GetDecimal())
	dur, err := t.ConvertToDuration()
	return dur, err != nil, err
}

type sysDateFunctionClass struct {
	baseFunctionClass
}

func (c *sysDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	fsp, err := getFspByIntArg(ctx, args, c.funcName)
	if err != nil {
		return nil, err
	}
	var argTps = make([]types.EvalType, 0)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)
	// Illegal parameters have been filtered out in the parser, so the result is always not null.
	bf.tp.SetFlag(bf.tp.GetFlag() | mysql.NotNullFlag)

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinSysDateWithFspSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_SysDateWithFsp)
	} else {
		sig = &builtinSysDateWithoutFspSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_SysDateWithoutFsp)
	}
	return sig, nil
}

type builtinSysDateWithFspSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSysDateWithFspSig) Clone() builtinFunc {
	newSig := &builtinSysDateWithFspSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithFspSig) evalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}

	loc := location(ctx)
	now := time.Now().In(loc)
	result, err := convertTimeToMysqlTime(now, int(fsp), types.ModeHalfUp)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type builtinSysDateWithoutFspSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSysDateWithoutFspSig) Clone() builtinFunc {
	newSig := &builtinSysDateWithoutFspSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithoutFspSig) evalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	tz := location(ctx)
	now := time.Now().In(tz)
	result, err := convertTimeToMysqlTime(now, 0, types.ModeHalfUp)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
	sig := &builtinCurrentDateSig{bf}
	return sig, nil
}

type builtinCurrentDateSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCurrentDateSig) Clone() builtinFunc {
	newSig := &builtinCurrentDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals CURDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) evalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	tz := location(ctx)
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	year, month, day := nowTs.In(tz).Date()
	result := types.NewTime(types.FromDate(year, int(month), day, 0, 0, 0, 0), mysql.TypeDate, 0)
	return result, false, nil
}

type currentTimeFunctionClass struct {
	baseFunctionClass
}

func (c *currentTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	fsp, err := getFspByIntArg(ctx, args, c.funcName)
	if err != nil {
		return nil, err
	}
	var argTps = make([]types.EvalType, 0)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, argTps...)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)
	// 1. no sign.
	// 2. hour is in the 2-digit range.
	bf.tp.SetFlen(bf.tp.GetFlen() - 2)
	if len(args) == 0 {
		sig = &builtinCurrentTime0ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CurrentTime0Arg)
		return sig, nil
	}
	sig = &builtinCurrentTime1ArgSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_CurrentTime1Arg)
	return sig, nil
}

type builtinCurrentTime0ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCurrentTime0ArgSig) Clone() builtinFunc {
	newSig := &builtinCurrentTime0ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime0ArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	tz := location(ctx)
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, _, err := types.ParseDuration(typeCtx(ctx), dur, types.MinFsp)
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type builtinCurrentTime1ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCurrentTime1ArgSig) Clone() builtinFunc {
	newSig := &builtinCurrentTime1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime1ArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	fsp, _, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return types.Duration{}, true, err
	}
	tz := location(ctx)
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	tc := typeCtx(ctx)
	res, _, err := types.ParseDuration(tc, dur, int(fsp))
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type timeFunctionClass struct {
	baseFunctionClass
}

func (c *timeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	fsp, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)
	sig := &builtinTimeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Time)
	return sig, nil
}

type builtinTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTimeSig) Clone() builtinFunc {
	newSig := &builtinTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time.
func (b *builtinTimeSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	expr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	fsp := 0
	if idx := strings.Index(expr, "."); idx != -1 {
		fsp = len(expr) - idx - 1
	}

	var tmpFsp int
	if tmpFsp, err = types.CheckFsp(fsp); err != nil {
		return res, isNull, err
	}
	fsp = tmpFsp

	tc := typeCtx(ctx)
	res, _, err = types.ParseDuration(tc, expr, fsp)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = tc.HandleTruncate(err)
	}
	return res, isNull, err
}

type timeLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timeLiteralFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for time literal")
	}
	dt, err := con.Eval(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !isDuration(str) {
		return nil, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, str)
	}
	duration, _, err := types.ParseDuration(ctx.GetEvalCtx().TypeCtx(), str, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(duration.Fsp)
	sig := &builtinTimeLiteralSig{bf, duration}
	return sig, nil
}

type builtinTimeLiteralSig struct {
	baseBuiltinFunc
	duration types.Duration
}

func (b *builtinTimeLiteralSig) Clone() builtinFunc {
	newSig := &builtinTimeLiteralSig{duration: b.duration}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals TIME 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimeLiteralSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
	sig := &builtinUTCDateSig{bf}
	return sig, nil
}

type builtinUTCDateSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUTCDateSig) Clone() builtinFunc {
	newSig := &builtinUTCDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_DATE, UTC_DATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	year, month, day := nowTs.UTC().Date()
	result := types.NewTime(types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)), mysql.TypeDate, types.UnspecifiedFsp)
	return result, false, nil
}

type utcTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
	if err != nil {
		return nil, err
	}

	fsp, err := getFspByIntArg(ctx, args, c.funcName)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)
	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimestampWithArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimestampWithArg)
	} else {
		sig = &builtinUTCTimestampWithoutArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimestampWithoutArg)
	}
	return sig, nil
}

func evalUTCTimestampWithFsp(ctx EvalContext, fsp int) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	result, err := convertTimeToMysqlTime(nowTs.UTC(), fsp, types.ModeHalfUp)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type builtinUTCTimestampWithArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUTCTimestampWithArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimestampWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return types.ZeroTime, true, err
	}

	if !isNull {
		if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
			return types.ZeroTime, true, types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
		} else if fsp > int64(types.MaxFsp) {
			return types.ZeroTime, true, types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "utc_timestamp", types.MaxFsp)
		}
	}

	result, isNull, err := evalUTCTimestampWithFsp(ctx, int(fsp))
	return result, isNull, err
}

type builtinUTCTimestampWithoutArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUTCTimestampWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimestampWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalUTCTimestampWithFsp(ctx, 0)
	return result, isNull, err
}

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
	if err != nil {
		return nil, err
	}

	fsp, err := getFspByIntArg(ctx, args, c.funcName)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinNowWithArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_NowWithArg)
	} else {
		sig = &builtinNowWithoutArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_NowWithoutArg)
	}
	return sig, nil
}

// GetStmtTimestamp directly calls getTimeZone with timezone
func GetStmtTimestamp(ctx EvalContext) (time.Time, error) {
	tz := getTimeZone(ctx)
	tVal, err := getStmtTimestamp(ctx)
	if err != nil {
		return tVal, err
	}
	return tVal.In(tz), nil
}

func evalNowWithFsp(ctx EvalContext, fsp int) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	failpoint.Inject("injectNow", func(val failpoint.Value) {
		nowTs = time.Unix(int64(val.(int)), 0)
	})

	// In MySQL's implementation, now() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	// mysql> select now(6), now(3), now();
	//	+----------------------------+-------------------------+---------------------+
	//	| now(6)                     | now(3)                  | now()               |
	//	+----------------------------+-------------------------+---------------------+
	//	| 2019-03-25 15:57:56.612966 | 2019-03-25 15:57:56.612 | 2019-03-25 15:57:56 |
	//	+----------------------------+-------------------------+---------------------+
	result, err := convertTimeToMysqlTime(nowTs, fsp, types.ModeTruncate)
	if err != nil {
		return types.ZeroTime, true, err
	}

	err = result.ConvertTimeZone(nowTs.Location(), location(ctx))
	if err != nil {
		return types.ZeroTime, true, err
	}

	return result, false, nil
}

type builtinNowWithArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNowWithArgSig) Clone() builtinFunc {
	newSig := &builtinNowWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals NOW(fsp)
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)

	if err != nil {
		return types.ZeroTime, true, err
	}

	if isNull {
		fsp = 0
	} else if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
		return types.ZeroTime, true, types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
	} else if fsp > int64(types.MaxFsp) {
		return types.ZeroTime, true, types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "now", types.MaxFsp)
	}

	result, isNull, err := evalNowWithFsp(ctx, int(fsp))
	return result, isNull, err
}

type builtinNowWithoutArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinNowWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinNowWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals NOW()
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithoutArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalNowWithFsp(ctx, 0)
	return result, isNull, err
}

type extractFunctionClass struct {
	baseFunctionClass
}

func (c *extractFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	args[0] = WrapWithCastAsString(ctx, args[0])
	unit, _, err := args[0].EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	isClockUnit := types.IsClockUnit(unit)
	isDateUnit := types.IsDateUnit(unit)
	var bf baseBuiltinFunc
	if isClockUnit && isDateUnit {
		// For unit DAY_MICROSECOND/DAY_SECOND/DAY_MINUTE/DAY_HOUR, the interpretation of the second argument depends on its evaluation type:
		// 1. Datetime/timestamp are interpreted as datetime. For example:
		// extract(day_second from datetime('2001-01-01 02:03:04')) = 120304
		// Note that MySQL 5.5+ has a bug of no day portion in the result (20304) for this case, see https://bugs.mysql.com/bug.php?id=73240.
		// 2. Time is interpreted as is. For example:
		// extract(day_second from time('02:03:04')) = 20304
		// Note that time shouldn't be implicitly cast to datetime, or else the date portion will be padded with the current date and this will adjust time portion accordingly.
		// 3. Otherwise, string/int/float are interpreted as arbitrarily either datetime or time, depending on which fits. For example:
		// extract(day_second from '2001-01-01 02:03:04') = 1020304 // datetime
		// extract(day_second from 20010101020304) = 1020304 // datetime
		// extract(day_second from '01 02:03:04') = 260304 // time
		if args[1].GetType(ctx.GetEvalCtx()).EvalType() == types.ETDatetime || args[1].GetType(ctx.GetEvalCtx()).EvalType() == types.ETTimestamp {
			bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime)
			if err != nil {
				return nil, err
			}
			sig = &builtinExtractDatetimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetime)
		} else if args[1].GetType(ctx.GetEvalCtx()).EvalType() == types.ETDuration {
			bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDuration)
			if err != nil {
				return nil, err
			}
			sig = &builtinExtractDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ExtractDuration)
		} else {
			bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
			if err != nil {
				return nil, err
			}
			bf.args[1].GetType(ctx.GetEvalCtx()).SetDecimal(int(types.MaxFsp))
			sig = &builtinExtractDatetimeFromStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetimeFromString)
		}
	} else if isClockUnit {
		// Clock units interpret the second argument as time.
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDuration)
		if err != nil {
			return nil, err
		}
		sig = &builtinExtractDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExtractDuration)
	} else {
		// Date units interpret the second argument as datetime.
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime)
		if err != nil {
			return nil, err
		}
		sig = &builtinExtractDatetimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetime)
	}
	return sig, nil
}

type builtinExtractDatetimeFromStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExtractDatetimeFromStringSig) Clone() builtinFunc {
	newSig := &builtinExtractDatetimeFromStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDatetimeFromStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeFromStringSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dtStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	tc := typeCtx(ctx)
	if types.IsClockUnit(unit) && types.IsDateUnit(unit) {
		dur, _, err := types.ParseDuration(tc, dtStr, types.GetFsp(dtStr))
		if err != nil {
			return 0, true, err
		}
		res, err := types.ExtractDurationNum(&dur, unit)
		if err != nil {
			return 0, true, err
		}
		dt, err := types.ParseDatetime(tc, dtStr)
		if err != nil {
			return res, false, nil
		}
		if dt.Hour() == dur.Hour() && dt.Minute() == dur.Minute() && dt.Second() == dur.Second() && dt.Year() > 0 {
			res, err = types.ExtractDatetimeNum(&dt, unit)
		}
		return res, err != nil, err
	}

	panic("Unexpected unit for extract")
}

type builtinExtractDatetimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExtractDatetimeSig) Clone() builtinFunc {
	newSig := &builtinExtractDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDatetimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dt, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDatetimeNum(&dt, unit)
	return res, err != nil, err
}

type builtinExtractDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExtractDurationSig) Clone() builtinFunc {
	newSig := &builtinExtractDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDurationSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dur, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDurationNum(&dur, unit)
	return res, err != nil, err
}

// baseDateArithmetical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmetical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArithmeticalUtil() baseDateArithmetical {
	return baseDateArithmetical{
		intervalRegexp: regexp.MustCompile(`^[+-]?[\d]+`),
	}
}

func (du *baseDateArithmetical) getDateFromString(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateStr, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTime(tc, dateStr, dateTp, types.MaxFsp)
	if err != nil {
		err = handleInvalidTimeError(ctx, err)
		if err != nil {
			return types.ZeroTime, true, err
		}
		return date, true, handleInvalidTimeError(ctx, err)
	} else if sqlMode(ctx).HasNoZeroDateMode() && (date.Year() == 0 || date.Month() == 0 || date.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, dateStr))
	}
	return date, false, handleInvalidTimeError(ctx, err)
}

func (du *baseDateArithmetical) getDateFromInt(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateInt, isNull, err := args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTimeFromInt64(tc, dateInt)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	// The actual date.Type() might be date or datetime.
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getDateFromReal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateReal, isNull, err := args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTimeFromFloat64(tc, dateReal)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	// The actual date.Type() might be date or datetime.
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getDateFromDecimal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateDec, isNull, err := args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTimeFromDecimal(tc, dateDec)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	// The actual date.Type() might be date or datetime.
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getDateFromDatetime(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	date, isNull, err := args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	// The actual date.Type() might be date, datetime or timestamp.
	// Datetime is treated as is.
	// Timestamp is treated as datetime, as MySQL manual says: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) || date.Type() == mysql.TypeTimestamp {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getIntervalFromString(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	ec := errCtx(ctx)
	interval, err = du.intervalReformatString(ec, interval, unit)
	return interval, false, err
}

func (du *baseDateArithmetical) intervalReformatString(ec errctx.Context, str string, unit string) (interval string, err error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		str = strings.TrimSpace(str)
		// a single unit value has to be specially handled.
		interval = du.intervalRegexp.FindString(str)
		if interval == "" {
			interval = "0"
		}

		if interval != str {
			err = ec.HandleError(types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", str))
		}
	case "SECOND":
		// The unit SECOND is specially handled, for example:
		// date + INTERVAL "1e2" SECOND = date + INTERVAL 100 second
		// date + INTERVAL "1.6" SECOND = date + INTERVAL 1.6 second
		// But:
		// date + INTERVAL "1e2" MINUTE = date + INTERVAL 1 MINUTE
		// date + INTERVAL "1.6" MINUTE = date + INTERVAL 1 MINUTE
		var dec types.MyDecimal
		if err = dec.FromString([]byte(str)); err != nil {
			truncatedErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", str)
			err = ec.HandleErrorWithAlias(err, truncatedErr, truncatedErr)
		}
		interval = string(dec.ToString())
	default:
		interval = str
	}
	return interval, err
}

func (du *baseDateArithmetical) intervalDecimalToString(ec errctx.Context, dec *types.MyDecimal) (string, error) {
	var rounded types.MyDecimal
	err := dec.Round(&rounded, 0, types.ModeHalfUp)
	if err != nil {
		return "", err
	}

	intVal, err := rounded.ToInt()
	if err != nil {
		if err = ec.HandleError(types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", dec.String())); err != nil {
			return "", err
		}
	}

	return strconv.FormatInt(intVal, 10), nil
}

func (du *baseDateArithmetical) getIntervalFromDecimal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	decimal, isNull, err := args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	interval := decimal.String()

	switch strings.ToUpper(unit) {
	case "HOUR_MINUTE", "MINUTE_SECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE",
		"DAY_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND":
		neg := false
		if interval != "" && interval[0] == '-' {
			neg = true
			interval = interval[1:]
		}
		switch strings.ToUpper(unit) {
		case "HOUR_MINUTE", "MINUTE_SECOND":
			interval = strings.ReplaceAll(interval, ".", ":")
		case "YEAR_MONTH":
			interval = strings.ReplaceAll(interval, ".", "-")
		case "DAY_HOUR":
			interval = strings.ReplaceAll(interval, ".", " ")
		case "DAY_MINUTE":
			interval = "0 " + strings.ReplaceAll(interval, ".", ":")
		case "DAY_SECOND":
			interval = "0 00:" + strings.ReplaceAll(interval, ".", ":")
		case "DAY_MICROSECOND":
			interval = "0 00:00:" + interval
		case "HOUR_MICROSECOND":
			interval = "00:00:" + interval
		case "HOUR_SECOND":
			interval = "00:" + strings.ReplaceAll(interval, ".", ":")
		case "MINUTE_MICROSECOND":
			interval = "00:" + interval
		case "SECOND_MICROSECOND":
			/* keep interval as original decimal */
		}
		if neg {
			interval = "-" + interval
		}
	case "SECOND":
		// interval is already like the %f format.
	default:
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		ec := errCtx(ctx)
		interval, err = du.intervalDecimalToString(ec, decimal)
		if err != nil {
			return "", true, err
		}
	}

	return interval, false, nil
}

func (du *baseDateArithmetical) getIntervalFromInt(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	if mysql.HasUnsignedFlag(args[1].GetType(ctx).GetFlag()) {
		return strconv.FormatUint(uint64(interval), 10), false, nil
	}

	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmetical) getIntervalFromReal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatFloat(interval, 'f', args[1].GetType(ctx).GetDecimal(), 64), false, nil
}

func (du *baseDateArithmetical) add(ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	year, month, day, nano, _, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	return du.addDate(ctx, date, year, month, day, nano, resultFsp)
}

func (du *baseDateArithmetical) addDate(ctx EvalContext, date types.Time, year, month, day, nano int64, resultFsp int) (types.Time, bool, error) {
	goTime, err := date.GoTime(time.UTC)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	goTime = goTime.Add(time.Duration(nano))
	goTime, err = types.AddDate(year, month, day, goTime)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	// Adjust fsp as required by outer - always respect type inference.
	date.SetFsp(resultFsp)

	// fix https://github.com/pingcap/tidb/issues/11329
	if goTime.Year() == 0 {
		hour, minute, second := goTime.Clock()
		date.SetCoreTime(types.FromDate(0, 0, 0, hour, minute, second, goTime.Nanosecond()/1000))
		return date, false, nil
	}

	date.SetCoreTime(types.FromGoTime(goTime))
	tc := typeCtx(ctx)
	overflow, err := types.DateTimeIsOverflow(tc, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	if overflow {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

type funcDurationOp func(d, interval types.Duration) (types.Duration, error)

func (du *baseDateArithmetical) opDuration(ctx EvalContext, op funcDurationOp, d types.Duration, interval string, unit string, resultFsp int) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := op(d, dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	// Adjust fsp as required by outer - always respect type inference.
	retDur.Fsp = resultFsp
	return retDur, false, nil
}

func (du *baseDateArithmetical) addDuration(ctx EvalContext, d types.Duration, interval string, unit string, resultFsp int) (types.Duration, bool, error) {
	add := func(d, interval types.Duration) (types.Duration, error) {
		return d.Add(interval)
	}
	return du.opDuration(ctx, add, d, interval, unit, resultFsp)
}

func (du *baseDateArithmetical) subDuration(ctx EvalContext, d types.Duration, interval string, unit string, resultFsp int) (types.Duration, bool, error) {
	sub := func(d, interval types.Duration) (types.Duration, error) {
		return d.Sub(interval)
	}
	return du.opDuration(ctx, sub, d, interval, unit, resultFsp)
}

func (du *baseDateArithmetical) sub(ctx EvalContext, date types.Time, interval string, unit string, resultFsp int) (types.Time, bool, error) {
	year, month, day, nano, _, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	return du.addDate(ctx, date, -year, -month, -day, -nano, resultFsp)
}

func (du *baseDateArithmetical) vecGetDateFromInt(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	i64s := buf.Int64s()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		date, err := types.ParseTimeFromInt64(tc, i64s[i])
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		// The actual date.Type() might be date or datetime.
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit {
			date.SetType(mysql.TypeDatetime)
		}
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromReal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	f64s := buf.Float64s()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		date, err := types.ParseTimeFromFloat64(tc, f64s[i])
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		// The actual date.Type() might be date or datetime.
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit {
			date.SetType(mysql.TypeDatetime)
		}
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromDecimal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		dec := buf.GetDecimal(i)
		date, err := types.ParseTimeFromDecimal(tc, dec)
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		// The actual date.Type() might be date or datetime.
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit {
			date.SetType(mysql.TypeDatetime)
		}
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromString(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		dateStr := buf.GetString(i)
		dateTp := mysql.TypeDate
		if !types.IsDateFormat(dateStr) || isClockUnit {
			dateTp = mysql.TypeDatetime
		}

		date, err := types.ParseTime(tc, dateStr, dateTp, types.MaxFsp)
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
		} else if sqlMode(ctx).HasNoZeroDateMode() && (date.Year() == 0 || date.Month() == 0 || date.Day() == 0) {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, dateStr))
			if err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			dates[i] = date
		}
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromDatetime(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeTime(n, false)
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}

	dates := result.Times()
	isClockUnit := types.IsClockUnit(unit)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		// The actual date[i].Type() might be date, datetime or timestamp.
		// Datetime is treated as is.
		// Timestamp is treated as datetime, as MySQL manual says: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit || dates[i].Type() == mysql.TypeTimestamp {
			dates[i].SetType(mysql.TypeDatetime)
		}
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromString(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	ec := errCtx(ctx)
	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		interval, err := du.intervalReformatString(ec, buf.GetString(i), unit)
		if err != nil {
			return err
		}
		result.AppendString(interval)
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromDecimal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	isCompoundUnit := false
	amendInterval := func(val string, row *chunk.Row) (string, bool, error) {
		return val, false, nil
	}
	switch unitUpper := strings.ToUpper(unit); unitUpper {
	case "HOUR_MINUTE", "MINUTE_SECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE",
		"DAY_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND":
		isCompoundUnit = true
		switch strings.ToUpper(unit) {
		case "HOUR_MINUTE", "MINUTE_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.ReplaceAll(val, ".", ":"), false, nil
			}
		case "YEAR_MONTH":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.ReplaceAll(val, ".", "-"), false, nil
			}
		case "DAY_HOUR":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.ReplaceAll(val, ".", " "), false, nil
			}
		case "DAY_MINUTE":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 " + strings.ReplaceAll(val, ".", ":"), false, nil
			}
		case "DAY_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 00:" + strings.ReplaceAll(val, ".", ":"), false, nil
			}
		case "DAY_MICROSECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 00:00:" + val, false, nil
			}
		case "HOUR_MICROSECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "00:00:" + val, false, nil
			}
		case "HOUR_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "00:" + strings.ReplaceAll(val, ".", ":"), false, nil
			}
		case "MINUTE_MICROSECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "00:" + val, false, nil
			}
		case "SECOND_MICROSECOND":
			/* keep interval as original decimal */
		}
	case "SECOND":
		/* keep interval as original decimal */
	default:
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		amendInterval = func(_ string, row *chunk.Row) (string, bool, error) {
			dec, isNull, err := b.args[1].EvalDecimal(ctx, *row)
			if isNull || err != nil {
				return "", true, err
			}

			str, err := du.intervalDecimalToString(errCtx(ctx), dec)
			if err != nil {
				return "", true, err
			}

			return str, false, nil
		}
	}

	result.ReserveString(n)
	decs := buf.Decimals()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		interval := decs[i].String()
		row := input.GetRow(i)
		isNeg := false
		if isCompoundUnit && interval != "" && interval[0] == '-' {
			isNeg = true
			interval = interval[1:]
		}
		interval, isNull, err := amendInterval(interval, &row)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		if isCompoundUnit && isNeg {
			interval = "-" + interval
		}
		result.AppendString(interval)
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromInt(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	unsigned := mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag())
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
		} else if unsigned {
			result.AppendString(strconv.FormatUint(uint64(i64s[i]), 10))
		} else {
			result.AppendString(strconv.FormatInt(i64s[i], 10))
		}
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromReal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	f64s := buf.Float64s()
	prec := b.args[1].GetType(ctx).GetDecimal()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(strconv.FormatFloat(f64s[i], 'f', prec, 64))
		}
	}
	return nil
}

type funcTimeOpForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error)

func addTime(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	return da.add(ctx, date, interval, unit, resultFsp)
}

func subTime(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	return da.sub(ctx, date, interval, unit, resultFsp)
}

type funcDurationOpForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error)

func addDuration(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error) {
	return da.addDuration(ctx, d, interval, unit, resultFsp)
}

func subDuration(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error) {
	return da.subDuration(ctx, d, interval, unit, resultFsp)
}

type funcSetPbCodeOp func(b builtinFunc, add, sub tipb.ScalarFuncSig)

func setAdd(b builtinFunc, add, sub tipb.ScalarFuncSig) {
	b.setPbCode(add)
}

func setSub(b builtinFunc, add, sub tipb.ScalarFuncSig) {
	b.setPbCode(sub)
}

type funcGetDateForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error)

func getDateFromString(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromString(ctx, args, row, unit)
}

func getDateFromInt(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromInt(ctx, args, row, unit)
}

func getDateFromReal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromReal(ctx, args, row, unit)
}

func getDateFromDecimal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromDecimal(ctx, args, row, unit)
}

type funcVecGetDateForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error

func vecGetDateFromString(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromString(b, ctx, input, unit, result)
}

func vecGetDateFromInt(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromInt(b, ctx, input, unit, result)
}

func vecGetDateFromReal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromReal(b, ctx, input, unit, result)
}

func vecGetDateFromDecimal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromDecimal(b, ctx, input, unit, result)
}

type funcGetIntervalForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error)

func getIntervalFromString(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromString(ctx, args, row, unit)
}

func getIntervalFromInt(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromInt(ctx, args, row, unit)
}

func getIntervalFromReal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromReal(ctx, args, row, unit)
}

func getIntervalFromDecimal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromDecimal(ctx, args, row, unit)
}

type funcVecGetIntervalForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error

func vecGetIntervalFromString(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromString(b, ctx, input, unit, result)
}

func vecGetIntervalFromInt(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromInt(b, ctx, input, unit, result)
}

func vecGetIntervalFromReal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromReal(b, ctx, input, unit, result)
}

func vecGetIntervalFromDecimal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromDecimal(b, ctx, input, unit, result)
}
