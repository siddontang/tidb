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
	"strings"
	"time"

	"github.com/pingcap/failpoint"
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
