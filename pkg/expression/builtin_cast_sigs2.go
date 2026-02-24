// Copyright 2017 PingCAP, Inc.
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
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type builtinCastTimeAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	tc := typeCtx(ctx)
	if res, err = res.Convert(tc, b.tp.GetType()); err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	res, err = res.RoundFrac(tc, b.tp.GetDecimal())
	if b.tp.GetType() == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		res.SetType(b.tp.GetType())
	}
	return res, false, err
}

type builtinCastTimeAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	t, err := val.RoundFrac(tc, types.DefaultFsp)
	if err != nil {
		return res, false, err
	}
	res, err = t.ToNumber().ToInt()
	return res, false, err
}

type builtinCastTimeAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastTimeAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastTimeAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, val.ToNumber(), b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastTimeAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

type builtinCastTimeAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastTimeAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastTimeAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastTimeAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = val.ConvertToDuration()
	if err != nil {
		return res, false, err
	}
	res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
	return res, false, err
}

type builtinCastDurationAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
	return res, false, err
}

type builtinCastDurationAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if b.tp.GetType() == mysql.TypeYear {
		res, err = val.ConvertToYear(typeCtx(ctx))
	} else {
		var dur types.Duration
		dur, err = val.RoundFrac(types.DefaultFsp, location(ctx))
		if err != nil {
			return res, false, err
		}
		res, err = dur.ToNumber().ToInt()
	}
	return res, false, err
}

type builtinCastDurationAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(val.Fsp); err != nil {
		return res, false, err
	}
	res, err = val.ToNumber().ToFloat64()
	return res, false, err
}

type builtinCastDurationAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastDurationAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if val.Fsp, err = types.CheckFsp(val.Fsp); err != nil {
		return res, false, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ProduceDecWithSpecifiedTp(tc, val.ToNumber(), b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastDurationAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return padZeroForBinaryType(res, b.tp, ctx)
}

func padZeroForBinaryType(s string, tp *types.FieldType, ctx EvalContext) (string, bool, error) {
	flen := tp.GetFlen()
	if tp.GetType() == mysql.TypeString && types.IsBinaryStr(tp) && len(s) < flen {
		maxAllowedPacket := ctx.GetMaxAllowedPacket()
		if uint64(flen) > maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(ctx, "cast_as_binary", maxAllowedPacket)
		}
		padding := make([]byte, flen-len(s))
		s = string(append([]byte(s), padding...))
	}
	return s, false, nil
}

type builtinCastDurationAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastDurationAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastDurationAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastDurationAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc := typeCtx(ctx)
	ts, err := getStmtTimestamp(ctx)
	if err != nil {
		ts = gotime.Now()
	}
	res, err = val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	res, err = res.RoundFrac(tc, b.tp.GetDecimal())
	return res, false, err
}

type builtinCastJSONAsJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsJSONSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsJSONSig) evalJSON(ctx EvalContext, row chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	return b.args[0].EvalJSON(ctx, row)
}

type builtinCastJSONAsIntSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsIntSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsIntSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ConvertJSONToInt64(typeCtx(ctx), val, mysql.HasUnsignedFlag(b.tp.GetFlag()))
	ec := errCtx(ctx)
	err = ec.HandleError(err)
	return
}

type builtinCastJSONAsRealSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsRealSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsRealSig) evalReal(ctx EvalContext, row chunk.Row) (res float64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	res, err = types.ConvertJSONToFloat(typeCtx(ctx), val)
	return
}

type builtinCastJSONAsDecimalSig struct {
	baseBuiltinCastFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsDecimalSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinCastFunc)
	return newSig
}

func (b *builtinCastJSONAsDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (res *types.MyDecimal, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	res, err = types.ConvertJSONToDecimal(tc, val)
	if err != nil {
		return res, false, err
	}
	res, err = types.ProduceDecWithSpecifiedTp(tc, res, b.tp)
	err = ec.HandleError(err)
	return res, false, err
}

type builtinCastJSONAsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsStringSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return s, false, nil
}

type builtinCastVectorFloat32AsStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastVectorFloat32AsStringSig) Clone() builtinFunc {
	newSig := &builtinCastVectorFloat32AsStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastVectorFloat32AsStringSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalVectorFloat32(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	s, err := types.ProduceStrWithSpecifiedTp(val.String(), b.tp, typeCtx(ctx), false)
	if err != nil {
		return res, false, err
	}
	return s, false, nil
}

type builtinCastJSONAsTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsTimeSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsTimeSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	switch val.TypeCode {
	case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
		res = val.GetTimeWithFsp(b.tp.GetDecimal())
		res.SetType(b.tp.GetType())
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		}
		return res, isNull, err
	case types.JSONTypeCodeDuration:
		duration := val.GetDuration()

		tc := typeCtx(ctx)
		ts, err := getStmtTimestamp(ctx)
		if err != nil {
			ts = gotime.Now()
		}
		res, err = duration.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
		if err != nil {
			return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
		}
		res, err = res.RoundFrac(tc, b.tp.GetDecimal())
		return res, isNull, err
	case types.JSONTypeCodeString:
		s, err := val.Unquote()
		if err != nil {
			return res, false, err
		}
		res, err = types.ParseTime(typeCtx(ctx), s, b.tp.GetType(), b.tp.GetDecimal())
		if err != nil {
			return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
		}
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			res.SetCoreTime(types.FromDate(res.Year(), res.Month(), res.Day(), 0, 0, 0, 0))
		}
		return res, isNull, err
	default:
		ec := errCtx(ctx)
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs(types.TypeStr(b.tp.GetType()), val.String())
		return res, true, ec.HandleError(err)
	}
}

type builtinCastJSONAsDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCastJSONAsDurationSig) Clone() builtinFunc {
	newSig := &builtinCastJSONAsDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCastJSONAsDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	tc := typeCtx(ctx)

	switch val.TypeCode {
	case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
		time := val.GetTimeWithFsp(b.tp.GetDecimal())
		res, err = time.ConvertToDuration()
		if err != nil {
			return res, false, err
		}
		res, err = res.RoundFrac(b.tp.GetDecimal(), location(ctx))
		return res, isNull, err
	case types.JSONTypeCodeDuration:
		res = val.GetDuration()
		return res, isNull, err
	case types.JSONTypeCodeString:
		s, err := val.Unquote()
		if err != nil {
			return res, false, err
		}
		res, _, err = types.ParseDuration(tc, s, b.tp.GetDecimal())
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
		}
		return res, isNull, err
	default:
		err = types.ErrTruncatedWrongVal.GenWithStackByArgs("TIME", val.String())
		return res, true, tc.HandleTruncate(err)
	}
}

// CanImplicitEvalInt represents the builtin functions that have an implicit path to evaluate as integer,
// regardless of the type that type inference decides it to be.
// This is a nasty way to match the weird behavior of MySQL functions like `dayname()` being implicitly evaluated as integer.
// See https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/sql/item_timefunc.h#L423 for details.
func CanImplicitEvalInt(expr Expression) bool {
	if f, ok := expr.(*ScalarFunction); ok {
		return f.FuncName.L == ast.DayName
	}
	return false
}

// CanImplicitEvalReal represents the builtin functions that have an implicit path to evaluate as real,
// regardless of the type that type inference decides it to be.
// This is a nasty way to match the weird behavior of MySQL functions like `dayname()` being implicitly evaluated as real.
// See https://github.com/mysql/mysql-server/blob/ee4455a33b10f1b1886044322e4893f587b319ed/sql/item_timefunc.h#L423 for details.
func CanImplicitEvalReal(expr Expression) bool {
	if f, ok := expr.(*ScalarFunction); ok {
		return f.FuncName.L == ast.DayName
	}
	return false
}

// BuildCastFunction4Union build a implicitly CAST ScalarFunction from the Union
// Expression.
func BuildCastFunction4Union(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression) {
	res, err := BuildCastFunctionWithCheck(ctx, expr, tp, true, false)
	terror.Log(err)
	return
}

// BuildCastCollationFunction builds a ScalarFunction which casts the collation.
func BuildCastCollationFunction(ctx BuildContext, expr Expression, ec *ExprCollation, enumOrSetRealTypeIsStr bool) Expression {
	if expr.GetType(ctx.GetEvalCtx()).EvalType() != types.ETString {
		return expr
	}
	if expr.GetType(ctx.GetEvalCtx()).GetCollate() == ec.Collation {
		return expr
	}
	tp := expr.GetType(ctx.GetEvalCtx()).Clone()
	if expr.GetType(ctx.GetEvalCtx()).Hybrid() {
		if !enumOrSetRealTypeIsStr {
			return expr
		}
		tp = types.NewFieldType(mysql.TypeVarString)
	} else if ec.Charset == charset.CharsetBin {
		// When cast character string to binary string, if we still use fixed length representation,
		// then 0 padding will be used, which can affect later execution.
		// e.g. https://github.com/pingcap/tidb/issues/34823.
		// On the other hand, we can not directly return origin expr back,
		// since we need binary collation to do string comparison later.
		// e.g. https://github.com/pingcap/tidb/pull/35053#discussion_r894155052
		// Here we use VarString type of cast, i.e `cast(a as binary)`, to avoid this problem.
		tp.SetType(mysql.TypeVarString)
	}
	tp.SetCharset(ec.Charset)
	tp.SetCollate(ec.Collation)
	newExpr := BuildCastFunction(ctx, expr, tp)
	return newExpr
}

// BuildCastFunction builds a CAST ScalarFunction from the Expression.
func BuildCastFunction(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression) {
	res, err := BuildCastFunctionWithCheck(ctx, expr, tp, false, false)
	terror.Log(err)
	return
}

// BuildCastFunctionWithCheck builds a CAST ScalarFunction from the Expression and return error if any.
func BuildCastFunctionWithCheck(ctx BuildContext, expr Expression, tp *types.FieldType, inUnion bool, isExplicitCharset bool) (res Expression, err error) {
	argType := expr.GetType(ctx.GetEvalCtx())
	// If source argument's nullable, then target type should be nullable
	if !mysql.HasNotNullFlag(argType.GetFlag()) {
		tp.DelFlag(mysql.NotNullFlag)
	}
	expr = TryPushCastIntoControlFunctionForHybridType(ctx, expr, tp)
	var fc functionClass
	switch tp.EvalType() {
	case types.ETInt:
		fc = &castAsIntFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, inUnion}
	case types.ETDecimal:
		fc = &castAsDecimalFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, inUnion}
	case types.ETReal:
		fc = &castAsRealFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, inUnion}
	case types.ETDatetime, types.ETTimestamp:
		fc = &castAsTimeFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETDuration:
		fc = &castAsDurationFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETJson:
		if tp.IsArray() {
			fc = &castAsArrayFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		} else {
			fc = &castAsJSONFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
		}
	case types.ETVectorFloat32:
		fc = &castAsVectorFloat32FunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp}
	case types.ETString:
		fc = &castAsStringFunctionClass{baseFunctionClass{ast.Cast, 1, 1}, tp, isExplicitCharset}
		if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeBit {
			tp.SetFlen((expr.GetType(ctx.GetEvalCtx()).GetFlen() + 7) / 8)
		}
	default:
		return nil, errors.Errorf("cannot cast from %s", tp.EvalType())
	}
	f, err := fc.getFunction(ctx, []Expression{expr})
	res = &ScalarFunction{
		FuncName: ast.NewCIStr(ast.Cast),
		RetType:  tp,
		Function: f,
	}
	// We do not fold CAST if the eval type of this scalar function is ETJson
	// since we may reset the flag of the field type of CastAsJson later which
	// would affect the evaluation of it.
	if tp.EvalType() != types.ETJson && err == nil {
		return FoldConstant(ctx, res), nil
	}
	return res, err
}

// WrapWithCastAsInt wraps `expr` with `cast` if the return type of expr is not
// type int, otherwise, returns `expr` directly.
func WrapWithCastAsInt(ctx BuildContext, expr Expression, targetType *types.FieldType) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeEnum {
		// since column and correlated column may be referred in other places, deep
		// clone the one out with its field type as well before change the flag inside.
		if col, ok := expr.(*Column); ok {
			col = col.Clone().(*Column)
			col.RetType = col.RetType.Clone()
			expr = col
		}
		if col, ok := expr.(*CorrelatedColumn); ok {
			col = col.Clone().(*CorrelatedColumn)
			col.RetType = col.RetType.Clone()
			expr = col
		}
		expr.GetType(ctx.GetEvalCtx()).AddFlag(mysql.EnumSetAsIntFlag)
	}
	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.SetFlen(expr.GetType(ctx.GetEvalCtx()).GetFlen())
	tp.SetDecimal(0)
	types.SetBinChsClnFlag(tp)
	// inherit NotNullFlag from source type
	tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & mysql.NotNullFlag)
	if targetType == nil {
		// inherit UnsignedFlag from source type if targetType is nil
		tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & mysql.UnsignedFlag)
	} else {
		// otherwise set UnsignedFlag based on targetType
		tp.AddFlag(targetType.GetFlag() & mysql.UnsignedFlag)
	}
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsReal wraps `expr` with `cast` if the return type of expr is not
// type real, otherwise, returns `expr` directly.
func WrapWithCastAsReal(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETReal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDouble)
	tp.SetFlen(mysql.MaxRealWidth)
	tp.SetDecimal(types.UnspecifiedLength)
	types.SetBinChsClnFlag(tp)
	tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & (mysql.UnsignedFlag | mysql.NotNullFlag))
	return BuildCastFunction(ctx, expr, tp)
}

func minimalDecimalLenForHoldingInteger(tp byte) int {
	switch tp {
	case mysql.TypeTiny:
		return 3
	case mysql.TypeShort:
		return 5
	case mysql.TypeInt24:
		return 8
	case mysql.TypeLong:
		return 10
	case mysql.TypeLonglong:
		return 20
	case mysql.TypeYear:
		return 4
	default:
		return mysql.MaxIntWidth
	}
}

// WrapWithCastAsDecimal wraps `expr` with `cast` if the return type of expr is
// not type decimal, otherwise, returns `expr` directly.
func WrapWithCastAsDecimal(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETDecimal {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeNewDecimal)
	tp.SetFlenUnderLimit(expr.GetType(ctx.GetEvalCtx()).GetFlen())
	tp.SetDecimalUnderLimit(expr.GetType(ctx.GetEvalCtx()).GetDecimal())

	if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		tp.SetFlen(minimalDecimalLenForHoldingInteger(expr.GetType(ctx.GetEvalCtx()).GetType()))
		tp.SetDecimal(0)
	}
	if tp.GetFlen() == types.UnspecifiedLength || tp.GetFlen() > mysql.MaxDecimalWidth {
		tp.SetFlen(mysql.MaxDecimalWidth)
	}
	types.SetBinChsClnFlag(tp)
	tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & (mysql.UnsignedFlag | mysql.NotNullFlag))
	castExpr := BuildCastFunction(ctx, expr, tp)
	// For const item, we can use find-grained precision and scale by the result.
	if castExpr.ConstLevel() == ConstStrict {
		val, isnull, err := castExpr.EvalDecimal(ctx.GetEvalCtx(), chunk.Row{})
		if !isnull && err == nil {
			precision, frac := val.PrecisionAndFrac()
			castTp := castExpr.GetType(ctx.GetEvalCtx())
			castTp.SetDecimalUnderLimit(frac)
			castTp.SetFlenUnderLimit(precision)
		}
	}
	return castExpr
}

// WrapWithCastAsString wraps `expr` with `cast` if the return type of expr is
// not type string, otherwise, returns `expr` directly.
func WrapWithCastAsString(ctx BuildContext, expr Expression) Expression {
	exprTp := expr.GetType(ctx.GetEvalCtx())
	if exprTp.EvalType() == types.ETString {
		return expr
	}
	argLen := exprTp.GetFlen()
	// If expr is decimal, we should take the decimal point ,negative sign and the leading zero(0.xxx)
	// into consideration, so we set `expr.GetType(ctx.GetEvalCtx()).GetFlen() + 3` as the `argLen`.
	// Since the length of float and double is not accurate, we do not handle
	// them.
	if exprTp.GetType() == mysql.TypeNewDecimal && argLen != types.UnspecifiedFsp {
		argLen += 3
	}

	if exprTp.EvalType() == types.ETInt {
		argLen = mysql.MaxIntWidth
		// For TypeBit, castAsString will make length as int(( bit_len + 7 ) / 8) bytes due to
		// TiKV needs the bit's real len during calculating, eg: ascii(bit).
		if exprTp.GetType() == mysql.TypeBit {
			argLen = (exprTp.GetFlen() + 7) / 8
		}
	}

	// Because we can't control the length of cast(float as char) for now, we can't determine the argLen.
	if exprTp.GetType() == mysql.TypeFloat || exprTp.GetType() == mysql.TypeDouble {
		argLen = -1
	}
	tp := types.NewFieldType(mysql.TypeVarString)
	if expr.Coercibility() == CoercibilityExplicit {
		charset, collate := expr.CharsetAndCollation()
		tp.SetCharset(charset)
		tp.SetCollate(collate)
	} else if exprTp.GetType() == mysql.TypeBit {
		// Implicitly casting BIT to string will make it a binary
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
	} else {
		charset, collate := ctx.GetCharsetInfo()
		tp.SetCharset(charset)
		tp.SetCollate(collate)
	}
	tp.SetFlen(argLen)
	tp.SetDecimal(types.UnspecifiedLength)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsTime wraps `expr` with `cast` if the return type of expr is not
// same as type of the specified `tp` , otherwise, returns `expr` directly.
func WrapWithCastAsTime(ctx BuildContext, expr Expression, tp *types.FieldType) Expression {
	exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
	if tp.GetType() == exprTp {
		return expr
	} else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
		return expr
	}
	switch x := expr.GetType(ctx.GetEvalCtx()).EvalType(); x {
	case types.ETInt:
		tp.SetDecimal(types.MinFsp)
	case types.ETString, types.ETReal, types.ETJson:
		tp.SetDecimal(types.MaxFsp)
	case types.ETDatetime, types.ETTimestamp, types.ETDuration:
		tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
	case types.ETDecimal:
		tp.SetDecimal(expr.GetType(ctx.GetEvalCtx()).GetDecimal())
		if tp.GetDecimal() > types.MaxFsp {
			tp.SetDecimal(types.MaxFsp)
		}
	default:
	}
	switch tp.GetType() {
	case mysql.TypeDate:
		tp.SetFlen(mysql.MaxDateWidth)
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		tp.SetFlen(mysql.MaxDatetimeWidthNoFsp)
		if tp.GetDecimal() > 0 {
			tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
		}
	}
	types.SetBinChsClnFlag(tp)
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsDuration wraps `expr` with `cast` if the return type of expr is
// not type duration, otherwise, returns `expr` directly.
func WrapWithCastAsDuration(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDuration {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeDuration)
	switch x := expr.GetType(ctx.GetEvalCtx()); x.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDate:
		tp.SetDecimal(x.GetDecimal())
	default:
		tp.SetDecimal(types.MaxFsp)
	}
	tp.SetFlen(mysql.MaxDurationWidthNoFsp)
	if tp.GetDecimal() > 0 {
		tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
	}
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsJSON wraps `expr` with `cast` if the return type of expr is not
// type json, otherwise, returns `expr` directly.
func WrapWithCastAsJSON(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeJSON && !mysql.HasParseToJSONFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag()) {
		return expr
	}
	tp := types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).SetFlag(mysql.BinaryFlag).SetFlen(12582912).SetCharset(mysql.DefaultCharset).SetCollate(mysql.DefaultCollationName).BuildP()
	return BuildCastFunction(ctx, expr, tp)
}

// WrapWithCastAsVectorFloat32 wraps `expr` with `cast` if the return type of expr is not
// type VectorFloat32, otherwise, returns `expr` directly.
func WrapWithCastAsVectorFloat32(ctx BuildContext, expr Expression) Expression {
	if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeTiDBVectorFloat32 {
		return expr
	}
	tp := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	return BuildCastFunction(ctx, expr, tp)
}

// TryPushCastIntoControlFunctionForHybridType try to push cast into control function for Hybrid Type.
// If necessary, it will rebuild control function using changed args.
// When a hybrid type is the output of a control function, the result may be as a numeric type to subsequent calculation
// We should perform the `Cast` operation early to avoid using the wrong type for calculation
// For example, the condition `if(1, e, 'a') = 1`, `if` function will output `e` and compare with `1`.
// If the evaltype is ETString, it will get wrong result. So we can rewrite the condition to
// `IfInt(1, cast(e as int), cast('a' as int)) = 1` to get the correct result.
func TryPushCastIntoControlFunctionForHybridType(ctx BuildContext, expr Expression, tp *types.FieldType) (res Expression) {
	sf, ok := expr.(*ScalarFunction)
	if !ok {
		return expr
	}

	var wrapCastFunc func(ctx BuildContext, expr Expression) Expression
	switch tp.EvalType() {
	case types.ETInt:
		wrapCastFunc = func(ctx BuildContext, expr Expression) Expression {
			return WrapWithCastAsInt(ctx, expr, tp)
		}
	case types.ETReal:
		wrapCastFunc = WrapWithCastAsReal
	default:
		return expr
	}

	isHybrid := func(ft *types.FieldType) bool {
		// todo: compatible with mysql control function using bit type. issue 24725
		return ft.Hybrid() && ft.GetType() != mysql.TypeBit
	}

	args := sf.GetArgs()
	switch sf.FuncName.L {
	case ast.If:
		if isHybrid(args[1].GetType(ctx.GetEvalCtx())) || isHybrid(args[2].GetType(ctx.GetEvalCtx())) {
			args[1] = wrapCastFunc(ctx, args[1])
			args[2] = wrapCastFunc(ctx, args[2])
			f, err := funcs[ast.If].getFunction(ctx, args)
			if err != nil {
				return expr
			}
			sf.RetType, sf.Function = f.getRetTp(), f
			return sf
		}
	case ast.Case:
		hasHybrid := false
		for i := 0; i < len(args)-1; i += 2 {
			hasHybrid = hasHybrid || isHybrid(args[i+1].GetType(ctx.GetEvalCtx()))
		}
		if len(args)%2 == 1 {
			hasHybrid = hasHybrid || isHybrid(args[len(args)-1].GetType(ctx.GetEvalCtx()))
		}
		if !hasHybrid {
			return expr
		}

		for i := 0; i < len(args)-1; i += 2 {
			args[i+1] = wrapCastFunc(ctx, args[i+1])
		}
		if len(args)%2 == 1 {
			args[len(args)-1] = wrapCastFunc(ctx, args[len(args)-1])
		}
		f, err := funcs[ast.Case].getFunction(ctx, args)
		if err != nil {
			return expr
		}
		sf.RetType, sf.Function = f.getRetTp(), f
		return sf
	case ast.Elt:
		hasHybrid := false
		for i := 1; i < len(args); i++ {
			hasHybrid = hasHybrid || isHybrid(args[i].GetType(ctx.GetEvalCtx()))
		}
		if !hasHybrid {
			return expr
		}

		for i := 1; i < len(args); i++ {
			args[i] = wrapCastFunc(ctx, args[i])
		}
		f, err := funcs[ast.Elt].getFunction(ctx, args)
		if err != nil {
			return expr
		}
		sf.RetType, sf.Function = f.getRetTp(), f
		return sf
	default:
		return expr
	}
	return expr
}

func decimalPrecisionToLength(ft *types.FieldType) int {
	precision := ft.GetFlen()
	scale := ft.GetDecimal()
	unsigned := mysql.HasUnsignedFlag(ft.GetFlag())

	if precision == types.UnspecifiedLength || scale == types.UnspecifiedLength {
		return types.UnspecifiedLength
	}

	ret := precision
	if scale > 0 {
		ret++
	}

	if !unsigned && precision > 0 {
		ret++ // for negative sign
	}

	if ret == 0 {
		return 1
	}
	return ret
}
