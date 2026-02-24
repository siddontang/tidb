// Copyright 2016 PingCAP, Inc.
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

package types

import (
	gjson "encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ConvertTo converts a datum to the target field type.
// change this method need sync modification to type2Kind in rowcodec/types.go
func (d *Datum) ConvertTo(ctx Context, target *FieldType) (Datum, error) {
	if d.k == KindNull {
		return Datum{}, nil
	}
	switch target.GetType() { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		unsigned := mysql.HasUnsignedFlag(target.GetFlag())
		if unsigned {
			return d.convertToUint(ctx, target)
		}
		return d.convertToInt(ctx, target)
	case mysql.TypeFloat, mysql.TypeDouble:
		return d.convertToFloat(ctx, target)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return d.convertToString(ctx, target)
	case mysql.TypeTimestamp:
		return d.convertToMysqlTimestamp(ctx, target)
	case mysql.TypeDatetime, mysql.TypeDate:
		return d.convertToMysqlTime(ctx, target)
	case mysql.TypeDuration:
		return d.convertToMysqlDuration(ctx, target)
	case mysql.TypeNewDecimal:
		return d.convertToMysqlDecimal(ctx, target)
	case mysql.TypeYear:
		return d.ConvertToMysqlYear(ctx, target)
	case mysql.TypeEnum:
		return d.convertToMysqlEnum(ctx, target)
	case mysql.TypeBit:
		return d.convertToMysqlBit(ctx, target)
	case mysql.TypeSet:
		return d.convertToMysqlSet(ctx, target)
	case mysql.TypeJSON:
		return d.convertToMysqlJSON(target)
	case mysql.TypeTiDBVectorFloat32:
		return d.convertToVectorFloat32(ctx, target)
	case mysql.TypeNull:
		return Datum{}, nil
	default:
		panic("should never happen")
	}
}

func (d *Datum) convertToFloat(ctx Context, target *FieldType) (Datum, error) {
	var (
		f   float64
		ret Datum
		err error
	)
	switch d.k {
	case KindNull:
		return ret, nil
	case KindInt64:
		f = float64(d.GetInt64())
	case KindUint64:
		f = float64(d.GetUint64())
	case KindFloat32, KindFloat64:
		f = d.GetFloat64()
	case KindString, KindBytes:
		f, err = StrToFloat(ctx, d.GetString(), false)
	case KindMysqlTime:
		f, err = d.GetMysqlTime().ToNumber().ToFloat64()
	case KindMysqlDuration:
		f, err = d.GetMysqlDuration().ToNumber().ToFloat64()
	case KindMysqlDecimal:
		f, err = d.GetMysqlDecimal().ToFloat64()
	case KindMysqlSet:
		f = d.GetMysqlSet().ToNumber()
	case KindMysqlEnum:
		f = d.GetMysqlEnum().ToNumber()
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		f, err = float64(val), err1
	case KindMysqlJSON:
		f, err = ConvertJSONToFloat(ctx, d.GetMysqlJSON())
	default:
		return invalidConv(d, target.GetType())
	}
	f, err1 := ProduceFloatWithSpecifiedTp(f, target)
	if err == nil && err1 != nil {
		err = err1
	}
	if target.GetType() == mysql.TypeFloat {
		ret.SetFloat32(float32(f))
	} else {
		ret.SetFloat64(f)
	}
	return ret, errors.Trace(err)
}

// ProduceFloatWithSpecifiedTp produces a new float64 according to `flen` and `decimal`.
func ProduceFloatWithSpecifiedTp(f float64, target *FieldType) (_ float64, err error) {
	if math.IsNaN(f) {
		return 0, overflow(f, target.GetType())
	}
	if math.IsInf(f, 0) {
		return f, overflow(f, target.GetType())
	}
	// For float and following double type, we will only truncate it for float(M, D) format.
	// If no D is set, we will handle it like origin float whether M is set or not.
	if target.GetFlen() != UnspecifiedLength && target.GetDecimal() != UnspecifiedLength {
		f, err = TruncateFloat(f, target.GetFlen(), target.GetDecimal())
	}
	if mysql.HasUnsignedFlag(target.GetFlag()) && f < 0 {
		return 0, overflow(f, target.GetType())
	}

	if err != nil {
		// We must return the error got from TruncateFloat after checking whether the target is unsigned to make sure
		// the returned float is not negative when the target type is unsigned.
		return f, errors.Trace(err)
	}

	if target.GetType() == mysql.TypeFloat && (f > math.MaxFloat32 || f < -math.MaxFloat32) {
		if f > 0 {
			return math.MaxFloat32, overflow(f, target.GetType())
		}
		return -math.MaxFloat32, overflow(f, target.GetType())
	}
	return f, errors.Trace(err)
}

func (d *Datum) convertToString(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		s   string
		err error
	)
	switch d.k {
	case KindInt64:
		s = strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		s = strconv.FormatUint(d.GetUint64(), 10)
	case KindFloat32:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 32)
	case KindFloat64:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case KindString, KindBytes:
		fromBinary := d.Collation() == charset.CollationBin
		toBinary := target.GetCharset() == charset.CharsetBin
		if fromBinary && toBinary {
			s = d.GetString()
		} else if fromBinary {
			s, err = d.GetBinaryStringDecoded(ctx.Flags(), target.GetCharset())
		} else if toBinary {
			s = d.GetBinaryStringEncoded()
		} else {
			s, err = d.GetStringWithCheck(ctx.Flags(), target.GetCharset())
		}
	case KindMysqlTime:
		s = d.GetMysqlTime().String()
	case KindMysqlDuration:
		s = d.GetMysqlDuration().String()
	case KindMysqlDecimal:
		s = d.GetMysqlDecimal().String()
	case KindMysqlEnum:
		s = d.GetMysqlEnum().String()
	case KindMysqlSet:
		s = d.GetMysqlSet().String()
	case KindBinaryLiteral:
		s, err = d.GetBinaryStringDecoded(ctx.Flags(), target.GetCharset())
	case KindMysqlBit:
		// https://github.com/pingcap/tidb/issues/31124.
		// Consider converting to uint first.
		val, err := d.GetBinaryLiteral().ToInt(ctx)
		// The length of BIT is limited to 64, so this function will never fail / truncated.
		intest.AssertNoError(err)
		if err != nil {
			s = d.GetBinaryLiteral().ToString()
		} else {
			s = strconv.FormatUint(val, 10)
		}
	case KindMysqlJSON:
		s = d.GetMysqlJSON().String()
	case KindVectorFloat32:
		s = d.GetVectorFloat32().String()
	default:
		return invalidConv(d, target.GetType())
	}
	if err == nil {
		s, err = ProduceStrWithSpecifiedTp(s, target, ctx, true)
	}
	ret.SetString(s, target.GetCollate())
	if target.GetCharset() == charset.CharsetBin {
		ret.k = KindBytes
	}
	return ret, errors.Trace(err)
}

// ProduceStrWithSpecifiedTp produces a new string according to `flen` and `chs`. Param `padZero` indicates
// whether we should pad `\0` for `binary(flen)` type.
func ProduceStrWithSpecifiedTp(s string, tp *FieldType, ctx Context, padZero bool) (_ string, err error) {
	flen, chs := tp.GetFlen(), tp.GetCharset()
	if flen >= 0 {
		// overflowed stores the part of the string that is out of the length constraint, it is later checked to see if the
		// overflowed part is all whitespaces
		var overflowed string
		var characterLen int
		var needCalculateLen bool

		// For  mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob(defined in tidb)
		// and tinytext, text, mediumtext, longtext(not explicitly defined in tidb, corresponding to blob(s) in tidb) flen is the store length limit regardless of charset.
		if chs != charset.CharsetBin {
			switch tp.GetType() {
			case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				characterLen = len(s)
				// We need to truncate the value to a proper length that contains complete word.
				if characterLen > flen {
					var r rune
					var size int
					var tempStr string
					var truncateLen int
					// Find the truncate position.
					for truncateLen = flen; truncateLen > 0; truncateLen-- {
						tempStr = truncateStr(s, truncateLen)
						r, size = utf8.DecodeLastRuneInString(tempStr)
						if r == utf8.RuneError && size == 0 {
							// Empty string
							continue
						} else if r == utf8.RuneError && size == 1 {
							// Invalid string
							continue
						}
						// Get the truncate position
						break
					}
					overflowed = s[truncateLen:]
					s = truncateStr(s, truncateLen)
				}
			default:
				if len(s) > flen {
					characterLen = utf8.RuneCountInString(s)
					if characterLen > flen {
						// 1. If len(s) is 0 and flen is 0, truncateLen will be 0, don't truncate s.
						//    CREATE TABLE t (a char(0));
						//    INSERT INTO t VALUES (``);
						// 2. If len(s) is 10 and flen is 0, truncateLen will be 0 too, but we still need to truncate s.
						//    SELECT 1, CAST(1234 AS CHAR(0));
						// So truncateLen is not a suitable variable to determine to do truncate or not.
						var runeCount int
						var truncateLen int
						for i := range s {
							if runeCount == flen {
								truncateLen = i
								break
							}
							runeCount++
						}
						overflowed = s[truncateLen:]
						s = truncateStr(s, truncateLen)
					} else {
						needCalculateLen = true
					}
				}
			}
		} else if len(s) > flen {
			characterLen = len(s)
			overflowed = s[flen:]
			s = truncateStr(s, flen)
		}

		if len(overflowed) != 0 {
			trimmed := strings.TrimRight(overflowed, " \t\n\r")
			if len(trimmed) == 0 && !IsBinaryStr(tp) && IsTypeChar(tp.GetType()) {
				if tp.GetType() == mysql.TypeVarchar {
					if needCalculateLen {
						characterLen = utf8.RuneCountInString(s)
					}
					ctx.AppendWarning(ErrTruncated.FastGen("Data truncated, field len %d, data len %d", flen, characterLen))
				}
			} else {
				if needCalculateLen {
					characterLen = utf8.RuneCountInString(s)
				}
				err = ErrDataTooLong.FastGen("Data Too Long, field len %d, data len %d", flen, characterLen)
			}
		}

		if tp.GetType() == mysql.TypeString && IsBinaryStr(tp) && len(s) < flen && padZero {
			padding := make([]byte, flen-len(s))
			s = string(append([]byte(s), padding...))
		}
	}
	return s, errors.Trace(ctx.HandleTruncate(err))
}

func (d *Datum) convertToInt(ctx Context, target *FieldType) (Datum, error) {
	i64, err := d.toSignedInteger(ctx, target.GetType())
	return NewIntDatum(i64), errors.Trace(err)
}

func (d *Datum) convertToUint(ctx Context, target *FieldType) (Datum, error) {
	tp := target.GetType()
	upperBound := IntegerUnsignedUpperBound(tp)
	var (
		val uint64
		err error
		ret Datum
	)
	switch d.k {
	case KindInt64:
		val, err = ConvertIntToUint(ctx.Flags(), d.GetInt64(), upperBound, tp)
	case KindUint64:
		val, err = ConvertUintToUint(d.GetUint64(), upperBound, tp)
	case KindFloat32, KindFloat64:
		val, err = ConvertFloatToUint(ctx.Flags(), d.GetFloat64(), upperBound, tp)
	case KindString, KindBytes:
		var err1 error
		val, err1 = StrToUint(ctx, d.GetString(), false)
		val, err = ConvertUintToUint(val, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlTime:
		dec := d.GetMysqlTime().ToNumber()
		err = dec.Round(dec, 0, ModeHalfUp)
		ival, err1 := dec.ToInt()
		if err == nil {
			err = err1
		}
		val, err1 = ConvertIntToUint(ctx.Flags(), ival, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlDuration:
		dec := d.GetMysqlDuration().ToNumber()
		err = dec.Round(dec, 0, ModeHalfUp)
		var err1 error
		val, err1 = ConvertDecimalToUint(dec, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlDecimal:
		val, err = ConvertDecimalToUint(d.GetMysqlDecimal(), upperBound, tp)
	case KindMysqlEnum:
		val, err = ConvertFloatToUint(ctx.Flags(), d.GetMysqlEnum().ToNumber(), upperBound, tp)
	case KindMysqlSet:
		val, err = ConvertFloatToUint(ctx.Flags(), d.GetMysqlSet().ToNumber(), upperBound, tp)
	case KindBinaryLiteral, KindMysqlBit:
		val, err = d.GetBinaryLiteral().ToInt(ctx)
		if err == nil {
			val, err = ConvertUintToUint(val, upperBound, tp)
		}
	case KindMysqlJSON:
		var i64 int64
		i64, err = ConvertJSONToInt(ctx, d.GetMysqlJSON(), true, tp)
		val = uint64(i64)
	default:
		return invalidConv(d, target.GetType())
	}
	ret.SetUint64(val)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlTimestamp(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		t   Time
		err error
	)
	fsp := DefaultFsp
	if target.GetDecimal() != UnspecifiedLength {
		fsp = target.GetDecimal()
	}
	switch d.k {
	case KindMysqlTime:
		t, err = d.GetMysqlTime().Convert(ctx, target.GetType())
		if err != nil {
			// t might be an invalid Timestamp, but should still be comparable, since same representation (KindMysqlTime)
			ret.SetMysqlTime(t)
			return ret, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimestampStr, t.String()))
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(ctx, mysql.TypeTimestamp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindString, KindBytes:
		t, err = ParseTime(ctx, d.GetString(), mysql.TypeTimestamp, fsp)
	case KindInt64:
		t, err = ParseTimeFromNum(ctx, d.GetInt64(), mysql.TypeTimestamp, fsp)
	case KindMysqlDecimal:
		t, err = ParseTimeFromFloatString(ctx, d.GetMysqlDecimal().String(), mysql.TypeTimestamp, fsp)
	case KindMysqlJSON:
		j := d.GetMysqlJSON()
		var s string
		s, err = j.Unquote()
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, err
		}
		t, err = ParseTime(ctx, s, mysql.TypeTimestamp, fsp)
	default:
		return invalidConv(d, mysql.TypeTimestamp)
	}
	t.SetType(mysql.TypeTimestamp)
	ret.SetMysqlTime(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlTime(ctx Context, target *FieldType) (Datum, error) {
	tp := target.GetType()
	fsp := DefaultFsp
	if target.GetDecimal() != UnspecifiedLength {
		fsp = target.GetDecimal()
	}
	var (
		ret Datum
		t   Time
		err error
	)
	switch d.k {
	case KindMysqlTime:
		t, err = d.GetMysqlTime().Convert(ctx, tp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(ctx, tp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindMysqlDecimal:
		t, err = ParseTimeFromFloatString(ctx, d.GetMysqlDecimal().String(), tp, fsp)
	case KindString, KindBytes:
		t, err = ParseTime(ctx, d.GetString(), tp, fsp)
	case KindInt64:
		t, err = ParseTimeFromNum(ctx, d.GetInt64(), tp, fsp)
	case KindUint64:
		intOverflow64 := d.GetInt64() < 0
		if intOverflow64 {
			uNum := strconv.FormatUint(d.GetUint64(), 10)
			t, err = ZeroDate, ErrWrongValue.GenWithStackByArgs(TimeStr, uNum)
		} else {
			t, err = ParseTimeFromNum(ctx, d.GetInt64(), tp, fsp)
		}
	case KindMysqlJSON:
		j := d.GetMysqlJSON()
		var s string
		s, err = j.Unquote()
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, err
		}
		t, err = ParseTime(ctx, s, tp, fsp)
	default:
		return invalidConv(d, tp)
	}
	if tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		t.SetCoreTime(FromDate(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0))
	}
	ret.SetMysqlTime(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlDuration(typeCtx Context, target *FieldType) (Datum, error) {
	tp := target.GetType()
	fsp := DefaultFsp
	if target.GetDecimal() != UnspecifiedLength {
		fsp = target.GetDecimal()
	}
	var ret Datum
	switch d.k {
	case KindMysqlTime:
		dur, err := d.GetMysqlTime().ConvertToDuration()
		if err != nil {
			ret.SetMysqlDuration(dur)
			return ret, errors.Trace(err)
		}
		dur, err = dur.RoundFrac(fsp, typeCtx.Location())
		ret.SetMysqlDuration(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindMysqlDuration:
		dur, err := d.GetMysqlDuration().RoundFrac(fsp, typeCtx.Location())
		ret.SetMysqlDuration(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindInt64, KindUint64, KindFloat32, KindFloat64, KindMysqlDecimal:
		// TODO: We need a ParseDurationFromNum to avoid the cost of converting a num to string.
		timeStr, err := d.ToString()
		if err != nil {
			return ret, errors.Trace(err)
		}
		timeNum, err := d.ToInt64(typeCtx)
		if err != nil {
			return ret, errors.Trace(err)
		}
		// For huge numbers(>'0001-00-00 00-00-00') try full DATETIME in ParseDuration.
		if timeNum > MaxDuration && timeNum < 10000000000 {
			// mysql return max in no strict sql mode.
			ret.SetMysqlDuration(Duration{Duration: MaxTime, Fsp: 0})
			return ret, ErrWrongValue.GenWithStackByArgs(TimeStr, timeStr)
		}
		if timeNum < -MaxDuration {
			return ret, ErrWrongValue.GenWithStackByArgs(TimeStr, timeStr)
		}
		t, _, err := ParseDuration(typeCtx, timeStr, fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindString, KindBytes:
		t, _, err := ParseDuration(typeCtx, d.GetString(), fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindMysqlJSON:
		j := d.GetMysqlJSON()
		s, err := j.Unquote()
		if err != nil {
			return ret, errors.Trace(err)
		}
		t, _, err := ParseDuration(typeCtx, s, fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	default:
		return invalidConv(d, tp)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlDecimal(ctx Context, target *FieldType) (Datum, error) {
	var ret Datum
	ret.SetLength(target.GetFlen())
	ret.SetFrac(target.GetDecimal())
	var dec = &MyDecimal{}
	var err error
	switch d.k {
	case KindInt64:
		dec.FromInt(d.GetInt64())
	case KindUint64:
		dec.FromUint(d.GetUint64())
	case KindFloat32, KindFloat64:
		err = dec.FromFloat64(d.GetFloat64())
	case KindString, KindBytes:
		err = dec.FromString(d.GetBytes())
	case KindMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case KindMysqlTime:
		dec = d.GetMysqlTime().ToNumber()
	case KindMysqlDuration:
		dec = d.GetMysqlDuration().ToNumber()
	case KindMysqlEnum:
		err = dec.FromFloat64(d.GetMysqlEnum().ToNumber())
	case KindMysqlSet:
		err = dec.FromFloat64(d.GetMysqlSet().ToNumber())
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		err = err1
		dec.FromUint(val)
	case KindMysqlJSON:
		f, err1 := ConvertJSONToDecimal(ctx, d.GetMysqlJSON())
		if err1 != nil {
			return ret, errors.Trace(err1)
		}
		dec = f
	default:
		return invalidConv(d, target.GetType())
	}
	dec1, err1 := ProduceDecWithSpecifiedTp(ctx, dec, target)
	// If there is a error, dec1 may be nil.
	if dec1 != nil {
		dec = dec1
	}
	if err == nil && err1 != nil {
		err = err1
	}
	if dec.negative && mysql.HasUnsignedFlag(target.GetFlag()) {
		*dec = zeroMyDecimal
		if err == nil {
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", target.GetFlen(), target.GetDecimal()))
		}
	}
	ret.SetMysqlDecimal(dec)
	return ret, err
}

// ProduceDecWithSpecifiedTp produces a new decimal according to `flen` and `decimal`.
func ProduceDecWithSpecifiedTp(ctx Context, dec *MyDecimal, tp *FieldType) (_ *MyDecimal, err error) {
	flen, decimal := tp.GetFlen(), tp.GetDecimal()
	if flen != UnspecifiedLength && decimal != UnspecifiedLength {
		if flen < decimal {
			return nil, ErrMBiggerThanD.GenWithStackByArgs("")
		}

		var old *MyDecimal
		if int(dec.digitsFrac) > decimal {
			old = new(MyDecimal)
			*old = *dec
		}
		if int(dec.digitsFrac) != decimal {
			// Error doesn't matter because the following code will check the new decimal
			// and set error if any.
			_ = dec.Round(dec, decimal, ModeHalfUp)
		}

		_, digitsInt := dec.removeLeadingZeros()
		// After rounding decimal, the new decimal may have a longer integer length which may be longer than expected.
		// So the check of integer length must be after rounding.
		// E.g. "99.9999", flen 5, decimal 3, Round("99.9999", 3, ModelHalfUp) -> "100.000".
		if flen-decimal < digitsInt {
			// Integer length is longer, choose the max or min decimal.
			dec = NewMaxOrMinDec(dec.IsNegative(), flen, decimal)
			// select cast(111 as decimal(1)) causes a warning in MySQL.
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", flen, decimal))
		} else if old != nil && dec.Compare(old) != 0 {
			ctx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs("DECIMAL", old))
		}
	}

	unsigned := mysql.HasUnsignedFlag(tp.GetFlag())
	if unsigned && dec.IsNegative() {
		dec = dec.FromUint(0)
	}
	return dec, err
}

// ConvertToMysqlYear converts a datum to MySQLYear.
func (d *Datum) ConvertToMysqlYear(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret    Datum
		y      int64
		err    error
		adjust bool
	)
	switch d.k {
	case KindString, KindBytes:
		s := d.GetString()
		trimS := strings.TrimSpace(s)
		y, err = StrToInt(ctx, trimS, false)
		if err != nil {
			ret.SetInt64(0)
			return ret, errors.Trace(err)
		}
		// condition:
		// parsed to 0, not a string of length 4, the first valid char is a 0 digit
		if len(s) != 4 && y == 0 && strings.HasPrefix(trimS, "0") {
			adjust = true
		}
	case KindMysqlTime:
		y = int64(d.GetMysqlTime().Year())
	case KindMysqlDuration:
		y, err = d.GetMysqlDuration().ConvertToYear(ctx)
	case KindMysqlJSON:
		y, err = ConvertJSONToInt64(ctx, d.GetMysqlJSON(), false)
		if err != nil {
			ret.SetInt64(0)
			return ret, errors.Trace(err)
		}
	default:
		ret, err = d.convertToInt(ctx, NewFieldType(mysql.TypeLonglong))
		if err != nil {
			_, err = invalidConv(d, target.GetType())
			ret.SetInt64(0)
			return ret, err
		}
		y = ret.GetInt64()
	}

	// Duration has been adjusted in `Duration.ConvertToYear()`
	if d.k != KindMysqlDuration {
		y, err = AdjustYear(y, adjust)
	}
	ret.SetInt64(y)
	return ret, errors.Trace(err)
}

func (d *Datum) convertToMysqlBit(ctx Context, target *FieldType) (Datum, error) {
	var ret Datum
	var uintValue uint64
	var err error
	switch d.k {
	case KindString, KindBytes:
		uintValue, err = BinaryLiteral(d.b).ToInt(ctx)
	case KindInt64:
		// if input kind is int64 (signed), when trans to bit, we need to treat it as unsigned
		d.k = KindUint64
		fallthrough
	default:
		uintDatum, err1 := d.convertToUint(ctx, target)
		uintValue, err = uintDatum.GetUint64(), err1
	}
	// Avoid byte size panic, never goto this branch.
	if target.GetFlen() <= 0 || target.GetFlen() >= 128 {
		return Datum{}, errors.Trace(ErrDataTooLong.GenWithStack("Data Too Long, field len %d", target.GetFlen()))
	}
	if target.GetFlen() < 64 && uintValue >= 1<<(uint64(target.GetFlen())) {
		uintValue = (1 << (uint64(target.GetFlen()))) - 1
		err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d", target.GetFlen())
	}
	byteSize := (target.GetFlen() + 7) >> 3
	ret.SetMysqlBit(NewBinaryLiteralFromUint(uintValue, byteSize))
	return ret, errors.Trace(err)
}

func (d *Datum) convertToMysqlEnum(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		e   Enum
		err error
	)
	switch d.k {
	case KindString, KindBytes, KindBinaryLiteral:
		e, err = ParseEnum(target.GetElems(), d.GetString(), target.GetCollate())
	case KindMysqlEnum:
		if d.i == 0 {
			// MySQL enum zero value has an empty string name(Enum{Name: '', Value: 0}). It is
			// different from the normal enum string value(Enum{Name: '', Value: n}, n > 0).
			e = Enum{}
		} else {
			e, err = ParseEnum(target.GetElems(), d.GetMysqlEnum().Name, target.GetCollate())
		}
	case KindMysqlSet:
		e, err = ParseEnum(target.GetElems(), d.GetMysqlSet().Name, target.GetCollate())
	default:
		var uintDatum Datum
		uintDatum, err = d.convertToUint(ctx, target)
		if err == nil {
			e, err = ParseEnumValue(target.GetElems(), uintDatum.GetUint64())
		} else {
			err = errors.Wrap(ErrTruncated, "convert to MySQL enum failed: "+err.Error())
		}
	}
	ret.SetMysqlEnum(e, target.GetCollate())
	return ret, err
}

func (d *Datum) convertToMysqlSet(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		s   Set
		err error
	)
	switch d.k {
	case KindString, KindBytes, KindBinaryLiteral:
		s, err = ParseSet(target.GetElems(), d.GetString(), target.GetCollate())
	case KindMysqlEnum:
		s, err = ParseSet(target.GetElems(), d.GetMysqlEnum().Name, target.GetCollate())
	case KindMysqlSet:
		s, err = ParseSet(target.GetElems(), d.GetMysqlSet().Name, target.GetCollate())
	case KindVectorFloat32:
		return invalidConv(d, mysql.TypeSet)
	default:
		var uintDatum Datum
		uintDatum, err = d.convertToUint(ctx, target)
		if err == nil {
			s, err = ParseSetValue(target.GetElems(), uintDatum.GetUint64())
		}
	}
	if err != nil {
		err = errors.Wrap(ErrTruncated, "convert to MySQL set failed: "+err.Error())
	}
	ret.SetMysqlSet(s, target.GetCollate())
	return ret, err
}

func (d *Datum) convertToMysqlJSON(_ *FieldType) (ret Datum, err error) {
	switch d.k {
	case KindString, KindBytes:
		var j BinaryJSON
		if j, err = ParseBinaryJSONFromString(d.GetString()); err == nil {
			ret.SetMysqlJSON(j)
		}
	case KindMysqlSet, KindMysqlEnum:
		var j BinaryJSON
		var s string
		if s, err = d.ToString(); err == nil {
			if j, err = ParseBinaryJSONFromString(s); err == nil {
				ret.SetMysqlJSON(j)
			}
		}
	case KindInt64:
		i64 := d.GetInt64()
		ret.SetMysqlJSON(CreateBinaryJSON(i64))
	case KindUint64:
		u64 := d.GetUint64()
		ret.SetMysqlJSON(CreateBinaryJSON(u64))
	case KindFloat32, KindFloat64:
		f64 := d.GetFloat64()
		ret.SetMysqlJSON(CreateBinaryJSON(f64))
	case KindMysqlDecimal:
		var f64 float64
		if f64, err = d.GetMysqlDecimal().ToFloat64(); err == nil {
			ret.SetMysqlJSON(CreateBinaryJSON(f64))
		}
	case KindMysqlJSON:
		ret = *d
	case KindMysqlTime:
		tm := d.GetMysqlTime()
		ret.SetMysqlJSON(CreateBinaryJSON(tm))
	case KindMysqlDuration:
		dur := d.GetMysqlDuration()
		ret.SetMysqlJSON(CreateBinaryJSON(dur))
	case KindBinaryLiteral:
		err = ErrInvalidJSONCharset.GenWithStackByArgs(charset.CharsetBin)
	default:
		var s string
		if s, err = d.ToString(); err == nil {
			// TODO: fix precision of MysqlTime. For example,
			// On MySQL 5.7 CAST(NOW() AS JSON) -> "2011-11-11 11:11:11.111111",
			// But now we can only return "2011-11-11 11:11:11".
			ret.SetMysqlJSON(CreateBinaryJSON(s))
		}
	}
	return ret, errors.Trace(err)
}

func (d *Datum) convertToVectorFloat32(_ Context, target *FieldType) (ret Datum, err error) {
	switch d.k {
	case KindVectorFloat32:
		v := d.GetVectorFloat32()
		if err = v.CheckDimsFitColumn(target.GetFlen()); err != nil {
			return ret, errors.Trace(err)
		}
		ret = *d
	case KindString, KindBytes:
		var v VectorFloat32
		if v, err = ParseVectorFloat32(d.GetString()); err != nil {
			return ret, errors.Trace(err)
		}
		if err = v.CheckDimsFitColumn(target.GetFlen()); err != nil {
			return ret, errors.Trace(err)
		}
		ret.SetVectorFloat32(v)
	default:
		return invalidConv(d, mysql.TypeTiDBVectorFloat32)
	}
	return ret, errors.Trace(err)
}

// ToBool converts to a bool.
// We will use 1 for true, and 0 for false.
func (d *Datum) ToBool(ctx Context) (int64, error) {
	var err error
	isZero := false
	switch d.Kind() {
	case KindInt64:
		isZero = d.GetInt64() == 0
	case KindUint64:
		isZero = d.GetUint64() == 0
	case KindFloat32:
		isZero = d.GetFloat64() == 0
	case KindFloat64:
		isZero = d.GetFloat64() == 0
	case KindString, KindBytes:
		iVal, err1 := StrToFloat(ctx, d.GetString(), false)
		isZero, err = iVal == 0, err1

	case KindMysqlTime:
		isZero = d.GetMysqlTime().IsZero()
	case KindMysqlDuration:
		isZero = d.GetMysqlDuration().Duration == 0
	case KindMysqlDecimal:
		isZero = d.GetMysqlDecimal().IsZero()
	case KindMysqlEnum:
		isZero = d.GetMysqlEnum().ToNumber() == 0
	case KindMysqlSet:
		isZero = d.GetMysqlSet().ToNumber() == 0
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		isZero, err = val == 0, err1
	case KindMysqlJSON:
		val := d.GetMysqlJSON()
		isZero = val.IsZero()
	case KindVectorFloat32:
		isZero = d.GetVectorFloat32().IsZeroValue()
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to bool", d.GetValue(), d.GetValue())
	}
	var ret int64
	if isZero {
		ret = 0
	} else {
		ret = 1
	}
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

// ConvertDatumToDecimal converts datum to decimal.
func ConvertDatumToDecimal(ctx Context, d Datum) (*MyDecimal, error) {
	dec := new(MyDecimal)
	var err error
	switch d.Kind() {
	case KindInt64:
		dec.FromInt(d.GetInt64())
	case KindUint64:
		dec.FromUint(d.GetUint64())
	case KindFloat32:
		err = dec.FromFloat64(float64(d.GetFloat32()))
	case KindFloat64:
		err = dec.FromFloat64(d.GetFloat64())
	case KindString:
		err = ctx.HandleTruncate(dec.FromString(d.GetBytes()))
	case KindMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case KindMysqlEnum:
		dec.FromUint(d.GetMysqlEnum().Value)
	case KindMysqlSet:
		dec.FromUint(d.GetMysqlSet().Value)
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		dec.FromUint(val)
		err = err1
	case KindMysqlJSON:
		f, err1 := ConvertJSONToDecimal(ctx, d.GetMysqlJSON())
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		dec = f
	default:
		err = errors.Errorf("can't convert %v to decimal", d.GetValue())
	}
	return dec, errors.Trace(err)
}

// ToDecimal converts to a decimal.
func (d *Datum) ToDecimal(ctx Context) (*MyDecimal, error) {
	switch d.Kind() {
	case KindMysqlTime:
		return d.GetMysqlTime().ToNumber(), nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().ToNumber(), nil
	default:
		return ConvertDatumToDecimal(ctx, *d)
	}
}

// ToInt64 converts to a int64.
func (d *Datum) ToInt64(ctx Context) (int64, error) {
	if d.Kind() == KindMysqlBit {
		uintVal, err := d.GetBinaryLiteral().ToInt(ctx)
		return int64(uintVal), err
	}
	return d.toSignedInteger(ctx, mysql.TypeLonglong)
}

func (d *Datum) toSignedInteger(ctx Context, tp byte) (int64, error) {
	lowerBound := IntegerSignedLowerBound(tp)
	upperBound := IntegerSignedUpperBound(tp)
	switch d.Kind() {
	case KindInt64:
		return ConvertIntToInt(d.GetInt64(), lowerBound, upperBound, tp)
	case KindUint64:
		return ConvertUintToInt(d.GetUint64(), upperBound, tp)
	case KindFloat32:
		return ConvertFloatToInt(float64(d.GetFloat32()), lowerBound, upperBound, tp)
	case KindFloat64:
		return ConvertFloatToInt(d.GetFloat64(), lowerBound, upperBound, tp)
	case KindString, KindBytes:
		iVal, err := StrToInt(ctx, d.GetString(), false)
		iVal, err2 := ConvertIntToInt(iVal, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return iVal, errors.Trace(err)
	case KindMysqlTime:
		// 2011-11-10 11:11:11.999999 -> 20111110111112
		// 2011-11-10 11:59:59.999999 -> 20111110120000
		t, err := d.GetMysqlTime().RoundFrac(ctx, DefaultFsp)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := t.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case KindMysqlDuration:
		// 11:11:11.999999 -> 111112
		// 11:59:59.999999 -> 120000
		dur, err := d.GetMysqlDuration().RoundFrac(DefaultFsp, ctx.Location())
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := dur.ToNumber().ToInt()
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case KindMysqlDecimal:
		var to MyDecimal
		err := d.GetMysqlDecimal().Round(&to, 0, ModeHalfUp)
		ival, err1 := to.ToInt()
		if err == nil {
			err = err1
		}
		ival, err2 := ConvertIntToInt(ival, lowerBound, upperBound, tp)
		if err == nil {
			err = err2
		}
		return ival, errors.Trace(err)
	case KindMysqlEnum:
		fval := d.GetMysqlEnum().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case KindMysqlSet:
		fval := d.GetMysqlSet().ToNumber()
		return ConvertFloatToInt(fval, lowerBound, upperBound, tp)
	case KindMysqlJSON:
		return ConvertJSONToInt(ctx, d.GetMysqlJSON(), false, tp)
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		ival, err := ConvertUintToInt(val, upperBound, tp)
		return ival, errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to int64", d.GetValue(), d.GetValue())
	}
}

// ToFloat64 converts to a float64
func (d *Datum) ToFloat64(ctx Context) (float64, error) {
	switch d.Kind() {
	case KindInt64:
		return float64(d.GetInt64()), nil
	case KindUint64:
		return float64(d.GetUint64()), nil
	case KindFloat32:
		return float64(d.GetFloat32()), nil
	case KindFloat64:
		return d.GetFloat64(), nil
	case KindString:
		return StrToFloat(ctx, d.GetString(), false)
	case KindBytes:
		return StrToFloat(ctx, string(d.GetBytes()), false)
	case KindMysqlTime:
		f, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlDuration:
		f, err := d.GetMysqlDuration().ToNumber().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlDecimal:
		f, err := d.GetMysqlDecimal().ToFloat64()
		return f, errors.Trace(err)
	case KindMysqlEnum:
		return d.GetMysqlEnum().ToNumber(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().ToNumber(), nil
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral().ToInt(ctx)
		return float64(val), errors.Trace(err)
	case KindMysqlJSON:
		f, err := ConvertJSONToFloat(ctx, d.GetMysqlJSON())
		return f, errors.Trace(err)
	default:
		return 0, errors.Errorf("cannot convert %v(type %T) to float64", d.GetValue(), d.GetValue())
	}
}

// ToString gets the string representation of the datum.
func (d *Datum) ToString() (string, error) {
	switch d.Kind() {
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10), nil
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10), nil
	case KindFloat32:
		return strconv.FormatFloat(float64(d.GetFloat32()), 'f', -1, 32), nil
	case KindFloat64:
		return strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64), nil
	case KindString:
		return d.GetString(), nil
	case KindBytes:
		return d.GetString(), nil
	case KindMysqlTime:
		return d.GetMysqlTime().String(), nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().String(), nil
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().String(), nil
	case KindMysqlEnum:
		return d.GetMysqlEnum().String(), nil
	case KindMysqlSet:
		return d.GetMysqlSet().String(), nil
	case KindMysqlJSON:
		return d.GetMysqlJSON().String(), nil
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral().ToString(), nil
	case KindVectorFloat32:
		return d.GetVectorFloat32().String(), nil
	case KindNull:
		return "", nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", d.GetValue(), d.GetValue())
	}
}

// ToBytes gets the bytes representation of the datum.
func (d *Datum) ToBytes() ([]byte, error) {
	switch d.k {
	case KindString, KindBytes:
		return d.GetBytes(), nil
	default:
		str, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return []byte(str), nil
	}
}

// ToHashKey gets the bytes representation of the datum considering collation.
func (d *Datum) ToHashKey() ([]byte, error) {
	switch d.k {
	case KindString, KindBytes:
		return collate.GetCollator(d.Collation()).Key(d.GetString()), nil
	default:
		str, err := d.ToString()
		if err != nil {
			return nil, errors.Trace(err)
		}
		return collate.GetCollator(d.Collation()).Key(str), nil
	}
}

// ToMysqlJSON is similar to convertToMysqlJSON, except the
// latter parses from string, but the former uses it as primitive.
func (d *Datum) ToMysqlJSON() (j BinaryJSON, err error) {
	var in any
	switch d.Kind() {
	case KindMysqlJSON:
		j = d.GetMysqlJSON()
		return
	case KindInt64:
		in = d.GetInt64()
	case KindUint64:
		in = d.GetUint64()
	case KindFloat32, KindFloat64:
		in = d.GetFloat64()
	case KindMysqlDecimal:
		in, err = d.GetMysqlDecimal().ToFloat64()
	case KindString, KindBytes:
		in = d.GetString()
	case KindBinaryLiteral, KindMysqlBit:
		in = d.GetBinaryLiteral().ToString()
	case KindNull:
		in = nil
	case KindMysqlTime:
		in = d.GetMysqlTime()
	case KindMysqlDuration:
		in = d.GetMysqlDuration()
	default:
		in, err = d.ToString()
	}
	if err != nil {
		err = errors.Trace(err)
		return
	}
	j = CreateBinaryJSON(in)
	return
}

// MemUsage gets the memory usage of datum.
func (d *Datum) MemUsage() (sum int64) {
	// d.x is not considered now since MemUsage is now only used by analyze samples which is bytesDatum
	return EmptyDatumSize + int64(cap(d.b)) + int64(len(d.collation))
}

type jsonDatum struct {
	K         byte       `json:"k"`
	Decimal   uint16     `json:"decimal,omitempty"`
	Length    uint32     `json:"length,omitempty"`
	I         int64      `json:"i,omitempty"`
	Collation string     `json:"collation,omitempty"`
	B         []byte     `json:"b,omitempty"`
	Time      Time       `json:"time,omitempty"`
	MyDecimal *MyDecimal `json:"mydecimal,omitempty"`
}

// MarshalJSON implements Marshaler.MarshalJSON interface.
func (d *Datum) MarshalJSON() ([]byte, error) {
	jd := &jsonDatum{
		K:         d.k,
		Decimal:   d.decimal,
		Length:    d.length,
		I:         d.i,
		Collation: d.collation,
		B:         d.b,
	}
	switch d.k {
	case KindMysqlTime:
		jd.Time = d.GetMysqlTime()
	case KindMysqlDecimal:
		jd.MyDecimal = d.GetMysqlDecimal()
	default:
		if d.x != nil {
			return nil, fmt.Errorf("unsupported type: %d", d.k)
		}
	}
	return gjson.Marshal(jd)
}

// UnmarshalJSON implements Unmarshaler.UnmarshalJSON interface.
func (d *Datum) UnmarshalJSON(data []byte) error {
	var jd jsonDatum
	if err := gjson.Unmarshal(data, &jd); err != nil {
		return err
	}
	d.k = jd.K
	d.decimal = jd.Decimal
	d.length = jd.Length
	d.i = jd.I
	d.collation = jd.Collation
	d.b = jd.B

	switch jd.K {
	case KindMysqlTime:
		d.SetMysqlTime(jd.Time)
	case KindMysqlDecimal:
		d.SetMysqlDecimal(jd.MyDecimal)
	}
	return nil
}

func invalidConv(d *Datum, tp byte) (Datum, error) {
	return Datum{}, errors.Errorf("cannot convert datum from %s to type %s", KindStr(d.Kind()), TypeStr(tp))
}

// NewDatum creates a new Datum from an interface{}.
func NewDatum(in any) (d Datum) {
	switch x := in.(type) {
	case []any:
		d.SetValueWithDefaultCollation(MakeDatums(x...))
	default:
		d.SetValueWithDefaultCollation(in)
	}
	return d
}

// NewIntDatum creates a new Datum from an int64 value.
func NewIntDatum(i int64) (d Datum) {
	d.SetInt64(i)
	return d
}

// NewUintDatum creates a new Datum from an uint64 value.
func NewUintDatum(i uint64) (d Datum) {
	d.SetUint64(i)
	return d
}

// NewBytesDatum creates a new Datum from a byte slice.
func NewBytesDatum(b []byte) (d Datum) {
	d.SetBytes(b)
	return d
}

// NewStringDatum creates a new Datum from a string.
func NewStringDatum(s string) (d Datum) {
	d.SetString(s, mysql.DefaultCollationName)
	return d
}

// NewCollationStringDatum creates a new Datum from a string with collation.
func NewCollationStringDatum(s string, collation string) (d Datum) {
	d.SetString(s, collation)
	return d
}

// NewFloat64Datum creates a new Datum from a float64 value.
func NewFloat64Datum(f float64) (d Datum) {
	d.SetFloat64(f)
	return d
}

// NewFloat32Datum creates a new Datum from a float32 value.
func NewFloat32Datum(f float32) (d Datum) {
	d.SetFloat32(f)
	return d
}

// NewDurationDatum creates a new Datum from a Duration value.
func NewDurationDatum(dur Duration) (d Datum) {
	d.SetMysqlDuration(dur)
	return d
}

// NewTimeDatum creates a new Time from a Time value.
func NewTimeDatum(t Time) (d Datum) {
	d.SetMysqlTime(t)
	return d
}

// NewDecimalDatum creates a new Datum from a MyDecimal value.
func NewDecimalDatum(dec *MyDecimal) (d Datum) {
	d.SetMysqlDecimal(dec)
	return d
}

// NewJSONDatum creates a new Datum from a BinaryJSON value
func NewJSONDatum(j BinaryJSON) (d Datum) {
	d.SetMysqlJSON(j)
	return d
}

// NewVectorFloat32Datum creates a new Datum from a VectorFloat32 value
func NewVectorFloat32Datum(v VectorFloat32) (d Datum) {
	d.SetVectorFloat32(v)
	return d
}

// NewBinaryLiteralDatum creates a new BinaryLiteral Datum for a BinaryLiteral value.
func NewBinaryLiteralDatum(b BinaryLiteral) (d Datum) {
	d.SetBinaryLiteral(b)
	return d
}

// NewMysqlBitDatum creates a new MysqlBit Datum for a BinaryLiteral value.
func NewMysqlBitDatum(b BinaryLiteral) (d Datum) {
	d.SetMysqlBit(b)
	return d
}

// NewMysqlEnumDatum creates a new MysqlEnum Datum for a Enum value.
func NewMysqlEnumDatum(e Enum) (d Datum) {
	d.SetMysqlEnum(e, mysql.DefaultCollationName)
	return d
}

// NewCollateMysqlEnumDatum create a new MysqlEnum Datum for a Enum value with collation information.
func NewCollateMysqlEnumDatum(e Enum, collation string) (d Datum) {
	d.SetMysqlEnum(e, collation)
	return d
}

// NewMysqlSetDatum creates a new MysqlSet Datum for a Enum value.
func NewMysqlSetDatum(e Set, collation string) (d Datum) {
	d.SetMysqlSet(e, collation)
	return d
}

// MakeDatums creates datum slice from interfaces.
func MakeDatums(args ...any) []Datum {
	datums := make([]Datum, len(args))
	for i, v := range args {
		datums[i] = NewDatum(v)
	}
	return datums
}

// MinNotNullDatum returns a datum represents minimum not null value.
func MinNotNullDatum() Datum {
	return Datum{k: KindMinNotNull}
}

// MaxValueDatum returns a datum represents max value.
func MaxValueDatum() Datum {
	return Datum{k: KindMaxValue}
}

// SortDatums sorts a slice of datum.
func SortDatums(ctx Context, datums []Datum) error {
	var err error
	slices.SortFunc(datums, func(a, b Datum) int {
		var cmp int
		cmp, err = a.Compare(ctx, &b, collate.GetCollator(b.Collation()))
		if err != nil {
			return 0
		}
		return cmp
	})
	if err != nil {
		err = errors.Trace(err)
	}
	return err
}

// Check if a string is considered printable
//
// Checks
// 1. Must be valid UTF-8
// 2. Must not contain control characters like NUL (0x0) and backspace (0x8)
func isPrintable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// DatumsToString converts several datums to formatted string.
func DatumsToString(datums []Datum, handleSpecialValue bool) (string, error) {
	return datumsToString(datums, handleSpecialValue, false)
}

// DatumsToStringSmart is like DatumsToString, but with smart detection of non-printable data
func DatumsToStringSmart(datums []Datum, handleSpecialValue bool) (string, error) {
	return datumsToString(datums, handleSpecialValue, true)
}

func datumsToString(datums []Datum, handleSpecialValue bool, binaryAsHex bool) (string, error) {
	n := len(datums)
	builder := &strings.Builder{}
	builder.Grow(8 * n)
	if n > 1 {
		builder.WriteString("(")
	}
	for i, datum := range datums {
		if i > 0 {
			builder.WriteString(", ")
		}
		if handleSpecialValue {
			switch datum.Kind() {
			case KindNull:
				builder.WriteString("NULL")
				continue
			case KindMinNotNull:
				builder.WriteString("-inf")
				continue
			case KindMaxValue:
				builder.WriteString("+inf")
				continue
			}
		}
		str, err := datum.ToString()
		if err != nil {
			return "", errors.Trace(err)
		}
		const logDatumLen = 2048
		originalLen := -1
		if len(str) > logDatumLen {
			originalLen = len(str)
			str = str[:logDatumLen]
		}
		if datum.Kind() == KindString {
			if !binaryAsHex || isPrintable(str) {
				builder.WriteString(`"`)
				builder.WriteString(str)
				builder.WriteString(`"`)
			} else {
				// Print as hex-literal instead
				fmt.Fprintf(builder, "0x%X", str)
			}
		} else {
			builder.WriteString(str)
		}
		if originalLen != -1 {
			builder.WriteString(" len(")
			builder.WriteString(strconv.Itoa(originalLen))
			builder.WriteString(")")
		}
	}
	if n > 1 {
		builder.WriteString(")")
	}
	return builder.String(), nil
}

// DatumsToStrNoErr converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
func DatumsToStrNoErr(datums []Datum) string {
	str, err := DatumsToString(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// DatumsToStrNoErrSmart converts some datums to a formatted string.
// If an error occurs, it will print a log instead of returning an error.
// It also enables detection of non-pritable arguments
func DatumsToStrNoErrSmart(datums []Datum) string {
	str, err := DatumsToStringSmart(datums, true)
	terror.Log(errors.Trace(err))
	return str
}

// CloneRow deep copies a Datum slice.
func CloneRow(dr []Datum) []Datum {
	c := make([]Datum, len(dr))
	for i, d := range dr {
		d.Copy(&c[i])
	}
	return c
}

// GetMaxValue returns the max value datum for each type.
func GetMaxValue(ft *FieldType) (maxVal Datum) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			maxVal.SetUint64(IntegerUnsignedUpperBound(ft.GetType()))
		} else {
			maxVal.SetInt64(IntegerSignedUpperBound(ft.GetType()))
		}
	case mysql.TypeFloat:
		maxVal.SetFloat32(float32(GetMaxFloat(ft.GetFlen(), ft.GetDecimal())))
	case mysql.TypeDouble:
		maxVal.SetFloat64(GetMaxFloat(ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		// codec.Encode KindMaxValue, to avoid import circle
		bytes := []byte{250}
		maxVal.SetString(string(bytes), ft.GetCollate())
	case mysql.TypeNewDecimal:
		maxVal.SetMysqlDecimal(NewMaxOrMinDec(false, ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeDuration:
		maxVal.SetMysqlDuration(Duration{Duration: MaxTime})
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		if ft.GetType() == mysql.TypeDate || ft.GetType() == mysql.TypeDatetime {
			maxVal.SetMysqlTime(NewTime(MaxDatetime, ft.GetType(), 0))
		} else {
			maxVal.SetMysqlTime(MaxTimestamp)
		}
	}
	return
}

// GetMinValue returns the min value datum for each type.
func GetMinValue(ft *FieldType) (minVal Datum) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			minVal.SetUint64(0)
		} else {
			minVal.SetInt64(IntegerSignedLowerBound(ft.GetType()))
		}
	case mysql.TypeFloat:
		minVal.SetFloat32(float32(-GetMaxFloat(ft.GetFlen(), ft.GetDecimal())))
	case mysql.TypeDouble:
		minVal.SetFloat64(-GetMaxFloat(ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		// codec.Encode KindMinNotNull, to avoid import circle
		bytes := []byte{1}
		minVal.SetString(string(bytes), ft.GetCollate())
	case mysql.TypeNewDecimal:
		minVal.SetMysqlDecimal(NewMaxOrMinDec(true, ft.GetFlen(), ft.GetDecimal()))
	case mysql.TypeDuration:
		minVal.SetMysqlDuration(Duration{Duration: MinTime})
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		if ft.GetType() == mysql.TypeDate || ft.GetType() == mysql.TypeDatetime {
			minVal.SetMysqlTime(NewTime(MinDatetime, ft.GetType(), 0))
		} else {
			minVal.SetMysqlTime(MinTimestamp)
		}
	}
	return
}

// RoundingType is used to indicate the rounding type for reversing evaluation.
type RoundingType uint8

const (
	// Ceiling means rounding up.
	Ceiling RoundingType = iota
	// Floor means rounding down.
	Floor
)

func getDatumBound(retType *FieldType, rType RoundingType) Datum {
	if rType == Ceiling {
		return GetMaxValue(retType)
	}
	return GetMinValue(retType)
}

// ChangeReverseResultByUpperLowerBound is for expression's reverse evaluation.
// Here is an example for what's effort for the function: CastRealAsInt(t.a),
//
//			if the type of column `t.a` is mysql.TypeDouble, and there is a row that t.a == MaxFloat64
//			then the cast function will arrive a result MaxInt64. But when we do the reverse evaluation,
//	     if the result is MaxInt64, and the rounding type is ceiling. Then we should get the MaxFloat64
//	     instead of float64(MaxInt64).
//
// Another example: cast(1.1 as signed) = 1,
//
//	when we get the answer 1, we can only reversely evaluate 1.0 as the column value. So in this
//	case, we should judge whether the rounding type are ceiling. If it is, then we should plus one for
//	1.0 and get the reverse result 2.0.
func ChangeReverseResultByUpperLowerBound(
	ctx Context,
	retType *FieldType,
	res Datum,
	rType RoundingType) (Datum, error) {
	d, err := res.ConvertTo(ctx, retType)
	if terror.ErrorEqual(err, ErrOverflow) {
		return d, nil
	}
	if err != nil {
		return d, err
	}
	resRetType := FieldType{}
	switch res.Kind() {
	case KindInt64:
		resRetType.SetType(mysql.TypeLonglong)
	case KindUint64:
		resRetType.SetType(mysql.TypeLonglong)
		resRetType.AddFlag(mysql.UnsignedFlag)
	case KindFloat32:
		resRetType.SetType(mysql.TypeFloat)
	case KindFloat64:
		resRetType.SetType(mysql.TypeDouble)
	case KindMysqlDecimal:
		resRetType.SetType(mysql.TypeNewDecimal)
		resRetType.SetFlenUnderLimit(int(res.GetMysqlDecimal().GetDigitsFrac() + res.GetMysqlDecimal().GetDigitsInt()))
		resRetType.SetDecimalUnderLimit(int(res.GetMysqlDecimal().GetDigitsInt()))
	}
	bound := getDatumBound(&resRetType, rType)
	cmp, err := d.Compare(ctx, &bound, collate.GetCollator(resRetType.GetCollate()))
	if err != nil {
		return d, err
	}
	if cmp == 0 {
		d = getDatumBound(retType, rType)
	} else if rType == Ceiling {
		switch retType.GetType() {
		case mysql.TypeShort:
			if mysql.HasUnsignedFlag(retType.GetFlag()) {
				if d.GetUint64() != math.MaxUint16 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt16 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case mysql.TypeLong:
			if mysql.HasUnsignedFlag(retType.GetFlag()) {
				if d.GetUint64() != math.MaxUint32 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt32 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case mysql.TypeLonglong:
			if mysql.HasUnsignedFlag(retType.GetFlag()) {
				if d.GetUint64() != math.MaxUint64 {
					d.SetUint64(d.GetUint64() + 1)
				}
			} else {
				if d.GetInt64() != math.MaxInt64 {
					d.SetInt64(d.GetInt64() + 1)
				}
			}
		case mysql.TypeFloat:
			if d.GetFloat32() != math.MaxFloat32 {
				d.SetFloat32(d.GetFloat32() + 1.0)
			}
		case mysql.TypeDouble:
			if d.GetFloat64() != math.MaxFloat64 {
				d.SetFloat64(d.GetFloat64() + 1.0)
			}
		case mysql.TypeNewDecimal:
			if d.GetMysqlDecimal().Compare(NewMaxOrMinDec(false, retType.GetFlen(), retType.GetDecimal())) != 0 {
				var decimalOne, newD MyDecimal
				one := decimalOne.FromInt(1)
				err = DecimalAdd(d.GetMysqlDecimal(), one, &newD)
				if err != nil {
					return d, err
				}
				d = NewDecimalDatum(&newD)
			}
		}
	}
	return d, nil
}

const (
	sizeOfEmptyDatum = int(unsafe.Sizeof(Datum{}))
	sizeOfMysqlTime  = int(unsafe.Sizeof(ZeroTime))
	sizeOfMyDecimal  = MyDecimalStructSize
)

// EstimatedMemUsage returns the estimated bytes consumed of a one-dimensional
// or two-dimensional datum array.
func EstimatedMemUsage(array []Datum, numOfRows int) int64 {
	if numOfRows == 0 {
		return 0
	}
	var bytesConsumed int64
	for _, d := range array {
		bytesConsumed += d.EstimatedMemUsage()
	}
	return bytesConsumed * int64(numOfRows)
}

// EstimatedMemUsage returns the estimated bytes consumed of a Datum.
func (d Datum) EstimatedMemUsage() int64 {
	bytesConsumed := sizeOfEmptyDatum
	switch d.Kind() {
	case KindMysqlDecimal:
		bytesConsumed += sizeOfMyDecimal
	case KindMysqlTime:
		bytesConsumed += sizeOfMysqlTime
	case KindVectorFloat32:
		bytesConsumed += d.GetVectorFloat32().EstimatedMemUsage()
	default:
		bytesConsumed += len(d.b)
	}
	return int64(bytesConsumed)
}

// DatumsContainNull return true if any value is null
func DatumsContainNull(vals []Datum) bool {
	for _, val := range vals {
		if val.IsNull() {
			return true
		}
	}
	return false
}
