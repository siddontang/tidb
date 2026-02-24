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
	"bytes"
	"cmp"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Kind constants.
const (
	KindNull          byte = 0
	KindInt64         byte = 1
	KindUint64        byte = 2
	KindFloat32       byte = 3
	KindFloat64       byte = 4
	KindString        byte = 5
	KindBytes         byte = 6
	KindBinaryLiteral byte = 7 // Used for BIT / HEX literals.
	KindMysqlDecimal  byte = 8
	KindMysqlDuration byte = 9
	KindMysqlEnum     byte = 10
	KindMysqlBit      byte = 11 // Used for BIT table column values.
	KindMysqlSet      byte = 12
	KindMysqlTime     byte = 13
	KindInterface     byte = 14
	KindMinNotNull    byte = 15
	KindMaxValue      byte = 16
	KindRaw           byte = 17
	KindMysqlJSON     byte = 18
	KindVectorFloat32 byte = 19
)

// Datum is a data box holds different kind of data.
// It has better performance and is easier to use than `interface{}`.
type Datum struct {
	k         byte   // datum kind.
	decimal   uint16 // decimal can hold uint16 values.
	length    uint32 // length can hold uint32 values.
	i         int64  // i can hold int64 uint64 float64 values.
	collation string // collation hold the collation information for string value.
	b         []byte // b can hold string or []byte values.
	x         any    // x hold all other types.
}

// EmptyDatumSize is the size of empty datum.
// 72 = 1 + 1 (byte) + 2 (uint16) + 4 (uint32) + 8 (int64) + 16 (string) + 24 ([]byte) + 16 (interface{})
const EmptyDatumSize = int64(unsafe.Sizeof(Datum{}))

// Clone create a deep copy of the Datum.
func (d *Datum) Clone() *Datum {
	ret := new(Datum)
	d.Copy(ret)
	return ret
}

// Copy deep copies a Datum into destination.
func (d *Datum) Copy(dst *Datum) {
	*dst = *d
	if d.b != nil {
		dst.b = make([]byte, len(d.b))
		copy(dst.b, d.b)
	}
	switch dst.Kind() {
	case KindMysqlDecimal:
		d := *d.GetMysqlDecimal()
		dst.SetMysqlDecimal(&d)
	case KindMysqlTime:
		dst.SetMysqlTime(d.GetMysqlTime())
	}
}

// Kind gets the kind of the datum.
func (d *Datum) Kind() byte {
	return d.k
}

// Collation gets the collation of the datum.
func (d *Datum) Collation() string {
	return d.collation
}

// SetCollation sets the collation of the datum.
func (d *Datum) SetCollation(collation string) {
	d.collation = collation
}

// Frac gets the frac of the datum.
func (d *Datum) Frac() int {
	return int(d.decimal)
}

// SetFrac sets the frac of the datum.
func (d *Datum) SetFrac(frac int) {
	d.decimal = uint16(frac)
}

// Length gets the length of the datum.
func (d *Datum) Length() int {
	return int(d.length)
}

// SetLength sets the length of the datum.
func (d *Datum) SetLength(l int) {
	d.length = uint32(l)
}

// IsNull checks if datum is null.
func (d *Datum) IsNull() bool {
	return d.k == KindNull
}

// GetInt64 gets int64 value.
func (d *Datum) GetInt64() int64 {
	return d.i
}

// SetInt64 sets int64 value.
func (d *Datum) SetInt64(i int64) {
	d.k = KindInt64
	d.i = i
}

// GetUint64 gets uint64 value.
func (d *Datum) GetUint64() uint64 {
	return uint64(d.i)
}

// SetUint64 sets uint64 value.
func (d *Datum) SetUint64(i uint64) {
	d.k = KindUint64
	d.i = int64(i)
}

// GetFloat64 gets float64 value.
func (d *Datum) GetFloat64() float64 {
	return math.Float64frombits(uint64(d.i))
}

// SetFloat64 sets float64 value.
func (d *Datum) SetFloat64(f float64) {
	d.k = KindFloat64
	d.i = int64(math.Float64bits(f))
}

// GetFloat32 gets float32 value.
func (d *Datum) GetFloat32() float32 {
	return float32(math.Float64frombits(uint64(d.i)))
}

// SetFloat32 sets float32 value.
func (d *Datum) SetFloat32(f float32) {
	d.k = KindFloat32
	d.i = int64(math.Float64bits(float64(f)))
}

// SetFloat32FromF64 sets float32 values from f64
func (d *Datum) SetFloat32FromF64(f float64) {
	d.k = KindFloat32
	d.i = int64(math.Float64bits(f))
}

// GetString gets string value.
func (d *Datum) GetString() string {
	return string(hack.String(d.b))
}

// GetBinaryStringEncoded gets the string value encoded with given charset.
func (d *Datum) GetBinaryStringEncoded() string {
	coll, err := charset.GetCollationByName(d.Collation())
	if err != nil {
		logutil.BgLogger().Warn("unknown collation", zap.Error(err))
		return d.GetString()
	}
	enc := charset.FindEncodingTakeUTF8AsNoop(coll.CharsetName)
	replace, _ := enc.Transform(nil, d.GetBytes(), charset.OpEncodeNoErr)
	return string(hack.String(replace))
}

// GetBinaryStringDecoded gets the string value decoded with given charset.
func (d *Datum) GetBinaryStringDecoded(flags Flags, chs string) (string, error) {
	enc, skip := findEncoding(flags, chs)
	if skip {
		return d.GetString(), nil
	}
	trim, err := enc.Transform(nil, d.GetBytes(), charset.OpDecode)
	return string(hack.String(trim)), err
}

// GetStringWithCheck gets the string and checks if it is valid in a given charset.
func (d *Datum) GetStringWithCheck(flags Flags, chs string) (string, error) {
	enc, skip := findEncoding(flags, chs)
	if skip {
		return d.GetString(), nil
	}
	str := d.GetBytes()
	if !enc.IsValid(str) {
		replace, err := enc.Transform(nil, str, charset.OpReplace)
		return string(hack.String(replace)), err
	}
	return d.GetString(), nil
}

func findEncoding(flags Flags, chs string) (enc charset.Encoding, skip bool) {
	enc = charset.FindEncoding(chs)
	if enc.Tp() == charset.EncodingTpUTF8 && flags.SkipUTF8Check() ||
		enc.Tp() == charset.EncodingTpASCII && flags.SkipASCIICheck() {
		return nil, true
	}
	if chs == charset.CharsetUTF8 && !flags.SkipUTF8MB4Check() {
		enc = charset.EncodingUTF8MB3StrictImpl
	}
	return enc, false
}

// SetString sets string value.
func (d *Datum) SetString(s string, collation string) {
	d.k = KindString
	sink(s)
	d.b = hack.Slice(s)
	d.collation = collation
}

// sink prevents s from being allocated on the stack.
var sink = func(s string) {
}

// GetBytes gets bytes value.
func (d *Datum) GetBytes() []byte {
	if d.b != nil {
		return d.b
	}
	return []byte{}
}

// SetBytes sets bytes value to datum.
func (d *Datum) SetBytes(b []byte) {
	d.k = KindBytes
	d.b = b
	d.collation = charset.CollationBin
}

// SetBytesAsString sets bytes value to datum as string type.
func (d *Datum) SetBytesAsString(b []byte, collation string, length uint32) {
	d.k = KindString
	d.b = b
	d.length = length
	d.collation = collation
}

// GetInterface gets interface value.
func (d *Datum) GetInterface() any {
	return d.x
}

// SetInterface sets interface to datum.
func (d *Datum) SetInterface(x any) {
	d.k = KindInterface
	d.x = x
}

// SetNull sets datum to nil.
func (d *Datum) SetNull() {
	d.k = KindNull
	d.x = nil
}

// SetMinNotNull sets datum to minNotNull value.
func (d *Datum) SetMinNotNull() {
	d.k = KindMinNotNull
	d.x = nil
}

// GetBinaryLiteral4Cmp gets Bit value, and remove it's prefix 0 for comparison.
func (d *Datum) GetBinaryLiteral4Cmp() BinaryLiteral {
	bitLen := len(d.b)
	if bitLen == 0 {
		return d.b
	}
	for i := range bitLen {
		// Remove the prefix 0 in the bit array.
		if d.b[i] != 0 {
			return d.b[i:]
		}
	}
	// The result is 0x000...00, we just the return 0x00.
	return d.b[bitLen-1:]
}

// GetBinaryLiteral gets Bit value
func (d *Datum) GetBinaryLiteral() BinaryLiteral {
	return d.b
}

// GetMysqlBit gets MysqlBit value
func (d *Datum) GetMysqlBit() BinaryLiteral {
	return d.GetBinaryLiteral()
}

// SetBinaryLiteral sets Bit value
func (d *Datum) SetBinaryLiteral(b BinaryLiteral) {
	d.k = KindBinaryLiteral
	d.b = b
	d.collation = charset.CollationBin
}

// SetMysqlBit sets MysqlBit value
func (d *Datum) SetMysqlBit(b BinaryLiteral) {
	d.k = KindMysqlBit
	d.b = b
}

// GetMysqlDecimal gets decimal value
func (d *Datum) GetMysqlDecimal() *MyDecimal {
	return d.x.(*MyDecimal)
}

// SetMysqlDecimal sets decimal value
func (d *Datum) SetMysqlDecimal(b *MyDecimal) {
	d.k = KindMysqlDecimal
	d.x = b
}

// GetMysqlDuration gets Duration value
func (d *Datum) GetMysqlDuration() Duration {
	return Duration{Duration: time.Duration(d.i), Fsp: int(int8(d.decimal))}
}

// SetMysqlDuration sets Duration value
func (d *Datum) SetMysqlDuration(b Duration) {
	d.k = KindMysqlDuration
	d.i = int64(b.Duration)
	d.decimal = uint16(b.Fsp)
}

// GetMysqlEnum gets Enum value
func (d *Datum) GetMysqlEnum() Enum {
	str := string(hack.String(d.b))
	return Enum{Value: uint64(d.i), Name: str}
}

// SetMysqlEnum sets Enum value
func (d *Datum) SetMysqlEnum(b Enum, collation string) {
	d.k = KindMysqlEnum
	d.i = int64(b.Value)
	sink(b.Name)
	d.collation = collation
	d.b = hack.Slice(b.Name)
}

// GetMysqlSet gets Set value
func (d *Datum) GetMysqlSet() Set {
	str := string(hack.String(d.b))
	return Set{Value: uint64(d.i), Name: str}
}

// SetMysqlSet sets Set value
func (d *Datum) SetMysqlSet(b Set, collation string) {
	d.k = KindMysqlSet
	d.i = int64(b.Value)
	sink(b.Name)
	d.collation = collation
	d.b = hack.Slice(b.Name)
}

// GetMysqlJSON gets json.BinaryJSON value
func (d *Datum) GetMysqlJSON() BinaryJSON {
	return BinaryJSON{TypeCode: byte(d.i), Value: d.b}
}

// SetMysqlJSON sets json.BinaryJSON value
func (d *Datum) SetMysqlJSON(b BinaryJSON) {
	d.k = KindMysqlJSON
	d.i = int64(b.TypeCode)
	d.b = b.Value
}

// SetVectorFloat32 sets VectorFloat32 value
func (d *Datum) SetVectorFloat32(vec VectorFloat32) {
	d.k = KindVectorFloat32
	d.b = vec.ZeroCopySerialize()
}

// GetVectorFloat32 gets VectorFloat32 value
func (d *Datum) GetVectorFloat32() VectorFloat32 {
	v, _, err := ZeroCopyDeserializeVectorFloat32(d.b)
	if err != nil {
		panic(err)
	}
	return v
}

// GetMysqlTime gets types.Time value
func (d *Datum) GetMysqlTime() Time {
	return d.x.(Time)
}

// SetMysqlTime sets types.Time value
func (d *Datum) SetMysqlTime(b Time) {
	d.k = KindMysqlTime
	d.x = b
}

// SetRaw sets raw value.
func (d *Datum) SetRaw(b []byte) {
	d.k = KindRaw
	d.b = b
}

// GetRaw gets raw value.
func (d *Datum) GetRaw() []byte {
	return d.b
}

// SetAutoID set the auto increment ID according to its int flag.
// Don't use it directly, useless wrapped with setDatumAutoIDAndCast.
func (d *Datum) SetAutoID(id int64, flag uint) {
	if mysql.HasUnsignedFlag(flag) {
		d.SetUint64(uint64(id))
	} else {
		d.SetInt64(id)
	}
}

// String returns a human-readable description of Datum. It is intended only for debugging.
func (d Datum) String() string {
	var t string
	switch d.k {
	case KindNull:
		t = "KindNull"
	case KindInt64:
		t = "KindInt64"
	case KindUint64:
		t = "KindUint64"
	case KindFloat32:
		t = "KindFloat32"
	case KindFloat64:
		t = "KindFloat64"
	case KindString:
		t = "KindString"
	case KindBytes:
		t = "KindBytes"
	case KindBinaryLiteral:
		t = "KindBinaryLiteral"
	case KindMysqlDecimal:
		t = "KindMysqlDecimal"
	case KindMysqlDuration:
		t = "KindMysqlDuration"
	case KindMysqlEnum:
		t = "KindMysqlEnum"
	case KindMysqlBit:
		t = "KindMysqlBit"
	case KindMysqlSet:
		t = "KindMysqlSet"
	case KindMysqlTime:
		t = "KindMysqlTime"
	case KindInterface:
		t = "KindInterface"
	case KindMinNotNull:
		t = "KindMinNotNull"
	case KindMaxValue:
		t = "KindMaxValue"
	case KindRaw:
		t = "KindRaw"
	case KindMysqlJSON:
		t = "KindMysqlJSON"
	case KindVectorFloat32:
		t = "KindVectorFloat32"
	default:
		t = "Unknown"
	}
	v := d.GetValue()
	switch v.(type) {
	case []byte, string:
		quote := `"`
		// We only need the escape functionality of %q, the quoting is not needed,
		// so we trim the \" prefix and suffix here.
		v = strings.TrimSuffix(
			strings.TrimPrefix(
				fmt.Sprintf("%q", v),
				quote),
			quote)
	}
	return fmt.Sprintf("%v %v", t, v)
}

// GetValue gets the value of the datum of any kind.
func (d *Datum) GetValue() any {
	switch d.k {
	case KindInt64:
		return d.GetInt64()
	case KindUint64:
		return d.GetUint64()
	case KindFloat32:
		return d.GetFloat32()
	case KindFloat64:
		return d.GetFloat64()
	case KindString:
		return d.GetString()
	case KindBytes:
		return d.GetBytes()
	case KindMysqlDecimal:
		return d.GetMysqlDecimal()
	case KindMysqlDuration:
		return d.GetMysqlDuration()
	case KindMysqlEnum:
		return d.GetMysqlEnum()
	case KindBinaryLiteral, KindMysqlBit:
		return d.GetBinaryLiteral()
	case KindMysqlSet:
		return d.GetMysqlSet()
	case KindMysqlJSON:
		return d.GetMysqlJSON()
	case KindMysqlTime:
		return d.GetMysqlTime()
	case KindVectorFloat32:
		return d.GetVectorFloat32()
	default:
		return d.GetInterface()
	}
}

// TruncatedStringify returns the %v representation of the datum
// but truncated (for example, for strings, only first 64 bytes is printed).
// This function is useful in contexts like EXPLAIN.
func (d *Datum) TruncatedStringify() string {
	switch d.k {
	case KindString, KindBytes:
		return truncateStringIfNeeded(d.GetString())
	case KindMysqlJSON:
		return truncateStringIfNeeded(d.GetMysqlJSON().String())
	case KindVectorFloat32:
		// Vector supports native efficient truncation.
		return d.GetVectorFloat32().TruncatedString()
	case KindInt64:
		return strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		return strconv.FormatUint(d.GetUint64(), 10)
	default:
		// For other types, no truncation is needed.
		return fmt.Sprintf("%v", d.GetValue())
	}
}

func truncateStringIfNeeded(str string) string {
	const maxLen = 64
	if len(str) > maxLen {
		const suffix = "...(len:"
		lenStr := strconv.Itoa(len(str))
		buf := bytes.NewBuffer(make([]byte, 0, maxLen+len(suffix)+len(lenStr)+1))
		buf.WriteString(str[:maxLen])
		buf.WriteString(suffix)
		buf.WriteString(lenStr)
		buf.WriteByte(')')
		return buf.String()
	}
	return str
}

// SetValueWithDefaultCollation sets any kind of value.
func (d *Datum) SetValueWithDefaultCollation(val any) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x, mysql.DefaultCollationName)
	case []byte:
		d.SetBytes(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x, mysql.DefaultCollationName)
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		d.SetMysqlSet(x, mysql.DefaultCollationName)
	case BinaryJSON:
		d.SetMysqlJSON(x)
	case Time:
		d.SetMysqlTime(x)
	case VectorFloat32:
		d.SetVectorFloat32(x)
	default:
		d.SetInterface(x)
	}
}

// SetValue sets any kind of value.
func (d *Datum) SetValue(val any, tp *types.FieldType) {
	switch x := val.(type) {
	case nil:
		d.SetNull()
	case bool:
		if x {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case int:
		d.SetInt64(int64(x))
	case int64:
		d.SetInt64(x)
	case uint64:
		d.SetUint64(x)
	case float32:
		d.SetFloat32(x)
	case float64:
		d.SetFloat64(x)
	case string:
		d.SetString(x, tp.GetCollate())
	case []byte:
		d.SetBytes(x)
	case *MyDecimal:
		d.SetMysqlDecimal(x)
	case Duration:
		d.SetMysqlDuration(x)
	case Enum:
		d.SetMysqlEnum(x, tp.GetCollate())
	case BinaryLiteral:
		d.SetBinaryLiteral(x)
	case BitLiteral: // Store as BinaryLiteral for Bit and Hex literals
		d.SetBinaryLiteral(BinaryLiteral(x))
	case HexLiteral:
		d.SetBinaryLiteral(BinaryLiteral(x))
	case Set:
		d.SetMysqlSet(x, tp.GetCollate())
	case BinaryJSON:
		d.SetMysqlJSON(x)
	case Time:
		d.SetMysqlTime(x)
	case VectorFloat32:
		d.SetVectorFloat32(x)
	default:
		d.SetInterface(x)
	}
}

// Hash64ForDatum is a hash function for initialized by codec package.
var Hash64ForDatum func(h base.Hasher, d *Datum)

// Hash64 implements base.HashEquals<0th> interface.
func (d *Datum) Hash64(h base.Hasher) {
	Hash64ForDatum(h, d)
}

// Equals implements base.HashEquals.<1st> interface.
func (d *Datum) Equals(other any) bool {
	if other == nil {
		return false
	}
	var d2 *Datum
	switch x := other.(type) {
	case *Datum:
		d2 = x
	case Datum:
		d2 = &x
	default:
		return false
	}
	ok := d.k == d2.k &&
		d.decimal == d2.decimal &&
		d.length == d2.length &&
		d.i == d2.i &&
		d.collation == d2.collation &&
		string(d.b) == string(d2.b)
	if !ok {
		return false
	}
	// compare x
	switch d.k {
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Compare(d2.GetMysqlDecimal()) == 0
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(d2.GetMysqlTime()) == 0
	default:
		return true
	}
}

// Compare compares datum to another datum.
// Notes: don't rely on datum.collation to get the collator, it's tend to buggy.
func (d *Datum) Compare(ctx Context, ad *Datum, comparer collate.Collator) (int, error) {
	if d.k == KindMysqlJSON && ad.k != KindMysqlJSON {
		cmp, err := ad.Compare(ctx, d, comparer)
		return cmp * -1, errors.Trace(err)
	}
	switch ad.k {
	case KindNull:
		if d.k == KindNull {
			return 0, nil
		}
		return 1, nil
	case KindMinNotNull:
		if d.k == KindNull {
			return -1, nil
		} else if d.k == KindMinNotNull {
			return 0, nil
		}
		return 1, nil
	case KindMaxValue:
		if d.k == KindMaxValue {
			return 0, nil
		}
		return -1, nil
	case KindInt64:
		return d.compareInt64(ctx, ad.GetInt64())
	case KindUint64:
		return d.compareUint64(ctx, ad.GetUint64())
	case KindFloat32, KindFloat64:
		return d.compareFloat64(ctx, ad.GetFloat64())
	case KindString:
		return d.compareString(ctx, ad.GetString(), comparer)
	case KindBytes:
		return d.compareString(ctx, ad.GetString(), comparer)
	case KindMysqlDecimal:
		return d.compareMysqlDecimal(ctx, ad.GetMysqlDecimal())
	case KindMysqlDuration:
		return d.compareMysqlDuration(ctx, ad.GetMysqlDuration())
	case KindMysqlEnum:
		return d.compareMysqlEnum(ctx, ad.GetMysqlEnum(), comparer)
	case KindBinaryLiteral, KindMysqlBit:
		return d.compareBinaryLiteral(ctx, ad.GetBinaryLiteral4Cmp(), comparer)
	case KindMysqlSet:
		return d.compareMysqlSet(ctx, ad.GetMysqlSet(), comparer)
	case KindMysqlJSON:
		return d.compareMysqlJSON(ad.GetMysqlJSON())
	case KindMysqlTime:
		return d.compareMysqlTime(ctx, ad.GetMysqlTime())
	case KindVectorFloat32:
		return d.compareVectorFloat32(ctx, ad.GetVectorFloat32())
	default:
		return 0, nil
	}
}

func (d *Datum) compareInt64(ctx Context, i int64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return cmp.Compare(d.i, i), nil
	case KindUint64:
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			return 1, nil
		}
		return cmp.Compare(d.i, i), nil
	default:
		return d.compareFloat64(ctx, float64(i))
	}
}

func (d *Datum) compareUint64(ctx Context, u uint64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		if d.i < 0 || u > math.MaxInt64 {
			return -1, nil
		}
		return cmp.Compare(d.i, int64(u)), nil
	case KindUint64:
		return cmp.Compare(d.GetUint64(), u), nil
	default:
		return d.compareFloat64(ctx, float64(u))
	}
}

func (d *Datum) compareFloat64(ctx Context, f float64) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return cmp.Compare(float64(d.i), f), nil
	case KindUint64:
		return cmp.Compare(float64(d.GetUint64()), f), nil
	case KindFloat32, KindFloat64:
		return cmp.Compare(d.GetFloat64(), f), nil
	case KindString, KindBytes:
		fVal, err := StrToFloat(ctx, d.GetString(), false)
		return cmp.Compare(fVal, f), errors.Trace(err)
	case KindMysqlDecimal:
		fVal, err := d.GetMysqlDecimal().ToFloat64()
		return cmp.Compare(fVal, f), errors.Trace(err)
	case KindMysqlDuration:
		fVal := d.GetMysqlDuration().Seconds()
		return cmp.Compare(fVal, f), nil
	case KindMysqlEnum:
		fVal := d.GetMysqlEnum().ToNumber()
		return cmp.Compare(fVal, f), nil
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral4Cmp().ToInt(ctx)
		fVal := float64(val)
		return cmp.Compare(fVal, f), errors.Trace(err)
	case KindMysqlSet:
		fVal := d.GetMysqlSet().ToNumber()
		return cmp.Compare(fVal, f), nil
	case KindMysqlTime:
		fVal, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return cmp.Compare(fVal, f), errors.Trace(err)
	default:
		return -1, nil
	}
}

func (d *Datum) compareString(ctx Context, s string, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		return comparer.Compare(d.GetString(), s), nil
	case KindMysqlDecimal:
		dec := new(MyDecimal)
		err := ctx.HandleTruncate(dec.FromString(hack.Slice(s)))
		return d.GetMysqlDecimal().Compare(dec), errors.Trace(err)
	case KindMysqlTime:
		dt, err := ParseDatetime(ctx, s)
		return d.GetMysqlTime().Compare(dt), errors.Trace(err)
	case KindMysqlDuration:
		dur, _, err := ParseDuration(ctx, s, MaxFsp)
		return d.GetMysqlDuration().Compare(dur), errors.Trace(err)
	case KindMysqlSet:
		return comparer.Compare(d.GetMysqlSet().String(), s), nil
	case KindMysqlEnum:
		return comparer.Compare(d.GetMysqlEnum().String(), s), nil
	case KindBinaryLiteral, KindMysqlBit:
		return comparer.Compare(d.GetBinaryLiteral4Cmp().ToString(), s), nil
	default:
		fVal, err := StrToFloat(ctx, s, false)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(ctx, fVal)
	}
}

func (d *Datum) compareMysqlDecimal(ctx Context, dec *MyDecimal) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Compare(dec), nil
	case KindString, KindBytes:
		dDec := new(MyDecimal)
		err := ctx.HandleTruncate(dDec.FromString(d.GetBytes()))
		return dDec.Compare(dec), errors.Trace(err)
	default:
		dVal, err := d.ConvertTo(ctx, NewFieldType(mysql.TypeNewDecimal))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return dVal.GetMysqlDecimal().Compare(dec), nil
	}
}

func (d *Datum) compareMysqlDuration(ctx Context, dur Duration) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().Compare(dur), nil
	case KindString, KindBytes:
		dDur, _, err := ParseDuration(ctx, d.GetString(), MaxFsp)
		return dDur.Compare(dur), errors.Trace(err)
	default:
		return d.compareFloat64(ctx, dur.Seconds())
	}
}

func (d *Datum) compareMysqlEnum(sc Context, enum Enum, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes, KindMysqlEnum, KindMysqlSet:
		return comparer.Compare(d.GetString(), enum.String()), nil
	default:
		return d.compareFloat64(sc, enum.ToNumber())
	}
}

func (d *Datum) compareBinaryLiteral(ctx Context, b BinaryLiteral, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		fallthrough // in this case, d is converted to Binary and then compared with b
	case KindBinaryLiteral, KindMysqlBit:
		return comparer.Compare(d.GetBinaryLiteral4Cmp().ToString(), b.ToString()), nil
	default:
		val, err := b.ToInt(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		result, err := d.compareFloat64(ctx, float64(val))
		return result, errors.Trace(err)
	}
}

func (d *Datum) compareMysqlSet(ctx Context, set Set, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes, KindMysqlEnum, KindMysqlSet:
		return comparer.Compare(d.GetString(), set.String()), nil
	default:
		return d.compareFloat64(ctx, set.ToNumber())
	}
}

func (d *Datum) compareMysqlJSON(target BinaryJSON) (int, error) {
	// json is not equal with NULL
	if d.k == KindNull {
		return 1, nil
	}

	origin, err := d.ToMysqlJSON()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return CompareBinaryJSON(origin, target), nil
}

func (d *Datum) compareMysqlTime(ctx Context, time Time) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		dt, err := ParseDatetime(ctx, d.GetString())
		return dt.Compare(time), errors.Trace(err)
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(time), nil
	default:
		fVal, err := time.ToNumber().ToFloat64()
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(ctx, fVal)
	}
}

func (d *Datum) compareVectorFloat32(ctx Context, vec VectorFloat32) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindVectorFloat32:
		return d.GetVectorFloat32().Compare(vec), nil
	// Note: We expect cast is applied before compare, when comparing with String and other vector types.
	default:
		return 0, errors.New("cannot compare vector and non-vector, cast is required")
	}
}
