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

package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/size"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag           byte = 0
	bytesFlag         byte = 1
	compactBytesFlag  byte = 2
	intFlag           byte = 3
	uintFlag          byte = 4
	floatFlag         byte = 5
	decimalFlag       byte = 6
	durationFlag      byte = 7
	varintFlag        byte = 8
	uvarintFlag       byte = 9
	jsonFlag          byte = 10
	vectorFloat32Flag byte = 20
	maxFlag           byte = 250
)

// IntHandleFlag is only used to encode int handle key.
const IntHandleFlag = intFlag

const (
	sizeUint64  = unsafe.Sizeof(uint64(0))
	sizeUint8   = unsafe.Sizeof(uint8(0))
	sizeUint32  = unsafe.Sizeof(uint32(0))
	sizeFloat64 = unsafe.Sizeof(float64(0))
)

func preRealloc(b []byte, vals []types.Datum, comparable1 bool) []byte {
	var size int
	for i := range vals {
		switch vals[i].Kind() {
		case types.KindInt64, types.KindUint64, types.KindMysqlEnum, types.KindMysqlSet, types.KindMysqlBit, types.KindBinaryLiteral:
			size += sizeInt(comparable1)
		case types.KindString, types.KindBytes:
			size += sizeBytes(vals[i].GetBytes(), comparable1)
		case types.KindMysqlTime, types.KindMysqlDuration, types.KindFloat32, types.KindFloat64:
			size += 9
		case types.KindNull, types.KindMinNotNull, types.KindMaxValue:
			size++
		case types.KindMysqlJSON:
			size += 2 + len(vals[i].GetBytes())
		case types.KindVectorFloat32:
			size += 1 + vals[i].GetVectorFloat32().SerializedSize()
		case types.KindMysqlDecimal:
			size += 1 + types.MyDecimalStructSize
		default:
			return b
		}
	}
	return reallocBytes(b, size)
}

// encode will encode a datum and append it to a byte slice. If comparable1 is true, the encoded bytes can be sorted as it's original order.
// If hash is true, the encoded bytes can be checked equal as it's original value.
func encode(loc *time.Location, b []byte, vals []types.Datum, comparable1 bool) (_ []byte, err error) {
	b = preRealloc(b, vals, comparable1)
	for i, length := 0, len(vals); i < length; i++ {
		switch vals[i].Kind() {
		case types.KindInt64:
			b = encodeSignedInt(b, vals[i].GetInt64(), comparable1)
		case types.KindUint64:
			b = encodeUnsignedInt(b, vals[i].GetUint64(), comparable1)
		case types.KindFloat32, types.KindFloat64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, vals[i].GetFloat64())
		case types.KindString:
			b = encodeString(b, vals[i], comparable1)
		case types.KindBytes:
			b = encodeBytes(b, vals[i].GetBytes(), comparable1)
		case types.KindMysqlTime:
			b = append(b, uintFlag)
			b, err = EncodeMySQLTime(loc, vals[i].GetMysqlTime(), mysql.TypeUnspecified, b)
			if err != nil {
				return b, err
			}
		case types.KindMysqlDuration:
			// duration may have negative value, so we cannot use String to encode directly.
			b = append(b, durationFlag)
			b = EncodeInt(b, int64(vals[i].GetMysqlDuration().Duration))
		case types.KindMysqlDecimal:
			b = append(b, decimalFlag)
			b, err = EncodeDecimal(b, vals[i].GetMysqlDecimal(), vals[i].Length(), vals[i].Frac())
		case types.KindMysqlEnum:
			b = encodeUnsignedInt(b, vals[i].GetMysqlEnum().Value, comparable1)
		case types.KindMysqlSet:
			b = encodeUnsignedInt(b, vals[i].GetMysqlSet().Value, comparable1)
		case types.KindMysqlBit, types.KindBinaryLiteral:
			// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
			var val uint64
			val, err = vals[i].GetBinaryLiteral().ToInt(types.StrictContext)
			terror.Log(errors.Trace(err))
			b = encodeUnsignedInt(b, val, comparable1)
		case types.KindMysqlJSON:
			b = append(b, jsonFlag)
			j := vals[i].GetMysqlJSON()
			b = append(b, j.TypeCode)
			b = append(b, j.Value...)
		case types.KindVectorFloat32:
			// Always do a small deser + ser for sanity check
			b = append(b, vectorFloat32Flag)
			v := vals[i].GetVectorFloat32()
			b = v.SerializeTo(b)
		case types.KindNull:
			b = append(b, NilFlag)
		case types.KindMinNotNull:
			b = append(b, bytesFlag)
		case types.KindMaxValue:
			b = append(b, maxFlag)
		default:
			return b, errors.Errorf("unsupport encode type %d", vals[i].Kind())
		}
	}

	return b, errors.Trace(err)
}

// EstimateValueSize uses to estimate the value  size of the encoded values.
func EstimateValueSize(typeCtx types.Context, val types.Datum) (int, error) {
	l := 0
	switch val.Kind() {
	case types.KindInt64:
		l = valueSizeOfSignedInt(val.GetInt64())
	case types.KindUint64:
		l = valueSizeOfUnsignedInt(val.GetUint64())
	case types.KindFloat32, types.KindFloat64, types.KindMysqlTime, types.KindMysqlDuration:
		l = 9
	case types.KindString, types.KindBytes:
		l = valueSizeOfBytes(val.GetBytes())
	case types.KindMysqlDecimal:
		var err error
		l, err = valueSizeOfDecimal(val.GetMysqlDecimal(), val.Length(), val.Frac())
		if err != nil {
			return 0, err
		}
		l = l + 1
	case types.KindMysqlEnum:
		l = valueSizeOfUnsignedInt(val.GetMysqlEnum().Value)
	case types.KindMysqlSet:
		l = valueSizeOfUnsignedInt(val.GetMysqlSet().Value)
	case types.KindMysqlBit, types.KindBinaryLiteral:
		val, err := val.GetBinaryLiteral().ToInt(typeCtx)
		terror.Log(errors.Trace(err))
		l = valueSizeOfUnsignedInt(val)
	case types.KindMysqlJSON:
		l = 2 + len(val.GetMysqlJSON().Value)
	case types.KindVectorFloat32:
		v := val.GetVectorFloat32()
		l = 1 + v.SerializedSize()
	case types.KindNull, types.KindMinNotNull, types.KindMaxValue:
		l = 1
	default:
		return l, errors.Errorf("unsupported encode type %d", val.Kind())
	}
	return l, nil
}

// EncodeMySQLTime encodes datum of `KindMysqlTime` to []byte.
func EncodeMySQLTime(loc *time.Location, t types.Time, tp byte, b []byte) (_ []byte, err error) {
	// Encoding timestamp need to consider timezone. If it's not in UTC, transform to UTC first.
	// This is compatible with `PBToExpr > convertTime`, and coprocessor assumes the passed timestamp is in UTC as well.
	if tp == mysql.TypeUnspecified {
		tp = t.Type()
	}
	if tp == mysql.TypeTimestamp && loc != time.UTC {
		err = t.ConvertTimeZone(loc, time.UTC)
		if err != nil {
			return nil, err
		}
	}
	var v uint64
	v, err = t.ToPackedUint()
	if err != nil {
		return nil, err
	}
	b = EncodeUint(b, v)
	return b, nil
}

func encodeString(b []byte, val types.Datum, comparable1 bool) []byte {
	if collate.NewCollationEnabled() && comparable1 {
		return encodeBytes(b, collate.GetCollator(val.Collation()).ImmutableKey(val.GetString()), true)
	}
	return encodeBytes(b, val.GetBytes(), comparable1)
}

func encodeBytes(b []byte, v []byte, comparable1 bool) []byte {
	if comparable1 {
		b = append(b, bytesFlag)
		b = EncodeBytes(b, v)
	} else {
		b = append(b, compactBytesFlag)
		b = EncodeCompactBytes(b, v)
	}
	return b
}

func valueSizeOfBytes(v []byte) int {
	return valueSizeOfSignedInt(int64(len(v))) + len(v)
}

func sizeBytes(v []byte, comparable1 bool) int {
	if comparable1 {
		reallocSize := (len(v)/encGroupSize + 1) * (encGroupSize + 1)
		return 1 + reallocSize
	}
	reallocSize := binary.MaxVarintLen64 + len(v)
	return 1 + reallocSize
}

func encodeSignedInt(b []byte, v int64, comparable1 bool) []byte {
	if comparable1 {
		b = append(b, intFlag)
		b = EncodeInt(b, v)
	} else {
		b = append(b, varintFlag)
		b = EncodeVarint(b, v)
	}
	return b
}

func valueSizeOfSignedInt(v int64) int {
	if v < 0 {
		v = 0 - v - 1
	}
	// flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 6
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func encodeUnsignedInt(b []byte, v uint64, comparable1 bool) []byte {
	if comparable1 {
		b = append(b, uintFlag)
		b = EncodeUint(b, v)
	} else {
		b = append(b, uvarintFlag)
		b = EncodeUvarint(b, v)
	}
	return b
}

func valueSizeOfUnsignedInt(v uint64) int {
	// flag occupy 1 bit and at lease 1 bit.
	size := 2
	v = v >> 7
	for v > 0 {
		size++
		v = v >> 7
	}
	return size
}

func sizeInt(comparable1 bool) int {
	if comparable1 {
		return 9
	}
	return 1 + binary.MaxVarintLen64
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For decimal type, datum must set datum's length and frac.
func EncodeKey(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
	return encode(loc, b, v, true)
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
	return encode(loc, b, v, false)
}

func encodeHashChunkRowIdx(typeCtx types.Context, row chunk.Row, tp *types.FieldType, idx int) (flag byte, b []byte, err error) {
	if row.IsNull(idx) {
		flag = NilFlag
		return
	}
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		flag = uvarintFlag
		if !mysql.HasUnsignedFlag(tp.GetFlag()) && row.GetInt64(idx) < 0 {
			flag = varintFlag
		}
		b = row.GetRaw(idx)
	case mysql.TypeFloat:
		flag = floatFlag
		f := float64(row.GetFloat32(idx))
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = unsafe.Slice((*byte)(unsafe.Pointer(&f)), unsafe.Sizeof(f))
	case mysql.TypeDouble:
		flag = floatFlag
		f := row.GetFloat64(idx)
		// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
		// It makes -0's hash val different from 0's.
		if f == 0 {
			f = 0
		}
		b = unsafe.Slice((*byte)(unsafe.Pointer(&f)), unsafe.Sizeof(f))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		flag = compactBytesFlag
		b = row.GetBytes(idx)
		b = ConvertByCollation(b, tp)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		flag = uintFlag
		t := row.GetTime(idx)

		var v uint64
		v, err = t.ToPackedUint()
		if err != nil {
			return
		}
		b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), unsafe.Sizeof(v))
	case mysql.TypeDuration:
		flag = durationFlag
		// duration may have negative value, so we cannot use String to encode directly.
		b = row.GetRaw(idx)
	case mysql.TypeNewDecimal:
		flag = decimalFlag
		// If hash is true, we only consider the original value of this decimal and ignore it's precision.
		dec := row.GetMyDecimal(idx)
		b, err = dec.ToHashKey()
		if err != nil {
			return
		}
	case mysql.TypeEnum:
		if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			flag = uvarintFlag
			v := row.GetEnum(idx).Value
			b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
		} else {
			flag = compactBytesFlag
			v := row.GetEnum(idx).Value
			str := ""
			if enum, err := types.ParseEnumValue(tp.GetElems(), v); err == nil {
				// str will be empty string if v out of definition of enum.
				str = enum.Name
			}
			b = ConvertByCollation(hack.Slice(str), tp)
		}
	case mysql.TypeSet:
		flag = compactBytesFlag
		s, err := types.ParseSetValue(tp.GetElems(), row.GetSet(idx).Value)
		if err != nil {
			return 0, nil, err
		}
		b = ConvertByCollation(hack.Slice(s.Name), tp)
	case mysql.TypeBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		flag = uvarintFlag
		v, err1 := types.BinaryLiteral(row.GetBytes(idx)).ToInt(typeCtx)
		terror.Log(errors.Trace(err1))
		b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), unsafe.Sizeof(v))
	case mysql.TypeJSON:
		flag = jsonFlag
		json := row.GetJSON(idx)
		b = json.HashValue(b)
	case mysql.TypeTiDBVectorFloat32:
		flag = vectorFloat32Flag
		v := row.GetVectorFloat32(idx)
		b = v.SerializeTo(nil)
	default:
		return 0, nil, errors.Errorf("unsupport column type for encode %d", tp.GetType())
	}
	return
}

// SerializeMode is for some special cases during serialize key
type SerializeMode int

const (
	// Normal means serialize in the normal way
	Normal SerializeMode = iota
	// NeedSignFlag when serialize integer column, if the join key is <signed, signed> or <unsigned, unsigned>,
	// the unsigned flag can be ignored, if the join key is <unsigned, signed> or <signed, unsigned>
	// the unsigned flag can not be ignored, if the unsigned flag can not be ignored, the key can not be inlined
	NeedSignFlag
	// KeepVarColumnLength when serialize var-length column, whether record the column length or not. If the join key only contains one var-length
	// column, and the key is not inlined, then no need to record the column length, otherwise, always need to record the column length
	KeepVarColumnLength
)

func preAllocForSerializedKeyBuffer(
	buildKeyIndexs []int,
	chk *chunk.Chunk,
	tps []*types.FieldType,
	usedRows []int,
	filterVector []bool,
	nullVector []bool,
	serializeModes []SerializeMode,
	serializedKeys [][]byte,
	serializedKeyLens []int,
	serializedKeysBuffer []byte) ([]byte, error) {
	for i, idx := range buildKeyIndexs {
		column := chk.Column(idx)
		canSkip := func(index int) bool {
			if column.IsNull(index) {
				nullVector[index] = true
			}
			return (filterVector != nil && !filterVector[index]) || (nullVector != nil && nullVector[index])
		}

		switch tps[i].GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			flagByteNum := int(0)
			if serializeModes[i] == NeedSignFlag {
				flagByteNum = int(size.SizeOfByte)
			}

			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += flagByteNum + 8
			}
		case mysql.TypeFloat, mysql.TypeDouble:
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += int(sizeFloat64)
			}
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			collator := collate.GetCollator(tps[i].GetCollate())

			sizeByteNum := int(0)
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int(sizeUint32)
			}

			for j, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				strLen := collator.MaxKeyLen(string(hack.String(column.GetBytes(physicalRowIndex))))
				serializedKeyLens[j] += sizeByteNum + strLen
			}
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += int(sizeUint64)
			}
		case mysql.TypeDuration:
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				serializedKeyLens[j] += 8
			}
		case mysql.TypeNewDecimal:
			sizeByteNum := int(0)
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int(sizeUint32)
			}

			ds := column.Decimals()
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				size, err := ds[physicalRowindex].HashKeySize()
				if err != nil {
					return serializedKeysBuffer, err
				}

				serializedKeyLens[j] += size + sizeByteNum
			}
		case mysql.TypeEnum:
			if mysql.HasEnumSetAsIntFlag(tps[i].GetFlag()) {
				elemLen := 0
				if serializeModes[i] == NeedSignFlag {
					elemLen += int(size.SizeOfByte)
				}
				elemLen += int(sizeUint64)

				for j, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}
					serializedKeyLens[j] += elemLen
				}
			} else {
				sizeByteNum := int64(0)
				if serializeModes[i] == KeepVarColumnLength {
					sizeByteNum = int64(sizeUint32)
				}

				collator := collate.GetCollator(tps[i].GetCollate())
				for j, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}

					v := column.GetEnum(physicalRowindex).Value
					str := ""
					if enum, err := types.ParseEnumValue(tps[i].GetElems(), v); err == nil {
						str = enum.Name
					}

					serializedKeyLens[j] += int(sizeByteNum) + collator.MaxKeyLen(str)
				}
			}
		case mysql.TypeSet:
			sizeByteNum := int64(0)
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int64(sizeUint32)
			}

			collator := collate.GetCollator(tps[i].GetCollate())
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				s, err := types.ParseSetValue(tps[i].GetElems(), column.GetSet(physicalRowindex).Value)
				if err != nil {
					return serializedKeysBuffer, err
				}

				serializedKeyLens[j] += int(sizeByteNum) + collator.MaxKeyLen(s.Name)
			}
		case mysql.TypeBit:
			signFlagLen := 0
			if serializeModes[i] == NeedSignFlag {
				signFlagLen = int(size.SizeOfByte)
			}
			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				serializedKeyLens[j] += signFlagLen + int(sizeUint64)
			}
		case mysql.TypeJSON:
			sizeByteNum := 0
			if serializeModes[i] == KeepVarColumnLength {
				sizeByteNum = int(sizeUint32)
			}

			for j, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}

				serializedKeyLens[j] += sizeByteNum + int(column.GetJSON(physicalRowindex).CalculateHashValueSize())
			}
		case mysql.TypeNull:
		default:
			return serializedKeysBuffer, errors.Errorf("unsupport column type for pre-alloc %d", tps[i].GetType())
		}
	}

	totalMemUsage := 0
	for _, usage := range serializedKeyLens {
		totalMemUsage += usage
	}

	if cap(serializedKeysBuffer) < totalMemUsage {
		serializedKeysBuffer = make([]byte, totalMemUsage)
	} else {
		serializedKeysBuffer = serializedKeysBuffer[:totalMemUsage]
	}

	start := 0
	for i := range serializedKeys {
		rowLen := serializedKeyLens[i]
		serializedKeys[i] = serializedKeysBuffer[start : start : start+rowLen]
		start += rowLen
	}
	return serializedKeysBuffer, nil
}

func serializeKeysImpl(
	typeCtx types.Context,
	chk *chunk.Chunk,
	tps []*types.FieldType,
	buildKeyIndexs []int,
	usedRows []int,
	filterVector []bool,
	nullVector []bool,
	serializeModes []SerializeMode,
	serializedKeys [][]byte) error {
	canSkip := func(index int) bool {
		return (filterVector != nil && !filterVector[index]) || (nullVector != nil && nullVector[index])
	}

	for i, idx := range buildKeyIndexs {
		column := chk.Column(idx)
		serializeMode := serializeModes[i]
		tp := tps[i]
		switch tp.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
			i64s := column.Int64s()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				if serializeMode == NeedSignFlag {
					if !mysql.HasUnsignedFlag(tp.GetFlag()) && i64s[physicalRowindex] < 0 {
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], intFlag)
					} else {
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], uintFlag)
					}
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], column.GetRaw(physicalRowindex)...)
			}
		case mysql.TypeFloat:
			f32s := column.Float32s()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				d := float64(f32s[physicalRowindex])
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if d == 0 {
					d = 0
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&d)), sizeFloat64)...)
			}
		case mysql.TypeDouble:
			f64s := column.Float64s()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				f := f64s[physicalRowindex]
				if f == 0 {
					f = 0
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&f)), sizeFloat64)...)
			}
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			collator := collate.GetCollator(tp.GetCollate())
			for logicalRowIndex, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				data := collator.ImmutableKey(string(hack.String(column.GetBytes(physicalRowIndex))))
				size := uint32(len(data))
				if serializeMode == KeepVarColumnLength {
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], data...)
			}
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
			ts := column.Times()
			for logicalRowIndex, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				v, err := ts[physicalRowIndex].ToPackedUint()
				if err != nil {
					return err
				}
				// don't need to check serializeMode since date/datetime/timestamp must be compared with date/datetime/timestamp, so the serializeMode must be normal
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
			}
		case mysql.TypeDuration:
			for logicalRowIndex, physicalRowIndex := range usedRows {
				if canSkip(physicalRowIndex) {
					continue
				}
				// don't need to check serializeMode since duration must be compared with duration, so the serializeMode must be normal
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], column.GetRaw(physicalRowIndex)...)
			}
		case mysql.TypeNewDecimal:
			ds := column.Decimals()
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				b, err := ds[physicalRowindex].ToHashKey()
				if err != nil {
					return err
				}
				if serializeMode == KeepVarColumnLength {
					// for decimal, the size must be less than uint8.MAX, so use uint8 here
					size := uint8(len(b))
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint8)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], b...)
			}
		case mysql.TypeEnum:
			if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
				for logicalRowIndex, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}
					v := column.GetEnum(physicalRowindex).Value
					// check serializeMode here because enum maybe compare to integer type directly
					if serializeMode == NeedSignFlag {
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], uintFlag)
					}
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
				}
			} else {
				collator := collate.GetCollator(tp.GetCollate())
				for logicalRowIndex, physicalRowindex := range usedRows {
					if canSkip(physicalRowindex) {
						continue
					}
					v := column.GetEnum(physicalRowindex).Value
					str := ""
					if enum, err := types.ParseEnumValue(tp.GetElems(), v); err == nil {
						str = enum.Name
					}
					b := collator.ImmutableKey(str)
					if serializeMode == KeepVarColumnLength {
						// for enum, the size must be less than uint32.MAX, so use uint32 here
						size := uint32(len(b))
						serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
					}
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], b...)
				}
			}
		case mysql.TypeSet:
			collator := collate.GetCollator(tp.GetCollate())
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				s, err := types.ParseSetValue(tp.GetElems(), column.GetSet(physicalRowindex).Value)
				if err != nil {
					return err
				}
				b := collator.ImmutableKey(s.Name)
				if serializeMode == KeepVarColumnLength {
					// for enum, the size must be less than uint32.MAX, so use uint32 here
					size := uint32(len(b))
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], b...)
			}
		case mysql.TypeBit:
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				v, err1 := types.BinaryLiteral(column.GetBytes(physicalRowindex)).ToInt(typeCtx)
				terror.Log(errors.Trace(err1))
				// check serializeMode here because bit maybe compare to integer type directly
				if serializeMode == NeedSignFlag {
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], uintFlag)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)...)
			}
		case mysql.TypeJSON:
			jsonHashBuffer := make([]byte, 0)
			for logicalRowIndex, physicalRowindex := range usedRows {
				if canSkip(physicalRowindex) {
					continue
				}
				jsonHashBuffer = jsonHashBuffer[:0]
				jsonHashBuffer = column.GetJSON(physicalRowindex).HashValue(jsonHashBuffer)
				if serializeMode == KeepVarColumnLength {
					size := uint32(len(jsonHashBuffer))
					serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], unsafe.Slice((*byte)(unsafe.Pointer(&size)), sizeUint32)...)
				}
				serializedKeys[logicalRowIndex] = append(serializedKeys[logicalRowIndex], jsonHashBuffer...)
			}
		case mysql.TypeNull:
		default:
			return errors.Errorf("unsupport column type for encode %d", tp.GetType())
		}
	}
	return nil
}

// SerializeKeys is used in join
func SerializeKeys(
	typeCtx types.Context,
	chk *chunk.Chunk,
	tps []*types.FieldType,
	buildKeyIndexs []int,
	usedRows []int,
	filterVector []bool,
	nullVector []bool,
	serializeModes []SerializeMode,
	serializedKeys [][]byte,
	serializedKeyLens []int,
	serializedKeysBuffer []byte) ([]byte, error) {
	serializedKeysBuffer, err := preAllocForSerializedKeyBuffer(
		buildKeyIndexs,
		chk,
		tps,
		usedRows,
		filterVector,
		nullVector,
		serializeModes,
		serializedKeys,
		serializedKeyLens,
		serializedKeysBuffer)
	if err != nil {
		return serializedKeysBuffer, err
	}

	var serializedKeyVectorBufferCapsForTest []int
	if intest.InTest {
		serializedKeyVectorBufferCapsForTest = make([]int, len(serializedKeys))
		for i := range serializedKeys {
			serializedKeyVectorBufferCapsForTest[i] = cap(serializedKeys[i])
		}
	}

	err = serializeKeysImpl(
		typeCtx,
		chk,
		tps,
		buildKeyIndexs,
		usedRows,
		filterVector,
		nullVector,
		serializeModes,
		serializedKeys)
	if err != nil {
		return serializedKeysBuffer, err
	}

	if intest.InTest {
		for i := range serializedKeys {
			if serializedKeyVectorBufferCapsForTest[i] < cap(serializedKeys[i]) {
				panic(fmt.Sprintf("Before: %d, After: %d", serializedKeyVectorBufferCapsForTest[i], cap(serializedKeys[i])))
			}
		}
	}

	return serializedKeysBuffer, nil
}

// HashChunkColumns writes the encoded value of each row's column, which of index `colIdx`, to h.
func HashChunkColumns(typeCtx types.Context, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte, isNull []bool) (err error) {
	return HashChunkSelected(typeCtx, h, chk, tp, colIdx, buf, isNull, nil, false)
}

// HashChunkSelected writes the encoded value of selected row's column, which of index `colIdx`, to h.
// sel indicates which rows are selected. If it is nil, all rows are selected.
func HashChunkSelected(typeCtx types.Context, h []hash.Hash64, chk *chunk.Chunk, tp *types.FieldType, colIdx int, buf []byte,
	isNull, sel []bool, ignoreNull bool) (err error) {
	var b []byte
	column := chk.Column(colIdx)
	rows := chk.NumRows()
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		i64s := column.Int64s()
		for i, v := range i64s {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = uvarintFlag
				if !mysql.HasUnsignedFlag(tp.GetFlag()) && v < 0 {
					buf[0] = varintFlag
				}
				b = column.GetRaw(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeFloat:
		f32s := column.Float32s()
		for i, f := range f32s {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = floatFlag
				d := float64(f)
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if d == 0 {
					d = 0
				}
				b = unsafe.Slice((*byte)(unsafe.Pointer(&d)), sizeFloat64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDouble:
		f64s := column.Float64s()
		for i := range f64s {
			f := f64s[i]
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = floatFlag
				// For negative zero. In memory, 0 is [0, 0, 0, 0, 0, 0, 0, 0] and -0 is [0, 0, 0, 0, 0, 0, 0, 128].
				// It makes -0's hash val different from 0's.
				if f == 0 {
					f = 0
				}
				b = unsafe.Slice((*byte)(unsafe.Pointer(&f)), sizeFloat64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		collator := collate.GetCollator(tp.GetCollate())
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				b = column.GetBytes(i)
				b = collator.ImmutableKey(string(hack.String(b)))
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		ts := column.Times()
		for i, t := range ts {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = uintFlag

				var v uint64
				v, err = t.ToPackedUint()
				if err != nil {
					return
				}
				b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeDuration:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = durationFlag
				// duration may have negative value, so we cannot use String to encode directly.
				b = column.GetRaw(i)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeNewDecimal:
		ds := column.Decimals()
		for i, d := range ds {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = decimalFlag
				// If hash is true, we only consider the original value of this decimal and ignore it's precision.
				b, err = d.ToHashKey()
				if err != nil {
					return
				}
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeEnum:
		var collator collate.Collator
		if !mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			collator = collate.GetCollator(tp.GetCollate())
		}
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
				buf[0] = uvarintFlag
				v := column.GetEnum(i).Value
				b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
			} else {
				buf[0] = compactBytesFlag
				v := column.GetEnum(i).Value
				str := ""
				if enum, err := types.ParseEnumValue(tp.GetElems(), v); err == nil {
					// str will be empty string if v out of definition of enum.
					str = enum.Name
				}
				b = collator.ImmutableKey(str)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeSet:
		collator := collate.GetCollator(tp.GetCollate())
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = compactBytesFlag
				s, err := types.ParseSetValue(tp.GetElems(), column.GetSet(i).Value)
				if err != nil {
					return err
				}
				b = collator.ImmutableKey(s.Name)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeBit:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
				buf[0] = uvarintFlag
				v, err1 := types.BinaryLiteral(column.GetBytes(i)).ToInt(typeCtx)
				terror.Log(errors.Trace(err1))
				b = unsafe.Slice((*byte)(unsafe.Pointer(&v)), sizeUint64)
			}

			// As the golang doc described, `Hash.Write` never returns an error.
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeJSON:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = jsonFlag
				json := column.GetJSON(i)
				b = b[:0]
				b = json.HashValue(b)
			}

			// As the golang doc described, `Hash.Write` never returns an error..
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeTiDBVectorFloat32:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			if column.IsNull(i) {
				buf[0], b = NilFlag, nil
				isNull[i] = !ignoreNull
			} else {
				buf[0] = vectorFloat32Flag
				v := column.GetVectorFloat32(i)
				b = v.SerializeTo(nil)
			}

			// As the golang doc described, `Hash.Write` never returns an error..
			// See https://golang.org/pkg/hash/#Hash
			_, _ = h[i].Write(buf)
			_, _ = h[i].Write(b)
		}
	case mysql.TypeNull:
		for i := range rows {
			if sel != nil && !sel[i] {
				continue
			}
			isNull[i] = !ignoreNull
			buf[0] = NilFlag
			_, _ = h[i].Write(buf)
		}
	default:
		return errors.Errorf("unsupport column type for encode %d", tp.GetType())
	}
	return
}

// HashChunkRow writes the encoded values to w.
// If two rows are logically equal, it will generate the same bytes.
func HashChunkRow(typeCtx types.Context, w io.Writer, row chunk.Row, allTypes []*types.FieldType, colIdx []int, buf []byte) (err error) {
	var b []byte
	for i, idx := range colIdx {
		buf[0], b, err = encodeHashChunkRowIdx(typeCtx, row, allTypes[i], idx)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = w.Write(buf)
		if err != nil {
			return
		}
		_, err = w.Write(b)
		if err != nil {
			return
		}
	}
	return err
}

// EqualChunkRow returns a boolean reporting whether row1 and row2
// with their types and column index are logically equal.
func EqualChunkRow(typeCtx types.Context,
	row1 chunk.Row, allTypes1 []*types.FieldType, colIdx1 []int,
	row2 chunk.Row, allTypes2 []*types.FieldType, colIdx2 []int,
) (bool, error) {
	if len(colIdx1) != len(colIdx2) {
		return false, errors.Errorf("Internal error: Hash columns count mismatch, col1: %d, col2: %d", len(colIdx1), len(colIdx2))
	}
	for i := range colIdx1 {
		idx1, idx2 := colIdx1[i], colIdx2[i]
		flag1, b1, err := encodeHashChunkRowIdx(typeCtx, row1, allTypes1[i], idx1)
		if err != nil {
			return false, errors.Trace(err)
		}
		flag2, b2, err := encodeHashChunkRowIdx(typeCtx, row2, allTypes2[i], idx2)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !(flag1 == flag2 && bytes.Equal(b1, b2)) {
			return false, nil
		}
	}
	return true, nil
}
