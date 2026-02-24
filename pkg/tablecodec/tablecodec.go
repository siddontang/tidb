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

package tablecodec

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/tikv/client-go/v2/tikv"
)

var (
	errInvalidKey       = dbterror.ClassXEval.NewStd(errno.ErrInvalidKey)
	errInvalidRecordKey = dbterror.ClassXEval.NewStd(errno.ErrInvalidRecordKey)
	errInvalidIndexKey  = dbterror.ClassXEval.NewStd(errno.ErrInvalidIndexKey)
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
	metaPrefix      = []byte{'m'}
)

const (
	idLen     = 8
	prefixLen = 1 + idLen /*tableID*/ + 2
	// RecordRowKeyLen is public for calculating average row size.
	RecordRowKeyLen       = prefixLen + idLen /*handle*/
	tablePrefixLength     = 1
	recordPrefixSepLength = 2
	metaPrefixLength      = 1
	// MaxOldEncodeValueLen is the maximum len of the old encoding of index value.
	MaxOldEncodeValueLen = 9

	// CommonHandleFlag is the flag used to decode the common handle in an unique index value.
	CommonHandleFlag byte = 127
	// PartitionIDFlag is the flag used to decode the partition ID.
	// Used in both global index values and global index keys (for V1+ non-unique indexes).
	// In keys: PartitionIDFlag + partition_id (8 bytes) + inner_handle_encoded (IntHandle)
	// In values: PartitionIDFlag + partition_id (8 bytes)
	PartitionIDFlag byte = 126
	// IndexVersionFlag is the flag used to decode the index's version info.
	IndexVersionFlag byte = 125
	// RestoreDataFlag is the flag that RestoreData begin with.
	// See rowcodec.Encoder.Encode and rowcodec.row.toBytes
	RestoreDataFlag byte = rowcodec.CodecVer
)

// TableSplitKeyLen is the length of key 't{table_id}' which is used for table split.
const TableSplitKeyLen = 1 + idLen

func init() {
	// help kv package to refer the tablecodec package to resolve the kv.Key functions.
	kv.DecodeTableIDFunc = func(key kv.Key) int64 {
		//preCheck, avoid the noise error log.
		if hasTablePrefix(key) && len(key) >= TableSplitKeyLen {
			return DecodeTableID(key)
		}
		return 0
	}
}

// TablePrefix returns table's prefix 't'.
func TablePrefix() []byte {
	return tablePrefix
}

// MetaPrefix returns meta prefix 'm'.
func MetaPrefix() []byte {
	return metaPrefix
}

// EncodeRowKey encodes the table id and record handle into a kv.Key
func EncodeRowKey(tableID int64, encodedHandle []byte) kv.Key {
	buf := make([]byte, 0, prefixLen+len(encodedHandle))
	buf = appendTableRecordPrefix(buf, tableID)
	buf = append(buf, encodedHandle...)
	return buf
}

// EncodeRowKeyWithHandle encodes the table id, row handle into a kv.Key
func EncodeRowKeyWithHandle(tableID int64, handle kv.Handle) kv.Key {
	return EncodeRowKey(tableID, handle.Encoded())
}

// CutRowKeyPrefix cuts the row key prefix.
func CutRowKeyPrefix(key kv.Key) []byte {
	return key[prefixLen:]
}

// EncodeRecordKey encodes the recordPrefix, row handle into a kv.Key.
func EncodeRecordKey(recordPrefix kv.Key, h kv.Handle) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+h.Len())
	if ph, ok := h.(kv.PartitionHandle); ok {
		recordPrefix = GenTableRecordPrefix(ph.PartitionID)
	}
	buf = append(buf, recordPrefix...)
	buf = append(buf, h.Encoded()...)
	return buf
}

func hasTablePrefix(key kv.Key) bool {
	return key[0] == tablePrefix[0]
}

func hasRecordPrefixSep(key kv.Key) bool {
	return key[0] == recordPrefixSep[0] && key[1] == recordPrefixSep[1]
}

// DecodeRecordKey decodes the key and gets the tableID, handle.
func DecodeRecordKey(key kv.Key) (tableID int64, handle kv.Handle, err error) {
	if len(key) <= prefixLen {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}

	k := key
	if !hasTablePrefix(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[tablePrefixLength:]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	if !hasRecordPrefixSep(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[recordPrefixSepLength:]
	if len(key) == 8 {
		var intHandle int64
		key, intHandle, err = codec.DecodeInt(key)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		return tableID, kv.IntHandle(intHandle), nil
	}
	h, err := kv.NewCommonHandle(key)
	if err != nil {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q %v", k, err)
	}
	return tableID, h, nil
}

// DecodeIndexKey decodes the key and gets the tableID, indexID, indexValues.
func DecodeIndexKey(key kv.Key) (tableID int64, indexID int64, indexValues []string, err error) {
	k := key

	tableID, indexID, isRecord, err := DecodeKeyHead(key)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	if isRecord {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q", k)
		return 0, 0, nil, err
	}
	indexKey := key[prefixLen+idLen:]
	indexValues, err = DecodeValuesBytesToStrings(indexKey)
	if err != nil {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, err)
		return 0, 0, nil, err
	}
	return tableID, indexID, indexValues, nil
}

// DecodeValuesBytesToStrings decode the raw bytes to strings for each columns.
// FIXME: Without the schema information, we can only decode the raw kind of
// the column. For instance, MysqlTime is internally saved as uint64.
func DecodeValuesBytesToStrings(b []byte) ([]string, error) {
	var datumValues []string
	for len(b) > 0 {
		remain, d, e := codec.DecodeOne(b)
		if e != nil {
			return nil, e
		}
		str, e1 := d.ToString()
		if e1 != nil {
			return nil, e
		}
		datumValues = append(datumValues, str)
		b = remain
	}
	return datumValues, nil
}

// EncodeMetaKey encodes the key and field into meta key.
func EncodeMetaKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, len(metaPrefix)+codec.EncodedBytesLength(len(key))+8+codec.EncodedBytesLength(len(field)))
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(structure.HashData))
	ek = codec.EncodeBytes(ek, field)
	return ek
}

// EncodeMetaKeyPrefix encodes the key prefix into meta key
func EncodeMetaKeyPrefix(key []byte) kv.Key {
	ek := make([]byte, 0, len(metaPrefix)+codec.EncodedBytesLength(len(key))+8)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(structure.HashData))
	return ek
}

// DecodeMetaKey decodes the key and get the meta key and meta field.
func DecodeMetaKey(ek kv.Key) (key []byte, field []byte, err error) {
	var tp uint64
	if !bytes.HasPrefix(ek, metaPrefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}
	ek = ek[metaPrefixLength:]
	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if structure.TypeFlag(tp) != structure.HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}
	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, errors.Trace(err)
}

// DecodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key kv.Key) (tableID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !key.HasPrefix(tablePrefix) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if key.HasPrefix(recordPrefixSep) {
		isRecordKey = true
		return
	}
	if !key.HasPrefix(indexPrefixSep) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(indexPrefixSep):]

	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// DecodeIndexID decodes indexID from the key.
// this method simply extract index id part, and no other checking.
// Caller should make sure the key is an index key.
func DecodeIndexID(key kv.Key) (int64, error) {
	key = key[len(tablePrefix)+8+len(indexPrefixSep):]

	_, indexID, err := codec.DecodeInt(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return indexID, nil
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func DecodeTableID(key kv.Key) int64 {
	if !key.HasPrefix(tablePrefix) {
		// If the key is in API V2, then ignore the prefix
		_, k, err := tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)
		if err != nil {
			terror.Log(errors.Trace(err))
			return 0
		}
		key = k
		if !key.HasPrefix(tablePrefix) {
			return 0
		}
	}
	key = key[len(tablePrefix):]
	_, tableID, err := codec.DecodeInt(key)
	// TODO: return error.
	terror.Log(errors.Trace(err))
	return tableID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (kv.Handle, error) {
	// In the read path, remove the keyspace prefix
	// to ensure compatibility with the key parsing implemented in the mock.
	tempKey := rowcodec.RemoveKeyspacePrefix(key)

	if len(tempKey) < RecordRowKeyLen || !hasTablePrefix(tempKey) || !hasRecordPrefixSep(tempKey[prefixLen-2:]) {
		return kv.IntHandle(0), errInvalidKey.GenWithStack("invalid key - %q", tempKey)
	}
	if len(tempKey) == RecordRowKeyLen {
		u := binary.BigEndian.Uint64(tempKey[prefixLen:])
		return kv.IntHandle(codec.DecodeCmpUintToInt(u)), nil
	}
	return kv.NewCommonHandle(tempKey[prefixLen:])
}

// EncodeValue encodes a go value to bytes.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
func EncodeValue(loc *time.Location, b []byte, raw types.Datum) ([]byte, error) {
	var v types.Datum
	err := flatten(loc, raw, &v)
	if err != nil {
		return nil, err
	}

	val, err := codec.EncodeValue(loc, b, v)

	return val, err
}

// EncodeRow encode row data and column ids into a slice of byte.
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
func EncodeRow(loc *time.Location, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum, checksum rowcodec.Checksum, e *rowcodec.Encoder) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	if e.Enable {
		valBuf = valBuf[:0]
		return e.Encode(loc, colIDs, row, checksum, valBuf)
	}
	return EncodeOldRow(loc, row, colIDs, valBuf, values)
}

// EncodeOldRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeOldRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeOldRow will allocate it.
func EncodeOldRow(loc *time.Location, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	valBuf = valBuf[:0]
	if values == nil {
		values = make([]types.Datum, len(row)*2)
	}
	for i, c := range row {
		id := colIDs[i]
		values[2*i].SetInt64(id)
		err := flatten(loc, c, &values[2*i+1])
		if err != nil {
			return valBuf, errors.Trace(err)
		}
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return append(valBuf, codec.NilFlag), nil
	}
	return codec.EncodeValue(loc, valBuf, values...)
}

func flatten(loc *time.Location, data types.Datum, ret *types.Datum) error {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type() == mysql.TypeTimestamp && loc != nil && loc != time.UTC {
			err := t.ConvertTimeZone(loc, time.UTC)
			if err != nil {
				return errors.Trace(err)
			}
		}
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return errors.Trace(err)
	case types.KindMysqlDuration:
		// for mysql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.KindMysqlEnum:
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.KindMysqlSet:
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(types.StrictContext)
		if err != nil {
			return errors.Trace(err)
		}
		ret.SetUint64(val)
		return nil
	default:
		*ret = data
		return nil
	}
}

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	colDatum, err := Unflatten(d, ft, loc)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return colDatum, nil
}

// DecodeColumnValueWithDatum decodes data to an existing Datum according to the column info.
func DecodeColumnValueWithDatum(data []byte, ft *types.FieldType, loc *time.Location, result *types.Datum) error {
	var err error
	_, *result, err = codec.DecodeOne(data)
	if err != nil {
		return errors.Trace(err)
	}
	*result, err = Unflatten(*result, ft, loc)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DecodeRowWithMapNew decode a row to datum map.
func DecodeRowWithMapNew(b []byte, cols map[int64]*types.FieldType,
	loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}

	reqCols := make([]rowcodec.ColInfo, len(cols))
	var idx int
	for id, tp := range cols {
		reqCols[idx] = rowcodec.ColInfo{
			ID: id,
			Ft: tp,
		}
		idx++
	}
	rd := rowcodec.NewDatumMapDecoder(reqCols, loc)
	return rd.DecodeToDatumMap(b, row)
}

// DecodeRowWithMap decodes a byte slice into datums with an existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWithMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v, err = Unflatten(v, ft, loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row[id] = v
			cnt++
			if cnt == len(cols) {
				// Get enough data.
				break
			}
		}
	}
	return row, nil
}

// DecodeRowToDatumMap decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
// Default value columns, generated columns and handle columns are unprocessed.
func DecodeRowToDatumMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, error) {
	if !rowcodec.IsNewFormat(b) {
		return DecodeRowWithMap(b, cols, loc, nil)
	}
	return DecodeRowWithMapNew(b, cols, loc, nil)
}

// DecodeHandleToDatumMap decodes a handle into datum map.
func DecodeHandleToDatumMap(handle kv.Handle, handleColIDs []int64,
	cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if handle == nil || len(handleColIDs) == 0 {
		return row, nil
	}
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	for idx, id := range handleColIDs {
		ft, ok := cols[id]
		if !ok {
			continue
		}
		if types.NeedRestoredData(ft) {
			continue
		}
		d, err := decodeHandleToDatum(handle, ft, idx)
		if err != nil {
			return row, err
		}
		d, err = Unflatten(d, ft, loc)
		if err != nil {
			return row, err
		}
		if _, exists := row[id]; !exists {
			row[id] = d
		}
	}
	return row, nil
}

// decodeHandleToDatum decodes a handle to a specific column datum.
func decodeHandleToDatum(handle kv.Handle, ft *types.FieldType, idx int) (types.Datum, error) {
	var d types.Datum
	var err error
	if handle.IsInt() {
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			d = types.NewUintDatum(uint64(handle.IntValue()))
		} else {
			d = types.NewIntDatum(handle.IntValue())
		}
		return d, nil
	}
	// Decode common handle to Datum.
	_, d, err = codec.DecodeOne(handle.EncodedCol(idx))
	return d, err
}

// CutRowNew cuts encoded row into byte slices and return columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRowNew(data []byte, colIDs map[int64]int) ([][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil, nil
	}

	var (
		cnt int
		b   []byte
		err error
		cid int64
	)
	row := make([][]byte, len(colIDs))
	for len(data) > 0 && cnt < len(colIDs) {
		// Get col id.
		data, cid, err = codec.CutColumnID(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Get col value.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		offset, ok := colIDs[cid]
		if ok {
			row[offset] = b
			cnt++
		}
	}
	return row, nil
}

// UnflattenDatums converts raw datums to column datums.
func UnflattenDatums(datums []types.Datum, fts []*types.FieldType, loc *time.Location) ([]types.Datum, error) {
	for i, datum := range datums {
		ft := fts[i]
		uDatum, err := Unflatten(datum, ft, loc)
		if err != nil {
			return datums, errors.Trace(err)
		}
		datums[i] = uDatum
	}
	return datums, nil
}

// Unflatten converts a raw datum to a column datum.
func Unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	if datum.IsNull() {
		return datum, nil
	}
	switch ft.GetType() {
	case mysql.TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		datum.SetString(datum.GetString(), ft.GetCollate())
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble:
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.GetType(), ft.GetDecimal())
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		if ft.GetType() == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return datum, errors.Trace(err)
			}
		}
		datum.SetUint64(0)
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration: // duration should read fsp from column meta data
		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: ft.GetDecimal()}
		datum.SetMysqlDuration(dur)
		return datum, nil
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.GetElems(), datum.GetUint64())
		if err != nil {
			enum = types.Enum{}
		}
		datum.SetMysqlEnum(enum, ft.GetCollate())
		return datum, nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.GetElems(), datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlSet(set, ft.GetCollate())
		return datum, nil
	case mysql.TypeBit:
		val := datum.GetUint64()
		byteSize := (ft.GetFlen() + 7) >> 3
		datum.SetUint64(0)
		datum.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	return datum, nil
}

// EncodeIndexSeekKey encodes an index value to kv.Key.
func EncodeIndexSeekKey(tableID int64, idxID int64, encodedValue []byte) kv.Key {
	key := make([]byte, 0, RecordRowKeyLen+len(encodedValue))
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key kv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte, len(colIDs))
	for _, id := range colIDs {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values[id] = val
	}
	return
}

// CutIndexPrefix cuts the index prefix.
func CutIndexPrefix(key kv.Key) []byte {
	return key[prefixLen+idLen:]
}

// CutIndexKeyTo cuts encoded index key into colIDs to bytes slices.
// The caller should prepare the memory of the result values.
func CutIndexKeyTo(key kv.Key, values [][]byte) (b []byte, err error) {
	b = key[prefixLen+idLen:]
	length := len(values)
	for i := range length {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[i] = val
	}
	return
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	values = make([][]byte, length)
	b, err = CutIndexKeyTo(key, values)
	return
}

// CutCommonHandle cuts encoded common handle key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutCommonHandle(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen:]
	values = make([][]byte, 0, length)
	for range length {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// HandleStatus is the handle status in index.
type HandleStatus int

const (
	// HandleDefault means decode handle value as int64 or bytes when DecodeIndexKV.
	HandleDefault HandleStatus = iota
	// HandleIsUnsigned means decode handle value as uint64 when DecodeIndexKV.
	HandleIsUnsigned
	// HandleNotNeeded means no need to decode handle value when DecodeIndexKV.
	HandleNotNeeded
)

// reEncodeHandle encodes the handle as a Datum so it can be properly decoded later.
// If it is common handle, it returns the encoded column values.
// If it is int handle, it is encoded as int Datum or uint Datum decided by the unsigned.
func reEncodeHandle(handle kv.Handle, unsigned bool) ([][]byte, error) {
	handleColLen := 1
	if !handle.IsInt() {
		handleColLen = handle.NumCols()
	}
	result := make([][]byte, 0, handleColLen)
	return reEncodeHandleTo(handle, unsigned, nil, result)
}

func reEncodeHandleTo(handle kv.Handle, unsigned bool, buf []byte, result [][]byte) ([][]byte, error) {
	if !handle.IsInt() {
		handleColLen := handle.NumCols()
		for i := range handleColLen {
			result = append(result, handle.EncodedCol(i))
		}
		return result, nil
	}
	handleDatum := types.NewIntDatum(handle.IntValue())
	if unsigned {
		handleDatum.SetUint64(handleDatum.GetUint64())
	}
	intHandleBytes, err := codec.EncodeValue(time.UTC, buf, handleDatum)
	result = append(result, intHandleBytes)
	return result, err
}

// reEncodeHandleConsiderNewCollation encodes the handle as a Datum so it can be properly decoded later.
func reEncodeHandleConsiderNewCollation(handle kv.Handle, columns []rowcodec.ColInfo, restoreData []byte) ([][]byte, error) {
	handleColLen := handle.NumCols()
	cHandleBytes := make([][]byte, 0, handleColLen)
	for i := range handleColLen {
		cHandleBytes = append(cHandleBytes, handle.EncodedCol(i))
	}
	if len(restoreData) == 0 {
		return cHandleBytes, nil
	}
	// Remove some extra columns(ID < 0), such like `model.ExtraPhysTblID`.
	// They are not belong to common handle and no need to restore data.
	idx := len(columns)
	for idx > 0 && columns[idx-1].ID < 0 {
		idx--
	}
	return decodeRestoredValuesV5(columns[:idx], cHandleBytes, restoreData)
}

func decodeRestoredValues(columns []rowcodec.ColInfo, restoredVal []byte) ([][]byte, error) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}
	// We don't need to decode handle here, and colIDs >= 0 always.
	rd := rowcodec.NewByteDecoder(columns, []int64{-1}, nil, nil)
	resultValues, err := rd.DecodeToBytesNoHandle(colIDs, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resultValues, nil
}

// decodeRestoredValuesV5 decodes index values whose format is introduced in TiDB 5.0.
// Unlike the format in TiDB 4.0, the new format is optimized for storage space:
// 1. If the index is a composed index, only the non-binary string column's value need to write to value, not all.
// 2. If a string column's collation is _bin, then we only write the number of the truncated spaces to value.
// 3. If a string column is char, not varchar, then we use the sortKey directly.
func decodeRestoredValuesV5(columns []rowcodec.ColInfo, results [][]byte, restoredVal []byte) ([][]byte, error) {
	colIDOffsets := buildColumnIDOffsets(columns)
	colInfosNeedRestore := buildRestoredColumn(columns)
	rd := rowcodec.NewByteDecoder(colInfosNeedRestore, nil, nil, nil)
	newResults, err := rd.DecodeToBytesNoHandle(colIDOffsets, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := range newResults {
		noRestoreData := len(newResults[i]) == 0
		if noRestoreData {
			newResults[i] = results[i]
			continue
		}
		if collate.IsBinCollation(columns[i].Ft.GetCollate()) {
			noPaddingDatum, err := DecodeColumnValue(results[i], columns[i].Ft, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			paddingCountDatum, err := DecodeColumnValue(newResults[i], types.NewFieldType(mysql.TypeLonglong), nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			noPaddingStr, paddingCount := noPaddingDatum.GetString(), int(paddingCountDatum.GetInt64())
			// Skip if padding count is 0.
			if paddingCount == 0 {
				newResults[i] = results[i]
				continue
			}
			newDatum := &noPaddingDatum
			newDatum.SetString(noPaddingStr+strings.Repeat(" ", paddingCount), newDatum.Collation())
			newResults[i] = newResults[i][:0]
			newResults[i] = append(newResults[i], rowcodec.BytesFlag)
			newResults[i] = codec.EncodeBytes(newResults[i], newDatum.GetBytes())
		}
	}
	return newResults, nil
}

func buildColumnIDOffsets(allCols []rowcodec.ColInfo) map[int64]int {
	colIDOffsets := make(map[int64]int, len(allCols))
	for i, col := range allCols {
		colIDOffsets[col.ID] = i
	}
	return colIDOffsets
}

func buildRestoredColumn(allCols []rowcodec.ColInfo) []rowcodec.ColInfo {
	restoredColumns := make([]rowcodec.ColInfo, 0, len(allCols))
	for i, col := range allCols {
		if !types.NeedRestoredData(col.Ft) {
			continue
		}
		copyColInfo := rowcodec.ColInfo{
			ID: col.ID,
		}
		if collate.IsBinCollation(col.Ft.GetCollate()) {
			// Change the fieldType from string to uint since we store the number of the truncated spaces.
			// NOTE: the corresponding datum is generated as `types.NewUintDatum(paddingSize)`, and the raw data is
			// encoded via `encodeUint`. Thus we should mark the field type as unsigened here so that the BytesDecoder
			// can decode it correctly later. Otherwise there might be issues like #47115.
			copyColInfo.Ft = types.NewFieldType(mysql.TypeLonglong)
			copyColInfo.Ft.AddFlag(mysql.UnsignedFlag)
		} else {
			copyColInfo.Ft = allCols[i].Ft
		}
		restoredColumns = append(restoredColumns, copyColInfo)
	}
	return restoredColumns
}

func decodeIndexKvOldCollation(key, value []byte, hdStatus HandleStatus, buf []byte, resultValues [][]byte) ([][]byte, error) {
	b, err := CutIndexKeyTo(key, resultValues)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}
	var handle kv.Handle
	if len(b) > 0 {
		// non-unique index
		handle, err = decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// In unique int handle index.
		handle = DecodeIntHandleInIndexValue(value)
		resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return resultValues, nil
}

func getIndexVersion(value []byte) int {
	if len(value) <= MaxOldEncodeValueLen {
		return 0
	}
	tailLen := int(value[0])
	if (tailLen == 0 || tailLen == 1) && value[1] == IndexVersionFlag {
		return int(value[2])
	}
	return 0
}

// DecodeIndexKVEx looks like DecodeIndexKV, the difference is that it tries to reduce allocations.
func DecodeIndexKVEx(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo, buf []byte, preAlloc [][]byte) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		return decodeIndexKvOldCollation(key, value, hdStatus, buf, preAlloc)
	}
	if getIndexVersion(value) == 1 {
		return decodeIndexKvForClusteredIndexVersion1(key, value, colsLen, hdStatus, columns)
	}
	return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)
}

// DecodeIndexKV uses to decode index key values.
//
//	`colsLen` is expected to be index columns count.
//	`columns` is expected to be index columns + handle columns(if hdStatus is not HandleNotNeeded).
func DecodeIndexKV(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		preAlloc := make([][]byte, colsLen, colsLen+len(columns))
		return decodeIndexKvOldCollation(key, value, hdStatus, nil, preAlloc)
	}
	if getIndexVersion(value) == 1 {
		return decodeIndexKvForClusteredIndexVersion1(key, value, colsLen, hdStatus, columns)
	}
	return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)
}

// DecodeIndexHandle uses to decode the handle from index key/value.
func DecodeIndexHandle(key, value []byte, colsLen int) (kv.Handle, error) {
	var err error
	b := key[prefixLen+idLen:]
	for range colsLen {
		_, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if len(b) > 0 {
		handle, err := decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		// If len(value) >= 9, it may contain partition id.
		// We should decode it and return a partition handle.
		if len(value) >= 9 {
			seg := SplitIndexValue(value)
			if len(seg.PartitionID) != 0 {
				_, pid, err := codec.DecodeInt(seg.PartitionID)
				if err != nil {
					return nil, err
				}
				// For GlobalIndexVersionV1+, the handle from the key may already be a
				// PartitionHandle (partition ID encoded in key). To avoid creating a
				// nested PartitionHandle, extract the inner handle first.
				// For V1: use partition ID from value (authoritative source).
				// TODO: For V2+, use partition ID from key (PartitionHandle) instead.
				if ph, ok := handle.(kv.PartitionHandle); ok {
					handle = ph.Handle
				}
				handle = kv.NewPartitionHandle(pid, handle)
			}
		}
		return handle, nil
	} else if len(value) >= 8 {
		return DecodeHandleInIndexValue(value)
	}
	// Should never execute to here.
	return nil, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

func decodeHandleInIndexKey(keySuffix []byte) (kv.Handle, error) {
	// Check if this is a PartitionHandle (for global non-unique indexes V1+)
	if len(keySuffix) > 0 && keySuffix[0] == PartitionIDFlag {
		// Format: PartitionIDFlag + partition_id (8 bytes) + inner_handle
		keySuffix = keySuffix[1:] // Skip the flag
		remain, partID, err := codec.DecodeInt(keySuffix)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Decode the inner handle
		innerHandle, err := decodeHandleInIndexKey(remain)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return kv.NewPartitionHandle(partID, innerHandle), nil
	}

	remain, d, err := codec.DecodeOne(keySuffix)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(remain) == 0 && d.Kind() == types.KindInt64 {
		return kv.IntHandle(d.GetInt64()), nil
	}
	return kv.NewCommonHandle(keySuffix)
}

// DecodeHandleInIndexValue decodes handle in unqiue index value.
func DecodeHandleInIndexValue(value []byte) (handle kv.Handle, err error) {
	if len(value) <= MaxOldEncodeValueLen {
		return DecodeIntHandleInIndexValue(value), nil
	}
	seg := SplitIndexValue(value)
	if len(seg.IntHandle) != 0 {
		handle = DecodeIntHandleInIndexValue(seg.IntHandle)
	}
	if len(seg.CommonHandle) != 0 {
		handle, err = kv.NewCommonHandle(seg.CommonHandle)
		if err != nil {
			return nil, err
		}
	}
	if len(seg.PartitionID) != 0 {
		_, pid, err := codec.DecodeInt(seg.PartitionID)
		if err != nil {
			return nil, err
		}
		handle = kv.NewPartitionHandle(pid, handle)
	}
	return handle, nil
}

// DecodeIntHandleInIndexValue uses to decode index value as int handle id.
func DecodeIntHandleInIndexValue(data []byte) kv.Handle {
	return kv.IntHandle(binary.BigEndian.Uint64(data))
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	key := make([]byte, 0, prefixLen+idLen)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes the table prefix to generate a key
func EncodeTablePrefix(tableID int64) kv.Key {
	key := make([]byte, 0, tablePrefixLength+idLen)
	key = append(key, tablePrefix...)
	key = codec.EncodeInt(key, tableID)
	return key
}

// appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// GenTableRecordPrefix composes record prefix with tableID: "t[tableID]_r".
func GenTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, tableID)
}

// GenTableIndexPrefix composes index prefix with tableID: "t[tableID]_i".
func GenTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, tableID)
}

// IsRecordKey is used to check whether the key is an record key.
func IsRecordKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'r'
}

// IsIndexKey is used to check whether the key is an index key.
func IsIndexKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'i'
}

// IsTableKey is used to check whether the key is a table key.
func IsTableKey(k []byte) bool {
	return len(k) == 9 && k[0] == 't'
}

// IsUntouchedIndexKValue uses to check whether the key is index key, and the value is untouched,
// since the untouched index key/value is no need to commit.
func IsUntouchedIndexKValue(k, v []byte) bool {
	if !IsIndexKey(k) {
		return false
	}
	vLen := len(v)
	if IsTempIndexKey(k) {
		return vLen > 0 && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	if vLen <= MaxOldEncodeValueLen {
		return (vLen == 1 || vLen == 9) && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	// New index value format
	tailLen := int(v[0])
	if tailLen < 8 {
		// Non-unique index.
		return tailLen >= 1 && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	// Unique index
	return tailLen == 9
}

// GenTablePrefix composes table record and index prefix: "t[tableID]".
func GenTablePrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	return buf
}

// TruncateToRowKeyLen truncates the key to row key length if the key is longer than row key.
func TruncateToRowKeyLen(key kv.Key) kv.Key {
	if len(key) > RecordRowKeyLen {
		return key[:RecordRowKeyLen]
	}
	return key
}

// GetTableHandleKeyRange returns table handle's key range with tableID.
func GetTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	startKey = EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MinInt64))
	endKey = EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxInt64))
	return
}

// GetTableIndexKeyRange returns table index's key range with tableID and indexID.
func GetTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	startKey = EncodeIndexSeekKey(tableID, indexID, nil)
	endKey = EncodeIndexSeekKey(tableID, indexID, []byte{255})
	return
}

// GetIndexKeyBuf reuse or allocate buffer
func GetIndexKeyBuf(buf []byte, defaultCap int) []byte {
	if buf != nil {
		return buf[:0]
	}
	return make([]byte, 0, defaultCap)
}
