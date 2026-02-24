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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package tables

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// RowWithCols is used to get the corresponding column datum values with the given handle.
func RowWithCols(t table.Table, ctx sessionctx.Context, h kv.Handle, cols []*table.Column) ([]types.Datum, error) {
	// Get raw row data from kv.
	key := tablecodec.EncodeRecordKey(t.RecordPrefix(), h)
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	value, err := kv.GetValue(context.TODO(), txn, key)
	if err != nil {
		return nil, err
	}
	v, _, err := DecodeRawRowData(ctx.GetExprCtx(), t.Meta(), h, cols, value)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func containFullColInHandle(meta *model.TableInfo, col *table.Column) (containFullCol bool, idxInHandle int) {
	pkIdx := FindPrimaryIndex(meta)
	for i, idxCol := range pkIdx.Columns {
		if meta.Columns[idxCol.Offset].ID == col.ID {
			idxInHandle = i
			containFullCol = idxCol.Length == types.UnspecifiedLength
			return
		}
	}
	return
}

// DecodeRawRowData decodes raw row data into a datum slice and a (columnID:columnValue) map.
func DecodeRawRowData(ctx expression.BuildContext, meta *model.TableInfo, h kv.Handle, cols []*table.Column,
	value []byte) ([]types.Datum, map[int64]types.Datum, error) {
	v := make([]types.Datum, len(cols))
	colTps := make(map[int64]*types.FieldType, len(cols))
	prefixCols := make(map[int64]struct{})
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) {
			if mysql.HasUnsignedFlag(col.GetFlag()) {
				v[i].SetUint64(uint64(h.IntValue()))
			} else {
				v[i].SetInt64(h.IntValue())
			}
			continue
		}
		if col.IsCommonHandleColumn(meta) && !types.NeedRestoredData(&col.FieldType) {
			if containFullCol, idxInHandle := containFullColInHandle(meta, col); containFullCol {
				dtBytes := h.EncodedCol(idxInHandle)
				_, dt, err := codec.DecodeOne(dtBytes)
				if err != nil {
					return nil, nil, err
				}
				dt, err = tablecodec.Unflatten(dt, &col.FieldType, ctx.GetEvalCtx().Location())
				if err != nil {
					return nil, nil, err
				}
				v[i] = dt
				continue
			}
			prefixCols[col.ID] = struct{}{}
		}
		colTps[col.ID] = &col.FieldType
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTps, ctx.GetEvalCtx().Location())
	if err != nil {
		return nil, rowMap, err
	}
	defaultVals := make([]types.Datum, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		if col.IsPKHandleColumn(meta) || (col.IsCommonHandleColumn(meta) && !types.NeedRestoredData(&col.FieldType)) {
			if _, isPrefix := prefixCols[col.ID]; !isPrefix {
				continue
			}
		}
		ri, ok := rowMap[col.ID]
		if ok {
			v[i] = ri
			continue
		}
		if col.IsVirtualGenerated() {
			continue
		}
		if col.ChangeStateInfo != nil {
			v[i], _, err = GetChangingColVal(ctx, cols, col, rowMap, defaultVals)
		} else {
			v[i], err = GetColDefaultValue(ctx, col, defaultVals)
		}
		if err != nil {
			return nil, rowMap, err
		}
	}
	return v, rowMap, nil
}

// GetChangingColVal gets the changing column value when executing "modify/change column" statement.
// For statement like update-where, it will fetch the old row out and insert it into kv again.
// Since update statement can see the writable columns, it is responsible for the casting relative column / get the fault value here.
// old row : a-b-[nil]
// new row : a-b-[a'/default]
// Thus the writable new row is corresponding to Write-Only constraints.
func GetChangingColVal(ctx exprctx.BuildContext, cols []*table.Column, col *table.Column, rowMap map[int64]types.Datum, defaultVals []types.Datum) (_ types.Datum, isDefaultVal bool, err error) {
	relativeCol := cols[col.ChangeStateInfo.DependencyColumnOffset]
	idxColumnVal, ok := rowMap[relativeCol.ID]
	if ok {
		idxColumnVal, err = table.CastColumnValue(ctx, idxColumnVal, col.ColumnInfo, false, false)
		// TODO: Consider sql_mode and the error msg(encounter this error check whether to rollback).
		if err != nil {
			return idxColumnVal, false, errors.Trace(err)
		}
		return idxColumnVal, false, nil
	}

	idxColumnVal, err = GetColDefaultValue(ctx, col, defaultVals)
	if err != nil {
		return idxColumnVal, false, errors.Trace(err)
	}

	return idxColumnVal, true, nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *TableCommon) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	opt := table.NewRemoveRecordOpt(opts...)
	return t.removeRecord(ctx, txn, h, r, opt)
}

func (t *TableCommon) removeRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opt *table.RemoveRecordOpt) error {
	memBuffer := txn.GetMemBuffer()
	sh := memBuffer.Staging()
	defer memBuffer.Cleanup(sh)

	err := t.removeRowData(ctx, txn, h)
	if err != nil {
		return err
	}

	if m := t.Meta(); m.TempTableType != model.TempTableNone {
		if tmpTable, sizeLimit, ok := addTemporaryTable(ctx, m); ok {
			if err = checkTempTableSize(tmpTable, sizeLimit); err != nil {
				return err
			}
			defer handleTempTableSize(tmpTable, txn.Size(), txn)
		}
	}

	// The table has non-public column and this column is doing the operation of "modify/change column".
	// DELETE will use deletable columns, which is the same as the full columns of the table.
	// INSERT and UPDATE will only use the writable columns. So they will not see columns under MODIFY/CHANGE state.
	// This if block is for the INSERT and UPDATE.
	// And, only DELETE will make opt.HasIndexesLayout() to be true currently.
	if !opt.HasIndexesLayout() && len(t.Columns) > len(r) && t.Columns[len(r)].ChangeStateInfo != nil {
		// The changing column datum derived from related column should be casted here.
		// Otherwise, the existed changing indexes will not be deleted.
		relatedColDatum := r[t.Columns[len(r)].ChangeStateInfo.DependencyColumnOffset]
		value, err := table.CastColumnValue(ctx.GetExprCtx(), relatedColDatum, t.Columns[len(r)].ColumnInfo, false, false)
		if err != nil {
			logutil.BgLogger().Info("remove record cast value failed", zap.Any("col", t.Columns[len(r)]),
				zap.String("handle", h.String()), zap.Any("val", relatedColDatum), zap.Error(err))
			return err
		}
		r = append(r, value)
	}
	err = t.removeRowIndices(ctx, txn, h, r, opt)
	if err != nil {
		return err
	}

	if err = injectMutationError(t, txn, sh); err != nil {
		return err
	}
	failpoint.InjectCall("duringTableCommonRemoveRecord", t.meta)

	tc := ctx.GetExprCtx().GetEvalCtx().TypeCtx()
	if ctx.EnableMutationChecker() {
		if err = checkDataConsistency(txn, tc, t, nil, r, memBuffer, sh, opt.GetIndexesLayout()); err != nil {
			return errors.Trace(err)
		}
	}
	memBuffer.Release(sh)

	if s, ok := ctx.GetStatisticsSupport(); ok {
		s.UpdatePhysicalTableDelta(
			t.physicalTableID, -1, 1,
		)
	}
	return err
}

func (t *TableCommon) removeRowData(ctx table.MutateContext, txn kv.Transaction, h kv.Handle) (err error) {
	// Remove row data.
	key := t.RecordKey(h)
	failpoint.Inject("removeRecordForceAssertNotExist", func() {
		// Assert the key doesn't exist while it actually exists. This is helpful to test if assertion takes effect.
		// Since only the first assertion takes effect, set the injected assertion before setting the correct one to
		// override it.
		if ctx.ConnectionID() != 0 {
			logutil.BgLogger().Info("force asserting not exist on RemoveRecord", zap.String("category", "failpoint"), zap.Uint64("startTS", txn.StartTS()))
			if err = setAssertion(txn, key, kv.AssertNotExist); err != nil {
				failpoint.Return(err)
			}
		}
	})
	if t.skipAssert {
		err = setAssertion(txn, key, kv.AssertUnknown)
	} else {
		err = setAssertion(txn, key, kv.AssertExist)
	}
	if err != nil {
		return err
	}
	return txn.Delete(key)
}

// removeRowIndices removes all the indices of a row.
func (t *TableCommon) removeRowIndices(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, rec []types.Datum, opt *table.RemoveRecordOpt) (err error) {
	for _, v := range t.DeletableIndices() {
		if v.Meta().Primary && (t.Meta().IsCommonHandle || t.Meta().PKIsHandle) {
			continue
		}
		if v.Meta().IsColumnarIndex() {
			continue
		}
		intest.AssertFunc(func() bool {
			// if the index is partial index, it shouldn't have index layout.
			return !(opt.HasIndexesLayout() && v.Meta().HasCondition())
		})
		meetPartialCondition, err := v.MeetPartialCondition(rec)
		if err != nil {
			return err
		}
		if !meetPartialCondition {
			// If the partial index condition is not met, we don't need to delete it because
			// it has never been written.
			continue
		}

		var vals []types.Datum
		if opt.HasIndexesLayout() {
			vals, err = fetchIndexRow(v.Meta(), rec, nil, opt.GetIndexLayout(v.Meta().ID))
		} else {
			vals, err = fetchIndexRow(v.Meta(), rec, nil, nil)
		}
		if err != nil {
			logutil.BgLogger().Info("remove row index failed", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()), zap.Any("record", rec), zap.Error(err))
			return err
		}
		if err = v.Delete(ctx, txn, vals, h); err != nil {
			if v.Meta().State != model.StatePublic && kv.ErrNotExist.Equal(err) {
				// If the index is not in public state, we may have not created the index,
				// or already deleted the index, so skip ErrNotExist error.
				logutil.BgLogger().Debug("row index not exists", zap.Any("index", v.Meta()), zap.Uint64("txnStartTS", txn.StartTS()), zap.String("handle", h.String()))
				continue
			}
			return err
		}
	}
	return nil
}

func (t *TableCommon) buildIndexForRow(ctx table.MutateContext, h kv.Handle, vals []types.Datum, newData []types.Datum, idx *index, txn kv.Transaction, untouched bool, opt *table.CreateIdxOpt) error {
	rsData := TryGetHandleRestoredDataWrapper(t.meta, newData, nil, idx.Meta())
	if _, err := idx.create(ctx, txn, vals, h, rsData, untouched, opt); err != nil {
		if kv.ErrKeyExists.Equal(err) {
			// Make error message consistent with MySQL.
			tablecodec.TruncateIndexValues(t.meta, idx.Meta(), vals)
			colStrVals, err1 := genIndexKeyStrs(vals)
			if err1 != nil {
				// if genIndexKeyStrs failed, return the original error.
				return err
			}

			return kv.GenKeyExistsErr(colStrVals, fmt.Sprintf("%s.%s", idx.TableMeta().Name.String(), idx.Meta().Name.String()))
		}
		return err
	}
	return nil
}

// IterRecords iterates records in the table and calls fn.
func IterRecords(t table.Table, ctx sessionctx.Context, cols []*table.Column,
	fn table.RecordIterFunc) error {
	prefix := t.RecordPrefix()
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}

	startKey := tablecodec.EncodeRecordKey(t.RecordPrefix(), kv.IntHandle(math.MinInt64))
	it, err := txn.Iter(startKey, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	logutil.BgLogger().Debug("iterate records", zap.ByteString("startKey", startKey), zap.ByteString("key", it.Key()), zap.ByteString("value", it.Value()))

	colMap := make(map[int64]*types.FieldType, len(cols))
	for _, col := range cols {
		colMap[col.ID] = &col.FieldType
	}
	defaultVals := make([]types.Datum, len(cols))
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return err
		}
		rowMap, err := tablecodec.DecodeRowToDatumMap(it.Value(), colMap, ctx.GetSessionVars().Location())
		if err != nil {
			return err
		}
		pkIds, decodeLoc := TryGetCommonPkColumnIds(t.Meta()), ctx.GetSessionVars().Location()
		data := make([]types.Datum, len(cols))
		for _, col := range cols {
			if col.IsPKHandleColumn(t.Meta()) {
				if mysql.HasUnsignedFlag(col.GetFlag()) {
					data[col.Offset].SetUint64(uint64(handle.IntValue()))
				} else {
					data[col.Offset].SetInt64(handle.IntValue())
				}
				continue
			} else if mysql.HasPriKeyFlag(col.GetFlag()) {
				data[col.Offset], err = tryDecodeColumnFromCommonHandle(col, handle, pkIds, decodeLoc)
				if err != nil {
					return err
				}
				continue
			}
			if _, ok := rowMap[col.ID]; ok {
				data[col.Offset] = rowMap[col.ID]
				continue
			}
			data[col.Offset], err = GetColDefaultValue(ctx.GetExprCtx(), col, defaultVals)
			if err != nil {
				return err
			}
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return err
		}

		rk := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return err
		}
	}

	return nil
}

func tryDecodeColumnFromCommonHandle(col *table.Column, handle kv.Handle, pkIds []int64, decodeLoc *time.Location) (types.Datum, error) {
	for i, hid := range pkIds {
		if hid != col.ID {
			continue
		}
		_, d, err := codec.DecodeOne(handle.EncodedCol(i))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		if d, err = tablecodec.Unflatten(d, &col.FieldType, decodeLoc); err != nil {
			return types.Datum{}, err
		}
		return d, nil
	}
	return types.Datum{}, nil
}

// GetColDefaultValue gets a column default value.
// The defaultVals is used to avoid calculating the default value multiple times.
func GetColDefaultValue(ctx exprctx.BuildContext, col *table.Column, defaultVals []types.Datum) (
	colVal types.Datum, err error) {
	if col.GetOriginDefaultValue() == nil && mysql.HasNotNullFlag(col.GetFlag()) {
		return colVal, errors.New("Miss column")
	}
	if defaultVals[col.Offset].IsNull() {
		colVal, err = table.GetColOriginDefaultValue(ctx, col.ToInfo())
		if err != nil {
			return colVal, err
		}
		defaultVals[col.Offset] = colVal
	} else {
		colVal = defaultVals[col.Offset]
	}

	return colVal, nil
}

// AllocHandle allocate a new handle.
// A statement could reserve some ID in the statement context, try those ones first.
func AllocHandle(ctx context.Context, mctx table.MutateContext, t table.Table) (kv.IntHandle,
	error) {
	if mctx != nil {
		if reserved, ok := mctx.GetReservedRowIDAlloc(); ok {
			// First try to alloc if the statement has reserved auto ID.
			if rowID, ok := reserved.Consume(); ok {
				return kv.IntHandle(rowID), nil
			}
		}
	}

	_, rowID, err := AllocHandleIDs(ctx, mctx, t, 1)
	return kv.IntHandle(rowID), err
}

// AllocHandleIDs allocates n handle ids (_tidb_rowid), and caches the range
// in the table.MutateContext.
func AllocHandleIDs(ctx context.Context, mctx table.MutateContext, t table.Table, n uint64) (int64, int64, error) {
	meta := t.Meta()
	base, maxID, err := t.Allocators(mctx).Get(autoid.RowIDAllocType).Alloc(ctx, n, 1, 1)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	if meta.ShardRowIDBits > 0 {
		shardFmt := autoid.NewShardIDFormat(types.NewFieldType(mysql.TypeLonglong), meta.ShardRowIDBits, autoid.RowIDBitLength)
		// Use max record ShardRowIDBits to check overflow.
		if OverflowShardBits(maxID, meta.MaxShardRowIDBits, autoid.RowIDBitLength, true) {
			// If overflow, the rowID may be duplicated. For examples,
			// t.meta.ShardRowIDBits = 4
			// rowID = 0010111111111111111111111111111111111111111111111111111111111111
			// shard = 0100000000000000000000000000000000000000000000000000000000000000
			// will be duplicated with:
			// rowID = 0100111111111111111111111111111111111111111111111111111111111111
			// shard = 0010000000000000000000000000000000000000000000000000000000000000
			return 0, 0, autoid.ErrAutoincReadFailed
		}
		shard := mctx.GetRowIDShardGenerator().GetCurrentShard(int(n))
		base = shardFmt.Compose(shard, base)
		maxID = shardFmt.Compose(shard, maxID)
	}
	return base, maxID, nil
}

// OverflowShardBits checks whether the recordID overflow `1<<(typeBitsLength-shardRowIDBits-1) -1`.
func OverflowShardBits(recordID int64, shardRowIDBits uint64, typeBitsLength uint64, reservedSignBit bool) bool {
	var signBit uint64
	if reservedSignBit {
		signBit = 1
	}
	mask := (1<<shardRowIDBits - 1) << (typeBitsLength - shardRowIDBits - signBit)
	return recordID&int64(mask) > 0
}

// Allocators implements table.Table Allocators interface.
func (t *TableCommon) Allocators(ctx table.AllocatorContext) autoid.Allocators {
	if ctx == nil {
		return t.allocs
	}
	if alloc, ok := ctx.AlternativeAllocators(t.meta); ok {
		return alloc
	}
	return t.allocs
}

// Type implements table.Table Type interface.
func (t *TableCommon) Type() table.Type {
	return table.NormalTable
}

func (t *TableCommon) canSkip(col *table.Column, value *types.Datum) bool {
	return CanSkip(t.Meta(), col, value)
}

// CanSkip is for these cases, we can skip the columns in encoded row:
// 1. the column is included in primary key;
// 2. the column's default value is null, and the value equals to that but has no origin default;
// 3. the column is virtual generated.
func CanSkip(info *model.TableInfo, col *table.Column, value *types.Datum) bool {
	if col.IsPKHandleColumn(info) {
		return true
	}
	if col.IsCommonHandleColumn(info) {
		pkIdx := FindPrimaryIndex(info)
		for _, idxCol := range pkIdx.Columns {
			if info.Columns[idxCol.Offset].ID != col.ID {
				continue
			}
			canSkip := idxCol.Length == types.UnspecifiedLength
			canSkip = canSkip && !types.NeedRestoredData(&col.FieldType)
			return canSkip
		}
	}
	if col.GetDefaultValue() == nil && value.IsNull() && col.GetOriginDefaultValue() == nil {
		return true
	}
	if col.IsVirtualGenerated() {
		return true
	}
	return false
}

// FindIndexByColName returns a public table index containing only one column named `name`.
func FindIndexByColName(t table.Table, name string) table.Index {
	for _, idx := range t.Indices() {
		// only public index can be read.
		if idx.Meta().State != model.StatePublic {
			continue
		}

		if len(idx.Meta().Columns) == 1 && strings.EqualFold(idx.Meta().Columns[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

func getDuplicateError(tblInfo *model.TableInfo, handle kv.Handle, row []types.Datum) error {
	keyName := tblInfo.Name.String() + ".PRIMARY"

	if handle.IsInt() {
		return kv.GenKeyExistsErr([]string{handle.String()}, keyName)
	}
	pkIdx := FindPrimaryIndex(tblInfo)
	if pkIdx == nil {
		handleData, err := handle.Data()
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
		}
		colStrVals, err := genIndexKeyStrs(handleData)
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
		}
		return kv.GenKeyExistsErr(colStrVals, keyName)
	}
	pkDts := make([]types.Datum, 0, len(pkIdx.Columns))
	for _, idxCol := range pkIdx.Columns {
		pkDts = append(pkDts, row[idxCol.Offset])
	}
	tablecodec.TruncateIndexValues(tblInfo, pkIdx, pkDts)
	colStrVals, err := genIndexKeyStrs(pkDts)
	if err != nil {
		// if genIndexKeyStrs failed, return ErrKeyExists with handle.String().
		return kv.ErrKeyExists.FastGenByArgs(handle.String(), keyName)
	}
	return kv.GenKeyExistsErr(colStrVals, keyName)
}

func init() {
	table.TableFromMeta = TableFromMeta
	table.MockTableFromMeta = MockTableFromMeta
	tableutil.TempTableFromMeta = TempTableFromMeta
}

// sequenceCommon cache the sequence value.
// `alter sequence` will invalidate the cached range.
// `setval` will recompute the start position of cached value.
type sequenceCommon struct {
	meta *model.SequenceInfo
	// base < end when increment > 0.
	// base > end when increment < 0.
	end  int64
	base int64
	// round is used to count the cycle times.
	round int64
	mu    sync.RWMutex
}

// GetSequenceBaseEndRound is used in test.
func (s *sequenceCommon) GetSequenceBaseEndRound() (int64, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.base, s.end, s.round
}

// GetSequenceNextVal implements util.SequenceTable GetSequenceNextVal interface.
// Caching the sequence value in table, we can easily be notified with the cache empty,
// and write the binlogInfo in table level rather than in allocator.
func (t *TableCommon) GetSequenceNextVal(ctx any, dbName, seqName string) (nextVal int64, err error) {
	seq := t.sequence
	if seq == nil {
		// TODO: refine the error.
		return 0, errors.New("sequenceCommon is nil")
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()

	err = func() error {
		// Check if need to update the cache batch from storage.
		// Because seq.base is not always the last allocated value (may be set by setval()).
		// So we should try to seek the next value in cache (not just add increment to seq.base).
		var (
			updateCache bool
			offset      int64
			ok          bool
		)
		if seq.base == seq.end {
			// There is no cache yet.
			updateCache = true
		} else {
			// Seek the first valid value in cache.
			offset = seq.getOffset()
			if seq.meta.Increment > 0 {
				nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.base, seq.end)
			} else {
				nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.end, seq.base)
			}
			if !ok {
				updateCache = true
			}
		}
		if !updateCache {
			return nil
		}
		// Update batch alloc from kv storage.
		sequenceAlloc, err1 := getSequenceAllocator(t.allocs)
		if err1 != nil {
			return err1
		}
		var base, end, round int64
		base, end, round, err1 = sequenceAlloc.AllocSeqCache()
		if err1 != nil {
			return err1
		}
		// Only update local cache when alloc succeed.
		seq.base = base
		seq.end = end
		seq.round = round
		// Seek the first valid value in new cache.
		// Offset may have changed cause the round is updated.
		offset = seq.getOffset()
		if seq.meta.Increment > 0 {
			nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.base, seq.end)
		} else {
			nextVal, ok = autoid.SeekToFirstSequenceValue(seq.base, seq.meta.Increment, offset, seq.end, seq.base)
		}
		if !ok {
			return errors.New("can't find the first value in sequence cache")
		}
		return nil
	}()
	// Sequence alloc in kv store error.
	if err != nil {
		if err == autoid.ErrAutoincReadFailed {
			return 0, table.ErrSequenceHasRunOut.GenWithStackByArgs(dbName, seqName)
		}
		return 0, err
	}
	seq.base = nextVal
	return nextVal, nil
}

// SetSequenceVal implements util.SequenceTable SetSequenceVal interface.
// The returned bool indicates the newVal is already under the base.
func (t *TableCommon) SetSequenceVal(ctx any, newVal int64, dbName, seqName string) (int64, bool, error) {
	seq := t.sequence
	if seq == nil {
		// TODO: refine the error.
		return 0, false, errors.New("sequenceCommon is nil")
	}
	seq.mu.Lock()
	defer seq.mu.Unlock()

	if seq.meta.Increment > 0 {
		if newVal <= t.sequence.base {
			return 0, true, nil
		}
		if newVal <= t.sequence.end {
			t.sequence.base = newVal
			return newVal, false, nil
		}
	} else {
		if newVal >= t.sequence.base {
			return 0, true, nil
		}
		if newVal >= t.sequence.end {
			t.sequence.base = newVal
			return newVal, false, nil
		}
	}

	// Invalid the current cache.
	t.sequence.base = t.sequence.end

	// Rebase from kv storage.
	sequenceAlloc, err := getSequenceAllocator(t.allocs)
	if err != nil {
		return 0, false, err
	}
	res, alreadySatisfied, err := sequenceAlloc.RebaseSeq(newVal)
	if err != nil {
		return 0, false, err
	}
	// Record the current end after setval succeed.
	// Consider the following case.
	// create sequence seq
	// setval(seq, 100) setval(seq, 50)
	// Because no cache (base, end keep 0), so the second setval won't return NULL.
	t.sequence.base, t.sequence.end = newVal, newVal
	return res, alreadySatisfied, nil
}

// getOffset is used in under GetSequenceNextVal & SetSequenceVal, which mu is locked.
func (s *sequenceCommon) getOffset() int64 {
	offset := s.meta.Start
	if s.meta.Cycle && s.round > 0 {
		if s.meta.Increment > 0 {
			offset = s.meta.MinValue
		} else {
			offset = s.meta.MaxValue
		}
	}
	return offset
}

// GetSequenceID implements util.SequenceTable GetSequenceID interface.
func (t *TableCommon) GetSequenceID() int64 {
	return t.tableID
}

// GetSequenceCommon is used in test to get sequenceCommon.
func (t *TableCommon) GetSequenceCommon() *sequenceCommon {
	return t.sequence
}

// TryGetHandleRestoredDataWrapper tries to get the restored data for handle if needed. The argument can be a slice or a map.
func TryGetHandleRestoredDataWrapper(tblInfo *model.TableInfo, row []types.Datum, rowMap map[int64]types.Datum, idx *model.IndexInfo) []types.Datum {
	if !collate.NewCollationEnabled() || !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return nil
	}
	rsData := make([]types.Datum, 0, 4)
	pkIdx := FindPrimaryIndex(tblInfo)
	for _, pkIdxCol := range pkIdx.Columns {
		pkCol := tblInfo.Columns[pkIdxCol.Offset]
		if !types.NeedRestoredData(&pkCol.FieldType) {
			continue
		}
		var datum types.Datum
		if len(rowMap) > 0 {
			datum = rowMap[pkCol.ID]
		} else {
			datum = row[pkCol.Offset]
		}
		TryTruncateRestoredData(&datum, pkCol, pkIdxCol, idx)
		ConvertDatumToTailSpaceCount(&datum, pkCol)
		rsData = append(rsData, datum)
	}
	return rsData
}

// TryTruncateRestoredData tries to truncate index values.
// Says that primary key(a (8)),
// For index t(a), don't truncate the value.
// For index t(a(9)), truncate to a(9).
// For index t(a(7)), truncate to a(8).
func TryTruncateRestoredData(datum *types.Datum, pkCol *model.ColumnInfo,
	pkIdxCol *model.IndexColumn, idx *model.IndexInfo) {
	truncateTargetCol := pkIdxCol
	for _, idxCol := range idx.Columns {
		if idxCol.Offset == pkIdxCol.Offset {
			truncateTargetCol = maxIndexLen(pkIdxCol, idxCol)
			break
		}
	}
	tablecodec.TruncateIndexValue(datum, truncateTargetCol, pkCol)
}

// ConvertDatumToTailSpaceCount converts a string datum to an int datum that represents the tail space count.
func ConvertDatumToTailSpaceCount(datum *types.Datum, col *model.ColumnInfo) {
	if collate.IsBinCollation(col.GetCollate()) {
		*datum = types.NewIntDatum(stringutil.GetTailSpaceCount(datum.GetString()))
	}
}

func maxIndexLen(idxA, idxB *model.IndexColumn) *model.IndexColumn {
	if idxA.Length == types.UnspecifiedLength {
		return idxA
	}
	if idxB.Length == types.UnspecifiedLength {
		return idxB
	}
	if idxA.Length > idxB.Length {
		return idxA
	}
	return idxB
}

func getSequenceAllocator(allocs autoid.Allocators) (autoid.Allocator, error) {
	for _, alloc := range allocs.Allocs {
		if alloc.GetType() == autoid.SequenceType {
			return alloc, nil
		}
	}
	// TODO: refine the error.
	return nil, errors.New("sequence allocator is nil")
}

// BuildTableScanFromInfos build tipb.TableScan with *model.TableInfo and *model.ColumnInfo.
func BuildTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo, isTiFlashStore bool) *tipb.TableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.TableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false, isTiFlashStore),
		PrimaryColumnIds: pkColIDs,
	}
	if tableInfo.IsCommonHandle {
		tsExec.PrimaryPrefixColumnIds = PrimaryPrefixColumnIDs(tableInfo)
	}
	return tsExec
}

// BuildPartitionTableScanFromInfos build tipb.PartitonTableScan with *model.TableInfo and *model.ColumnInfo.
func BuildPartitionTableScanFromInfos(tableInfo *model.TableInfo, columnInfos []*model.ColumnInfo, fastScan bool) *tipb.PartitionTableScan {
	pkColIDs := TryGetCommonPkColumnIds(tableInfo)
	tsExec := &tipb.PartitionTableScan{
		TableId:          tableInfo.ID,
		Columns:          util.ColumnsToProto(columnInfos, tableInfo.PKIsHandle, false, false),
		PrimaryColumnIds: pkColIDs,
		IsFastScan:       &fastScan,
	}
	if tableInfo.IsCommonHandle {
		tsExec.PrimaryPrefixColumnIds = PrimaryPrefixColumnIDs(tableInfo)
	}
	return tsExec
}

// SetPBColumnsDefaultValue sets the default values of tipb.ColumnInfo.
func SetPBColumnsDefaultValue(ctx expression.BuildContext, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		// For virtual columns, we set their default values to NULL so that TiKV will return NULL properly,
		// They real values will be computed later.
		if c.IsGenerated() && !c.GeneratedStored {
			pbColumns[i].DefaultVal = []byte{codec.NilFlag}
		}
		if c.GetOriginDefaultValue() == nil {
			continue
		}

		evalCtx := ctx.GetEvalCtx()
		d, err := table.GetColOriginDefaultValueWithoutStrictSQLMode(ctx, c)
		if err != nil {
			return err
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(evalCtx.Location(), nil, d)
		ec := evalCtx.ErrCtx()
		err = ec.HandleError(err)
		if err != nil {
			return err
		}
	}
	return nil
}

// TemporaryTable is used to store transaction-specific or session-specific information for global / local temporary tables.
// For example, stats and autoID should have their own copies of data, instead of being shared by all sessions.
type TemporaryTable struct {
	// Whether it's modified in this transaction.
	modified bool
	// The stats of this table. So far it's always pseudo stats.
	stats *statistics.Table
	// The autoID allocator of this table.
	autoIDAllocator autoid.Allocator
	// Table size.
	size int64

	meta *model.TableInfo
}

// TempTableFromMeta builds a TempTable from model.TableInfo.
func TempTableFromMeta(tblInfo *model.TableInfo) tableutil.TempTable {
	return &TemporaryTable{
		modified:        false,
		stats:           statistics.PseudoTable(tblInfo, false, false),
		autoIDAllocator: autoid.NewAllocatorFromTempTblInfo(tblInfo),
		meta:            tblInfo,
	}
}

// GetAutoIDAllocator is implemented from TempTable.GetAutoIDAllocator.
func (t *TemporaryTable) GetAutoIDAllocator() autoid.Allocator {
	return t.autoIDAllocator
}

// SetModified is implemented from TempTable.SetModified.
func (t *TemporaryTable) SetModified(modified bool) {
	t.modified = modified
}

// GetModified is implemented from TempTable.GetModified.
func (t *TemporaryTable) GetModified() bool {
	return t.modified
}

// GetStats is implemented from TempTable.GetStats.
func (t *TemporaryTable) GetStats() any {
	return t.stats
}

// GetSize gets the table size.
func (t *TemporaryTable) GetSize() int64 {
	return t.size
}

// SetSize sets the table size.
func (t *TemporaryTable) SetSize(v int64) {
	t.size = v
}

// GetMeta gets the table meta.
func (t *TemporaryTable) GetMeta() *model.TableInfo {
	return t.meta
}
