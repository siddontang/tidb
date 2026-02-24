// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/keydecoder"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/zap"
)

// tidbTrxTableRetriever is the memtable retriever for the TIDB_TRX and CLUSTER_TIDB_TRX table.
type tidbTrxTableRetriever struct {
	dummyCloser
	batchRetrieverHelper
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	txnInfo     []*txninfo.TxnInfo
	initialized bool
}

func (e *tidbTrxTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}

	if !e.initialized {
		e.initialized = true

		sm := sctx.GetSessionManager()
		if sm == nil {
			e.retrieved = true
			return nil, nil
		}

		loginUser := sctx.GetSessionVars().User
		hasProcessPriv := hasPriv(sctx, mysql.ProcessPriv)
		infoList := sm.ShowTxnList()
		e.txnInfo = make([]*txninfo.TxnInfo, 0, len(infoList))
		for _, info := range infoList {
			// If you have the PROCESS privilege, you can see all running transactions.
			// Otherwise, you can see only your own transactions.
			if !hasProcessPriv && loginUser != nil && info.ProcessInfo.Username != loginUser.Username {
				continue
			}
			e.txnInfo = append(e.txnInfo, info)
		}

		e.batchRetrieverHelper.totalRows = len(e.txnInfo)
		e.batchRetrieverHelper.batchSize = 1024
	}

	sqlExec := sctx.GetRestrictedSQLExecutor()

	var err error
	// The current TiDB node's address is needed by the CLUSTER_TIDB_TRX table.
	var instanceAddr string
	if e.table.Name.O == infoschema.ClusterTableTiDBTrx {
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}

	var res [][]types.Datum
	err = e.nextBatch(func(start, end int) error {
		// Before getting rows, collect the SQL digests that needs to be retrieved first.
		var sqlRetriever *expression.SQLDigestTextRetriever
		for _, c := range e.columns {
			if c.Name.O == txninfo.CurrentSQLDigestTextStr {
				if sqlRetriever == nil {
					sqlRetriever = expression.NewSQLDigestTextRetriever()
				}

				for i := start; i < end; i++ {
					sqlRetriever.SQLDigestsMap[e.txnInfo[i].CurrentSQLDigest] = ""
				}
			}
		}
		// Retrieve the SQL texts if necessary.
		if sqlRetriever != nil {
			err1 := sqlRetriever.RetrieveLocal(ctx, sqlExec)
			if err1 != nil {
				return errors.Trace(err1)
			}
		}

		res = make([][]types.Datum, 0, end-start)

		// Calculate rows.
		for i := start; i < end; i++ {
			row := make([]types.Datum, 0, len(e.columns))
			for _, c := range e.columns {
				if c.Name.O == metadef.ClusterTableInstanceColumnName {
					row = append(row, types.NewDatum(instanceAddr))
				} else if c.Name.O == txninfo.CurrentSQLDigestTextStr {
					if text, ok := sqlRetriever.SQLDigestsMap[e.txnInfo[i].CurrentSQLDigest]; ok && len(text) != 0 {
						row = append(row, types.NewDatum(text))
					} else {
						row = append(row, types.NewDatum(nil))
					}
				} else {
					switch c.Name.O {
					case txninfo.MemBufferBytesStr:
						memDBFootprint := sctx.GetSessionVars().MemDBFootprint
						var bytesConsumed int64
						if memDBFootprint != nil {
							bytesConsumed = memDBFootprint.BytesConsumed()
						}
						row = append(row, types.NewDatum(bytesConsumed))
					default:
						row = append(row, e.txnInfo[i].ToDatum(c.Name.O))
					}
				}
			}
			res = append(res, row)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// dataLockWaitsTableRetriever is the memtable retriever for the DATA_LOCK_WAITS table.
type dataLockWaitsTableRetriever struct {
	dummyCloser
	batchRetrieverHelper
	table          *model.TableInfo
	columns        []*model.ColumnInfo
	lockWaits      []*deadlock.WaitForEntry
	resolvingLocks []txnlock.ResolvingLock
	initialized    bool
}

func (r *dataLockWaitsTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.retrieved {
		return nil, nil
	}

	if !r.initialized {
		if !hasPriv(sctx, mysql.ProcessPriv) {
			return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}

		r.initialized = true
		var err error
		r.lockWaits, err = sctx.GetStore().GetLockWaits()
		tikvStore, _ := sctx.GetStore().(helper.Storage)
		r.resolvingLocks = tikvStore.GetLockResolver().Resolving()
		if err != nil {
			r.retrieved = true
			return nil, err
		}

		r.batchRetrieverHelper.totalRows = len(r.lockWaits) + len(r.resolvingLocks)
		r.batchRetrieverHelper.batchSize = 1024
	}

	var res [][]types.Datum

	err := r.nextBatch(func(start, end int) error {
		// Before getting rows, collect the SQL digests that needs to be retrieved first.
		var needDigest bool
		var needSQLText bool
		for _, c := range r.columns {
			if c.Name.O == infoschema.DataLockWaitsColumnSQLDigestText {
				needSQLText = true
			} else if c.Name.O == infoschema.DataLockWaitsColumnSQLDigest {
				needDigest = true
			}
		}

		var digests []string
		if needDigest || needSQLText {
			digests = make([]string, end-start)
			for i, lockWait := range r.lockWaits {
				digest, err := resourcegrouptag.DecodeResourceGroupTag(lockWait.ResourceGroupTag)
				if err != nil {
					// Ignore the error if failed to decode the digest from resource_group_tag. We still want to show
					// as much information as possible even we can't retrieve some of them.
					logutil.Logger(ctx).Warn("failed to decode resource group tag", zap.Error(err))
				} else {
					digests[i] = hex.EncodeToString(digest)
				}
			}
			// todo: support resourcegrouptag for resolvingLocks
		}

		// Fetch the SQL Texts of the digests above if necessary.
		var sqlRetriever *expression.SQLDigestTextRetriever
		if needSQLText {
			sqlRetriever = expression.NewSQLDigestTextRetriever()
			for _, digest := range digests {
				if len(digest) > 0 {
					sqlRetriever.SQLDigestsMap[digest] = ""
				}
			}

			err := sqlRetriever.RetrieveGlobal(ctx, sctx.GetRestrictedSQLExecutor())
			if err != nil {
				return errors.Trace(err)
			}
		}

		// Calculate rows.
		res = make([][]types.Datum, 0, end-start)
		// data_lock_waits contains both lockWaits (pessimistic lock waiting)
		// and resolving (optimistic lock "waiting") info
		// first we'll return the lockWaits, and then resolving, so we need to
		// do some index calculation here
		lockWaitsStart := min(start, len(r.lockWaits))
		resolvingStart := start - lockWaitsStart
		lockWaitsEnd := min(end, len(r.lockWaits))
		resolvingEnd := end - lockWaitsEnd
		for rowIdx, lockWait := range r.lockWaits[lockWaitsStart:lockWaitsEnd] {
			row := make([]types.Datum, 0, len(r.columns))

			for _, col := range r.columns {
				switch col.Name.O {
				case infoschema.DataLockWaitsColumnKey:
					row = append(row, types.NewDatum(strings.ToUpper(hex.EncodeToString(lockWait.Key))))
				case infoschema.DataLockWaitsColumnKeyInfo:
					infoSchema := sctx.GetInfoSchema().(infoschema.InfoSchema)
					var decodedKeyStr any
					decodedKey, err := keydecoder.DecodeKey(lockWait.Key, infoSchema)
					if err == nil {
						decodedKeyBytes, err := json.Marshal(decodedKey)
						if err != nil {
							logutil.BgLogger().Warn("marshal decoded key info to JSON failed", zap.Error(err))
						} else {
							decodedKeyStr = string(decodedKeyBytes)
						}
					} else {
						logutil.Logger(ctx).Warn("decode key failed", zap.Error(err))
					}
					row = append(row, types.NewDatum(decodedKeyStr))
				case infoschema.DataLockWaitsColumnTrxID:
					row = append(row, types.NewDatum(lockWait.Txn))
				case infoschema.DataLockWaitsColumnCurrentHoldingTrxID:
					row = append(row, types.NewDatum(lockWait.WaitForTxn))
				case infoschema.DataLockWaitsColumnSQLDigest:
					digest := digests[rowIdx]
					if len(digest) == 0 {
						row = append(row, types.NewDatum(nil))
					} else {
						row = append(row, types.NewDatum(digest))
					}
				case infoschema.DataLockWaitsColumnSQLDigestText:
					text := sqlRetriever.SQLDigestsMap[digests[rowIdx]]
					if len(text) > 0 {
						row = append(row, types.NewDatum(text))
					} else {
						row = append(row, types.NewDatum(nil))
					}
				default:
					row = append(row, types.NewDatum(nil))
				}
			}

			res = append(res, row)
		}
		for _, resolving := range r.resolvingLocks[resolvingStart:resolvingEnd] {
			row := make([]types.Datum, 0, len(r.columns))

			for _, col := range r.columns {
				switch col.Name.O {
				case infoschema.DataLockWaitsColumnKey:
					row = append(row, types.NewDatum(strings.ToUpper(hex.EncodeToString(resolving.Key))))
				case infoschema.DataLockWaitsColumnKeyInfo:
					infoSchema := domain.GetDomain(sctx).InfoSchema()
					var decodedKeyStr any
					decodedKey, err := keydecoder.DecodeKey(resolving.Key, infoSchema)
					if err == nil {
						decodedKeyBytes, err := json.Marshal(decodedKey)
						if err != nil {
							logutil.Logger(ctx).Warn("marshal decoded key info to JSON failed", zap.Error(err))
						} else {
							decodedKeyStr = string(decodedKeyBytes)
						}
					} else {
						logutil.Logger(ctx).Warn("decode key failed", zap.Error(err))
					}
					row = append(row, types.NewDatum(decodedKeyStr))
				case infoschema.DataLockWaitsColumnTrxID:
					row = append(row, types.NewDatum(resolving.TxnID))
				case infoschema.DataLockWaitsColumnCurrentHoldingTrxID:
					row = append(row, types.NewDatum(resolving.LockTxnID))
				case infoschema.DataLockWaitsColumnSQLDigest:
					// todo: support resourcegrouptag for resolvingLocks
					row = append(row, types.NewDatum(nil))
				case infoschema.DataLockWaitsColumnSQLDigestText:
					// todo: support resourcegrouptag for resolvingLocks
					row = append(row, types.NewDatum(nil))
				default:
					row = append(row, types.NewDatum(nil))
				}
			}

			res = append(res, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

// deadlocksTableRetriever is the memtable retriever for the DEADLOCKS and CLUSTER_DEADLOCKS table.
type deadlocksTableRetriever struct {
	dummyCloser
	batchRetrieverHelper

	currentIdx          int
	currentWaitChainIdx int

	table       *model.TableInfo
	columns     []*model.ColumnInfo
	deadlocks   []*deadlockhistory.DeadlockRecord
	initialized bool
}

// nextIndexPair advances a index pair (where `idx` is the index of the DeadlockRecord, and `waitChainIdx` is the index
// of the wait chain item in the `idx`-th DeadlockRecord. This function helps iterate over each wait chain item
// in all DeadlockRecords.
func (r *deadlocksTableRetriever) nextIndexPair(idx, waitChainIdx int) (a, b int) {
	waitChainIdx++
	if waitChainIdx >= len(r.deadlocks[idx].WaitChain) {
		waitChainIdx = 0
		idx++
		for idx < len(r.deadlocks) && len(r.deadlocks[idx].WaitChain) == 0 {
			idx++
		}
	}
	return idx, waitChainIdx
}

func (r *deadlocksTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if r.retrieved {
		return nil, nil
	}

	if !r.initialized {
		if !hasPriv(sctx, mysql.ProcessPriv) {
			return nil, plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("PROCESS")
		}

		r.initialized = true
		r.deadlocks = deadlockhistory.GlobalDeadlockHistory.GetAll()

		r.batchRetrieverHelper.totalRows = 0
		for _, d := range r.deadlocks {
			r.batchRetrieverHelper.totalRows += len(d.WaitChain)
		}
		r.batchRetrieverHelper.batchSize = 1024
	}

	// The current TiDB node's address is needed by the CLUSTER_DEADLOCKS table.
	var err error
	var instanceAddr string
	if r.table.Name.O == infoschema.ClusterTableDeadlocks {
		instanceAddr, err = infoschema.GetInstanceAddr(sctx)
		if err != nil {
			return nil, err
		}
	}

	infoSchema := sctx.GetInfoSchema().(infoschema.InfoSchema)

	var res [][]types.Datum

	err = r.nextBatch(func(start, end int) error {
		// Before getting rows, collect the SQL digests that needs to be retrieved first.
		var sqlRetriever *expression.SQLDigestTextRetriever
		for _, c := range r.columns {
			if c.Name.O == deadlockhistory.ColCurrentSQLDigestTextStr {
				if sqlRetriever == nil {
					sqlRetriever = expression.NewSQLDigestTextRetriever()
				}

				idx, waitChainIdx := r.currentIdx, r.currentWaitChainIdx
				for i := start; i < end; i++ {
					if idx >= len(r.deadlocks) {
						return errors.New("reading information_schema.(cluster_)deadlocks table meets corrupted index")
					}

					sqlRetriever.SQLDigestsMap[r.deadlocks[idx].WaitChain[waitChainIdx].SQLDigest] = ""
					// Step to the next entry
					idx, waitChainIdx = r.nextIndexPair(idx, waitChainIdx)
				}
			}
		}
		// Retrieve the SQL texts if necessary.
		if sqlRetriever != nil {
			err1 := sqlRetriever.RetrieveGlobal(ctx, sctx.GetRestrictedSQLExecutor())
			if err1 != nil {
				return errors.Trace(err1)
			}
		}

		res = make([][]types.Datum, 0, end-start)

		for i := start; i < end; i++ {
			if r.currentIdx >= len(r.deadlocks) {
				return errors.New("reading information_schema.(cluster_)deadlocks table meets corrupted index")
			}

			row := make([]types.Datum, 0, len(r.columns))
			deadlock := r.deadlocks[r.currentIdx]
			waitChainItem := deadlock.WaitChain[r.currentWaitChainIdx]

			for _, c := range r.columns {
				if c.Name.O == metadef.ClusterTableInstanceColumnName {
					row = append(row, types.NewDatum(instanceAddr))
				} else if c.Name.O == deadlockhistory.ColCurrentSQLDigestTextStr {
					if text, ok := sqlRetriever.SQLDigestsMap[waitChainItem.SQLDigest]; ok && len(text) > 0 {
						row = append(row, types.NewDatum(text))
					} else {
						row = append(row, types.NewDatum(nil))
					}
				} else if c.Name.O == deadlockhistory.ColKeyInfoStr {
					value := types.NewDatum(nil)
					if len(waitChainItem.Key) > 0 {
						decodedKey, err := keydecoder.DecodeKey(waitChainItem.Key, infoSchema)
						if err == nil {
							decodedKeyJSON, err := json.Marshal(decodedKey)
							if err != nil {
								logutil.BgLogger().Warn("marshal decoded key info to JSON failed", zap.Error(err))
							} else {
								value = types.NewDatum(string(decodedKeyJSON))
							}
						} else {
							logutil.Logger(ctx).Warn("decode key failed", zap.Error(err))
						}
					}
					row = append(row, value)
				} else {
					row = append(row, deadlock.ToDatum(r.currentWaitChainIdx, c.Name.O))
				}
			}

			res = append(res, row)
			// Step to the next entry
			r.currentIdx, r.currentWaitChainIdx = r.nextIndexPair(r.currentIdx, r.currentWaitChainIdx)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return res, nil
}

func adjustColumns(input [][]types.Datum, outColumns []*model.ColumnInfo, table *model.TableInfo) [][]types.Datum {
	if len(outColumns) == len(table.Columns) {
		return input
	}
	rows := make([][]types.Datum, len(input))
	for i, fullRow := range input {
		row := make([]types.Datum, len(outColumns))
		for j, col := range outColumns {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows
}

// TiFlashSystemTableRetriever is used to read system table from tiflash.
type TiFlashSystemTableRetriever struct {
	dummyCloser
	table         *model.TableInfo
	outputCols    []*model.ColumnInfo
	instanceCount int
	instanceIdx   int
	instanceIDs   []string
	rowIdx        int
	retrieved     bool
	initialized   bool
	extractor     *plannercore.TiFlashSystemTableExtractor
}

func (e *TiFlashSystemTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx, e.extractor.TiFlashInstances)
		if err != nil {
			return nil, err
		}
	}
	if e.instanceCount == 0 || e.instanceIdx >= e.instanceCount {
		e.retrieved = true
		return nil, nil
	}

	for {
		rows, err := e.dataForTiFlashSystemTables(ctx, sctx, e.extractor.TiDBDatabases, e.extractor.TiDBTables)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 || e.instanceIdx >= e.instanceCount {
			return rows, nil
		}
	}
}

func (e *TiFlashSystemTableRetriever) initialize(sctx sessionctx.Context, tiflashInstances set.StringSet) error {
	storeInfo, err := infoschema.GetStoreServerInfo(sctx.GetStore())
	if err != nil {
		return err
	}

	for _, info := range storeInfo {
		if info.ServerType != kv.TiFlash.Name() {
			continue
		}
		info.ResolveLoopBackAddr()
		if len(tiflashInstances) > 0 && !tiflashInstances.Exist(info.Address) {
			continue
		}
		hostAndStatusPort := strings.Split(info.StatusAddr, ":")
		if len(hostAndStatusPort) != 2 {
			return errors.Errorf("node status addr: %s format illegal", info.StatusAddr)
		}
		e.instanceIDs = append(e.instanceIDs, info.Address)
		e.instanceCount++
	}
	e.initialized = true
	return nil
}

type tiFlashSQLExecuteResponseMetaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type tiFlashSQLExecuteResponse struct {
	Meta []tiFlashSQLExecuteResponseMetaColumn `json:"meta"`
	Data [][]any                               `json:"data"`
}

var tiflashTargetTableName = map[string]string{
	"tiflash_tables":   "dt_tables",
	"tiflash_segments": "dt_segments",
	"tiflash_indexes":  "dt_local_indexes",
}

func (e *TiFlashSystemTableRetriever) dataForTiFlashSystemTables(ctx context.Context, sctx sessionctx.Context, tidbDatabases string, tidbTables string) ([][]types.Datum, error) {
	maxCount := 1024
	targetTable := tiflashTargetTableName[e.table.Name.L]

	var filters []string
	// Add filter for keyspace_id if tidb is running in the keyspace mode.
	if keyspace.GetKeyspaceNameBySettings() != "" {
		keyspaceID := uint32(sctx.GetStore().GetCodec().GetKeyspaceID())
		filters = append(filters, fmt.Sprintf("keyspace_id=%d", keyspaceID))
	}
	if len(tidbDatabases) > 0 {
		filters = append(filters, fmt.Sprintf("tidb_database IN (%s)", strings.ReplaceAll(tidbDatabases, "\"", "'")))
	}
	if len(tidbTables) > 0 {
		filters = append(filters, fmt.Sprintf("tidb_table IN (%s)", strings.ReplaceAll(tidbTables, "\"", "'")))
	}
	sql := fmt.Sprintf("SELECT * FROM system.%s", targetTable)
	if len(filters) > 0 {
		sql = fmt.Sprintf("%s WHERE %s", sql, strings.Join(filters, " AND "))
	}
	sql = fmt.Sprintf("%s LIMIT %d, %d", sql, e.rowIdx, maxCount)
	request := tikvrpc.Request{
		Type:    tikvrpc.CmdGetTiFlashSystemTable,
		StoreTp: tikvrpc.TiFlash,
		Req: &kvrpcpb.TiFlashSystemTableRequest{
			Sql: sql,
		},
	}

	store := sctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.New("Get tiflash system tables can only run with tikv compatible storage")
	}
	// send request to tiflash, use 5 minutes as per-request timeout
	instanceID := e.instanceIDs[e.instanceIdx]
	timeout := time.Duration(5*60) * time.Second
	resp, err := tikvStore.GetTiKVClient().SendRequest(ctx, instanceID, &request, timeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var result tiFlashSQLExecuteResponse
	tiflashResp, ok := resp.Resp.(*kvrpcpb.TiFlashSystemTableResponse)
	if !ok {
		return nil, errors.Errorf("Unexpected response type: %T", resp.Resp)
	}
	err = json.Unmarshal(tiflashResp.Data, &result)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to decode JSON from TiFlash")
	}

	// Map result columns back to our columns. It is possible that some columns cannot be
	// recognized and some other columns are missing. This may happen during upgrading.
	outputColIndexMap := map[string]int{} // Map from TiDB Column name to Output Column Index
	for idx, c := range e.outputCols {
		outputColIndexMap[c.Name.L] = idx
	}
	tiflashColIndexMap := map[int]int{} // Map from TiFlash Column index to Output Column Index
	for tiFlashColIdx, col := range result.Meta {
		if outputIdx, ok := outputColIndexMap[strings.ToLower(col.Name)]; ok {
			tiflashColIndexMap[tiFlashColIdx] = outputIdx
		}
	}
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	outputRows := make([][]types.Datum, 0, len(result.Data))
	for _, rowFields := range result.Data {
		if len(rowFields) == 0 {
			continue
		}
		outputRow := make([]types.Datum, len(e.outputCols))
		for tiFlashColIdx, fieldValue := range rowFields {
			outputIdx, ok := tiflashColIndexMap[tiFlashColIdx]
			if !ok {
				// Discard this field, we don't know which output column is the destination
				continue
			}
			if fieldValue == nil {
				continue
			}
			valStr := fmt.Sprint(fieldValue)
			column := e.outputCols[outputIdx]
			if column.GetType() == mysql.TypeVarchar {
				outputRow[outputIdx].SetString(valStr, mysql.DefaultCollationName)
			} else if column.GetType() == mysql.TypeLonglong {
				value, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				outputRow[outputIdx].SetInt64(value)
			} else if column.GetType() == mysql.TypeDouble {
				value, err := strconv.ParseFloat(valStr, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				outputRow[outputIdx].SetFloat64(value)
			} else {
				return nil, errors.Errorf("Meet column of unknown type %v", column)
			}
		}
		outputRow[len(e.outputCols)-1].SetString(instanceID, mysql.DefaultCollationName)

		// for "tiflash_indexes", set the column_name and index_name according to the TableInfo
		if e.table.Name.L == "tiflash_indexes" {
			logicalTableID := outputRow[outputColIndexMap["table_id"]].GetInt64()
			if !outputRow[outputColIndexMap["belonging_table_id"]].IsNull() {
				// Old TiFlash versions may not have this column. In this case we will try to get by the "table_id"
				belongingTableID := outputRow[outputColIndexMap["belonging_table_id"]].GetInt64()
				if belongingTableID != -1 && belongingTableID != 0 {
					logicalTableID = belongingTableID
				}
			}
			if table, ok := is.TableByID(ctx, logicalTableID); ok {
				tableInfo := table.Meta()
				getInt64DatumVal := func(datum_name string, default_val int64) int64 {
					datum := outputRow[outputColIndexMap[datum_name]]
					if !datum.IsNull() {
						return datum.GetInt64()
					}
					return default_val
				}
				// set column_name
				columnID := getInt64DatumVal("column_id", 0)
				columnName := tableInfo.FindColumnNameByID(columnID)
				outputRow[outputColIndexMap["column_name"]].SetString(columnName, mysql.DefaultCollationName)
				// set index_name
				indexID := getInt64DatumVal("index_id", 0)
				indexName := tableInfo.FindIndexNameByID(indexID)
				outputRow[outputColIndexMap["index_name"]].SetString(indexName, mysql.DefaultCollationName)
			}
		}

		outputRows = append(outputRows, outputRow)
	}
	e.rowIdx += len(outputRows)
	if len(outputRows) < maxCount {
		e.instanceIdx++
		e.rowIdx = 0
	}
	return outputRows, nil
}
