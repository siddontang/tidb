// Copyright 2018 PingCAP, Inc.
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

package tikvhandler

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/pd/client/clients/router"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

// ServeHTTP handles table related requests, such as table's region information, disk usage.
func (h *TableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)
	dbName := params[handler.DBName]
	tableName := params[handler.TableName]
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	tableName, partitionName := handler.ExtractTableAndPartitionName(tableName)
	tableVal, err := schema.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	switch h.op {
	case OpTableRegions:
		h.handleRegionRequest(tableVal, w)
	case OpTableRanges:
		h.handleRangeRequest(tableVal, w)
	case OpTableDiskUsage:
		h.handleDiskUsageRequest(tableVal, w)
	case OpTableScatter:
		// supports partition table, only get one physical table, prevent too many scatter schedulers.
		ptbl, err := h.GetPartition(tableVal, partitionName)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		h.handleScatterTableRequest(ptbl, w)
	case OpStopTableScatter:
		ptbl, err := h.GetPartition(tableVal, partitionName)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		h.handleStopScatterTableRequest(ptbl, w)
	default:
		handler.WriteError(w, errors.New("method not found"))
	}
}

// ServeHTTP handles request of ddl jobs history.
func (h DDLHistoryJobHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var (
		jobID   = 0
		limitID = 0
		err     error
	)
	if jobValue := req.FormValue(handler.JobID); len(jobValue) > 0 {
		jobID, err = strconv.Atoi(jobValue)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		if jobID < 1 {
			handler.WriteError(w, errors.New("ddl history start_job_id must be greater than 0"))
			return
		}
	}
	if limitValue := req.FormValue(handler.Limit); len(limitValue) > 0 {
		limitID, err = strconv.Atoi(limitValue)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		if limitID < 1 || limitID > ddl.DefNumGetDDLHistoryJobs {
			handler.WriteError(w,
				errors.Errorf("ddl history limit must be greater than 0 and less than or equal to %v", ddl.DefNumGetDDLHistoryJobs))
			return
		}
	}

	jobs, err := h.getHistoryDDL(jobID, limitID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, jobs)
}

func (h DDLHistoryJobHandler) getHistoryDDL(jobID, limit int) (jobs []*model.Job, err error) {
	txn, err := h.Store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	txnMeta := meta.NewMutator(txn)

	jobs, err = ddl.ScanHistoryDDLJobs(txnMeta, int64(jobID), limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func (h DDLResignOwnerHandler) resignDDLOwner() error {
	dom, err := session.GetDomain(h.store)
	if err != nil {
		return errors.Trace(err)
	}

	ownerMgr := dom.DDL().OwnerManager()
	err = ownerMgr.ResignOwner(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ServeHTTP handles request of resigning ddl owner.
func (h DDLResignOwnerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}

	err := h.resignDDLOwner()
	if err != nil {
		log.Error("failed to resign DDL owner", zap.Error(err))
		handler.WriteError(w, err)
		return
	}

	handler.WriteData(w, "success!")
}

func (h *TableHandler) getPDAddr() ([]string, error) {
	etcd, ok := h.Store.(kv.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	pdAddrs, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(pdAddrs) == 0 {
		return nil, errors.New("pd unavailable")
	}
	return pdAddrs, nil
}

func (h *TableHandler) addScatterSchedule(startKey, endKey []byte, name string) error {
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		return err
	}
	input := map[string]string{
		"name":       "scatter-range",
		"start_key":  url.QueryEscape(string(startKey)),
		"end_key":    url.QueryEscape(string(endKey)),
		"range_name": name,
	}
	v, err := json.Marshal(input)
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), pdAddrs[0], pd.Schedulers)
	resp, err := util.InternalHTTPClient().Post(scheduleURL, "application/json", bytes.NewBuffer(v))
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error("failed to close response body", zap.Error(err))
	}
	return nil
}

func (h *TableHandler) deleteScatterSchedule(name string) error {
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), pdAddrs[0], pd.ScatterRangeSchedulerWithName(name))
	req, err := http.NewRequest(http.MethodDelete, scheduleURL, nil)
	if err != nil {
		return err
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error("failed to close response body", zap.Error(err))
	}
	return nil
}

func (h *TableHandler) handleScatterTableRequest(tbl table.PhysicalTable, w http.ResponseWriter) {
	// for record
	tableID := tbl.GetPhysicalID()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	tableName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tableID)
	err := h.addScatterSchedule(startKey, endKey, tableName)
	if err != nil {
		handler.WriteError(w, errors.Annotate(err, "scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indexName := index.Meta().Name.String()
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		startKey = codec.EncodeBytes([]byte{}, startKey)
		endKey = codec.EncodeBytes([]byte{}, endKey)
		name := tableName + "-" + indexName
		err := h.addScatterSchedule(startKey, endKey, name)
		if err != nil {
			handler.WriteError(w, errors.Annotatef(err, "scatter index(%s) error", name))
			return
		}
	}
	handler.WriteData(w, "success!")
}

func (h *TableHandler) handleStopScatterTableRequest(tbl table.PhysicalTable, w http.ResponseWriter) {
	// for record
	tableName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tbl.GetPhysicalID())
	err := h.deleteScatterSchedule(tableName)
	if err != nil {
		handler.WriteError(w, errors.Annotate(err, "stop scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexName := index.Meta().Name.String()
		name := tableName + "-" + indexName
		err := h.deleteScatterSchedule(name)
		if err != nil {
			handler.WriteError(w, errors.Annotatef(err, "delete scatter index(%s) error", name))
			return
		}
	}
	handler.WriteData(w, "success!")
}

func (h *TableHandler) handleRegionRequest(tbl table.Table, w http.ResponseWriter) {
	pi := tbl.Meta().GetPartitionInfo()
	if pi != nil {
		// Partitioned table.
		var data []*TableRegions
		for _, def := range pi.Definitions {
			tableRegions, err := h.getRegionsByID(tbl, def.ID, def.Name.O)
			if err != nil {
				handler.WriteError(w, err)
				return
			}

			data = append(data, tableRegions)
		}
		handler.WriteData(w, data)
		return
	}

	meta := tbl.Meta()
	tableRegions, err := h.getRegionsByID(tbl, meta.ID, meta.Name.O)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	handler.WriteData(w, tableRegions)
}

func createTableRanges(tblID int64, tblName string, indices []*model.IndexInfo) *TableRanges {
	indexPrefix := tablecodec.GenTableIndexPrefix(tblID)
	recordPrefix := tablecodec.GenTableRecordPrefix(tblID)
	tableEnd := tablecodec.EncodeTablePrefix(tblID + 1)
	ranges := &TableRanges{
		TableName: tblName,
		TableID:   tblID,
		Range:     createRangeDetail(tablecodec.EncodeTablePrefix(tblID), tableEnd),
		Record:    createRangeDetail(recordPrefix, tableEnd),
		Index:     createRangeDetail(indexPrefix, recordPrefix),
	}
	if len(indices) != 0 {
		indexRanges := make(map[string]RangeDetail)
		for _, index := range indices {
			start := tablecodec.EncodeTableIndexPrefix(tblID, index.ID)
			end := tablecodec.EncodeTableIndexPrefix(tblID, index.ID+1)
			indexRanges[index.Name.String()] = createRangeDetail(start, end)
		}
		ranges.Indices = indexRanges
	}
	return ranges
}

func (*TableHandler) handleRangeRequest(tbl table.Table, w http.ResponseWriter) {
	meta := tbl.Meta()
	pi := meta.GetPartitionInfo()
	if pi != nil {
		// Partitioned table.
		var data []*TableRanges
		for _, def := range pi.Definitions {
			data = append(data, createTableRanges(def.ID, def.Name.String(), meta.Indices))
		}
		handler.WriteData(w, data)
		return
	}

	handler.WriteData(w, createTableRanges(meta.ID, meta.Name.String(), meta.Indices))
}

func (h *TableHandler) getRegionsByID(tbl table.Table, id int64, name string) (*TableRegions, error) {
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(id)
	ctx := context.Background()
	pdCli := h.RegionCache.PDClient()
	regions, err := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1, opt.WithAllowFollowerHandle())
	if err != nil {
		return nil, err
	}

	recordRegions := make([]handler.RegionMeta, 0, len(regions))
	for _, region := range regions {
		meta := handler.RegionMeta{
			ID:          region.Meta.Id,
			Leader:      region.Leader,
			Peers:       region.Meta.Peers,
			RegionEpoch: region.Meta.RegionEpoch,
		}
		recordRegions = append(recordRegions, meta)
	}

	// for indices
	indices := make([]IndexRegions, len(tbl.Indices()))
	for i, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := tablecodec.GetTableIndexKeyRange(id, indexID)
		regions, err := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1, opt.WithAllowFollowerHandle())
		if err != nil {
			return nil, err
		}
		indexRegions := make([]handler.RegionMeta, 0, len(regions))
		for _, region := range regions {
			meta := handler.RegionMeta{
				ID:          region.Meta.Id,
				Leader:      region.Leader,
				Peers:       region.Meta.Peers,
				RegionEpoch: region.Meta.RegionEpoch,
			}
			indexRegions = append(indexRegions, meta)
		}
		indices[i].Regions = indexRegions
	}

	return &TableRegions{
		TableName:     name,
		TableID:       id,
		Indices:       indices,
		RecordRegions: recordRegions,
	}, nil
}

func (h *TableHandler) handleDiskUsageRequest(tbl table.Table, w http.ResponseWriter) {
	stats, err := h.GetPDRegionStats(context.Background(), tbl.Meta().ID, false)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, stats.StorageSize)
}

// ServeHTTP handles request of get region by ID.
func (h RegionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	if _, ok := params[handler.RegionID]; !ok {
		router := mux.CurrentRoute(req).GetName()
		if router == "RegionsMeta" {
			startKey := []byte{'m'}
			endKey := []byte{'n'}

			recordRegionIDs, err := h.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 500, nil), startKey, endKey)
			if err != nil {
				handler.WriteError(w, err)
				return
			}

			recordRegions, err := h.GetRegionsMeta(recordRegionIDs)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			handler.WriteData(w, recordRegions)
			return
		}
		if router == "RegionHot" {
			schema, err := h.Schema()
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			ctx := context.Background()
			hotRead, err := h.ScrapeHotInfo(ctx, helper.HotRead, schema, nil)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			hotWrite, err := h.ScrapeHotInfo(ctx, helper.HotWrite, schema, nil)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			handler.WriteData(w, map[string]any{
				"write": hotWrite,
				"read":  hotRead,
			})
			return
		}
		return
	}

	regionIDInt, err := strconv.ParseInt(params[handler.RegionID], 0, 64)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// locate region
	region, err := h.RegionCache.LocateRegionByID(tikv.NewBackofferWithVars(context.Background(), 500, nil), regionID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	frameRange, err := helper.NewRegionFrameRange(region)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	// create RegionDetail from RegionFrameRange
	regionDetail := &RegionDetail{
		RegionID:    regionID,
		RangeDetail: createRangeDetail(region.StartKey, region.EndKey),
	}
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	// Since we need a database's name for each frame, and a table's database name can not
	// get from table's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstTableID,frameRange.endTableID]`
	// on [frameRange.firstTableID,frameRange.endTableID] is small enough.
	for _, dbName := range schema.AllSchemaNames() {
		if metadef.IsMemDB(dbName.L) {
			continue
		}
		tables, err := schema.SchemaTableInfos(context.Background(), dbName)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		for _, tableVal := range tables {
			regionDetail.addTableInRange(dbName.String(), tableVal, frameRange)
		}
	}
	handler.WriteData(w, regionDetail)
}

// parseQuery is used to parse query string in URL with shouldUnescape, due to golang http package can not distinguish
// query like "?a=" and "?a". We rewrite it to separate these two queries. e.g.
// "?a=" which means that a is an empty string "";
// "?a"  which means that a is null.
// If shouldUnescape is true, we use QueryUnescape to handle keys and values that will be put in m.
// If shouldUnescape is false, we don't use QueryUnescap to handle.
func parseQuery(query string, m url.Values, shouldUnescape bool) error {
	var err error
	for query != "" {
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if key == "" {
			continue
		}
		if i := strings.Index(key, "="); i >= 0 {
			value := ""
			key, value = key[:i], key[i+1:]
			if shouldUnescape {
				key, err = url.QueryUnescape(key)
				if err != nil {
					return errors.Trace(err)
				}
				value, err = url.QueryUnescape(value)
				if err != nil {
					return errors.Trace(err)
				}
			}
			m[key] = append(m[key], value)
		} else {
			if shouldUnescape {
				key, err = url.QueryUnescape(key)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if _, ok := m[key]; !ok {
				m[key] = nil
			}
		}
	}
	return errors.Trace(err)
}

// ServeHTTP handles request of list a table's regions.
func (h MvccTxnHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var data any
	params := mux.Vars(req)
	var err error
	switch h.op {
	case OpMvccGetByHex:
		data, err = h.HandleMvccGetByHex(params)
	case OpMvccGetByIdx, OpMvccGetByKey:
		if req.URL == nil {
			err = errors.BadRequestf("Invalid URL")
			break
		}
		values := make(url.Values)
		err = parseQuery(req.URL.RawQuery, values, true)
		if err == nil {
			if h.op == OpMvccGetByIdx {
				data, err = h.handleMvccGetByIdx(params, values)
			} else {
				data, err = h.handleMvccGetByKey(params, values)
			}
		}
	case OpMvccGetByTxn:
		data, err = h.handleMvccGetByTxn(params)
	default:
		err = errors.NotSupportedf("Operation not supported.")
	}
	if err != nil {
		handler.WriteError(w, err)
	} else {
		handler.WriteData(w, data)
	}
}

// handleMvccGetByIdx gets MVCC info by an index key.
func (h MvccTxnHandler) handleMvccGetByIdx(params map[string]string, values url.Values) (any, error) {
	dbName := params[handler.DBName]
	tableName := params[handler.TableName]

	t, err := h.GetTable(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle, err := h.GetHandle(t, params, values)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var idxCols []*model.ColumnInfo
	var idx table.Index
	for _, v := range t.Indices() {
		if strings.EqualFold(v.Meta().Name.String(), params[handler.IndexName]) {
			for _, c := range v.Meta().Columns {
				idxCols = append(idxCols, t.Meta().Columns[c.Offset])
			}
			idx = v
			break
		}
	}
	if idx == nil {
		return nil, errors.NotFoundf("Index %s not found!", params[handler.IndexName])
	}
	return h.GetMvccByIdxValue(idx, values, idxCols, handle)
}

func (h MvccTxnHandler) handleMvccGetByKey(params map[string]string, values url.Values) (any, error) {
	dbName := params[handler.DBName]
	tableName := params[handler.TableName]
	tb, err := h.GetTable(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle, err := h.GetHandle(tb, params, values)
	if err != nil {
		return nil, err
	}

	encodedKey := tablecodec.EncodeRecordKey(tb.RecordPrefix(), handle)
	data, err := h.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := h.GetRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	resp := &helper.MvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), Value: data, RegionID: regionID}
	if len(values.Get("decode")) == 0 {
		return resp, nil
	}
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range tb.Meta().Columns {
		colMap[col.ID] = &(col.FieldType)
	}

	respValue := resp.Value
	var result any = resp
	if respValue.Info != nil {
		datas := make(map[string]map[string]string)
		for _, w := range respValue.Info.Writes {
			if len(w.ShortValue) > 0 {
				datas[strconv.FormatUint(w.StartTs, 10)], err = h.decodeMvccData(w.ShortValue, colMap, tb.Meta())
			}
		}

		for _, v := range respValue.Info.Values {
			if len(v.Value) > 0 {
				datas[strconv.FormatUint(v.StartTs, 10)], err = h.decodeMvccData(v.Value, colMap, tb.Meta())
			}
		}

		if len(datas) > 0 {
			re := map[string]any{
				"key":  resp.Key,
				"info": respValue.Info,
				"data": datas,
			}
			if err != nil {
				re["decode_error"] = err.Error()
			}
			result = re
		}
	}

	return result, nil
}

func (MvccTxnHandler) decodeMvccData(bs []byte, colMap map[int64]*types.FieldType, tb *model.TableInfo) (map[string]string, error) {
	rs, err := tablecodec.DecodeRowToDatumMap(bs, colMap, time.UTC)
	record := make(map[string]string, len(tb.Columns))
	for _, col := range tb.Columns {
		if c, ok := rs[col.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			record[col.Name.O] = data
		}
	}
	return record, err
}

func (h *MvccTxnHandler) handleMvccGetByTxn(params map[string]string) (any, error) {
	startTS, err := strconv.ParseInt(params[handler.StartTS], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableID, err := h.GetTableID(params[handler.DBName], params[handler.TableName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey := tablecodec.EncodeTablePrefix(tableID)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxInt64))
	return h.GetMvccByStartTs(uint64(startTS), startKey, endKey)
}

// ServerInfo is used to report the servers info when do http request.
