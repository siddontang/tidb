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

package infoschema

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GetTiFlashServerInfo returns all TiFlash server infos
func GetTiFlashServerInfo(store kv.Storage) ([]ServerInfo, error) {
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		return nil, table.ErrUnsupportedOp
	}
	serversInfo, err := GetStoreServerInfo(store)
	if err != nil {
		return nil, err
	}
	serversInfo = FilterClusterServerInfo(serversInfo, set.NewStringSet(kv.TiFlash.Name()), set.NewStringSet())
	return serversInfo, nil
}

// FetchClusterServerInfoWithoutPrivilegeCheck fetches cluster server information
func FetchClusterServerInfoWithoutPrivilegeCheck(ctx context.Context, vars *variable.SessionVars, serversInfo []ServerInfo, serverInfoType diagnosticspb.ServerInfoType, recordWarningInStmtCtx bool) ([][]types.Datum, error) {
	type result struct {
		idx  int
		rows [][]types.Datum
		err  error
	}
	wg := sync.WaitGroup{}
	ch := make(chan result, len(serversInfo))
	infoTp := serverInfoType
	finalRows := make([][]types.Datum, 0, len(serversInfo)*10)
	for i, srv := range serversInfo {
		address := srv.Address
		remote := address
		if srv.ServerType == "tidb" || srv.ServerType == "tiproxy" {
			remote = srv.StatusAddr
		}
		wg.Add(1)
		go func(index int, remote, address, serverTP string) {
			util.WithRecovery(func() {
				defer wg.Done()
				items, err := getServerInfoByGRPC(ctx, remote, infoTp)
				if err != nil {
					ch <- result{idx: index, err: err}
					return
				}
				partRows := serverInfoItemToRows(items, serverTP, address)
				ch <- result{idx: index, rows: partRows}
			}, nil)
		}(i, remote, address, srv.ServerType)
	}
	wg.Wait()
	close(ch)
	// Keep the original order to make the result more stable
	var results []result //nolint: prealloc
	for result := range ch {
		if result.err != nil {
			if recordWarningInStmtCtx {
				vars.StmtCtx.AppendWarning(result.err)
			} else {
				log.Warn(result.err.Error())
			}
			continue
		}
		results = append(results, result)
	}
	slices.SortFunc(results, func(i, j result) int { return cmp.Compare(i.idx, j.idx) })
	for _, result := range results {
		finalRows = append(finalRows, result.rows...)
	}
	return finalRows, nil
}

func serverInfoItemToRows(items []*diagnosticspb.ServerInfoItem, tp, addr string) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(items))
	for _, v := range items {
		for _, item := range v.Pairs {
			row := types.MakeDatums(
				tp,
				addr,
				v.Tp,
				v.Name,
				item.Key,
				item.Value,
			)
			rows = append(rows, row)
		}
	}
	return rows
}

func getServerInfoByGRPC(ctx context.Context, address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	security := config.GetGlobalConfig().Security
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	conn, err := grpc.Dial(address, opt)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Error("close grpc connection error", zap.Error(err))
		}
	}()

	cli := diagnosticspb.NewDiagnosticsClient(conn)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		return nil, err
	}
	return r.Items, nil
}

// FilterClusterServerInfo filters serversInfo by nodeTypes and addresses
func FilterClusterServerInfo(serversInfo []ServerInfo, nodeTypes, addresses set.StringSet) []ServerInfo {
	if len(nodeTypes) == 0 && len(addresses) == 0 {
		return serversInfo
	}

	filterServers := make([]ServerInfo, 0, len(serversInfo))
	for _, srv := range serversInfo {
		// Skip some node type which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE type='tikv'
		if len(nodeTypes) > 0 && !nodeTypes.Exist(srv.ServerType) {
			continue
		}
		// Skip some node address which has been filtered in WHERE clause
		// e.g: SELECT * FROM cluster_config WHERE address='192.16.8.12:2379'
		if len(addresses) > 0 && !addresses.Exist(srv.Address) {
			continue
		}
		filterServers = append(filterServers, srv)
	}
	return filterServers
}

// GetDataFromStatusByConn is getting the per-connection status for `performance_schema.status_by_connection`
func GetDataFromStatusByConn(sctx sessionctx.Context) ([][]types.Datum, error) {
	sm := sctx.GetSessionManager()
	if sm == nil {
		return nil, nil
	}
	statusVars := sm.GetStatusVars()
	rows := make([][]types.Datum, 0, 2*len(statusVars))
	for pid, svar := range statusVars {
		for varkey, varval := range svar {
			row := types.MakeDatums(
				pid,
				varkey,
				varval,
			)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

const (
	// PrimaryKeyType is the string constant of PRIMARY KEY.
	PrimaryKeyType = "PRIMARY KEY"
	// PrimaryConstraint is the string constant of PRIMARY.
	PrimaryConstraint = "PRIMARY"
	// UniqueKeyType is the string constant of UNIQUE.
	UniqueKeyType = "UNIQUE"
	// ForeignKeyType is the string constant of Foreign Key.
	ForeignKeyType = "FOREIGN KEY"
)

const (
	// TiFlashWrite is the TiFlash write node in disaggregated mode.
	TiFlashWrite = "tiflash_write"
)

// ServerInfo represents the basic server information of single cluster component
type ServerInfo struct {
	ServerType     string
	Address        string
	StatusAddr     string
	Version        string
	GitHash        string
	StartTimestamp int64
	ServerID       uint64
	EngineRole     string
}

func (s *ServerInfo) isLoopBackOrUnspecifiedAddr(addr string) bool {
	tcpAddr, err := net.ResolveTCPAddr("", addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(tcpAddr.IP.String())
	return ip != nil && (ip.IsUnspecified() || ip.IsLoopback())
}

// ResolveLoopBackAddr exports for testing.
func (s *ServerInfo) ResolveLoopBackAddr() {
	if s.isLoopBackOrUnspecifiedAddr(s.Address) && !s.isLoopBackOrUnspecifiedAddr(s.StatusAddr) {
		addr, err1 := net.ResolveTCPAddr("", s.Address)
		statusAddr, err2 := net.ResolveTCPAddr("", s.StatusAddr)
		if err1 == nil && err2 == nil {
			addr.IP = statusAddr.IP
			s.Address = addr.String()
		}
	} else if !s.isLoopBackOrUnspecifiedAddr(s.Address) && s.isLoopBackOrUnspecifiedAddr(s.StatusAddr) {
		addr, err1 := net.ResolveTCPAddr("", s.Address)
		statusAddr, err2 := net.ResolveTCPAddr("", s.StatusAddr)
		if err1 == nil && err2 == nil {
			statusAddr.IP = addr.IP
			s.StatusAddr = statusAddr.String()
		}
	}
}

// GetClusterServerInfo returns all components information of cluster
func GetClusterServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	failpoint.Inject("mockClusterInfo", func(val failpoint.Value) {
		// The cluster topology is injected by `failpoint` expression and
		// there is no extra checks for it. (let the test fail if the expression invalid)
		if s := val.(string); len(s) > 0 {
			var servers []ServerInfo
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				serverID, err := strconv.ParseUint(parts[5], 10, 64)
				if err != nil {
					panic("convert parts[5] to uint64 failed")
				}
				servers = append(servers, ServerInfo{
					ServerType: parts[0],
					Address:    parts[1],
					StatusAddr: parts[2],
					Version:    parts[3],
					GitHash:    parts[4],
					ServerID:   serverID,
				})
			}
			failpoint.Return(servers, nil)
		}
	})

	type retriever func(ctx sessionctx.Context) ([]ServerInfo, error)
	retrievers := []retriever{GetTiDBServerInfo, GetPDServerInfo, func(ctx sessionctx.Context) ([]ServerInfo, error) {
		return GetStoreServerInfo(ctx.GetStore())
	}, GetTiProxyServerInfo, GetTiCDCServerInfo, GetTSOServerInfo, GetSchedulingServerInfo}
	//nolint: prealloc
	var servers []ServerInfo
	for _, r := range retrievers {
		nodes, err := r(ctx)
		if err != nil {
			return nil, err
		}

		// Create an error group with Panic recovery and concurrency limit
		resolveGroup := util.NewErrorGroupWithRecover()
		resolveGroup.SetLimit(runtime.GOMAXPROCS(0)) //Limit concurrency to number of CPU cores

		// Resolve loopback addresses concurrently for each node
		for i := range nodes {
			resolveGroup.Go(func() error {
				nodes[i].ResolveLoopBackAddr()
				return nil
			})
		}

		// Wait for all address resolutions to complete and check for errors
		if err := resolveGroup.Wait(); err != nil {
			return nil, err
		}
		servers = append(servers, nodes...)
	}
	return servers, nil
}

// GetTiDBServerInfo returns all TiDB nodes information of cluster
func GetTiDBServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	// Get TiDB servers info.
	tidbNodes, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var isDefaultVersion bool
	if len(config.GetGlobalConfig().ServerVersion) == 0 {
		isDefaultVersion = true
	}
	var servers = make([]ServerInfo, 0, len(tidbNodes))
	for _, node := range tidbNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "tidb",
			Address:        net.JoinHostPort(node.IP, strconv.Itoa(int(node.Port))),
			StatusAddr:     net.JoinHostPort(node.IP, strconv.Itoa(int(node.StatusPort))),
			Version:        FormatTiDBVersion(node.Version, isDefaultVersion),
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
			ServerID:       node.ServerIDGetter(),
		})
	}
	return servers, nil
}

// FormatTiDBVersion make TiDBVersion consistent to TiKV and PD.
// The default TiDBVersion is 5.7.25-TiDB-${TiDBReleaseVersion}.
func FormatTiDBVersion(TiDBVersion string, isDefaultVersion bool) string {
	var version, nodeVersion string

	// The user hasn't set the config 'ServerVersion'.
	if isDefaultVersion {
		nodeVersion = TiDBVersion[strings.Index(TiDBVersion, "TiDB-")+len("TiDB-"):]
		if len(nodeVersion) > 0 && nodeVersion[0] == 'v' {
			nodeVersion = nodeVersion[1:]
		}
		nodeVersions := strings.SplitN(nodeVersion, "-", 2)
		if len(nodeVersions) == 1 {
			version = nodeVersions[0]
		} else if len(nodeVersions) >= 2 {
			version = fmt.Sprintf("%s-%s", nodeVersions[0], nodeVersions[1])
		}
	} else { // The user has already set the config 'ServerVersion',it would be a complex scene, so just use the 'ServerVersion' as version.
		version = TiDBVersion
	}

	return version
}

// GetPDServerInfo returns all PD nodes information of cluster
func GetPDServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	// Get PD servers info.
	members, err := getEtcdMembers(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: maybe we should unify the PD API request interface.
	var (
		memberNum = len(members)
		servers   = make([]ServerInfo, 0, memberNum)
		errs      = make([]error, 0, memberNum)
	)
	if memberNum == 0 {
		return servers, nil
	}
	// Try on each member until one succeeds or all fail.
	for _, addr := range members {
		// Get PD version, git_hash
		url := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), addr, pd.Status)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("create pd server info request error", zap.String("url", url), zap.Error(err))
			errs = append(errs, err)
			continue
		}
		req.Header.Add("PD-Allow-follower-handle", "true")
		resp, err := util.InternalHTTPClient().Do(req)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("request pd server info error", zap.String("url", url), zap.Error(err))
			errs = append(errs, err)
			continue
		}
		var content = struct {
			Version        string `json:"version"`
			GitHash        string `json:"git_hash"`
			StartTimestamp int64  `json:"start_timestamp"`
		}{}
		err = json.NewDecoder(resp.Body).Decode(&content)
		terror.Log(resp.Body.Close())
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("close pd server info request error", zap.String("url", url), zap.Error(err))
			errs = append(errs, err)
			continue
		}
		if len(content.Version) > 0 && content.Version[0] == 'v' {
			content.Version = content.Version[1:]
		}

		servers = append(servers, ServerInfo{
			ServerType:     "pd",
			Address:        addr,
			StatusAddr:     addr,
			Version:        content.Version,
			GitHash:        content.GitHash,
			StartTimestamp: content.StartTimestamp,
		})
	}
	// Return the errors if all members' requests fail.
	if len(errs) == memberNum {
		errorMsg := ""
		for idx, err := range errs {
			errorMsg += err.Error()
			if idx < memberNum-1 {
				errorMsg += "; "
			}
		}
		return nil, errors.Trace(fmt.Errorf("%s", errorMsg))
	}
	return servers, nil
}

// GetTSOServerInfo returns all TSO nodes information of cluster
func GetTSOServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	return getMicroServiceServerInfo(ctx, tsoServiceName)
}

// GetSchedulingServerInfo returns all scheduling nodes information of cluster
func GetSchedulingServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	return getMicroServiceServerInfo(ctx, schedulingServiceName)
}

func getMicroServiceServerInfo(ctx sessionctx.Context, serviceName string) ([]ServerInfo, error) {
	members, err := getEtcdMembers(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: maybe we should unify the PD API request interface.
	var servers []ServerInfo

	if len(members) == 0 {
		return servers, nil
	}
	// Try on each member until one succeeds or all fail.
	for _, addr := range members {
		// Get members
		url := fmt.Sprintf("%s://%s%s/%s", util.InternalHTTPSchema(), addr, "/pd/api/v2/ms/members", serviceName)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("create microservice server info request error", zap.String("service", serviceName), zap.String("url", url), zap.Error(err))
			continue
		}
		req.Header.Add("PD-Allow-follower-handle", "true")
		resp, err := util.InternalHTTPClient().Do(req)
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("request microservice server info error", zap.String("service", serviceName), zap.String("url", url), zap.Error(err))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			terror.Log(resp.Body.Close())
			continue
		}
		var content = []struct {
			ServiceAddr    string `json:"service-addr"`
			Version        string `json:"version"`
			GitHash        string `json:"git-hash"`
			DeployPath     string `json:"deploy-path"`
			StartTimestamp int64  `json:"start-timestamp"`
		}{}
		err = json.NewDecoder(resp.Body).Decode(&content)
		terror.Log(resp.Body.Close())
		if err != nil {
			ctx.GetSessionVars().StmtCtx.AppendWarning(err)
			logutil.BgLogger().Warn("close microservice server info request error", zap.String("service", serviceName), zap.String("url", url), zap.Error(err))
			continue
		}

		for _, c := range content {
			addr := strings.TrimPrefix(c.ServiceAddr, "http://")
			addr = strings.TrimPrefix(addr, "https://")
			if len(c.Version) > 0 && c.Version[0] == 'v' {
				c.Version = c.Version[1:]
			}
			servers = append(servers, ServerInfo{
				ServerType:     serviceName,
				Address:        addr,
				StatusAddr:     addr,
				Version:        c.Version,
				GitHash:        c.GitHash,
				StartTimestamp: c.StartTimestamp,
			})
		}
		return servers, nil
	}
	return servers, nil
}

func getEtcdMembers(ctx sessionctx.Context) ([]string, error) {
	store := ctx.GetStore()
	etcd, ok := store.(kv.EtcdBackend)
	if !ok {
		return nil, errors.Errorf("%T not an etcd backend", store)
	}
	members, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func isTiFlashStore(store *metapb.Store) bool {
	return slices.ContainsFunc(store.Labels, func(label *metapb.StoreLabel) bool {
		return label.GetKey() == placement.EngineLabelKey && label.GetValue() == placement.EngineLabelTiFlash
	})
}

func isTiFlashWriteNode(store *metapb.Store) bool {
	return slices.ContainsFunc(store.Labels, func(label *metapb.StoreLabel) bool {
		return label.GetKey() == placement.EngineRoleLabelKey && label.GetValue() == placement.EngineRoleLabelWrite
	})
}

// GetStoreServerInfo returns all store nodes(TiKV or TiFlash) cluster information
func GetStoreServerInfo(store kv.Storage) ([]ServerInfo, error) {
	failpoint.Inject("mockStoreServerInfo", func(val failpoint.Value) {
		if s := val.(string); len(s) > 0 {
			var servers []ServerInfo
			for _, server := range strings.Split(s, ";") {
				parts := strings.Split(server, ",")
				servers = append(servers, ServerInfo{
					ServerType:     parts[0],
					Address:        parts[1],
					StatusAddr:     parts[2],
					Version:        parts[3],
					GitHash:        parts[4],
					StartTimestamp: 0,
				})
			}
			failpoint.Return(servers, nil)
		}
	})

	// Get TiKV servers info.
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.Errorf("%T is not an TiKV or TiFlash store instance", store)
	}
	pdClient := tikvStore.GetRegionCache().PDClient()
	if pdClient == nil {
		return nil, errors.New("pd unavailable")
	}
	stores, err := pdClient.GetAllStores(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	servers := make([]ServerInfo, 0, len(stores))
	for _, store := range stores {
		failpoint.Inject("mockStoreTombstone", func(val failpoint.Value) {
			if val.(bool) {
				store.State = metapb.StoreState_Tombstone
			}
		})

		if store.GetState() == metapb.StoreState_Tombstone {
			continue
		}
		var tp string
		if isTiFlashStore(store) {
			tp = kv.TiFlash.Name()
		} else {
			tp = tikv.GetStoreTypeByMeta(store).Name()
		}
		var engineRole string
		if isTiFlashWriteNode(store) {
			engineRole = placement.EngineRoleLabelWrite
		}
		servers = append(servers, ServerInfo{
			ServerType:     tp,
			Address:        store.Address,
			StatusAddr:     store.StatusAddress,
			Version:        FormatStoreServerVersion(store.Version),
			GitHash:        store.GitHash,
			StartTimestamp: store.StartTimestamp,
			EngineRole:     engineRole,
		})
	}
	return servers, nil
}

// FormatStoreServerVersion format version of store servers(Tikv or TiFlash)
func FormatStoreServerVersion(version string) string {
	if len(version) >= 1 && version[0] == 'v' {
		version = version[1:]
	}
	return version
}

// GetTiFlashStoreCount returns the count of tiflash server.
func GetTiFlashStoreCount(store kv.Storage) (cnt uint64, err error) {
	failpoint.Inject("mockTiFlashStoreCount", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(uint64(10), nil)
		}
	})

	stores, err := GetStoreServerInfo(store)
	if err != nil {
		return cnt, err
	}
	for _, store := range stores {
		if store.ServerType == kv.TiFlash.Name() {
			cnt++
		}
	}
	return cnt, nil
}

// GetTiProxyServerInfo gets server info of TiProxy from PD.
func GetTiProxyServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	tiproxyNodes, err := infosync.GetTiProxyServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers = make([]ServerInfo, 0, len(tiproxyNodes))
	for _, node := range tiproxyNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "tiproxy",
			Address:        net.JoinHostPort(node.IP, node.Port),
			StatusAddr:     net.JoinHostPort(node.IP, node.StatusPort),
			Version:        node.Version,
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
		})
	}
	return servers, nil
}

// GetTiCDCServerInfo gets server info of TiCDC from PD.
func GetTiCDCServerInfo(ctx sessionctx.Context) ([]ServerInfo, error) {
	ticdcNodes, err := infosync.GetTiCDCServerInfo(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var servers = make([]ServerInfo, 0, len(ticdcNodes))
	for _, node := range ticdcNodes {
		servers = append(servers, ServerInfo{
			ServerType:     "ticdc",
			Address:        node.Address,
			StatusAddr:     node.Address,
			Version:        node.Version,
			GitHash:        node.GitHash,
			StartTimestamp: node.StartTimestamp,
		})
	}
	return servers, nil
}
