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

package local

import (
	"context"
	"database/sql"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	backendkv "github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/tikv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/engine"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/retry"
	sd "github.com/tikv/pd/client/servicediscovery"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout             = 5 * time.Minute
	maxRetryTimes           = 20
	defaultRetryBackoffTime = 3 * time.Second

	gRPCKeepAliveTime    = 10 * time.Minute
	gRPCKeepAliveTimeout = 5 * time.Minute
	gRPCBackOffMaxDelay  = 10 * time.Minute

	propRangeIndex = "tikv.range_index"

	defaultPropSizeIndexDistance = 4 * units.MiB
	defaultPropKeysIndexDistance = 40 * 1024

	// the lower threshold of max open files for pebble db.
	openFilesLowerThreshold = 128

	duplicateDBName = "duplicates"
	scanRegionLimit = 128
)

var (
	// Local backend is compatible with TiDB [4.0.0, NextMajorVersion).
	localMinTiDBVersion    = *semver.New("4.0.0")
	localMinTiKVVersion    = *semver.New("4.0.0")
	localMinPDVersion      = *semver.New("4.0.0")
	localMaxTiDBVersion    = version.NextMajorVersion()
	localMaxTiKVVersion    = version.NextMajorVersion()
	localMaxPDVersion      = version.NextMajorVersion()
	tiFlashMinVersion      = *semver.New("4.0.5")
	tikvSideFreeSpaceCheck = *semver.New("8.0.0")

	errorEngineClosed     = errors.New("engine is closed")
	maxRetryBackoffSecond = 30

	// MaxWriteAndIngestRetryTimes is the max retry times for write and ingest.
	// A large retry times is for tolerating tikv cluster failures.
	MaxWriteAndIngestRetryTimes = 30

	// Unlimited RPC receive message size for TiKV importer
	unlimitedRPCRecvMsgSize = math.MaxInt32

	// ForcePartitionRegionThreshold is the threshold of regions to force partition range.
	// It is exported for testing.
	ForcePartitionRegionThreshold = 100
)

// importClientFactory is factory to create new import client for specific store.
type importClientFactory interface {
	create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error)
	close()
}

type importClientFactoryImpl struct {
	conns           *common.GRPCConns
	splitCli        split.SplitClient
	tls             *common.TLS
	tcpConcurrency  int
	compressionType config.CompressionType
}

func newImportClientFactoryImpl(
	splitCli split.SplitClient,
	tls *common.TLS,
	tcpConcurrency int,
	compressionType config.CompressionType,
) *importClientFactoryImpl {
	return &importClientFactoryImpl{
		conns:           common.NewGRPCConns(),
		splitCli:        splitCli,
		tls:             tls,
		tcpConcurrency:  tcpConcurrency,
		compressionType: compressionType,
	}
}

func (f *importClientFactoryImpl) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := f.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var opts []grpc.DialOption
	if f.tls.TLSConfig() != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(f.tls.TLSConfig())))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	opts = append(opts,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(unlimitedRPCRecvMsgSize)),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	switch f.compressionType {
	case config.CompressionNone:
		// do nothing
	case config.CompressionGzip:
		// Use custom compressor/decompressor to speed up compression/decompression.
		// Note that here we don't use grpc.UseCompressor option although it's the recommended way.
		// Because gprc-go uses a global registry to store compressor/decompressor, we can't make sure
		// the compressor/decompressor is not registered by other components.
		opts = append(opts, grpc.WithCompressor(&gzipCompressor{}), grpc.WithDecompressor(&gzipDecompressor{}))
	default:
		return nil, common.ErrInvalidConfig.GenWithStack("unsupported compression type %s", f.compressionType)
	}

	failpoint.Inject("LoggingImportBytes", func() {
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
			if err != nil {
				return nil, err
			}
			return &loggingConn{Conn: conn}, nil
		}))
	})

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}

func (f *importClientFactoryImpl) getGrpcConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	return f.conns.GetGrpcConn(ctx, storeID, f.tcpConcurrency,
		func(ctx context.Context) (*grpc.ClientConn, error) {
			return f.makeConn(ctx, storeID)
		})
}

// create creates a new import client for specific store.
func (f *importClientFactoryImpl) create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	conn, err := f.getGrpcConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

// close closes the factory.
func (f *importClientFactoryImpl) close() {
	f.conns.Close()
}

type loggingConn struct {
	net.Conn
}

// Write implements net.Conn.Write
func (c loggingConn) Write(b []byte) (int, error) {
	log.L().Debug("import write", zap.Int("bytes", len(b)))
	return c.Conn.Write(b)
}

type encodingBuilder struct {
	metrics *metric.Metrics
}

// NewEncodingBuilder creates an KVEncodingBuilder with local backend implementation.
func NewEncodingBuilder(ctx context.Context) encode.EncodingBuilder {
	result := new(encodingBuilder)
	if m, ok := metric.FromContext(ctx); ok {
		result.metrics = m
	}
	return result
}

// NewEncoder creates a KV encoder.
// It implements the `backend.EncodingBuilder` interface.
func (b *encodingBuilder) NewEncoder(_ context.Context, config *encode.EncodingConfig) (encode.Encoder, error) {
	return backendkv.NewTableKVEncoder(config, b.metrics)
}

// MakeEmptyRows creates an empty KV rows.
// It implements the `backend.EncodingBuilder` interface.
func (*encodingBuilder) MakeEmptyRows() encode.Rows {
	return backendkv.MakeRowsFromKvPairs(nil)
}

type targetInfoGetter struct {
	tls       *common.TLS
	targetDB  *sql.DB
	pdHTTPCli pdhttp.Client
}

// NewTargetInfoGetter creates an TargetInfoGetter with local backend
// implementation. `pdHTTPCli` should not be nil when need to check component
// versions in CheckRequirements.
func NewTargetInfoGetter(
	tls *common.TLS,
	db *sql.DB,
	pdHTTPCli pdhttp.Client,
) backend.TargetInfoGetter {
	return &targetInfoGetter{
		tls:       tls,
		targetDB:  db,
		pdHTTPCli: pdHTTPCli,
	}
}

// FetchRemoteDBModels implements the `backend.TargetInfoGetter` interface.
func (g *targetInfoGetter) FetchRemoteDBModels(ctx context.Context) ([]*model.DBInfo, error) {
	return tikv.FetchRemoteDBModelsFromTLS(ctx, g.tls)
}

// FetchRemoteTableModels obtains the models of all tables given the schema name.
// It implements the `TargetInfoGetter` interface.
func (g *targetInfoGetter) FetchRemoteTableModels(
	ctx context.Context,
	schemaName string,
	tableNames []string,
) (map[string]*model.TableInfo, error) {
	allTablesInDB, err := tikv.FetchRemoteTableModelsFromTLS(ctx, g.tls, schemaName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableNamesSet := make(map[string]struct{}, len(tableNames))
	for _, name := range tableNames {
		tableNamesSet[strings.ToLower(name)] = struct{}{}
	}
	ret := make(map[string]*model.TableInfo, len(tableNames))
	for _, tbl := range allTablesInDB {
		if _, ok := tableNamesSet[tbl.Name.L]; ok {
			ret[tbl.Name.L] = tbl
		}
	}
	return ret, nil
}

// CheckRequirements performs the check whether the backend satisfies the version requirements.
// It implements the `TargetInfoGetter` interface.
func (g *targetInfoGetter) CheckRequirements(ctx context.Context, checkCtx *backend.CheckCtx) error {
	// TODO: support lightning via SQL
	versionStr, err := version.FetchVersion(ctx, g.targetDB)
	if err != nil {
		return errors.Trace(err)
	}
	if err := checkTiDBVersion(ctx, versionStr, localMinTiDBVersion, localMaxTiDBVersion); err != nil {
		return err
	}
	if g.pdHTTPCli == nil {
		return common.ErrUnknown.GenWithStack("pd HTTP client is required for component version check in local backend")
	}
	if err := tikv.CheckPDVersion(ctx, g.pdHTTPCli, localMinPDVersion, localMaxPDVersion); err != nil {
		return err
	}
	if err := tikv.CheckTiKVVersion(ctx, g.pdHTTPCli, localMinTiKVVersion, localMaxTiKVVersion); err != nil {
		return err
	}

	serverInfo := version.ParseServerInfo(versionStr)
	return checkTiFlashVersion(ctx, g.targetDB, checkCtx, *serverInfo.ServerVersion)
}

func checkTiDBVersion(_ context.Context, versionStr string, requiredMinVersion, requiredMaxVersion semver.Version) error {
	return version.CheckTiDBVersion(versionStr, requiredMinVersion, requiredMaxVersion)
}

var tiFlashReplicaQuery = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TIFLASH_REPLICA WHERE REPLICA_COUNT > 0;"

// TiFlashReplicaQueryForTest is only used for tests.
var TiFlashReplicaQueryForTest = tiFlashReplicaQuery

type tblName struct {
	schema string
	name   string
}

type tblNames []tblName

// String implements fmt.Stringer
func (t tblNames) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, n := range t {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(common.UniqueTable(n.schema, n.name))
	}
	b.WriteByte(']')
	return b.String()
}

// CheckTiFlashVersionForTest is only used for tests.
var CheckTiFlashVersionForTest = checkTiFlashVersion

// check TiFlash replicas.
// local backend doesn't support TiFlash before tidb v4.0.5
func checkTiFlashVersion(ctx context.Context, db *sql.DB, checkCtx *backend.CheckCtx, tidbVersion semver.Version) error {
	if tidbVersion.Compare(tiFlashMinVersion) >= 0 {
		return nil
	}

	exec := common.SQLWithRetry{
		DB:     db,
		Logger: log.Wrap(tidblogutil.Logger(ctx)),
	}

	res, err := exec.QueryStringRows(ctx, "fetch tiflash replica info", tiFlashReplicaQuery)
	if err != nil {
		return errors.Annotate(err, "fetch tiflash replica info failed")
	}

	tiFlashTablesMap := make(map[tblName]struct{}, len(res))
	for _, tblInfo := range res {
		name := tblName{schema: tblInfo[0], name: tblInfo[1]}
		tiFlashTablesMap[name] = struct{}{}
	}

	tiFlashTables := make(tblNames, 0)
	for _, dbMeta := range checkCtx.DBMetas {
		for _, tblMeta := range dbMeta.Tables {
			if len(tblMeta.DataFiles) == 0 {
				continue
			}
			name := tblName{schema: tblMeta.DB, name: tblMeta.Name}
			if _, ok := tiFlashTablesMap[name]; ok {
				tiFlashTables = append(tiFlashTables, name)
			}
		}
	}

	if len(tiFlashTables) > 0 {
		helpInfo := "Please either upgrade TiDB to version >= 4.0.5 or add TiFlash replica after load data."
		return errors.Errorf("lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: %s. "+helpInfo, tiFlashTables)
	}
	return nil
}

// BackendConfig is the config for local backend.
type BackendConfig struct {
	// comma separated list of PD endpoints.
	PDAddr        string
	LocalStoreDir string
	// max number of cached grpc.ClientConn to a store.
	// note: this is not the limit of actual connections, each grpc.ClientConn can have one or more of it.
	MaxConnPerStore int
	// compress type when write or ingest into tikv
	ConnCompressType config.CompressionType
	// concurrency is used in these places:
	// 1. generateJobForRange parallelism, only for local engine.
	// 2. data size loaded from LoadIngestData, only for external engine.
	// 3. region job worker pool size
	WorkerConcurrency atomic.Int32
	// batch kv size when writing to TiKV
	KVWriteBatchSize       int64
	RegionSplitBatchSize   int
	RegionSplitConcurrency int
	CheckpointEnabled      bool
	// memory table size of pebble. since pebble can have multiple mem tables, the max memory used is
	// MemTableSize * MemTableStopWritesThreshold, see pebble.Options for more details.
	MemTableSize int
	// LocalWriterMemCacheSize is the memory threshold for one local writer of
	// engines. If the KV payload size exceeds LocalWriterMemCacheSize, local writer
	// will flush them into the engine.
	//
	// It has lower priority than LocalWriterConfig.Local.MemCacheSize.
	LocalWriterMemCacheSize int64
	// whether check TiKV capacity before write & ingest.
	ShouldCheckTiKV    bool
	DupeDetectEnabled  bool
	DuplicateDetectOpt common.DupDetectOpt
	TiKVWorkerURL      string
	// max write speed in bytes per second to each store(burst is allowed), 0 means no limit
	StoreWriteBWLimit int
	// When TiKV is in normal mode, ingesting too many SSTs will cause TiKV write stall.
	// To avoid this, we should check write stall before ingesting SSTs. Note that, we
	// must check both leader node and followers in client side, because followers will
	// not check write stall as long as ingest command is accepted by leader.
	ShouldCheckWriteStall bool
	// soft limit on the number of open files that can be used by pebble DB.
	// the minimum value is 128.
	MaxOpenFiles int
	KeyspaceName string
	// the scope when pause PD schedulers.
	PausePDSchedulerScope     config.PausePDSchedulerScope
	ResourceGroupName         string
	TaskType                  string
	RaftKV2SwitchModeDuration time.Duration
	// whether disable automatic compactions of pebble db of engine.
	// deduplicate pebble db is not affected by this option.
	// see DisableAutomaticCompactions of pebble.Options for more details.
	// default true.
	DisableAutomaticCompactions bool
	BlockSize                   int
}

// NewBackendConfig creates a new BackendConfig.
func NewBackendConfig(cfg *config.Config, maxOpenFiles int, keyspaceName, resourceGroupName, taskType string, raftKV2SwitchModeDuration time.Duration) BackendConfig {
	return BackendConfig{
		PDAddr:                      cfg.TiDB.PdAddr,
		LocalStoreDir:               cfg.TikvImporter.SortedKVDir,
		MaxConnPerStore:             cfg.TikvImporter.RangeConcurrency,
		ConnCompressType:            cfg.TikvImporter.CompressKVPairs,
		WorkerConcurrency:           *atomic.NewInt32(int32(cfg.TikvImporter.RangeConcurrency) * 2),
		BlockSize:                   int(cfg.TikvImporter.BlockSize),
		KVWriteBatchSize:            int64(cfg.TikvImporter.SendKVSize),
		RegionSplitBatchSize:        cfg.TikvImporter.RegionSplitBatchSize,
		RegionSplitConcurrency:      cfg.TikvImporter.RegionSplitConcurrency,
		CheckpointEnabled:           cfg.Checkpoint.Enable,
		MemTableSize:                int(cfg.TikvImporter.EngineMemCacheSize),
		LocalWriterMemCacheSize:     int64(cfg.TikvImporter.LocalWriterMemCacheSize),
		ShouldCheckTiKV:             cfg.App.CheckRequirements,
		DupeDetectEnabled:           cfg.Conflict.Strategy != config.NoneOnDup,
		DuplicateDetectOpt:          common.DupDetectOpt{ReportErrOnDup: cfg.Conflict.Strategy == config.ErrorOnDup},
		StoreWriteBWLimit:           int(cfg.TikvImporter.StoreWriteBWLimit),
		ShouldCheckWriteStall:       cfg.Cron.SwitchMode.Duration == 0,
		MaxOpenFiles:                maxOpenFiles,
		KeyspaceName:                keyspaceName,
		PausePDSchedulerScope:       cfg.TikvImporter.PausePDSchedulerScope,
		ResourceGroupName:           resourceGroupName,
		TaskType:                    taskType,
		RaftKV2SwitchModeDuration:   raftKV2SwitchModeDuration,
		DisableAutomaticCompactions: true,
	}
}

func (c *BackendConfig) adjust() {
	c.MaxOpenFiles = max(c.MaxOpenFiles, openFilesLowerThreshold)
}

// GetWorkerConcurrency gets the current concurrency of the backend
func (c *BackendConfig) GetWorkerConcurrency() int {
	return int(c.WorkerConcurrency.Load())
}

// SetWorkerConcurrency sets the current concurrency of the backend
func (c *BackendConfig) SetWorkerConcurrency(concurrency int) {
	c.WorkerConcurrency.Store(int32(concurrency))
}

// Backend is a local backend.
type Backend struct {
	pdCli     pd.Client
	pdHTTPCli pdhttp.Client
	splitCli  split.SplitClient
	tikvCli   *tikvclient.KVStore
	tls       *common.TLS
	tikvCodec tikvclient.Codec

	collector execute.Collector

	BackendConfig
	engineMgr *engineManager

	supportMultiIngest  bool
	importClientFactory importClientFactory

	metrics       *metric.Common
	writeLimiter  StoreWriteLimiter
	ingestLimiter atomic.Pointer[ingestLimiter]
	logger        log.Logger

	nextgenHTTPCli *http.Client
}

var _ DiskUsage = (*Backend)(nil)
var _ StoreHelper = (*Backend)(nil)
var _ backend.Backend = (*Backend)(nil)

const (
	pdCliMaxMsgSize = int(128 * units.MiB) // pd.ScanRegion may return a large response
)

var (
	maxCallMsgSize = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(pdCliMaxMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(pdCliMaxMsgSize)),
	}
)

// NewBackend creates new connections to tikv.
func NewBackend(
	ctx context.Context,
	tls *common.TLS,
	config BackendConfig,
	pdSvcDiscovery sd.ServiceDiscovery,
) (b *Backend, err error) {
	var (
		pdCli                pd.Client
		spkv                 *tikvclient.EtcdSafePointKV
		pdCliForTiKV         *tikvclient.CodecPDClient
		rpcCli               tikvclient.Client
		tikvCli              *tikvclient.KVStore
		pdHTTPCli            pdhttp.Client
		importClientFactory  *importClientFactoryImpl
		multiIngestSupported bool
	)
	defer func() {
		if err == nil {
			return
		}
		if importClientFactory != nil {
			importClientFactory.close()
		}
		if pdHTTPCli != nil {
			pdHTTPCli.Close()
		}
		if tikvCli != nil {
			// tikvCli uses pdCliForTiKV(which wraps pdCli) , spkv and rpcCli, so
			// close tikvCli will close all of them.
			_ = tikvCli.Close()
		} else {
			if rpcCli != nil {
				_ = rpcCli.Close()
			}
			if spkv != nil {
				_ = spkv.Close()
			}
			// pdCliForTiKV wraps pdCli, so we only need close pdCli
			if pdCli != nil {
				pdCli.Close()
			}
		}
	}()
	config.adjust()
	var pdAddrs []string
	if pdSvcDiscovery != nil {
		pdAddrs = pdSvcDiscovery.GetServiceURLs()
		// TODO(lance6716): if PD client can support creating a client with external
		// service discovery, we can directly pass pdSvcDiscovery.
	} else {
		pdAddrs = strings.Split(config.PDAddr, ",")
	}
	pdCli, err = pd.NewClientWithContext(
		ctx, caller.Component("lightning-local-backend"), pdAddrs, tls.ToPDSecurityOption(),
		opt.WithGRPCDialOptions(maxCallMsgSize...),
		// If the time too short, we may scatter a region many times, because
		// the interface `ScatterRegions` may time out.
		opt.WithCustomTimeoutOption(60*time.Second),
	)
	if err != nil {
		return nil, common.NormalizeOrWrapErr(common.ErrCreatePDClient, err)
	}

	// The following copies tikv.NewTxnClient without creating yet another pdClient.
	spkv, err = tikvclient.NewEtcdSafePointKV(pdAddrs, tls.TLSConfig())
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}

	if config.KeyspaceName == "" {
		pdCliForTiKV = tikvclient.NewCodecPDClient(tikvclient.ModeTxn, pdCli)
	} else {
		pdCliForTiKV, err = tikvclient.NewCodecPDClientWithKeyspace(tikvclient.ModeTxn, pdCli, config.KeyspaceName)
		if err != nil {
			return nil, common.ErrCreatePDClient.Wrap(err).GenWithStackByArgs()
		}
	}

	tikvCodec := pdCliForTiKV.GetCodec()
	rpcCli = tikvclient.NewRPCClient(tikvclient.WithSecurity(tls.ToTiKVSecurityConfig()), tikvclient.WithCodec(tikvCodec))
	tikvCli, err = tikvclient.NewKVStore("lightning-local-backend", pdCliForTiKV, spkv, rpcCli)
	if err != nil {
		return nil, common.ErrCreateKVClient.Wrap(err).GenWithStackByArgs()
	}
	pdHTTPCli = pdhttp.NewClientWithServiceDiscovery(
		"lightning",
		pdCli.GetServiceDiscovery(),
		pdhttp.WithTLSConfig(tls.TLSConfig()),
	).WithBackoffer(retry.InitialBackoffer(time.Second, time.Second, pdutil.PDRequestRetryTime*time.Second))
	splitCli := split.NewClient(pdCli, pdHTTPCli, tls.TLSConfig(), config.RegionSplitBatchSize, config.RegionSplitConcurrency)
	importClientFactory = newImportClientFactoryImpl(splitCli, tls, config.MaxConnPerStore, config.ConnCompressType)

	multiIngestSupported, err = checkMultiIngestSupport(ctx, pdCli, importClientFactory)
	if err != nil {
		return nil, common.ErrCheckMultiIngest.Wrap(err).GenWithStackByArgs()
	}

	writeLimiter := newStoreWriteLimiter(config.StoreWriteBWLimit)
	local := &Backend{
		pdCli:     pdCli,
		pdHTTPCli: pdHTTPCli,
		splitCli:  splitCli,
		tikvCli:   tikvCli,
		tls:       tls,
		tikvCodec: tikvCodec,

		BackendConfig: config,

		supportMultiIngest:  multiIngestSupported,
		importClientFactory: importClientFactory,
		writeLimiter:        writeLimiter,
		logger:              log.Wrap(tidblogutil.Logger(ctx)),
	}
	local.engineMgr, err = newEngineManager(config, local, local.logger)
	if err != nil {
		return nil, err
	}
	if m, ok := metric.GetCommonMetric(ctx); ok {
		local.metrics = m
	}
	local.tikvSideCheckFreeSpace(ctx)

	return local, nil
}

// NewBackendForTest creates a new Backend for test.
func NewBackendForTest(ctx context.Context, config BackendConfig, storeHelper StoreHelper) (*Backend, error) {
	config.adjust()

	logger := log.Wrap(tidblogutil.Logger(ctx))
	engineMgr, err := newEngineManager(config, storeHelper, logger)
	if err != nil {
		return nil, err
	}
	local := &Backend{
		BackendConfig: config,
		logger:        logger,
		engineMgr:     engineMgr,
	}
	if m, ok := metric.GetCommonMetric(ctx); ok {
		local.metrics = m
	}

	return local, nil
}

// SetCollector sets the collector for the local backend
func (local *Backend) SetCollector(c execute.Collector) {
	local.collector = c
}

// TotalMemoryConsume returns the total memory usage of the local backend.
func (local *Backend) TotalMemoryConsume() int64 {
	return local.engineMgr.totalMemoryConsume()
}

func checkMultiIngestSupport(ctx context.Context, pdCli pd.Client, factory importClientFactory) (bool, error) {
	stores, err := pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return false, errors.Trace(err)
	}

	hasTiFlash := false
	for _, s := range stores {
		if s.State == metapb.StoreState_Up && engine.IsTiFlash(s) {
			hasTiFlash = true
			break
		}
	}

	for _, s := range stores {
		// skip stores that are not online
		if s.State != metapb.StoreState_Up || engine.IsTiFlash(s) {
			continue
		}
		var err error
		for i := range maxRetryTimes {
			if i > 0 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}
			client, err1 := factory.create(ctx, s.Id)
			if err1 != nil {
				err = err1
				tidblogutil.Logger(ctx).Warn("get import client failed", zap.Error(err), zap.String("store", s.Address))
				continue
			}
			_, err = client.MultiIngest(ctx, &sst.MultiIngestRequest{})
			if err == nil {
				break
			}
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unimplemented {
					tidblogutil.Logger(ctx).Info("multi ingest not support", zap.Any("unsupported store", s))
					return false, nil
				}
			}
			tidblogutil.Logger(ctx).Warn("check multi ingest support failed", zap.Error(err), zap.String("store", s.Address),
				zap.Int("retry", i))
		}
		if err != nil {
			// if the cluster contains no TiFlash store, we don't need the multi-ingest feature,
			// so in this condition, downgrade the logic instead of return an error.
			if hasTiFlash {
				return false, errors.Trace(err)
			}
			tidblogutil.Logger(ctx).Warn("check multi failed all retry, fallback to false", log.ShortError(err))
			return false, nil
		}
	}

	tidblogutil.Logger(ctx).Info("multi ingest support")
	return true, nil
}

func (local *Backend) tikvSideCheckFreeSpace(ctx context.Context) {
	if !local.ShouldCheckTiKV {
		return
	}
	err := tikv.ForTiKVVersions(
		ctx,
		local.pdHTTPCli,
		func(version *semver.Version, addrMsg string) error {
			if version.Compare(tikvSideFreeSpaceCheck) < 0 {
				return errors.Errorf(
					"%s has version %s, it does not support server side free space check",
					addrMsg, version,
				)
			}
			return nil
		},
	)
	if err == nil {
		local.logger.Info("TiKV server side free space check is enabled, so lightning will turn it off")
		local.ShouldCheckTiKV = false
	} else {
		local.logger.Info("", zap.Error(err))
	}
}

// Close the local backend.
func (local *Backend) Close() {
	local.engineMgr.close()
	local.importClientFactory.close()

	_ = local.tikvCli.Close()
	local.pdHTTPCli.Close()
	local.pdCli.Close()
	if local.nextgenHTTPCli != nil {
		local.nextgenHTTPCli.CloseIdleConnections()
	}
}

// FlushEngine ensure the written data is saved successfully, to make sure no data lose after restart
func (local *Backend) FlushEngine(ctx context.Context, engineID uuid.UUID) error {
	return local.engineMgr.flushEngine(ctx, engineID)
}

// FlushAllEngines flush all engines.
func (local *Backend) FlushAllEngines(parentCtx context.Context) (err error) {
	return local.engineMgr.flushAllEngines(parentCtx)
}

// CleanupAllLocalEngines closes and removes all local engines, used for best-effort cleanup on error.
func (local *Backend) CleanupAllLocalEngines(ctx context.Context) error {
	return local.engineMgr.cleanupAllLocalEngines(ctx)
}

// RetryImportDelay returns the delay time before retrying to import a file.
func (*Backend) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

// ShouldPostProcess returns true if the backend should post process the data.
func (*Backend) ShouldPostProcess() bool {
	return true
}

// OpenEngine must be called with holding mutex of Engine.
func (local *Backend) OpenEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	return local.engineMgr.openEngine(ctx, cfg, engineUUID)
}

// CloseEngine closes backend engine by uuid.
func (local *Backend) CloseEngine(ctx context.Context, cfg *backend.EngineConfig, engineUUID uuid.UUID) error {
	return local.engineMgr.closeEngine(ctx, cfg, engineUUID)
}

// forceTableSplitRange turns on force_partition_range for importing.
// See https://github.com/tikv/tikv/pull/18866 for detail.
// It returns a resetter to turn off.
func (local *Backend) forceTableSplitRange(ctx context.Context,
	startKey, endKey kv.Key, stores []*metapb.Store) (resetter func()) {
	subctx, cancel := context.WithCancel(ctx)
	var wg util.WaitGroupWrapper
	clients := make([]sst.ImportSSTClient, 0, len(stores))
	storeAddrs := make([]string, 0, len(stores))
	const ttlSecond = uint64(3600) // default 1 hour
	addReq := &sst.AddPartitionRangeRequest{
		Range: &sst.Range{
			Start: startKey,
			End:   endKey,
		},
		TtlSeconds: ttlSecond,
	}
	removeReq := &sst.RemovePartitionRangeRequest{
		Range: &sst.Range{
			Start: startKey,
			End:   endKey,
		},
	}

	for _, store := range stores {
		if store.StatusAddress == "" || engine.IsTiFlash(store) {
			continue
		}
		importCli, err := local.importClientFactory.create(subctx, store.Id)
		if err != nil {
			tidblogutil.Logger(subctx).Warn("create import client failed", zap.Error(err), zap.String("store", store.StatusAddress))
			continue
		}
		clients = append(clients, importCli)
		storeAddrs = append(storeAddrs, store.StatusAddress)
	}

	addTableSplitRange := func() {
		var (
			firstErr      error
			mu            sync.Mutex
			successStores = make([]string, 0, len(clients))
			failedStores  = make([]string, 0, len(clients))
		)
		concurrency := int(local.WorkerConcurrency.Load())
		if concurrency <= 0 {
			concurrency = 1
		}
		eg, _ := util.NewErrorGroupWithRecoverWithCtx(subctx)
		eg.SetLimit(concurrency)
		for i, c := range clients {
			client, addr := c, storeAddrs[i]
			eg.Go(func() error {
				failpoint.InjectCall("AddPartitionRangeForTable")
				_, err := client.AddForcePartitionRange(subctx, addReq)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successStores = append(successStores, addr)
				} else {
					failedStores = append(failedStores, addr)
					if firstErr == nil {
						firstErr = err
					}
				}
				return nil
			})
		}
		_ = eg.Wait()
		tidblogutil.Logger(subctx).Info("call AddForcePartitionRange",
			zap.Strings("success stores", successStores),
			zap.Strings("failed stores", failedStores),
			zap.Error(firstErr),
		)
	}

	addTableSplitRange()
	wg.Run(func() {
		timeout := time.Duration(ttlSecond) * time.Second / 5 // 12min
		ticker := time.NewTicker(timeout)
		defer ticker.Stop()
		for {
			select {
			case <-subctx.Done():
				return
			case <-ticker.C:
				addTableSplitRange()
			}
		}
	})

	resetter = func() {
		cancel()
		wg.Wait()

		var (
			firstErr      error
			mu            sync.Mutex
			successStores = make([]string, 0, len(clients))
			failedStores  = make([]string, 0, len(clients))
		)
		concurrency := int(local.WorkerConcurrency.Load())
		if concurrency <= 0 {
			concurrency = 1
		}
		eg, _ := util.NewErrorGroupWithRecoverWithCtx(ctx)
		eg.SetLimit(concurrency)
		for i, c := range clients {
			client, addr := c, storeAddrs[i]
			eg.Go(func() error {
				failpoint.InjectCall("RemovePartitionRangeRequest")
				_, err := client.RemoveForcePartitionRange(ctx, removeReq)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successStores = append(successStores, addr)
				} else {
					failedStores = append(failedStores, addr)
					if firstErr == nil {
						firstErr = err
					}
				}
				return nil
			})
		}
		_ = eg.Wait()
		tidblogutil.Logger(ctx).Info("call RemoveForcePartitionRange",
			zap.Strings("success stores", successStores),
			zap.Strings("failed stores", failedStores),
			zap.Error(firstErr),
		)
	}
	return resetter
}
