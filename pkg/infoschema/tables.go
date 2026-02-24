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
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
)

const (
	// TableSchemata is the string constant of infoschema table.
	TableSchemata = "SCHEMATA"
	// TableTables is the string constant of infoschema table.
	TableTables = "TABLES"
	// TableColumns is the string constant of infoschema table
	TableColumns          = "COLUMNS"
	tableColumnStatistics = "COLUMN_STATISTICS"
	// TableStatistics is the string constant of infoschema table
	TableStatistics = "STATISTICS"
	// TableCharacterSets is the string constant of infoschema charactersets memory table
	TableCharacterSets = "CHARACTER_SETS"
	// TableCollations is the string constant of infoschema collations memory table.
	TableCollations = "COLLATIONS"
	tableFiles      = "FILES"
	// CatalogVal is the string constant of TABLE_CATALOG.
	CatalogVal = "def"
	// TableProfiling is the string constant of infoschema table.
	TableProfiling = "PROFILING"
	// TablePartitions is the string constant of infoschema table.
	TablePartitions = "PARTITIONS"
	// TableKeyColumn is the string constant of KEY_COLUMN_USAGE.
	TableKeyColumn = "KEY_COLUMN_USAGE"
	// TableReferConst is the string constant of REFERENTIAL_CONSTRAINTS.
	TableReferConst = "REFERENTIAL_CONSTRAINTS"
	tablePlugins    = "PLUGINS"
	// TableConstraints is the string constant of TABLE_CONSTRAINTS.
	TableConstraints = "TABLE_CONSTRAINTS"
	tableTriggers    = "TRIGGERS"
	// TableUserPrivileges is the string constant of infoschema user privilege table.
	TableUserPrivileges   = "USER_PRIVILEGES"
	tableSchemaPrivileges = "SCHEMA_PRIVILEGES"
	tableTablePrivileges  = "TABLE_PRIVILEGES"
	tableColumnPrivileges = "COLUMN_PRIVILEGES"
	// TableEngines is the string constant of infoschema table.
	TableEngines = "ENGINES"
	// TableViews is the string constant of infoschema table.
	TableViews          = "VIEWS"
	tableRoutines       = "ROUTINES"
	tableParameters     = "PARAMETERS"
	tableEvents         = "EVENTS"
	tableOptimizerTrace = "OPTIMIZER_TRACE"
	tableTableSpaces    = "TABLESPACES"
	// TableCollationCharacterSetApplicability is the string constant of infoschema memory table.
	TableCollationCharacterSetApplicability = "COLLATION_CHARACTER_SET_APPLICABILITY"
	// TableProcesslist is the string constant of infoschema table.
	TableProcesslist = "PROCESSLIST"
	// TableTiDBIndexes is the string constant of infoschema table
	TableTiDBIndexes = "TIDB_INDEXES"
	// TableTiDBHotRegions is the string constant of infoschema table
	TableTiDBHotRegions = "TIDB_HOT_REGIONS"
	// TableTiDBHotRegionsHistory is the string constant of infoschema table
	TableTiDBHotRegionsHistory = "TIDB_HOT_REGIONS_HISTORY"
	// TableTiKVStoreStatus is the string constant of infoschema table
	TableTiKVStoreStatus = "TIKV_STORE_STATUS"
	// TableAnalyzeStatus is the string constant of Analyze Status
	TableAnalyzeStatus = "ANALYZE_STATUS"
	// TableTiKVRegionStatus is the string constant of infoschema table
	TableTiKVRegionStatus = "TIKV_REGION_STATUS"
	// TableTiKVRegionPeers is the string constant of infoschema table
	TableTiKVRegionPeers = "TIKV_REGION_PEERS"
	// TableTiDBServersInfo is the string constant of TiDB server information table.
	TableTiDBServersInfo = "TIDB_SERVERS_INFO"
	// TableSlowQuery is the string constant of slow query memory table.
	TableSlowQuery = "SLOW_QUERY"
	// TableClusterInfo is the string constant of cluster info memory table.
	TableClusterInfo = "CLUSTER_INFO"
	// TableClusterConfig is the string constant of cluster configuration memory table.
	TableClusterConfig = "CLUSTER_CONFIG"
	// TableClusterLog is the string constant of cluster log memory table.
	TableClusterLog = "CLUSTER_LOG"
	// TableClusterLoad is the string constant of cluster load memory table.
	TableClusterLoad = "CLUSTER_LOAD"
	// TableClusterHardware is the string constant of cluster hardware table.
	TableClusterHardware = "CLUSTER_HARDWARE"
	// TableClusterSystemInfo is the string constant of cluster system info table.
	TableClusterSystemInfo = "CLUSTER_SYSTEMINFO"
	// TableTiFlashReplica is the string constant of tiflash replica table.
	TableTiFlashReplica = "TIFLASH_REPLICA"
	// TableInspectionResult is the string constant of inspection result table.
	TableInspectionResult = "INSPECTION_RESULT"
	// TableMetricTables is a table that contains all metrics table definition.
	TableMetricTables = "METRICS_TABLES"
	// TableMetricSummary is a summary table that contains all metrics.
	TableMetricSummary = "METRICS_SUMMARY"
	// TableMetricSummaryByLabel is a metric table that contains all metrics that group by label info.
	TableMetricSummaryByLabel = "METRICS_SUMMARY_BY_LABEL"
	// TableInspectionSummary is the string constant of inspection summary table.
	TableInspectionSummary = "INSPECTION_SUMMARY"
	// TableInspectionRules is the string constant of currently implemented inspection and summary rules.
	TableInspectionRules = "INSPECTION_RULES"
	// TableDDLJobs is the string constant of DDL job table.
	TableDDLJobs = "DDL_JOBS"
	// TableSequences is the string constant of all sequences created by user.
	TableSequences = "SEQUENCES"
	// TableStatementsSummary is the string constant of statement summary table.
	TableStatementsSummary = "STATEMENTS_SUMMARY"
	// TableStatementsSummaryHistory is the string constant of statements summary history table.
	TableStatementsSummaryHistory = "STATEMENTS_SUMMARY_HISTORY"
	// TableStatementsSummaryEvicted is the string constant of statements summary evicted table.
	TableStatementsSummaryEvicted = "STATEMENTS_SUMMARY_EVICTED"
	// TableTiDBStatementsStats is the string constant of the TiDB statement stats table.
	TableTiDBStatementsStats = "TIDB_STATEMENTS_STATS"
	// TableStorageStats is a table that contains all tables disk usage
	TableStorageStats = "TABLE_STORAGE_STATS"
	// TableTiFlashTables is the string constant of tiflash tables table.
	TableTiFlashTables = "TIFLASH_TABLES"
	// TableTiFlashSegments is the string constant of tiflash segments table.
	TableTiFlashSegments = "TIFLASH_SEGMENTS"
	// TableTiFlashIndexes is the string constant of tiflash indexes table.
	TableTiFlashIndexes = "TIFLASH_INDEXES"
	// TableClientErrorsSummaryGlobal is the string constant of client errors table.
	TableClientErrorsSummaryGlobal = "CLIENT_ERRORS_SUMMARY_GLOBAL"
	// TableClientErrorsSummaryByUser is the string constant of client errors table.
	TableClientErrorsSummaryByUser = "CLIENT_ERRORS_SUMMARY_BY_USER"
	// TableClientErrorsSummaryByHost is the string constant of client errors table.
	TableClientErrorsSummaryByHost = "CLIENT_ERRORS_SUMMARY_BY_HOST"
	// TableTiDBTrx is current running transaction status table.
	TableTiDBTrx = "TIDB_TRX"
	// TableDeadlocks is the string constant of deadlock table.
	TableDeadlocks = "DEADLOCKS"
	// TableDataLockWaits is current lock waiting status table.
	TableDataLockWaits = "DATA_LOCK_WAITS"
	// TableAttributes is the string constant of attributes table.
	TableAttributes = "ATTRIBUTES"
	// TablePlacementPolicies is the string constant of placement policies table.
	TablePlacementPolicies = "PLACEMENT_POLICIES"
	// TableTrxSummary is the string constant of transaction summary table.
	TableTrxSummary = "TRX_SUMMARY"
	// TableVariablesInfo is the string constant of variables_info table.
	TableVariablesInfo = "VARIABLES_INFO"
	// TableUserAttributes is the string constant of user_attributes view.
	TableUserAttributes = "USER_ATTRIBUTES"
	// TableMemoryUsage is the memory usage status of tidb instance.
	TableMemoryUsage = "MEMORY_USAGE"
	// TableMemoryUsageOpsHistory is the memory control operators history.
	TableMemoryUsageOpsHistory = "MEMORY_USAGE_OPS_HISTORY"
	// TableResourceGroups is the metadata of resource groups.
	TableResourceGroups = "RESOURCE_GROUPS"
	// TableRunawayWatches is the query list of runaway watch.
	TableRunawayWatches = "RUNAWAY_WATCHES"
	// TableCheckConstraints is the list of CHECK constraints.
	TableCheckConstraints = "CHECK_CONSTRAINTS"
	// TableTiDBCheckConstraints is the list of CHECK constraints, with non-standard TiDB extensions.
	TableTiDBCheckConstraints = "TIDB_CHECK_CONSTRAINTS"
	// TableKeywords is the list of keywords.
	TableKeywords = "KEYWORDS"
	// TableTiDBIndexUsage is a table to show the usage stats of indexes in the current instance.
	TableTiDBIndexUsage = "TIDB_INDEX_USAGE"
	// TableTiDBPlanCache is the plan cache table.
	TableTiDBPlanCache = "TIDB_PLAN_CACHE"
	// TableKeyspaceMeta is the table to show the keyspace meta.
	TableKeyspaceMeta = "KEYSPACE_META"
	// TableSchemataExtensions is the table to show read only status of database.
	TableSchemataExtensions = "SCHEMATA_EXTENSIONS"
)

const (
	// DataLockWaitsColumnKey is the name of the KEY column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnKey = "KEY"
	// DataLockWaitsColumnKeyInfo is the name of the KEY_INFO column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnKeyInfo = "KEY_INFO"
	// DataLockWaitsColumnTrxID is the name of the TRX_ID column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnTrxID = "TRX_ID"
	// DataLockWaitsColumnCurrentHoldingTrxID is the name of the CURRENT_HOLDING_TRX_ID column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnCurrentHoldingTrxID = "CURRENT_HOLDING_TRX_ID"
	// DataLockWaitsColumnSQLDigest is the name of the SQL_DIGEST column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnSQLDigest = "SQL_DIGEST"
	// DataLockWaitsColumnSQLDigestText is the name of the SQL_DIGEST_TEXT column of the DATA_LOCK_WAITS table.
	DataLockWaitsColumnSQLDigestText = "SQL_DIGEST_TEXT"
)

// The following variables will only be used when PD in the microservice mode.
const (
	// tsoServiceName is the name of TSO service.
	tsoServiceName = "tso"
	// schedulingServiceName is the name of scheduling service.
	schedulingServiceName = "scheduling"
)

var tableIDMap = map[string]int64{
	TableSchemata:         autoid.InformationSchemaDBID + 1,
	TableTables:           autoid.InformationSchemaDBID + 2,
	TableColumns:          autoid.InformationSchemaDBID + 3,
	tableColumnStatistics: autoid.InformationSchemaDBID + 4,
	TableStatistics:       autoid.InformationSchemaDBID + 5,
	TableCharacterSets:    autoid.InformationSchemaDBID + 6,
	TableCollations:       autoid.InformationSchemaDBID + 7,
	tableFiles:            autoid.InformationSchemaDBID + 8,
	CatalogVal:            autoid.InformationSchemaDBID + 9,
	TableProfiling:        autoid.InformationSchemaDBID + 10,
	TablePartitions:       autoid.InformationSchemaDBID + 11,
	TableKeyColumn:        autoid.InformationSchemaDBID + 12,
	TableReferConst:       autoid.InformationSchemaDBID + 13,
	// Removed, see https://github.com/pingcap/tidb/issues/9154
	// TableSessionVar:    autoid.InformationSchemaDBID + 14,
	tablePlugins:          autoid.InformationSchemaDBID + 15,
	TableConstraints:      autoid.InformationSchemaDBID + 16,
	tableTriggers:         autoid.InformationSchemaDBID + 17,
	TableUserPrivileges:   autoid.InformationSchemaDBID + 18,
	tableSchemaPrivileges: autoid.InformationSchemaDBID + 19,
	tableTablePrivileges:  autoid.InformationSchemaDBID + 20,
	tableColumnPrivileges: autoid.InformationSchemaDBID + 21,
	TableEngines:          autoid.InformationSchemaDBID + 22,
	TableViews:            autoid.InformationSchemaDBID + 23,
	tableRoutines:         autoid.InformationSchemaDBID + 24,
	tableParameters:       autoid.InformationSchemaDBID + 25,
	tableEvents:           autoid.InformationSchemaDBID + 26,
	// Removed, see https://github.com/pingcap/tidb/issues/9154
	// tableGlobalStatus:                    autoid.InformationSchemaDBID + 27,
	// tableGlobalVariables:                 autoid.InformationSchemaDBID + 28,
	// tableSessionStatus:                   autoid.InformationSchemaDBID + 29,
	tableOptimizerTrace:                     autoid.InformationSchemaDBID + 30,
	tableTableSpaces:                        autoid.InformationSchemaDBID + 31,
	TableCollationCharacterSetApplicability: autoid.InformationSchemaDBID + 32,
	TableProcesslist:                        autoid.InformationSchemaDBID + 33,
	TableTiDBIndexes:                        autoid.InformationSchemaDBID + 34,
	TableSlowQuery:                          autoid.InformationSchemaDBID + 35,
	TableTiDBHotRegions:                     autoid.InformationSchemaDBID + 36,
	TableTiKVStoreStatus:                    autoid.InformationSchemaDBID + 37,
	TableAnalyzeStatus:                      autoid.InformationSchemaDBID + 38,
	TableTiKVRegionStatus:                   autoid.InformationSchemaDBID + 39,
	TableTiKVRegionPeers:                    autoid.InformationSchemaDBID + 40,
	TableTiDBServersInfo:                    autoid.InformationSchemaDBID + 41,
	TableClusterInfo:                        autoid.InformationSchemaDBID + 42,
	TableClusterConfig:                      autoid.InformationSchemaDBID + 43,
	TableClusterLoad:                        autoid.InformationSchemaDBID + 44,
	TableTiFlashReplica:                     autoid.InformationSchemaDBID + 45,
	ClusterTableSlowLog:                     autoid.InformationSchemaDBID + 46,
	ClusterTableProcesslist:                 autoid.InformationSchemaDBID + 47,
	TableClusterLog:                         autoid.InformationSchemaDBID + 48,
	TableClusterHardware:                    autoid.InformationSchemaDBID + 49,
	TableClusterSystemInfo:                  autoid.InformationSchemaDBID + 50,
	TableInspectionResult:                   autoid.InformationSchemaDBID + 51,
	TableMetricSummary:                      autoid.InformationSchemaDBID + 52,
	TableMetricSummaryByLabel:               autoid.InformationSchemaDBID + 53,
	TableMetricTables:                       autoid.InformationSchemaDBID + 54,
	TableInspectionSummary:                  autoid.InformationSchemaDBID + 55,
	TableInspectionRules:                    autoid.InformationSchemaDBID + 56,
	TableDDLJobs:                            autoid.InformationSchemaDBID + 57,
	TableSequences:                          autoid.InformationSchemaDBID + 58,
	TableStatementsSummary:                  autoid.InformationSchemaDBID + 59,
	TableStatementsSummaryHistory:           autoid.InformationSchemaDBID + 60,
	ClusterTableStatementsSummary:           autoid.InformationSchemaDBID + 61,
	ClusterTableStatementsSummaryHistory:    autoid.InformationSchemaDBID + 62,
	TableStorageStats:                       autoid.InformationSchemaDBID + 63,
	TableTiFlashTables:                      autoid.InformationSchemaDBID + 64,
	TableTiFlashSegments:                    autoid.InformationSchemaDBID + 65,
	// Removed, see https://github.com/pingcap/tidb/issues/28890
	//TablePlacementPolicy:                    autoid.InformationSchemaDBID + 66,
	TableClientErrorsSummaryGlobal:       autoid.InformationSchemaDBID + 67,
	TableClientErrorsSummaryByUser:       autoid.InformationSchemaDBID + 68,
	TableClientErrorsSummaryByHost:       autoid.InformationSchemaDBID + 69,
	TableTiDBTrx:                         autoid.InformationSchemaDBID + 70,
	ClusterTableTiDBTrx:                  autoid.InformationSchemaDBID + 71,
	TableDeadlocks:                       autoid.InformationSchemaDBID + 72,
	ClusterTableDeadlocks:                autoid.InformationSchemaDBID + 73,
	TableDataLockWaits:                   autoid.InformationSchemaDBID + 74,
	TableStatementsSummaryEvicted:        autoid.InformationSchemaDBID + 75,
	ClusterTableStatementsSummaryEvicted: autoid.InformationSchemaDBID + 76,
	TableAttributes:                      autoid.InformationSchemaDBID + 77,
	TableTiDBHotRegionsHistory:           autoid.InformationSchemaDBID + 78,
	TablePlacementPolicies:               autoid.InformationSchemaDBID + 79,
	TableTrxSummary:                      autoid.InformationSchemaDBID + 80,
	ClusterTableTrxSummary:               autoid.InformationSchemaDBID + 81,
	TableVariablesInfo:                   autoid.InformationSchemaDBID + 82,
	TableUserAttributes:                  autoid.InformationSchemaDBID + 83,
	TableMemoryUsage:                     autoid.InformationSchemaDBID + 84,
	TableMemoryUsageOpsHistory:           autoid.InformationSchemaDBID + 85,
	ClusterTableMemoryUsage:              autoid.InformationSchemaDBID + 86,
	ClusterTableMemoryUsageOpsHistory:    autoid.InformationSchemaDBID + 87,
	TableResourceGroups:                  autoid.InformationSchemaDBID + 88,
	TableRunawayWatches:                  autoid.InformationSchemaDBID + 89,
	TableCheckConstraints:                autoid.InformationSchemaDBID + 90,
	TableTiDBCheckConstraints:            autoid.InformationSchemaDBID + 91,
	TableKeywords:                        autoid.InformationSchemaDBID + 92,
	TableTiDBIndexUsage:                  autoid.InformationSchemaDBID + 93,
	ClusterTableTiDBIndexUsage:           autoid.InformationSchemaDBID + 94,
	TableTiFlashIndexes:                  autoid.InformationSchemaDBID + 95,
	TableTiDBPlanCache:                   autoid.InformationSchemaDBID + 96,
	ClusterTableTiDBPlanCache:            autoid.InformationSchemaDBID + 97,
	TableTiDBStatementsStats:             autoid.InformationSchemaDBID + 98,
	ClusterTableTiDBStatementsStats:      autoid.InformationSchemaDBID + 99,
	TableKeyspaceMeta:                    autoid.InformationSchemaDBID + 100,
	TableSchemataExtensions:              autoid.InformationSchemaDBID + 101,
}

// columnInfo represents the basic column information of all kinds of INFORMATION_SCHEMA tables
type columnInfo struct {
	// name of column
	name string
	// tp is column type
	tp byte
	// represent size of bytes of the column
	size int
	// represent decimal length of the column
	decimal int
	// flag represent NotNull, Unsigned, PriKey flags etc.
	flag uint
	// deflt is default value
	deflt any
	// comment for the column
	comment string
	// enumElems represent all possible literal string values of an enum column
	enumElems []string
}

func buildColumnInfo(colID int64, col columnInfo) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeMediumBlob || col.tp == mysql.TypeBlob || col.tp == mysql.TypeLongBlob || col.tp == mysql.TypeEnum {
		mCharset = charset.CharsetUTF8MB4
		mCollation = charset.CollationUTF8MB4
	}
	fieldType := types.FieldType{}
	fieldType.SetType(col.tp)
	fieldType.SetCharset(mCharset)
	fieldType.SetCollate(mCollation)
	switch col.tp {
	case mysql.TypeBlob:
		fieldType.SetFlen(1 << 16)
	case mysql.TypeMediumBlob:
		fieldType.SetFlen(1 << 24)
	case mysql.TypeLongBlob:
		fieldType.SetFlen(1 << 32)
	default:
		fieldType.SetFlen(col.size)
	}
	fieldType.SetDecimal(col.decimal)
	fieldType.SetFlag(col.flag)
	fieldType.SetElems(col.enumElems)
	return &model.ColumnInfo{
		ID:           colID,
		Name:         ast.NewCIStr(col.name),
		FieldType:    fieldType,
		State:        model.StatePublic,
		DefaultValue: col.deflt,
		Comment:      col.comment,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, len(cs))
	primaryIndices := make([]*model.IndexInfo, 0, 1)
	tblInfo := &model.TableInfo{
		Name:    ast.NewCIStr(tableName),
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
	for offset, c := range cs {
		if tblInfo.Name.O == ClusterTableSlowLog && mysql.HasPriKeyFlag(c.flag) {
			switch c.tp {
			case mysql.TypeLong, mysql.TypeLonglong,
				mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24:
				tblInfo.PKIsHandle = true
			default:
				tblInfo.IsCommonHandle = true
				tblInfo.CommonHandleVersion = 1
				index := &model.IndexInfo{
					Name:    ast.NewCIStr("primary"),
					State:   model.StatePublic,
					Primary: true,
					Unique:  true,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr(c.name), Offset: offset, Length: types.UnspecifiedLength}},
				}
				primaryIndices = append(primaryIndices, index)
				tblInfo.Indices = primaryIndices
			}
		}
		cols = append(cols, buildColumnInfo(int64(offset), c))
	}
	for i, col := range cols {
		col.Offset = i
	}
	tblInfo.Columns = cols
	return tblInfo
}

// This function is exported for unit test.
func GetShardingInfo(dbInfo ast.CIStr, tableInfo *model.TableInfo) any {
	if tableInfo == nil || tableInfo.IsView() || metadef.IsMemOrSysDB(dbInfo.L) {
		return nil
	}
	shardingInfo := "NOT_SHARDED"
	if tableInfo.ContainsAutoRandomBits() {
		shardingInfo = "PK_AUTO_RANDOM_BITS=" + strconv.Itoa(int(tableInfo.AutoRandomBits))
		rangeBits := tableInfo.AutoRandomRangeBits
		if rangeBits != 0 && rangeBits != autoid.AutoRandomRangeBitsDefault {
			shardingInfo = fmt.Sprintf("%s, RANGE BITS=%d", shardingInfo, rangeBits)
		}
	} else if tableInfo.ShardRowIDBits > 0 {
		shardingInfo = "SHARD_BITS=" + strconv.Itoa(int(tableInfo.ShardRowIDBits))
	} else if tableInfo.PKIsHandle {
		shardingInfo = "NOT_SHARDED(PK_IS_HANDLE)"
	}
	return shardingInfo
}

// SysVarHiddenForSem checks if a given sysvar is hidden according to SEM and privileges.
func SysVarHiddenForSem(ctx sessionctx.Context, sysVarNameInLower string) bool {
	if !sem.IsEnabled() || !sem.IsInvisibleSysVar(sysVarNameInLower) {
		return false
	}
	checker := privilege.GetPrivilegeManager(ctx)
	if checker == nil || checker.RequestDynamicVerification(ctx.GetSessionVars().ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		return false
	}
	return true
}

// GetDataFromSessionVariables return the [name, value] of all session variables
func GetDataFromSessionVariables(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	sessionVars := sctx.GetSessionVars()
	sysVars := variable.GetSysVars()
	rows := make([][]types.Datum, 0, len(sysVars))
	for _, v := range sysVars {
		if SysVarHiddenForSem(sctx, v.Name) {
			continue
		}
		var value string
		value, err := sessionVars.GetSessionOrGlobalSystemVar(ctx, v.Name)
		if err != nil {
			return nil, err
		}
		row := types.MakeDatums(v.Name, value)
		rows = append(rows, row)
	}
	return rows, nil
}

// GetDataFromSessionConnectAttrs produces the rows for the session_connect_attrs table.
func GetDataFromSessionConnectAttrs(sctx sessionctx.Context, sameAccount bool) ([][]types.Datum, error) {
	sm := sctx.GetSessionManager()
	if sm == nil {
		return nil, nil
	}
	var user *auth.UserIdentity
	if sameAccount {
		user = sctx.GetSessionVars().User
	}
	allAttrs := sm.GetConAttrs(user)
	rows := make([][]types.Datum, 0, len(allAttrs)*10) // 10 Attributes per connection
	for pid, attrs := range allAttrs {                 // Note: PID is not ordered.
		// Sorts the attributes by key and gives ORDINAL_POSITION based on this. This is needed as we didn't store the
		// ORDINAL_POSITION and a map doesn't have a guaranteed sort order. This is needed to keep the ORDINAL_POSITION
		// stable over multiple queries.
		attrnames := make([]string, 0, len(attrs))
		for attrname := range attrs {
			attrnames = append(attrnames, attrname)
		}
		sort.Strings(attrnames)

		for ord, attrkey := range attrnames {
			row := types.MakeDatums(
				pid,
				attrkey,
				attrs[attrkey],
				ord,
			)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

var tableNameToColumns = map[string][]columnInfo{
	TableSchemata:                           schemataCols,
	TableTables:                             tablesCols,
	TableColumns:                            columnsCols,
	tableColumnStatistics:                   columnStatisticsCols,
	TableStatistics:                         statisticsCols,
	TableCharacterSets:                      charsetCols,
	TableCollations:                         collationsCols,
	tableFiles:                              filesCols,
	TableProfiling:                          profilingCols,
	TablePartitions:                         partitionsCols,
	TableKeyColumn:                          keyColumnUsageCols,
	TableReferConst:                         referConstCols,
	tablePlugins:                            pluginsCols,
	TableConstraints:                        tableConstraintsCols,
	tableTriggers:                           tableTriggersCols,
	TableUserPrivileges:                     tableUserPrivilegesCols,
	tableSchemaPrivileges:                   tableSchemaPrivilegesCols,
	tableTablePrivileges:                    tableTablePrivilegesCols,
	tableColumnPrivileges:                   tableColumnPrivilegesCols,
	TableEngines:                            tableEnginesCols,
	TableViews:                              tableViewsCols,
	tableRoutines:                           tableRoutinesCols,
	tableParameters:                         tableParametersCols,
	tableEvents:                             tableEventsCols,
	tableOptimizerTrace:                     tableOptimizerTraceCols,
	tableTableSpaces:                        tableTableSpacesCols,
	TableCollationCharacterSetApplicability: tableCollationCharacterSetApplicabilityCols,
	TableProcesslist:                        tableProcesslistCols,
	TableTiDBIndexes:                        tableTiDBIndexesCols,
	TableSlowQuery:                          slowQueryCols,
	TableTiDBHotRegions:                     TableTiDBHotRegionsCols,
	TableTiDBHotRegionsHistory:              TableTiDBHotRegionsHistoryCols,
	TableTiKVStoreStatus:                    TableTiKVStoreStatusCols,
	TableAnalyzeStatus:                      tableAnalyzeStatusCols,
	TableTiKVRegionStatus:                   TableTiKVRegionStatusCols,
	TableTiKVRegionPeers:                    TableTiKVRegionPeersCols,
	TableTiDBServersInfo:                    tableTiDBServersInfoCols,
	TableClusterInfo:                        tableClusterInfoCols,
	TableClusterConfig:                      tableClusterConfigCols,
	TableClusterLog:                         tableClusterLogCols,
	TableClusterLoad:                        tableClusterLoadCols,
	TableTiFlashReplica:                     tableTableTiFlashReplicaCols,
	TableClusterHardware:                    tableClusterHardwareCols,
	TableClusterSystemInfo:                  tableClusterSystemInfoCols,
	TableInspectionResult:                   tableInspectionResultCols,
	TableMetricSummary:                      tableMetricSummaryCols,
	TableMetricSummaryByLabel:               tableMetricSummaryByLabelCols,
	TableMetricTables:                       tableMetricTablesCols,
	TableInspectionSummary:                  tableInspectionSummaryCols,
	TableInspectionRules:                    tableInspectionRulesCols,
	TableDDLJobs:                            tableDDLJobsCols,
	TableSequences:                          tableSequencesCols,
	TableStatementsSummary:                  tableStatementsSummaryCols,
	TableStatementsSummaryHistory:           tableStatementsSummaryCols,
	TableStatementsSummaryEvicted:           tableStatementsSummaryEvictedCols,
	TableStorageStats:                       tableStorageStatsCols,
	TableTiDBStatementsStats:                tableTiDBStatementsStatsCols,
	TableTiFlashTables:                      tableTableTiFlashTablesCols,
	TableTiFlashSegments:                    tableTableTiFlashSegmentsCols,
	TableTiFlashIndexes:                     tableTiFlashIndexesCols,
	TableClientErrorsSummaryGlobal:          tableClientErrorsSummaryGlobalCols,
	TableClientErrorsSummaryByUser:          tableClientErrorsSummaryByUserCols,
	TableClientErrorsSummaryByHost:          tableClientErrorsSummaryByHostCols,
	TableTiDBTrx:                            tableTiDBTrxCols,
	TableDeadlocks:                          tableDeadlocksCols,
	TableDataLockWaits:                      tableDataLockWaitsCols,
	TableAttributes:                         tableAttributesCols,
	TablePlacementPolicies:                  tablePlacementPoliciesCols,
	TableTrxSummary:                         tableTrxSummaryCols,
	TableVariablesInfo:                      tableVariablesInfoCols,
	TableUserAttributes:                     tableUserAttributesCols,
	TableMemoryUsage:                        tableMemoryUsageCols,
	TableMemoryUsageOpsHistory:              tableMemoryUsageOpsHistoryCols,
	TableResourceGroups:                     tableResourceGroupsCols,
	TableRunawayWatches:                     tableRunawayWatchListCols,
	TableCheckConstraints:                   tableCheckConstraintsCols,
	TableTiDBCheckConstraints:               tableTiDBCheckConstraintsCols,
	TableKeywords:                           tableKeywords,
	TableTiDBIndexUsage:                     tableTiDBIndexUsage,
	TableTiDBPlanCache:                      tablePlanCache,
	TableKeyspaceMeta:                       tableKeyspaceMetaCols,
}
