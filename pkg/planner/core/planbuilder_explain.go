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

package core

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	semv1 "github.com/pingcap/tidb/pkg/util/sem"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// TraceFormatRow indicates row tracing format.
	TraceFormatRow = "row"
	// TraceFormatJSON indicates json tracing format.
	TraceFormatJSON = "json"
	// TraceFormatLog indicates log tracing format.
	TraceFormatLog = "log"

	// TracePlanTargetEstimation indicates CE trace target for optimizer trace.
	TracePlanTargetEstimation = "estimation"
	// TracePlanTargetDebug indicates debug trace target for optimizer trace.
	TracePlanTargetDebug = "debug"
)

// buildTrace builds a trace plan. Inside this method, it first optimize the
// underlying query and then constructs a schema, which will be used to constructs
// rows result.
func (b *PlanBuilder) buildTrace(trace *ast.TraceStmt) (base.Plan, error) {
	p := &Trace{
		StmtNode:             trace.Stmt,
		ResolveCtx:           b.resolveCtx,
		Format:               trace.Format,
		OptimizerTrace:       trace.TracePlan,
		OptimizerTraceTarget: trace.TracePlanTarget,
	}
	// TODO: forbid trace plan if the statement isn't select read-only statement
	if trace.TracePlan {
		switch trace.TracePlanTarget {
		case "":
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "Dump_link", mysql.TypeVarchar, 128))
			p.SetSchema(schema.col2Schema())
			p.SetOutputNames(schema.names)
		case TracePlanTargetEstimation:
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "CE_trace", mysql.TypeVarchar, mysql.MaxBlobWidth))
			p.SetSchema(schema.col2Schema())
			p.SetOutputNames(schema.names)
		case TracePlanTargetDebug:
			schema := newColumnsWithNames(1)
			schema.Append(buildColumnWithName("", "Debug_trace", mysql.TypeVarchar, mysql.MaxBlobWidth))
			p.SetSchema(schema.col2Schema())
			p.SetOutputNames(schema.names)
		default:
			return nil, errors.New("trace plan target should only be 'estimation'")
		}
		return p, nil
	}
	switch trace.Format {
	case TraceFormatRow:
		schema := newColumnsWithNames(3)
		schema.Append(buildColumnWithName("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "startTS", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "duration", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.SetOutputNames(schema.names)
	case TraceFormatJSON:
		schema := newColumnsWithNames(1)
		schema.Append(buildColumnWithName("", "operation", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.SetOutputNames(schema.names)
	case TraceFormatLog:
		schema := newColumnsWithNames(4)
		schema.Append(buildColumnWithName("", "time", mysql.TypeTimestamp, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "event", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "tags", mysql.TypeString, mysql.MaxBlobWidth))
		schema.Append(buildColumnWithName("", "spanName", mysql.TypeString, mysql.MaxBlobWidth))
		p.SetSchema(schema.col2Schema())
		p.SetOutputNames(schema.names)
	default:
		return nil, errors.New("trace format should be one of 'row', 'log' or 'json'")
	}
	return p, nil
}

func (b *PlanBuilder) buildExplainPlan(targetPlan base.Plan, format string, explainBriefBinary string, analyze, explore bool, execStmt ast.StmtNode, runtimeStats *execdetails.RuntimeStatsColl, sqlDigest string) (base.Plan, error) {
	format = strings.ToLower(format)
	if format == types.ExplainFormatTrueCardCost && !analyze {
		return nil, errors.Errorf("'explain format=%v' cannot work without 'analyze', please use 'explain analyze format=%v'", format, format)
	}

	p := &Explain{
		TargetPlan:       targetPlan,
		Format:           format,
		Analyze:          analyze,
		Explore:          explore,
		SQLDigest:        sqlDigest,
		ExecStmt:         execStmt,
		BriefBinaryPlan:  explainBriefBinary,
		RuntimeStatsColl: runtimeStats,
	}
	p.SetSCtx(b.ctx)
	return p, p.prepareSchema()
}

// buildExplainFor gets *last* (maybe running or finished) query plan from connection #connection id.
// See https://dev.mysql.com/doc/refman/8.0/en/explain-for-connection.html.
func (b *PlanBuilder) buildExplainFor(explainFor *ast.ExplainForStmt) (base.Plan, error) {
	processInfo, ok := b.ctx.GetSessionManager().GetProcessInfo(explainFor.ConnectionID)
	if !ok {
		return nil, plannererrors.ErrNoSuchThread.GenWithStackByArgs(explainFor.ConnectionID)
	}
	if b.ctx.GetSessionVars() != nil && b.ctx.GetSessionVars().User != nil {
		if b.ctx.GetSessionVars().User.Username != processInfo.User {
			err := plannererrors.ErrAccessDenied.GenWithStackByArgs(b.ctx.GetSessionVars().User.Username, b.ctx.GetSessionVars().User.Hostname)
			// Different from MySQL's behavior and document.
			b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", err)
		}
	}

	targetPlan, ok := processInfo.Plan.(base.Plan)
	explainForFormat := strings.ToLower(explainFor.Format)
	if !ok || targetPlan == nil {
		return &Explain{Format: explainForFormat}, nil
	}
	// TODO: support other explain formats for connection
	if explainForFormat != types.ExplainFormatBrief && explainForFormat != types.ExplainFormatROW && explainForFormat != types.ExplainFormatVerbose {
		return nil, errors.Errorf("explain format '%s' for connection is not supported now", explainForFormat)
	}
	return b.buildExplainPlan(targetPlan, explainForFormat, processInfo.BriefBinaryPlan, false, false, nil, processInfo.RuntimeStatsColl, "")
}

// getHintedStmtThroughPlanDigest gets the hinted SQL like `select /*+ ... */ * from t where ...` from `stmt_summary`
// for `explain [analyze] <plan_digest>` statements.
func getHintedStmtThroughPlanDigest(ctx base.PlanContext, planDigest string) (stmt ast.StmtNode, err error) {
	err = domain.GetDomain(ctx).AdvancedSysSessionPool().WithSession(func(se *syssession.Session) error {
		return se.WithSessionContext(func(sctx sessionctx.Context) error {
			defer func(warnings []stmtctx.SQLWarn) {
				sctx.GetSessionVars().StmtCtx.SetWarnings(warnings)
			}(sctx.GetSessionVars().StmtCtx.GetWarnings())
			// The warnings will be broken in fetchRecordFromClusterStmtSummary(), so we need to save and restore it to make the
			// warnings for repeated SQL Digest work.
			schema, query, planHint, characterSet, collation, err := fetchRecordFromClusterStmtSummary(sctx.GetPlanCtx(), planDigest)
			if err != nil {
				return err
			}
			if query == "" {
				return errors.NewNoStackErrorf("can't find any plans for '%s'", planDigest)
			}

			p := parser.New()
			originNode, err := p.ParseOneStmt(query, characterSet, collation)
			if err != nil {
				return errors.NewNoStackErrorf("failed to parse SQL for Plan Digest: %v", planDigest)
			}
			hintedSQL := bindinfo.GenerateBindingSQL(originNode, planHint, schema)
			stmt, err = p.ParseOneStmt(hintedSQL, characterSet, collation)
			return err
		})
	})
	return
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (base.Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}

	if explain.PlanDigest != "" { // explain [analyze] <SQLDigest>
		hintedStmt, err := getHintedStmtThroughPlanDigest(b.ctx, explain.PlanDigest)
		if err != nil {
			return nil, err
		}
		explain.Stmt = hintedStmt
	}

	sctx, err := AsSctx(b.ctx)
	if err != nil {
		return nil, err
	}

	stmtCtx := sctx.GetSessionVars().StmtCtx
	var targetPlan base.Plan
	if explain.Stmt != nil && !explain.Explore {
		nodeW := resolve.NewNodeWWithCtx(explain.Stmt, b.resolveCtx)
		if stmtCtx.ExplainFormat == types.ExplainFormatPlanCache {
			targetPlan, _, err = OptimizeAstNode(ctx, sctx, nodeW, b.is)
		} else {
			targetPlan, _, err = OptimizeAstNodeNoCache(ctx, sctx, nodeW, b.is)
		}
		if err != nil {
			return nil, err
		}
	}

	return b.buildExplainPlan(targetPlan, explain.Format, "", explain.Analyze, explain.Explore, explain.Stmt, nil, explain.SQLDigest)
}

func (b *PlanBuilder) buildSelectInto(ctx context.Context, sel *ast.SelectStmt) (base.Plan, error) {
	if semv1.IsEnabled() {
		return nil, plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("SELECT INTO")
	}
	selectIntoInfo := sel.SelectIntoOpt
	sel.SelectIntoOpt = nil
	sctx, err := AsSctx(b.ctx)
	if err != nil {
		return nil, err
	}
	nodeW := resolve.NewNodeWWithCtx(sel, b.resolveCtx)
	targetPlan, _, err := OptimizeAstNodeNoCache(ctx, sctx, nodeW, b.is)
	if err != nil {
		return nil, err
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.FilePriv, "", "", "", plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("FILE"))
	return &SelectInto{
		TargetPlan:     targetPlan,
		IntoOpt:        selectIntoInfo,
		LineFieldsInfo: NewLineFieldsInfo(selectIntoInfo.FieldsInfo, selectIntoInfo.LinesInfo),
	}, nil
}

func buildShowProcedureSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "ROUTINES"
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Modified", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Security_type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Comment", mysql.TypeBlob, 196605))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowTriggerSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "TRIGGERS"
	schema := newColumnsWithNames(11)
	schema.Append(buildColumnWithName(tblName, "Trigger", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Event", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Table", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Statement", mysql.TypeBlob, 196605))
	schema.Append(buildColumnWithName(tblName, "Timing", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Created", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "sql_mode", mysql.TypeBlob, 8192))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowEventsSchema() (*expression.Schema, []*types.FieldName) {
	tblName := "EVENTS"
	schema := newColumnsWithNames(15)
	schema.Append(buildColumnWithName(tblName, "Db", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Time zone", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Definer", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Type", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Execute At", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Interval Value", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Interval Field", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName(tblName, "Starts", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Ends", mysql.TypeDatetime, 19))
	schema.Append(buildColumnWithName(tblName, "Status", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Originator", mysql.TypeInt24, 4))
	schema.Append(buildColumnWithName(tblName, "character_set_client", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "collation_connection", mysql.TypeVarchar, 32))
	schema.Append(buildColumnWithName(tblName, "Database Collation", mysql.TypeVarchar, 32))
	return schema.col2Schema(), schema.names
}

func buildShowWarningsSchema() (*expression.Schema, types.NameSlice) {
	tblName := "WARNINGS"
	schema := newColumnsWithNames(3)
	schema.Append(buildColumnWithName(tblName, "Level", mysql.TypeVarchar, 64))
	schema.Append(buildColumnWithName(tblName, "Code", mysql.TypeLong, 19))
	schema.Append(buildColumnWithName(tblName, "Message", mysql.TypeVarchar, 64))
	return schema.col2Schema(), schema.names
}

// buildShowSchema builds column info for ShowStmt including column name and type.
func buildShowSchema(s *ast.ShowStmt, isView bool, isSequence bool) (schema *expression.Schema, outputNames []*types.FieldName) {
	var names []string
	var ftypes []byte
	var flags []uint

	switch s.Tp {
	case ast.ShowBinlogStatus:
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowProcedureStatus, ast.ShowFunctionStatus:
		return buildShowProcedureSchema()
	case ast.ShowTriggers:
		return buildShowTriggerSchema()
	case ast.ShowEvents:
		return buildShowEventsSchema()
	case ast.ShowWarnings, ast.ShowErrors:
		if s.CountWarningsOrErrors {
			names = []string{"Count"}
			ftypes = []byte{mysql.TypeLong}
			break
		}
		return buildShowWarningsSchema()
	case ast.ShowRegions:
		return buildTableRegionsSchema()
	case ast.ShowDistributions:
		return buildTableDistributionSchema()
	case ast.ShowEngines:
		names = []string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}
	case ast.ShowConfig:
		names = []string{"Type", "Instance", "Name", "Value"}
	case ast.ShowDatabases:
		fieldDB := "Database"
		if patternName := extractPatternLikeOrIlikeName(s.Pattern); patternName != "" {
			fieldDB = fmt.Sprintf("%s (%s)", fieldDB, patternName)
		}
		names = []string{fieldDB}
	case ast.ShowOpenTables:
		names = []string{"Database", "Table", "In_use", "Name_locked"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeLong}
	case ast.ShowTables:
		fieldTable := fmt.Sprintf("Tables_in_%s", s.DBName)
		if patternName := extractPatternLikeOrIlikeName(s.Pattern); patternName != "" {
			fieldTable = fmt.Sprintf("%s (%s)", fieldTable, patternName)
		}
		names = []string{fieldTable}
		if s.Full {
			names = append(names, "Table_type")
		}
	case ast.ShowTableStatus:
		names = []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length",
			"Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment",
			"Create_time", "Update_time", "Check_time", "Collation", "Checksum",
			"Create_options", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowColumns:
		names = table.ColDescFieldNames(s.Full)
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar}
		flags = []uint{0, 0, mysql.UnsignedFlag | mysql.NotNullFlag, 0, 0, 0, 0}
	case ast.ShowCreateTable, ast.ShowCreateSequence:
		if isSequence {
			names = []string{"Sequence", "Create Sequence"}
		} else if isView {
			names = []string{"View", "Create View", "character_set_client", "collation_connection"}
		} else {
			names = []string{"Table", "Create Table"}
		}
	case ast.ShowCreatePlacementPolicy:
		names = []string{"Policy", "Create Policy"}
	case ast.ShowCreateResourceGroup:
		names = []string{"Resource_Group", "Create Resource Group"}
	case ast.ShowCreateUser:
		if s.User != nil {
			names = []string{fmt.Sprintf("CREATE USER for %s", s.User)}
		}
	case ast.ShowCreateView:
		names = []string{"View", "Create View", "character_set_client", "collation_connection"}
	case ast.ShowCreateDatabase:
		names = []string{"Database", "Create Database"}
	case ast.ShowGrants:
		if s.User != nil {
			names = []string{fmt.Sprintf("Grants for %s", s.User)}
		} else {
			// Don't know the name yet, so just say "user"
			names = []string{"Grants for User"}
		}
	case ast.ShowIndex:
		names = []string{"Table", "Non_unique", "Key_name", "Seq_in_index",
			"Column_name", "Collation", "Cardinality", "Sub_part", "Packed",
			"Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression", "Clustered", "Global"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPlugins:
		names = []string{"Name", "Status", "Type", "Library", "License", "Version"}
		ftypes = []byte{
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar,
		}
	case ast.ShowProcessList:
		names = []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	case ast.ShowStatsMeta:
		names = []string{"Db_name", "Table_name", "Partition_name", "Update_time", "Modify_count", "Row_count", "Last_analyze_time"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDatetime}
	case ast.ShowStatsExtended:
		names = []string{"Db_name", "Table_name", "Stats_name", "Column_names", "Stats_type", "Stats_val", "Last_update_version"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsHistograms:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Update_time", "Distinct_count", "Null_count", "Avg_col_size", "Correlation",
			"Load_status", "Total_mem_usage", "Hist_mem_usage", "Topn_mem_usage", "Cms_mem_usage"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeDatetime,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeDouble,
			mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeLonglong}
	case ast.ShowStatsBuckets:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Bucket_id", "Count",
			"Repeats", "Lower_Bound", "Upper_Bound", "Ndv"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeLonglong,
			mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsTopN:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Is_index", "Value", "Count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeTiny, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowStatsHealthy:
		names = []string{"Db_name", "Table_name", "Partition_name", "Healthy"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowHistogramsInFlight:
		names = []string{"HistogramsInFlight"}
		ftypes = []byte{mysql.TypeLonglong}
	case ast.ShowColumnStatsUsage:
		names = []string{"Db_name", "Table_name", "Partition_name", "Column_name", "Last_used_at", "Last_analyzed_at"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime}
	case ast.ShowStatsLocked:
		names = []string{"Db_name", "Table_name", "Partition_name", "Status"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowProfiles: // ShowProfiles is deprecated.
		names = []string{"Query_ID", "Duration", "Query"}
		ftypes = []byte{mysql.TypeLong, mysql.TypeDouble, mysql.TypeVarchar}
	case ast.ShowMasterStatus:
		names = []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowPrivileges:
		names = []string{"Privilege", "Context", "Comment"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowBindings:
		names = []string{"Original_sql", "Bind_sql", "Default_db", "Status", "Create_time", "Update_time", "Charset", "Collation", "Source", "Sql_digest", "Plan_digest"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowBindingCacheStatus:
		names = []string{"bindings_in_cache", "bindings_in_table", "memory_usage", "memory_quota"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowAnalyzeStatus:
		names = []string{"Table_schema", "Table_name", "Partition_name", "Job_info", "Processed_rows", "Start_time",
			"End_time", "State", "Fail_reason", "Instance", "Process_ID", "Remaining_seconds", "Progress", "Estimated_total_rows"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeDouble, mysql.TypeLonglong}
	case ast.ShowBuiltins:
		names = []string{"Supported_builtin_functions"}
		ftypes = []byte{mysql.TypeVarchar}
	case ast.ShowBackups, ast.ShowRestores:
		names = []string{"Id", "Destination", "State", "Progress", "Queue_time", "Execution_time", "Finish_time", "Connection", "Message"}
		ftypes = []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeDouble, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeDatetime, mysql.TypeLonglong, mysql.TypeVarchar}
	case ast.ShowPlacementLabels:
		names = []string{"Key", "Values"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeJSON}
	case ast.ShowPlacement, ast.ShowPlacementForDatabase, ast.ShowPlacementForTable, ast.ShowPlacementForPartition:
		names = []string{"Target", "Placement", "Scheduling_State"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar}
	case ast.ShowSessionStates:
		names = []string{"Session_states", "Session_token"}
		ftypes = []byte{mysql.TypeJSON, mysql.TypeJSON}
	case ast.ShowImportJobs:
		names = importIntoSchemaNames
		ftypes = ImportIntoSchemaFTypes
	case ast.ShowImportGroups:
		names = showImportGroupsNames
		ftypes = showImportGroupsFTypes
	case ast.ShowDistributionJobs:
		names = distributionJobsSchemaNames
		ftypes = distributionJobsSchedulerFTypes
	case ast.ShowAffinity:
		names = []string{"Db_name", "Table_name", "Partition_name", "Leader_store_id", "Voter_store_ids", "Status", "Region_count", "Affinity_region_count"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong, mysql.TypeLonglong}
	}
	return convert2OutputSchemasAndNames(names, ftypes, flags)
}

func convert2OutputSchemasAndNames(names []string, ftypes []byte, flags []uint) (schema *expression.Schema, outputNames []*types.FieldName) {
	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.Column{}
		outputNames = append(outputNames, &types.FieldName{ColName: ast.NewCIStr(names[i])})
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[i] != mysql.TypeUnspecified {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)
		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		if len(flags) > 0 {
			fieldType.SetFlag(flags[i])
		}
		col.RetType = fieldType
		schema.Append(col)
	}
	return
}

func (b *PlanBuilder) buildPlanReplayer(pc *ast.PlanReplayerStmt) base.Plan {
	p := &PlanReplayer{ExecStmt: pc.Stmt, StmtList: pc.StmtList, Analyze: pc.Analyze, Load: pc.Load, File: pc.File,
		Capture: pc.Capture, Remove: pc.Remove, SQLDigest: pc.SQLDigest, PlanDigest: pc.PlanDigest}

	if pc.HistoricalStatsInfo != nil {
		p.HistoricalStatsTS = calcTSForPlanReplayer(b.ctx, pc.HistoricalStatsInfo.TsExpr)
	}

	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "File_token", mysql.TypeVarchar, 128))
	p.SetSchema(schema.col2Schema())
	p.SetOutputNames(schema.names)
	return p
}

func calcTSForPlanReplayer(sctx base.PlanContext, tsExpr ast.ExprNode) uint64 {
	tsVal, err := evalAstExprWithPlanCtx(sctx, tsExpr)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return 0
	}
	// mustn't be NULL
	if tsVal.IsNull() {
		return 0
	}

	// first, treat it as a TSO
	tpLonglong := types.NewFieldType(mysql.TypeLonglong)
	tpLonglong.SetFlag(mysql.UnsignedFlag)
	// We need a strict check, which means no truncate or any other warnings/errors, or it will wrongly try to parse
	// a date/time string into a TSO.
	// To achieve this, we create a new type context without re-using the one in statement context.
	tso, err := tsVal.ConvertTo(types.DefaultStmtNoWarningContext.WithLocation(sctx.GetSessionVars().Location()), tpLonglong)
	if err == nil {
		return tso.GetUint64()
	}

	// if failed, treat it as a date/time
	// this part is similar to CalculateAsOfTsExpr
	tpDateTime := types.NewFieldType(mysql.TypeDatetime)
	tpDateTime.SetDecimal(6)
	timestamp, err := tsVal.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), tpDateTime)
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return 0
	}
	goTime, err := timestamp.GetMysqlTime().GoTime(sctx.GetSessionVars().Location())
	if err != nil {
		sctx.GetSessionVars().StmtCtx.AppendWarning(err)
		return 0
	}
	return oracle.GoTimeToTS(goTime)
}

func buildChecksumTableSchema() (*expression.Schema, []*types.FieldName) {
	schema := newColumnsWithNames(5)
	schema.Append(buildColumnWithName("", "Db_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName("", "Table_name", mysql.TypeVarchar, 128))
	schema.Append(buildColumnWithName("", "Checksum_crc64_xor", mysql.TypeLonglong, 22))
	schema.Append(buildColumnWithName("", "Total_kvs", mysql.TypeLonglong, 22))
	schema.Append(buildColumnWithName("", "Total_bytes", mysql.TypeLonglong, 22))
	return schema.col2Schema(), schema.names
}

// adjustOverlongViewColname adjusts the overlong outputNames of a view to
// `new_exp_$off` where `$off` is the offset of the output column, $off starts from 1.
// There is still some MySQL compatible problems.
func adjustOverlongViewColname(plan base.LogicalPlan) {
	outputNames := plan.OutputNames()
	for i := range outputNames {
		if outputName := outputNames[i].ColName.L; len(outputName) > mysql.MaxColumnNameLength {
			outputNames[i].ColName = ast.NewCIStr(fmt.Sprintf("name_exp_%d", i+1))
		}
	}
}

// findStmtAsViewSchema finds the first SelectStmt as the schema for the view
func findStmtAsViewSchema(stmt ast.Node) *ast.SelectStmt {
	switch x := stmt.(type) {
	case *ast.CreateViewStmt:
		return findStmtAsViewSchema(x.Select)
	case *ast.SetOprStmt:
		return findStmtAsViewSchema(x.SelectList)
	case *ast.SetOprSelectList:
		return findStmtAsViewSchema(x.Selects[0])
	case *ast.SelectStmt:
		return x
	}
	return nil
}

func (b *PlanBuilder) buildTraffic(pc *ast.TrafficStmt) base.Plan {
	tf := &Traffic{
		OpType:  pc.OpType,
		Options: pc.Options,
		Dir:     pc.Dir,
	}
	var errMsg string
	var privs []string
	switch pc.OpType {
	case ast.TrafficOpCapture:
		errMsg = "SUPER or TRAFFIC_CAPTURE_ADMIN"
		privs = []string{"TRAFFIC_CAPTURE_ADMIN"}
	case ast.TrafficOpReplay:
		errMsg = "SUPER or TRAFFIC_REPLAY_ADMIN"
		privs = []string{"TRAFFIC_REPLAY_ADMIN"}
	case ast.TrafficOpShow:
		tf.SetSchemaAndNames(buildShowTrafficJobsSchema())
		fallthrough
	case ast.TrafficOpCancel:
		errMsg = "SUPER, TRAFFIC_CAPTURE_ADMIN or TRAFFIC_REPLAY_ADMIN"
		privs = []string{"TRAFFIC_CAPTURE_ADMIN", "TRAFFIC_REPLAY_ADMIN"}
	}
	err := plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs(errMsg)
	b.visitInfo = appendDynamicVisitInfo(b.visitInfo, privs, false, err)
	return tf
}

// buildCompactTable builds a plan for the "ALTER TABLE [NAME] COMPACT ..." statement.
func (b *PlanBuilder) buildCompactTable(node *ast.CompactTableStmt) (base.Plan, error) {
	var authErr error
	if b.ctx.GetSessionVars().User != nil {
		authErr = plannererrors.ErrTableaccessDenied.GenWithStackByArgs("ALTER", b.ctx.GetSessionVars().User.AuthUsername,
			b.ctx.GetSessionVars().User.AuthHostname, node.Table.Name.L)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.AlterPriv, node.Table.Schema.L,
		node.Table.Name.L, "", authErr)

	tnW := b.resolveCtx.GetTableName(node.Table)
	tblInfo := tnW.TableInfo
	p := &CompactTable{
		ReplicaKind:    node.ReplicaKind,
		TableInfo:      tblInfo,
		PartitionNames: node.PartitionNames,
	}
	return p, nil
}

func (*PlanBuilder) buildRecommendIndex(v *ast.RecommendIndexStmt) (base.Plan, error) {
	p := &RecommendIndexPlan{
		Action:   v.Action,
		SQL:      v.SQL,
		AdviseID: v.ID,
		Options:  v.Options,
	}

	switch v.Action {
	case "run":
		schema := newColumnsWithNames(7)
		schema.Append(buildColumnWithName("", "database", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "table", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "index_name", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "index_columns", mysql.TypeVarchar, 256))
		schema.Append(buildColumnWithName("", "est_index_size", mysql.TypeVarchar, 256))
		schema.Append(buildColumnWithName("", "reason", mysql.TypeVarchar, 256))
		schema.Append(buildColumnWithName("", "top_impacted_query", mysql.TypeBlob, -1))
		schema.Append(buildColumnWithName("", "create_index_statement", mysql.TypeBlob, -1))
		p.SetSchemaAndNames(schema.col2Schema(), schema.names)
	case "set":
		if len(p.Options) == 0 {
			return nil, fmt.Errorf("option is empty")
		}
	case "show":
		schema := newColumnsWithNames(3)
		schema.Append(buildColumnWithName("", "option", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "value", mysql.TypeVarchar, 64))
		schema.Append(buildColumnWithName("", "description", mysql.TypeVarchar, 256))
		p.SetSchemaAndNames(schema.col2Schema(), schema.names)
	default:
		return nil, fmt.Errorf("unsupported action %s", v.Action)
	}
	return p, nil
}

func extractPatternLikeOrIlikeName(patternLike *ast.PatternLikeOrIlikeExpr) string {
	if patternLike == nil {
		return ""
	}
	if v, ok := patternLike.Pattern.(*driver.ValueExpr); ok {
		return v.GetString()
	}
	return ""
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath() {
			return path
		}
	}
	return nil
}

func (b *PlanBuilder) buildAdminAlterDDLJob(ctx context.Context, as *ast.AdminStmt) (_ base.Plan, err error) {
	options := make([]*AlterDDLJobOpt, 0, len(as.AlterJobOptions))
	mockTablePlan := logicalop.LogicalTableDual{}.Init(b.ctx, b.getSelectOffset())
	for _, opt := range as.AlterJobOptions {
		_, ok := allowedAlterDDLJobParams[opt.Name]
		if !ok {
			return nil, fmt.Errorf("unsupported admin alter ddl jobs config: %s", opt.Name)
		}
		alterDDLJobOpt := AlterDDLJobOpt{Name: opt.Name}
		if opt.Value != nil {
			alterDDLJobOpt.Value, _, err = b.rewrite(ctx, opt.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		}
		if err = checkAlterDDLJobOptValue(&alterDDLJobOpt); err != nil {
			return nil, err
		}
		options = append(options, &alterDDLJobOpt)
	}
	p := &AlterDDLJob{
		JobID:   as.JobNumber,
		Options: options,
	}
	return p, nil
}

// check if the config value is valid.
func checkAlterDDLJobOptValue(opt *AlterDDLJobOpt) error {
	switch opt.Name {
	case AlterDDLJobThread:
		thread, err := GetThreadOrBatchSizeFromExpression(opt)
		if err != nil {
			return err
		}
		if thread < 1 || thread > vardef.MaxConfigurableConcurrency {
			return fmt.Errorf("the value %v for %s is out of range [1, %v]",
				thread, opt.Name, vardef.MaxConfigurableConcurrency)
		}
	case AlterDDLJobBatchSize:
		batchSize, err := GetThreadOrBatchSizeFromExpression(opt)
		if err != nil {
			return err
		}
		bs := int32(batchSize)
		if bs < vardef.MinDDLReorgBatchSize || bs > vardef.MaxDDLReorgBatchSize {
			return fmt.Errorf("the value %v for %s is out of range [%v, %v]",
				bs, opt.Name, vardef.MinDDLReorgBatchSize, vardef.MaxDDLReorgBatchSize)
		}
	case AlterDDLJobMaxWriteSpeed:
		speed, err := GetMaxWriteSpeedFromExpression(opt)
		if err != nil {
			return err
		}
		if speed < 0 || speed > units.PiB {
			return fmt.Errorf("the value %s for %s is out of range [%v, %v]",
				strconv.FormatInt(speed, 10), opt.Name, 0, units.PiB)
		}
	}
	return nil
}

// for nextgen import-into with SEM, we disallow user to set S3 external ID explicitly,
// and we will use the keyspace name as the S3 external ID.
func checkNextGenS3PathWithSem(u *url.URL) error {
	values := u.Query()
	for k := range values {
		lowerK := strings.ToLower(k)
		if lowerK == s3like.S3ExternalID {
			return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO with explicit external ID")
		}
	}

	return nil
}

// GetThreadOrBatchSizeFromExpression gets the numeric value of the thread or batch size from the expression.
func GetThreadOrBatchSizeFromExpression(opt *AlterDDLJobOpt) (int64, error) {
	v := opt.Value.(*expression.Constant)
	switch v.RetType.EvalType() {
	case types.ETInt:
		return v.Value.GetInt64(), nil
	default:
		return 0, fmt.Errorf("the value for %s is invalid, only integer is allowed", opt.Name)
	}
}

// GetMaxWriteSpeedFromExpression gets the numeric value of the max write speed from the expression.
func GetMaxWriteSpeedFromExpression(opt *AlterDDLJobOpt) (maxWriteSpeed int64, err error) {
	v := opt.Value.(*expression.Constant)
	switch v.RetType.EvalType() {
	case types.ETString:
		speedStr := v.Value.GetString()
		maxWriteSpeed, err = units.RAMInBytes(speedStr)
		if err != nil {
			return 0, errors.Annotate(err, "parse max_write_speed value error")
		}
	case types.ETInt:
		maxWriteSpeed = v.Value.GetInt64()
	default:
		return 0, fmt.Errorf("the value %v for %s is invalid", v.Value.GetValue(), opt.Name)
	}
	return maxWriteSpeed, nil
}

func (b *PlanBuilder) checkSEMStmt(stmt ast.Node) error {
	if !semv2.IsEnabled() {
		return nil
	}

	stmtNode, ok := stmt.(ast.StmtNode)
	intest.Assert(ok, "node.Node should be ast.StmtNode, but got %T", stmt)
	if !semv2.IsRestrictedSQL(stmtNode) {
		return nil
	}

	activeRoles := b.ctx.GetSessionVars().ActiveRoles
	checker := privilege.GetPrivilegeManager(b.ctx)
	hasPriv := checker.RequestDynamicVerification(activeRoles, "RESTRICTED_SQL_ADMIN", false)

	if hasPriv {
		return nil
	}

	return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs(stmtNode.Text())
}
