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

package executor

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// fetchShowInfoByName fetches the show info for `SHOW <FULL> TABLES like 'xxx'`
func (e *ShowExec) fetchShowInfoByName(ctx context.Context, name string) ([]*showInfo, error) {
	tb, err := e.is.TableByName(ctx, e.DBName, ast.NewCIStr(name))
	if err != nil {
		// do nothing if table not exists
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	if tb.Meta().TempTableType == model.TempTableLocal {
		return nil, nil
	}
	return []*showInfo{{Name: tb.Meta().Name, TableType: e.getTableType(tb.Meta())}}, nil
}

// fetchShowSimpleTables fetches the table info for `SHOW TABLE`.
func (e *ShowExec) fetchShowSimpleTables(ctx context.Context) ([]*showInfo, error) {
	tb, err := e.is.SchemaSimpleTableInfos(ctx, e.DBName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	showInfos := make([]*showInfo, 0, len(tb))
	for _, v := range tb {
		// TODO: consider add type info to TableNameInfo
		showInfos = append(showInfos, &showInfo{Name: v.Name})
	}
	return showInfos, nil
}

// fetchShowFullTables fetches the table info for `SHOW FULL TABLES`.
func (e *ShowExec) fetchShowFullTables(ctx context.Context) ([]*showInfo, error) {
	tb, err := e.is.SchemaTableInfos(ctx, e.DBName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	showInfos := make([]*showInfo, 0, len(tb))
	for _, v := range tb {
		showInfos = append(showInfos, &showInfo{Name: v.Name, TableType: e.getTableType(v)})
	}
	return showInfos, nil
}

func (e *ShowExec) fetchShowTables(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return exeerrors.ErrBadDB.GenWithStackByArgs(e.DBName)
	}
	var (
		tableNames = make([]string, 0)
		showInfos  []*showInfo
		err        error
	)
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	var (
		tableTypes        = make(map[string]string)
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}

	if fieldFilter != "" {
		showInfos, err = e.fetchShowInfoByName(ctx, fieldFilter)
	} else if e.Full {
		showInfos, err = e.fetchShowFullTables(ctx)
	} else {
		showInfos, err = e.fetchShowSimpleTables(ctx)
	}
	if err != nil {
		return errors.Trace(err)
	}
	for _, v := range showInfos {
		// Test with mysql.AllPrivMask means any privilege would be OK.
		// TODO: Should consider column privileges, which also make a table visible.
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, v.Name.O, "", mysql.AllPrivMask&(^mysql.CreateTMPTablePriv)) {
			continue
		} else if fieldFilter != "" && v.Name.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name.L) {
			continue
		}
		tableNames = append(tableNames, v.Name.O)
		if e.Full {
			tableTypes[v.Name.O] = v.TableType
		}
	}
	slices.Sort(tableNames)
	for _, v := range tableNames {
		if e.Full {
			e.appendRow([]any{v, tableTypes[v]})
		} else {
			e.appendRow([]any{v})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowTableStatus(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, e.DBName.O) {
			return e.dbAccessDenied()
		}
	}
	if !e.is.SchemaExists(e.DBName) {
		return exeerrors.ErrBadDB.GenWithStackByArgs(e.DBName)
	}

	exec := e.Ctx().GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStats)

	var snapshot uint64
	txn, err := e.Ctx().Txn(false)
	if err != nil {
		return errors.Trace(err)
	}
	if txn.Valid() {
		snapshot = txn.StartTS()
	}
	if e.Ctx().GetSessionVars().SnapshotTS != 0 {
		snapshot = e.Ctx().GetSessionVars().SnapshotTS
	}

	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(snapshot), sqlexec.ExecOptionUseCurSession},
		`SELECT table_name, engine, version, row_format, table_rows,
		avg_row_length, data_length, max_data_length, index_length,
		data_free, auto_increment, create_time, update_time, check_time,
		table_collation, IFNULL(checksum,''), create_options, table_comment
		FROM information_schema.tables
		WHERE lower(table_schema)=%? ORDER BY table_name`, e.DBName.L)
	if err != nil {
		return errors.Trace(err)
	}
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	for _, row := range rows {
		tableName := row.GetString(0)
		if checker != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tableName, "", mysql.AllPrivMask) {
			continue
		} else if fieldFilter != "" && strings.ToLower(tableName) != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(strings.ToLower(tableName)) {
			continue
		}
		e.result.AppendRow(row)
	}
	return nil
}

func (e *ShowExec) fetchShowColumns(ctx context.Context) error {
	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}
	// we will fill the column type information later, so clone a new table here.
	tb, err = table.TableFromMeta(tb.Allocators(e.Ctx().GetTableCtx()), tb.Meta().Clone())
	if err != nil {
		return errors.Trace(err)
	}
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}

	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if checker != nil && e.Ctx().GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.InsertPriv|mysql.SelectPriv|mysql.UpdatePriv|mysql.ReferencesPriv) {
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	var cols []*table.Column
	// The optional EXTENDED keyword causes the output to include information about hidden columns that MySQL uses internally and are not accessible by users.
	// See https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
	if e.Extended {
		cols = tb.Cols()
	} else {
		cols = tb.VisibleCols()
	}
	if err := tryFillViewColumnType(ctx, e.Ctx(), e.is, e.DBName, tb.Meta()); err != nil {
		return err
	}
	for _, col := range cols {
		if fieldFilter != "" && col.Name.L != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(col.Name.L) {
			continue
		}
		desc := table.NewColDesc(col)
		var columnDefault any
		if desc.DefaultValue != nil {
			// SHOW COLUMNS result expects string value
			defaultValStr := fmt.Sprintf("%v", desc.DefaultValue)
			// If column is timestamp, and default value is not current_timestamp, should convert the default value to the current session time zone.
			if col.GetType() == mysql.TypeTimestamp && defaultValStr != types.ZeroDatetimeStr && !strings.HasPrefix(strings.ToUpper(defaultValStr), strings.ToUpper(ast.CurrentTimestamp)) {
				timeValue, err := table.GetColDefaultValue(e.Ctx().GetExprCtx(), col.ToInfo())
				if err != nil {
					return errors.Trace(err)
				}
				defaultValStr = timeValue.GetMysqlTime().String()
			}
			if col.GetType() == mysql.TypeBit {
				defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
				columnDefault = defaultValBinaryLiteral.ToBitLiteralString(true)
			} else {
				columnDefault = defaultValStr
			}
		}

		// The FULL keyword causes the output to include the column collation and comments,
		// as well as the privileges you have for each column.
		if e.Full {
			e.appendRow([]any{
				desc.Field,
				desc.Type,
				desc.Collation,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
				desc.Privileges,
				desc.Comment,
			})
		} else {
			e.appendRow([]any{
				desc.Field,
				desc.Type,
				desc.Null,
				desc.Key,
				columnDefault,
				desc.Extra,
			})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowIndex() error {
	do := domain.GetDomain(e.Ctx())
	h := do.StatsHandle()

	tb, err := e.getTable()
	if err != nil {
		return errors.Trace(err)
	}

	statsTbl := h.GetPhysicalTableStats(tb.Meta().ID, tb.Meta())

	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles
	if checker != nil && e.Ctx().GetSessionVars().User != nil && !checker.RequestVerification(activeRoles, e.DBName.O, tb.Meta().Name.O, "", mysql.AllPrivMask) {
		return e.tableAccessDenied("SELECT", tb.Meta().Name.O)
	}

	if tb.Meta().PKIsHandle {
		var pkCol *table.Column
		for _, col := range tb.Cols() {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				pkCol = col
				break
			}
		}
		colStats := statsTbl.GetCol(pkCol.ID)
		var ndv int64
		if colStats != nil {
			ndv = colStats.NDV
		}
		e.appendRow([]any{
			tb.Meta().Name.O, // Table
			0,                // Non_unique
			"PRIMARY",        // Key_name
			1,                // Seq_in_index
			pkCol.Name.O,     // Column_name
			"A",              // Collation
			ndv,              // Cardinality
			nil,              // Sub_part
			nil,              // Packed
			"",               // Null
			"BTREE",          // Index_type
			"",               // Comment
			"",               // Index_comment
			"YES",            // Index_visible
			nil,              // Expression
			"YES",            // Clustered
			"NO",             // Global_index
		})
	}
	for _, idx := range tb.Indices() {
		idxInfo := idx.Meta()
		if idxInfo.State != model.StatePublic {
			continue
		}
		isClustered := "NO"
		if tb.Meta().IsCommonHandle && idxInfo.Primary {
			isClustered = "YES"
		}

		isGlobalIndex := "NO"
		if idxInfo.Global {
			isGlobalIndex = "YES"
		}

		for i, col := range idxInfo.Columns {
			nonUniq := 1
			if idx.Meta().Unique {
				nonUniq = 0
			}

			var subPart any
			if col.Length != types.UnspecifiedLength {
				subPart = col.Length
			}

			tblCol := tb.Meta().Columns[col.Offset]
			nullVal := "YES"
			if mysql.HasNotNullFlag(tblCol.GetFlag()) {
				nullVal = ""
			}

			visible := "YES"
			if idx.Meta().Invisible {
				visible = "NO"
			}

			colName := col.Name.O
			var expression any
			if tblCol.Hidden {
				colName = "NULL"
				expression = tblCol.GeneratedExprString
			}

			colStats := statsTbl.GetCol(tblCol.ID)
			var ndv int64
			if colStats != nil {
				ndv = colStats.NDV
			}

			e.appendRow([]any{
				tb.Meta().Name.O,       // Table
				nonUniq,                // Non_unique
				idx.Meta().Name.O,      // Key_name
				i + 1,                  // Seq_in_index
				colName,                // Column_name
				"A",                    // Collation
				ndv,                    // Cardinality
				subPart,                // Sub_part
				nil,                    // Packed
				nullVal,                // Null
				idx.Meta().Tp.String(), // Index_type
				"",                     // Comment
				idx.Meta().Comment,     // Index_comment
				visible,                // Index_visible
				expression,             // Expression
				isClustered,            // Clustered
				isGlobalIndex,          // Global_index
			})
		}
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetSupportedCharsets()
	sessVars := e.Ctx().GetSessionVars()
	for _, desc := range descs {
		defaultCollation := desc.DefaultCollation
		if desc.Name == charset.CharsetUTF8MB4 {
			var err error
			defaultCollation, err = sessVars.GetSessionOrGlobalSystemVar(context.Background(), vardef.DefaultCollationForUTF8MB4)
			if err != nil {
				return err
			}
		}
		e.appendRow([]any{
			desc.Name,
			desc.Desc,
			defaultCollation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowMasterStatus() error {
	tso := e.Ctx().GetSessionVars().TxnCtx.StartTS
	e.appendRow([]any{"tidb-binlog", tso, "", "", ""})
	return nil
}

func (e *ShowExec) fetchShowVariables(ctx context.Context) (err error) {
	var (
		value       string
		sessionVars = e.Ctx().GetSessionVars()
	)
	var (
		fieldPatternsLike collate.WildcardPattern
		fieldFilter       string
	)

	if e.Extractor != nil {
		fieldFilter = e.Extractor.Field()
		fieldPatternsLike = e.Extractor.FieldPatternLike()
	}
	if e.GlobalScope {
		// Collect global scope variables,
		// 1. Exclude the variables of ScopeSession in variable.SysVars;
		// 2. If the variable is ScopeNone, it's a read-only variable, return the default value of it,
		// 		otherwise, fetch the value from table `mysql.Global_Variables`.
		for _, v := range variable.GetSysVars() {
			if v.Scope != vardef.ScopeSession {
				if v.IsNoop && !vardef.EnableNoopVariables.Load() {
					continue
				}
				if fieldFilter != "" && v.Name != fieldFilter {
					continue
				} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
					continue
				}
				if infoschema.SysVarHiddenForSem(e.Ctx(), v.Name) {
					continue
				}
				value, err = sessionVars.GetGlobalSystemVar(ctx, v.Name)
				if err != nil {
					return errors.Trace(err)
				}
				e.appendRow([]any{v.Name, value})
			}
		}
		return nil
	}

	// Collect session scope variables,
	// If it is a session only variable, use the default value defined in code,
	//   otherwise, fetch the value from table `mysql.Global_Variables`.
	for _, v := range variable.GetSysVars() {
		if v.IsNoop && !vardef.EnableNoopVariables.Load() {
			continue
		}
		if fieldFilter != "" && v.Name != fieldFilter {
			continue
		} else if fieldPatternsLike != nil && !fieldPatternsLike.DoMatch(v.Name) {
			continue
		}
		if infoschema.SysVarHiddenForSem(e.Ctx(), v.Name) || v.InternalSessionVariable {
			continue
		}
		value, err = sessionVars.GetSessionOrGlobalSystemVar(context.Background(), v.Name)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]any{v.Name, value})
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	sessionVars := e.Ctx().GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return errors.Trace(err)
	}
	checker := privilege.GetPrivilegeManager(e.Ctx())
	for status, v := range statusVars {
		if e.GlobalScope && v.Scope == vardef.ScopeSession {
			continue
		}
		// Skip invisible status vars if permission fails.
		if sem.IsEnabled() && sem.IsInvisibleStatusVar(status) {
			if checker == nil || !checker.RequestDynamicVerification(sessionVars.ActiveRoles, "RESTRICTED_STATUS_ADMIN", false) {
				continue
			}
		}
		switch v.Value.(type) {
		case []any, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]any{status, value})
	}
	return nil
}
