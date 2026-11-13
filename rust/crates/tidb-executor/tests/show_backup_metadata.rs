//! Go br/pkg/task/show/cmd_test.go, exercised through the SQL session.
use tidb_datatype::{datum_text::sysvar_datum_text, FieldTypeCode};
use tidb_executor::compiler::PreparedAstCompile;
use tidb_executor::StmtOutput;
use tidb_session::Session;

const FULL_META: &[u8] = include_bytes!("../../../../br/pkg/task/show/testdata/full-schema.meta");
const V2: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../br/pkg/task/show/testdata/v2"
);

#[test]
fn show_backup_metadata_via_sql() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("full backup+1");
    std::fs::create_dir(&dir).unwrap();
    std::fs::write(dir.join("backupmeta"), FULL_META).unwrap();
    let sql = format!(
        "SHOW BACKUP METADATA FROM 'local://{}/full%20backup+1'",
        root.path().display()
    );
    let mut session = Session::new();

    for (zone, end) in [
        ("+08:00", "2023-04-10 11:18:21"),
        ("-08:00", "2023-04-09 19:18:21"),
    ] {
        session.run(&format!("SET @@time_zone='{zone}'")).unwrap();
        let StmtOutput::Rows { columns, rows } = session.run_with_columns(&sql).unwrap() else {
            panic!("expected a BR metadata result set");
        };
        use FieldTypeCode::{Datetime, LongLong, Varchar};
        assert_eq!(
            columns
                .iter()
                .map(|(name, ty)| (name.as_str(), ty.code()))
                .collect::<Vec<_>>(),
            [
                ("Database", Varchar),
                ("Table", Varchar),
                ("Total_kvs", LongLong),
                ("Total_bytes", LongLong),
                ("Time_range_start", Datetime),
                ("Time_range_end", Datetime)
            ]
        );
        let mut actual: Vec<_> = rows
            .iter()
            .map(|row| row.iter().map(sysvar_datum_text).collect::<Vec<_>>())
            .collect();
        actual.sort();
        let expected: Vec<_> = [
            "customer",
            "district",
            "history",
            "item",
            "new_order",
            "order_line",
            "orders",
            "stock",
            "warehouse",
        ]
        .into_iter()
        .map(|table| {
            [
                Some("tpcc"),
                Some(table),
                Some("0"),
                Some("0"),
                None,
                Some(end),
            ]
            .map(|s| s.map(str::to_owned))
        })
        .collect();
        assert_eq!(actual, expected);
    }
}

#[test]
fn show_backup_metadata_reads_indexed_schemas() {
    let sql = format!("SHOW BACKUP METADATA FROM 'local://{V2}'");
    let StmtOutput::Rows { rows, .. } = Session::new().run_with_columns(&sql).unwrap() else {
        panic!("expected a BR metadata result set");
    };
    assert_eq!(rows.len(), 500);
}

#[test]
fn show_backup_metadata_prepares_without_opening_storage() {
    let directory = tempfile::tempdir().unwrap();
    let sql = format!(
        "SHOW BACKUP METADATA FROM 'local://{}'",
        directory.path().display()
    );
    let mut session = Session::new();
    let prepared = session.prepare_ast(&sql).unwrap();
    let columns = session
        .plan_bound_prepared_columns(prepared.bind(&[]).unwrap())
        .unwrap();
    assert_eq!(columns.len(), 6);
    assert!(session
        .run(&sql)
        .unwrap_err()
        .to_string()
        .contains("load backupmeta failed"));
}

#[test]
fn show_backup_metadata_rejects_damaged_indexed_schemas() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::copy(
        std::path::Path::new(V2).join("backupmeta"),
        dir.path().join("backupmeta"),
    )
    .unwrap();
    std::fs::write(dir.path().join("backupmeta.schema.000000001"), b"damaged").unwrap();
    let sql = format!(
        "SHOW BACKUP METADATA FROM 'local://{}'",
        dir.path().display()
    );
    assert!(Session::new()
        .run(&sql)
        .unwrap_err()
        .to_string()
        .contains("checksum mismatch"));
}
