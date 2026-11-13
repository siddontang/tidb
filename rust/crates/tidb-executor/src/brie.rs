//! Go pkg/executor/brie.go, showMetaExec.Next.
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone};

use crate::{DriverError, MysqlError, StmtOutput};

pub(crate) fn show_backup_metadata(
    storage: &str,
    zone: &SessionTimeZone,
) -> Result<StmtOutput, DriverError> {
    let mut url = reqwest::Url::parse(storage)
        .map_err(|_| MysqlError::unknown("invalid backup storage URL"))?;
    if url.scheme() != "local" {
        return Err(DriverError::unsupported(
            "SHOW BACKUP METADATA supports local:// storage only",
        ));
    }
    // Go objstore.parseBackend uses URL.Path for local storage. Discard the
    // authority and query before converting the percent-escaped path.
    let invalid_path = || MysqlError::unknown("invalid local backup storage path");
    if !url.path().starts_with('/') {
        return Err(invalid_path().into());
    }
    url.set_host(None).map_err(|_| invalid_path())?;
    let path = url.to_file_path().map_err(|_| invalid_path())?;
    let meta = tidb_br_show::read_local_metadata(&path).map_err(MysqlError::unknown)?;
    let time = |version| {
        crate::show_stats::version_to_time(version, zone)
            .map(Datum::Time)
            .ok_or_else(|| MysqlError::unknown("invalid backup timestamp"))
    };
    let start = if meta.start_version == 0 {
        Datum::Null
    } else {
        time(meta.start_version)?
    };
    let end = time(meta.end_version)?;
    let rows = meta
        .tables
        .into_iter()
        .map(|table| {
            vec![
                Datum::Bytes(table.database.into_bytes()),
                Datum::Bytes(table.name.into_bytes()),
                Datum::Int(table.total_kvs as i64),
                Datum::Int(table.total_bytes as i64),
                start.clone(),
                end.clone(),
            ]
        })
        .collect();

    Ok(StmtOutput::Rows {
        rows,
        columns: backup_metadata_columns(),
    })
}

/// Go buildShowBackupMetaSchema, also returned at COM_STMT_PREPARE.
pub fn backup_metadata_columns() -> Vec<(String, FieldType)> {
    use FieldTypeCode::{Datetime, LongLong, Varchar};
    [
        ("Database", Varchar),
        ("Table", Varchar),
        ("Total_kvs", LongLong),
        ("Total_bytes", LongLong),
        ("Time_range_start", Datetime),
        ("Time_range_end", Datetime),
    ]
    .into_iter()
    .map(|(name, tp)| {
        let mut ty = FieldType::new(tp).with_decimal(0);
        if tp == Varchar {
            ty.set_flen(255);
        } else {
            ty.set_flen(i64::from(tp.get_default_field_length_and_decimal().0));
            ty.add_flags(FieldTypeFlags::UNSIGNED);
        }
        (name.to_owned(), ty)
    })
    .collect()
}
