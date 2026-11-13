//! Local plaintext metadata read by Go's br/pkg/task/show.CmdExecutor.
use std::path::{Component, Path};

use prost::Message;
use sha2::{Digest, Sha256};
use tidb_model::{DBInfo, TableInfo};
use tikv_client_kvproto::backup::{BackupMeta, MetaFile, Schema};

/// Fields of Go's show.Table returned by the SQL executor.
pub struct Table {
    /// Original database name.
    pub database: String,
    /// Original table name, empty for a database-only record.
    pub name: String,
    /// Key/value count recorded in the schema.
    pub total_kvs: u64,
    /// Byte count recorded in the schema.
    pub total_bytes: u64,
}

/// The SQL-visible fields of Go's show.ShowResult. Versions are TiKV timestamps.
pub struct ShowResult {
    /// Start timestamp, zero for a full backup.
    pub start_version: u64,
    /// End timestamp.
    pub end_version: u64,
    /// Table and empty database records.
    pub tables: Vec<Table>,
}

/// Go's CreateExec and CmdExecutor.Read for local storage. Read schemas only,
/// as MetaReader.ReadSchemasFiles does with SkipFiles and SkipStats.
pub fn read_local_metadata(directory: &Path) -> Result<ShowResult, String> {
    let bytes = std::fs::read(directory.join("backupmeta")).map_err(|e| {
        format!(
            "failed to create show exec: failed to create execution: load backupmeta failed: {e}"
        )
    })?;
    let meta = BackupMeta::decode(bytes.as_slice())
        .map_err(|e| format!("failed to create show exec: failed to create execution: parse backupmeta failed because of wrong aes cipher: {e}"))?;
    read_metadata(directory, meta)
        .map_err(|e| format!("failed to read metadata from backupmeta: {e}"))
}

fn read_metadata(directory: &Path, meta: BackupMeta) -> Result<ShowResult, String> {
    if meta.end_version < meta.start_version {
        return Err(format!(
            "invalid metafile: the start version({}) is greater than the end version({}), perhaps reading a backup meta from log backup",
            meta.start_version, meta.end_version
        ));
    }
    let mut result = ShowResult {
        start_version: meta.start_version,
        end_version: meta.end_version,
        tables: Vec::new(),
    };
    if meta.is_raw_kv {
        if meta.raw_range_index.is_some() {
            return Err(
                "invalid metafile: show raw kv with backup meta v2 isn't supported for now".into(),
            );
        }
        return Ok(result);
    }

    append_schemas(&mut result.tables, meta.schemas)?;
    // Go's readSchemas also walks SchemaIndex when inline (v1) schemas exist.
    let mut pending: Vec<_> = meta.schema_index.into_iter().collect();
    while let Some(index) = pending.pop() {
        if index.meta_files.is_empty() {
            append_schemas(&mut result.tables, index.schemas)?;
        } else {
            for file in index.meta_files {
                let path = Path::new(&file.name);
                if !path
                    .components()
                    .all(|c| matches!(c, Component::CurDir | Component::Normal(_)))
                {
                    return Err("invalid schema index file name".into());
                }
                let bytes = std::fs::read(directory.join(path)).map_err(|e| e.to_string())?;
                if Sha256::digest(&bytes)[..] != file.sha256 {
                    return Err("invalid metafile: schema file checksum mismatch".into());
                }
                pending.push(MetaFile::decode(bytes.as_slice()).map_err(|e| e.to_string())?);
            }
        }
    }
    Ok(result)
}

fn append_schemas(tables: &mut Vec<Table>, schemas: Vec<Schema>) -> Result<(), String> {
    for schema in schemas {
        let db: DBInfo = serde_json::from_slice(&schema.db).map_err(|e| e.to_string())?;
        // Go's parseSchemaFile also returns records for empty databases.
        let name = if schema.table.is_empty() {
            String::new()
        } else {
            let table: TableInfo =
                serde_json::from_slice(&schema.table).map_err(|e| e.to_string())?;
            table.name.original().to_owned()
        };
        tables.push(Table {
            database: db.name.original().to_owned(),
            name,
            total_kvs: schema.total_kvs,
            total_bytes: schema.total_bytes,
        });
    }
    Ok(())
}
