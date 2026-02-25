// Copyright 2019 PingCAP, Inc.
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

package checkpoints

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/joho/sqltocsv"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints/checkpointspb"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// FileCheckpointsDB is a file based checkpoints DB
type FileCheckpointsDB struct {
	lock        sync.Mutex // we need to ensure only a thread can access to `checkpoints` at a time
	checkpoints checkpointspb.CheckpointsModel
	ctx         context.Context
	path        string
	fileName    string
	exStorage   storeapi.Storage
}

func newFileCheckpointsDB(
	ctx context.Context,
	path string,
	exStorage storeapi.Storage,
	fileName string,
) (*FileCheckpointsDB, error) {
	cpdb := &FileCheckpointsDB{
		checkpoints: checkpointspb.CheckpointsModel{
			TaskCheckpoint: &checkpointspb.TaskCheckpointModel{},
			Checkpoints:    map[string]*checkpointspb.TableCheckpointModel{},
		},
		ctx:       ctx,
		path:      path,
		fileName:  fileName,
		exStorage: exStorage,
	}

	if cpdb.fileName == "" {
		return nil, errors.Errorf("the checkpoint DSN '%s' must not be a directory", path)
	}

	exist, err := cpdb.exStorage.FileExists(ctx, cpdb.fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		logutil.Logger(ctx).Info("open checkpoint file failed, going to create a new one",
			zap.String("path", path),
			log.ShortError(err),
		)
		return cpdb, nil
	}
	content, err := cpdb.exStorage.ReadFile(ctx, cpdb.fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = cpdb.checkpoints.Unmarshal(content)
	if err != nil {
		logutil.Logger(ctx).Error("checkpoint file is broken", zap.String("path", path), zap.Error(err))
	}
	// FIXME: patch for empty map may need initialize manually, because currently
	// FIXME: a map of zero size -> marshall -> unmarshall -> become nil, see checkpoint_test.go
	if cpdb.checkpoints.Checkpoints == nil {
		cpdb.checkpoints.Checkpoints = map[string]*checkpointspb.TableCheckpointModel{}
	}
	for _, table := range cpdb.checkpoints.Checkpoints {
		if table.Engines == nil {
			table.Engines = map[int32]*checkpointspb.EngineCheckpointModel{}
		}
		for _, engine := range table.Engines {
			if engine.Chunks == nil {
				engine.Chunks = map[string]*checkpointspb.ChunkCheckpointModel{}
			}
		}
	}
	return cpdb, nil
}

// NewFileCheckpointsDB creates a new FileCheckpointsDB
func NewFileCheckpointsDB(ctx context.Context, path string) (*FileCheckpointsDB, error) {
	// init Storage
	s, fileName, err := createExstorageByCompletePath(ctx, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newFileCheckpointsDB(ctx, path, s, fileName)
}

// NewFileCheckpointsDBWithExstorageFileName creates a new FileCheckpointsDB with external storage and file name
func NewFileCheckpointsDBWithExstorageFileName(
	ctx context.Context,
	path string,
	s storeapi.Storage,
	fileName string,
) (*FileCheckpointsDB, error) {
	return newFileCheckpointsDB(ctx, path, s, fileName)
}

// createExstorageByCompletePath create Storage by completePath and return fileName.
func createExstorageByCompletePath(ctx context.Context, completePath string) (storeapi.Storage, string, error) {
	if completePath == "" {
		return nil, "", nil
	}
	fileName, newPath, err := separateCompletePath(completePath)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	u, err := objstore.ParseBackend(newPath, nil)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	s, err := objstore.New(ctx, u, &storeapi.Options{})
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	return s, fileName, nil
}

// separateCompletePath separates fileName from completePath, returns fileName and newPath.
func separateCompletePath(completePath string) (fileName string, newPath string, err error) {
	if completePath == "" {
		return "", "", nil
	}
	purl, err := objstore.ParseRawURL(completePath)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	// not url format, we don't use url library to avoid being escaped or unescaped
	if purl.Scheme == "" {
		// no fileName, just path
		if strings.HasSuffix(completePath, "/") {
			return "", completePath, nil
		}
		fileName = path.Base(completePath)
		newPath = path.Dir(completePath)
	} else {
		if strings.HasSuffix(purl.Path, "/") {
			return "", completePath, nil
		}
		fileName = path.Base(purl.Path)
		purl.Path = path.Dir(purl.Path)
		newPath = purl.String()
	}
	return fileName, newPath, nil
}

func (cpdb *FileCheckpointsDB) save() error {
	serialized, err := cpdb.checkpoints.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	return cpdb.exStorage.WriteFile(cpdb.ctx, cpdb.fileName, serialized)
}

// Initialize implements CheckpointsDB.Initialize.
func (cpdb *FileCheckpointsDB) Initialize(_ context.Context, cfg *config.Config, dbInfo map[string]*TidbDBInfo) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	cpdb.checkpoints.TaskCheckpoint = &checkpointspb.TaskCheckpointModel{
		TaskId:       cfg.TaskID,
		SourceDir:    cfg.Mydumper.SourceDir,
		Backend:      cfg.TikvImporter.Backend,
		ImporterAddr: cfg.TikvImporter.Addr,
		TidbHost:     cfg.TiDB.Host,
		TidbPort:     int32(cfg.TiDB.Port),
		PdAddr:       cfg.TiDB.PdAddr,
		SortedKvDir:  cfg.TikvImporter.SortedKVDir,
		LightningVer: build.ReleaseVersion,
	}

	if cpdb.checkpoints.Checkpoints == nil {
		cpdb.checkpoints.Checkpoints = make(map[string]*checkpointspb.TableCheckpointModel)
	}

	var err error
	for _, db := range dbInfo {
		for _, table := range db.Tables {
			tableName := common.UniqueTable(db.Name, table.Name)
			if _, ok := cpdb.checkpoints.Checkpoints[tableName]; !ok {
				var tableInfo []byte
				if cfg.TikvImporter.AddIndexBySQL {
					// tableInfo is quite large in most case, when importing many
					// small tables, writing tableInfo will make checkpoint file
					// vary large, and save checkpoint will become bottleneck, so
					// we only store table info if we are going to add index by SQL
					// where it's required.
					tableInfo, err = json.Marshal(table.Desired)
					if err != nil {
						return errors.Trace(err)
					}
				}
				cpdb.checkpoints.Checkpoints[tableName] = &checkpointspb.TableCheckpointModel{
					Status:    uint32(CheckpointStatusLoaded),
					Engines:   map[int32]*checkpointspb.EngineCheckpointModel{},
					TableID:   table.ID,
					TableInfo: tableInfo,
				}
			}
			// TODO check if hash matches
		}
	}

	return errors.Trace(cpdb.save())
}

// TaskCheckpoint implements CheckpointsDB.TaskCheckpoint.
func (cpdb *FileCheckpointsDB) TaskCheckpoint(_ context.Context) (*TaskCheckpoint, error) {
	// this method is always called in lock
	cp := cpdb.checkpoints.TaskCheckpoint
	if cp == nil || cp.TaskId == 0 {
		return nil, nil
	}

	return &TaskCheckpoint{
		TaskID:       cp.TaskId,
		SourceDir:    cp.SourceDir,
		Backend:      cp.Backend,
		ImporterAddr: cp.ImporterAddr,
		TiDBHost:     cp.TidbHost,
		TiDBPort:     int(cp.TidbPort),
		PdAddr:       cp.PdAddr,
		SortedKVDir:  cp.SortedKvDir,
		LightningVer: cp.LightningVer,
	}, nil
}

// Close implements CheckpointsDB.Close.
func (cpdb *FileCheckpointsDB) Close() error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	return errors.Trace(cpdb.save())
}

// Get implements CheckpointsDB.Get.
func (cpdb *FileCheckpointsDB) Get(_ context.Context, tableName string) (*TableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	tableModel, ok := cpdb.checkpoints.Checkpoints[tableName]
	if !ok {
		return nil, errors.NotFoundf("checkpoint for table %s", tableName)
	}

	var tableInfo *model.TableInfo
	if len(tableModel.TableInfo) > 0 {
		if err := json.Unmarshal(tableModel.TableInfo, &tableInfo); err != nil {
			return nil, errors.Trace(err)
		}
	}

	cp := &TableCheckpoint{
		Status:        CheckpointStatus(tableModel.Status),
		Engines:       make(map[int32]*EngineCheckpoint, len(tableModel.Engines)),
		TableID:       tableModel.TableID,
		TableInfo:     tableInfo,
		Checksum:      verify.MakeKVChecksum(tableModel.KvBytes, tableModel.KvKvs, tableModel.KvChecksum),
		AutoRandBase:  tableModel.AutoRandBase,
		AutoIncrBase:  tableModel.AutoIncrBase,
		AutoRowIDBase: tableModel.AutoRowIDBase,
	}

	for engineID, engineModel := range tableModel.Engines {
		engine := &EngineCheckpoint{
			Status: CheckpointStatus(engineModel.Status),
			Chunks: make([]*ChunkCheckpoint, 0, len(engineModel.Chunks)),
		}

		for _, chunkModel := range engineModel.Chunks {
			colPerm := make([]int, 0, len(chunkModel.ColumnPermutation))
			for _, c := range chunkModel.ColumnPermutation {
				colPerm = append(colPerm, int(c))
			}
			engine.Chunks = append(engine.Chunks, &ChunkCheckpoint{
				Key: ChunkCheckpointKey{
					Path:   chunkModel.Path,
					Offset: chunkModel.Offset,
				},
				FileMeta: mydump.SourceFileMeta{
					Path:        chunkModel.Path,
					Type:        mydump.SourceType(chunkModel.Type),
					Compression: mydump.Compression(chunkModel.Compression),
					SortKey:     chunkModel.SortKey,
					FileSize:    chunkModel.FileSize,
				},
				ColumnPermutation: colPerm,
				Chunk: mydump.Chunk{
					Offset:       chunkModel.Pos,
					RealOffset:   chunkModel.RealPos,
					EndOffset:    chunkModel.EndOffset,
					PrevRowIDMax: chunkModel.PrevRowidMax,
					RowIDMax:     chunkModel.RowidMax,
				},
				Checksum:  verify.MakeKVChecksum(chunkModel.KvcBytes, chunkModel.KvcKvs, chunkModel.KvcChecksum),
				Timestamp: chunkModel.Timestamp,
			})
		}

		slices.SortFunc(engine.Chunks, func(i, j *ChunkCheckpoint) int {
			return i.Key.compare(&j.Key)
		})

		cp.Engines[engineID] = engine
	}

	return cp, nil
}

// InsertEngineCheckpoints implements CheckpointsDB.InsertEngineCheckpoints.
func (cpdb *FileCheckpointsDB) InsertEngineCheckpoints(_ context.Context, tableName string, checkpoints map[int32]*EngineCheckpoint) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	tableModel := cpdb.checkpoints.Checkpoints[tableName]
	for engineID, engine := range checkpoints {
		engineModel := &checkpointspb.EngineCheckpointModel{
			Status: uint32(CheckpointStatusLoaded),
			Chunks: make(map[string]*checkpointspb.ChunkCheckpointModel),
		}
		for _, value := range engine.Chunks {
			key := value.Key.String()
			chunk, ok := engineModel.Chunks[key]
			if !ok {
				chunk = &checkpointspb.ChunkCheckpointModel{
					Path:   value.Key.Path,
					Offset: value.Key.Offset,
				}
				engineModel.Chunks[key] = chunk
			}
			chunk.Type = int32(value.FileMeta.Type)
			chunk.Compression = int32(value.FileMeta.Compression)
			chunk.SortKey = value.FileMeta.SortKey
			chunk.FileSize = value.FileMeta.FileSize
			chunk.Pos = value.Chunk.Offset
			chunk.RealPos = value.Chunk.RealOffset
			chunk.EndOffset = value.Chunk.EndOffset
			chunk.PrevRowidMax = value.Chunk.PrevRowIDMax
			chunk.RowidMax = value.Chunk.RowIDMax
			chunk.Timestamp = value.Timestamp
			if len(value.ColumnPermutation) > 0 {
				chunk.ColumnPermutation = intSlice2Int32Slice(value.ColumnPermutation)
			}
		}
		tableModel.Engines[engineID] = engineModel
	}

	return errors.Trace(cpdb.save())
}

// Update implements CheckpointsDB.Update.
func (cpdb *FileCheckpointsDB) Update(_ context.Context, checkpointDiffs map[string]*TableCheckpointDiff) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, cpd := range checkpointDiffs {
		tableModel := cpdb.checkpoints.Checkpoints[tableName]
		if cpd.hasStatus {
			tableModel.Status = uint32(cpd.status)
		}
		if cpd.hasRebase {
			tableModel.AutoRandBase = max(tableModel.AutoRandBase, cpd.autoRandBase)
			tableModel.AutoIncrBase = max(tableModel.AutoIncrBase, cpd.autoIncrBase)
			tableModel.AutoRowIDBase = max(tableModel.AutoRowIDBase, cpd.autoRowIDBase)
		}
		if cpd.hasChecksum {
			tableModel.KvBytes = cpd.checksum.SumSize()
			tableModel.KvKvs = cpd.checksum.SumKVS()
			tableModel.KvChecksum = cpd.checksum.Sum()
		}
		for engineID, engineDiff := range cpd.engines {
			engineModel := tableModel.Engines[engineID]
			if engineDiff.hasStatus {
				engineModel.Status = uint32(engineDiff.status)
			}

			for key, diff := range engineDiff.chunks {
				chunkModel := engineModel.Chunks[key.String()]
				chunkModel.Pos = diff.pos
				chunkModel.RealPos = diff.realPos
				chunkModel.PrevRowidMax = diff.rowID
				chunkModel.KvcBytes = diff.checksum.SumSize()
				chunkModel.KvcKvs = diff.checksum.SumKVS()
				chunkModel.KvcChecksum = diff.checksum.Sum()
				chunkModel.ColumnPermutation = intSlice2Int32Slice(diff.columnPermutation)
			}
		}
	}

	return cpdb.save()
}

// Management functions ----------------------------------------------------------------------------

var errCannotManageNullDB = errors.New("cannot perform this function while checkpoints is disabled")

// RemoveCheckpoint implements CheckpointsDB.RemoveCheckpoint.
func (*NullCheckpointsDB) RemoveCheckpoint(context.Context, string) error {
	return errors.Trace(errCannotManageNullDB)
}

// MoveCheckpoints implements CheckpointsDB.MoveCheckpoints.
func (*NullCheckpointsDB) MoveCheckpoints(context.Context, int64) error {
	return errors.Trace(errCannotManageNullDB)
}

// GetLocalStoringTables implements CheckpointsDB.GetLocalStoringTables.
func (*NullCheckpointsDB) GetLocalStoringTables(context.Context) (map[string][]int32, error) {
	return nil, nil
}

// IgnoreErrorCheckpoint implements CheckpointsDB.IgnoreErrorCheckpoint.
func (*NullCheckpointsDB) IgnoreErrorCheckpoint(context.Context, string) error {
	return errors.Trace(errCannotManageNullDB)
}

// DestroyErrorCheckpoint implements CheckpointsDB.DestroyErrorCheckpoint.
func (*NullCheckpointsDB) DestroyErrorCheckpoint(context.Context, string) ([]DestroyedTableCheckpoint, error) {
	return nil, errors.Trace(errCannotManageNullDB)
}

// DumpTables implements CheckpointsDB.DumpTables.
func (*NullCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Trace(errCannotManageNullDB)
}

// DumpEngines implements CheckpointsDB.DumpEngines.
func (*NullCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Trace(errCannotManageNullDB)
}

// DumpChunks implements CheckpointsDB.DumpChunks.
func (*NullCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Trace(errCannotManageNullDB)
}

// RemoveCheckpoint implements CheckpointsDB.RemoveCheckpoint.
func (cpdb *MySQLCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.String("table", tableName)),
	}

	if tableName == allTables {
		return s.Exec(ctx, "remove all checkpoints", common.SprintfWithIdentifiers("DROP SCHEMA %s", cpdb.schema))
	}

	deleteChunkQuery := common.SprintfWithIdentifiers(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameChunk)
	deleteEngineQuery := common.SprintfWithIdentifiers(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameEngine)
	deleteTableQuery := common.SprintfWithIdentifiers(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameTable)

	return s.Transact(ctx, "remove checkpoints", func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, deleteChunkQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
}

// MoveCheckpoints implements CheckpointsDB.MoveCheckpoints.
func (cpdb *MySQLCheckpointsDB) MoveCheckpoints(ctx context.Context, taskID int64) error {
	newSchema := fmt.Sprintf("%s.%d.bak", cpdb.schema, taskID)
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.Int64("taskID", taskID)),
	}

	createSchemaQuery := common.SprintfWithIdentifiers("CREATE SCHEMA IF NOT EXISTS %s", newSchema)
	if e := s.Exec(ctx, "create backup checkpoints schema", createSchemaQuery); e != nil {
		return e
	}
	for _, tbl := range []string{
		CheckpointTableNameChunk, CheckpointTableNameEngine,
		CheckpointTableNameTable, CheckpointTableNameTask,
	} {
		query := common.SprintfWithIdentifiers("RENAME TABLE %[1]s.%[3]s TO %[2]s.%[3]s", cpdb.schema, newSchema, tbl)
		if e := s.Exec(ctx, fmt.Sprintf("move %s checkpoints table", tbl), query); e != nil {
			return e
		}
	}

	return nil
}

// GetLocalStoringTables implements CheckpointsDB.GetLocalStoringTables.
func (cpdb *MySQLCheckpointsDB) GetLocalStoringTables(ctx context.Context) (map[string][]int32, error) {
	var targetTables map[string][]int32

	// lightning didn't check CheckpointStatusMaxInvalid before this function is called, so we skip invalid ones
	// engines should exist if
	// 1. table status is earlier than CheckpointStatusIndexImported, and
	// 2. engine status is earlier than CheckpointStatusImported, and
	// 3. chunk has been read

	query := common.SprintfWithIdentifiers(`
		SELECT DISTINCT t.table_name, c.engine_id
		FROM %s.%s t, %s.%s c, %s.%s e
		WHERE t.table_name = c.table_name AND t.table_name = e.table_name AND c.engine_id = e.engine_id
			AND ? < t.status AND t.status < ?
			AND ? < e.status AND e.status < ?
			AND c.pos > c.offset;`,
		cpdb.schema, CheckpointTableNameTable, cpdb.schema, CheckpointTableNameChunk, cpdb.schema, CheckpointTableNameEngine)

	err := common.Retry("get local storing tables", log.Wrap(logutil.Logger(ctx)), func() error {
		targetTables = make(map[string][]int32)
		rows, err := cpdb.db.QueryContext(ctx, query,
			CheckpointStatusMaxInvalid, CheckpointStatusIndexImported,
			CheckpointStatusMaxInvalid, CheckpointStatusImported)
		if err != nil {
			return errors.Trace(err)
		}
		//nolint: errcheck
		defer rows.Close()
		for rows.Next() {
			var (
				tableName string
				engineID  int32
			)
			if err := rows.Scan(&tableName, &engineID); err != nil {
				return errors.Trace(err)
			}
			targetTables[tableName] = append(targetTables[tableName], engineID)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, err
}

// IgnoreErrorCheckpoint implements CheckpointsDB.IgnoreErrorCheckpoint.
func (cpdb *MySQLCheckpointsDB) IgnoreErrorCheckpoint(ctx context.Context, tableName string) error {
	var (
		query, query2 string
		args          []any
	)
	if tableName == allTables {
		query = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE status <= ?", cpdb.schema, CheckpointTableNameEngine)
		query2 = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE status <= ?", cpdb.schema, CheckpointTableNameTable)
		args = []any{CheckpointStatusLoaded, CheckpointStatusMaxInvalid}
	} else {
		query = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE table_name = ? AND status <= ?", cpdb.schema, CheckpointTableNameEngine)
		query2 = common.SprintfWithIdentifiers("UPDATE %s.%s SET status = ? WHERE table_name = ? AND status <= ?", cpdb.schema, CheckpointTableNameTable)
		args = []any{CheckpointStatusLoaded, tableName, CheckpointStatusMaxInvalid}
	}

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "ignore error checkpoints", func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, query, args...); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, query2, args...); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	return errors.Trace(err)
}

// DestroyErrorCheckpoint implements CheckpointsDB.DestroyErrorCheckpoint.
func (cpdb *MySQLCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error) {
	var (
		selectQuery, deleteChunkQuery, deleteEngineQuery, deleteTableQuery string
		args                                                               []any
	)
	if tableName == allTables {
		selectQuery = common.SprintfWithIdentifiers(`
			SELECT
				t.table_name,
				COALESCE(MIN(e.engine_id), 0),
				COALESCE(MAX(e.engine_id), -1)
			FROM %[1]s.%[2]s t
			LEFT JOIN %[1]s.%[3]s e ON t.table_name = e.table_name
			WHERE t.status <= ?
			GROUP BY t.table_name;
		`, cpdb.schema, CheckpointTableNameTable, CheckpointTableNameEngine)
		deleteChunkQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE status <= ?)
		`, cpdb.schema, CheckpointTableNameChunk, CheckpointTableNameTable)
		deleteEngineQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE status <= ?)
		`, cpdb.schema, CheckpointTableNameEngine, CheckpointTableNameTable)
		deleteTableQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %s.%s WHERE status <= ?
		`, cpdb.schema, CheckpointTableNameTable)
		args = []any{CheckpointStatusMaxInvalid}
	} else {
		selectQuery = common.SprintfWithIdentifiers(`
			SELECT
				t.table_name,
				COALESCE(MIN(e.engine_id), 0),
				COALESCE(MAX(e.engine_id), -1)
			FROM %[1]s.%[2]s t
			LEFT JOIN %[1]s.%[3]s e ON t.table_name = e.table_name
			WHERE t.table_name = ? AND t.status <= ?
			GROUP BY t.table_name;
		`, cpdb.schema, CheckpointTableNameTable, CheckpointTableNameEngine)
		deleteChunkQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE table_name = ? AND status <= ?)
		`, cpdb.schema, CheckpointTableNameChunk, CheckpointTableNameTable)
		deleteEngineQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %[1]s.%[2]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[3]s WHERE table_name = ? AND status <= ?)
		`, cpdb.schema, CheckpointTableNameEngine, CheckpointTableNameTable)
		deleteTableQuery = common.SprintfWithIdentifiers(`
			DELETE FROM %s.%s WHERE table_name = ? AND status <= ?
		`, cpdb.schema, CheckpointTableNameTable)
		args = []any{tableName, CheckpointStatusMaxInvalid}
	}

	var targetTables []DestroyedTableCheckpoint

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.Wrap(logutil.Logger(ctx)).With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "destroy error checkpoints", func(c context.Context, tx *sql.Tx) error {
		// Obtain the list of tables
		targetTables = nil
		rows, e := tx.QueryContext(c, selectQuery, args...)
		if e != nil {
			return errors.Trace(e)
		}
		//nolint: errcheck
		defer rows.Close()
		for rows.Next() {
			var dtc DestroyedTableCheckpoint
			if e := rows.Scan(&dtc.TableName, &dtc.MinEngineID, &dtc.MaxEngineID); e != nil {
				return errors.Trace(e)
			}
			targetTables = append(targetTables, dtc)
		}
		if e := rows.Err(); e != nil {
			return errors.Trace(e)
		}

		// Delete the checkpoints
		if _, e := tx.ExecContext(c, deleteChunkQuery, args...); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, args...); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, args...); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

// DumpTables implements CheckpointsDB.DumpTables.
//
//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpTables(ctx context.Context, writer io.Writer) error {
	//nolint: rowserrcheck
	rows, err := cpdb.db.QueryContext(ctx, common.SprintfWithIdentifiers(`
		SELECT
			task_id,
			table_name,
			hex(hash) AS hash,
			status,
			create_time,
			update_time,
			auto_rand_base,
			auto_incr_base,
			auto_row_id_base
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameTable))
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

// DumpEngines implements CheckpointsDB.DumpEngines.
//
//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpEngines(ctx context.Context, writer io.Writer) error {
	//nolint: rowserrcheck
	rows, err := cpdb.db.QueryContext(ctx, common.SprintfWithIdentifiers(`
		SELECT
			table_name,
			engine_id,
			status,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameEngine))
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

// DumpChunks implements CheckpointsDB.DumpChunks.
//
//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpChunks(ctx context.Context, writer io.Writer) error {
	//nolint: rowserrcheck
	rows, err := cpdb.db.QueryContext(ctx, common.SprintfWithIdentifiers(`
		SELECT
			table_name,
			path,
			offset,
			type,
			compression,
			sort_key,
			file_size,
			columns,
			pos,
			real_pos,
			end_offset,
			prev_rowid_max,
			rowid_max,
			kvc_bytes,
			kvc_kvs,
			kvc_checksum,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameChunk))
	if err != nil {
		return errors.Trace(err)
	}
	//nolint: errcheck
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

// RemoveCheckpoint implements CheckpointsDB.RemoveCheckpoint.
func (cpdb *FileCheckpointsDB) RemoveCheckpoint(_ context.Context, tableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	if tableName == allTables {
		cpdb.checkpoints.Reset()
		return errors.Trace(cpdb.exStorage.DeleteFile(cpdb.ctx, cpdb.fileName))
	}

	delete(cpdb.checkpoints.Checkpoints, tableName)
	return errors.Trace(cpdb.save())
}

// MoveCheckpoints implements CheckpointsDB.MoveCheckpoints.
func (cpdb *FileCheckpointsDB) MoveCheckpoints(_ context.Context, taskID int64) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	newFileName := fmt.Sprintf("%s.%d.bak", cpdb.fileName, taskID)
	return cpdb.exStorage.Rename(cpdb.ctx, cpdb.fileName, newFileName)
}

// GetLocalStoringTables implements CheckpointsDB.GetLocalStoringTables.
func (cpdb *FileCheckpointsDB) GetLocalStoringTables(_ context.Context) (map[string][]int32, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	targetTables := make(map[string][]int32)

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) ||
			tableModel.Status >= uint32(CheckpointStatusIndexImported) {
			continue
		}
		for engineID, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) ||
				engineModel.Status >= uint32(CheckpointStatusImported) {
				continue
			}

			for _, chunkModel := range engineModel.Chunks {
				if chunkModel.Pos > chunkModel.Offset {
					targetTables[tableName] = append(targetTables[tableName], engineID)
					break
				}
			}
		}
	}

	return targetTables, nil
}

// IgnoreErrorCheckpoint implements CheckpointsDB.IgnoreErrorCheckpoint.
func (cpdb *FileCheckpointsDB) IgnoreErrorCheckpoint(_ context.Context, targetTableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if !(targetTableName == allTables || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			tableModel.Status = uint32(CheckpointStatusLoaded)
		}
		for _, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) {
				engineModel.Status = uint32(CheckpointStatusLoaded)
			}
		}
	}
	return errors.Trace(cpdb.save())
}

// DestroyErrorCheckpoint implements CheckpointsDB.DestroyErrorCheckpoint.
func (cpdb *FileCheckpointsDB) DestroyErrorCheckpoint(_ context.Context, targetTableName string) ([]DestroyedTableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	var targetTables []DestroyedTableCheckpoint

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		// Obtain the list of tables
		if !(targetTableName == allTables || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			var minEngineID, maxEngineID int32 = math.MaxInt32, math.MinInt32
			for engineID := range tableModel.Engines {
				if engineID < minEngineID {
					minEngineID = engineID
				}
				if engineID > maxEngineID {
					maxEngineID = engineID
				}
			}

			targetTables = append(targetTables, DestroyedTableCheckpoint{
				TableName:   tableName,
				MinEngineID: minEngineID,
				MaxEngineID: maxEngineID,
			})
		}
	}

	// Delete the checkpoints
	for _, dtcp := range targetTables {
		delete(cpdb.checkpoints.Checkpoints, dtcp.TableName)
	}
	if err := cpdb.save(); err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

// DumpTables implements CheckpointsDB.DumpTables.
func (cpdb *FileCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

// DumpEngines implements CheckpointsDB.DumpEngines.
func (cpdb *FileCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

// DumpChunks implements CheckpointsDB.DumpChunks.
func (cpdb *FileCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func intSlice2Int32Slice(s []int) []int32 {
	res := make([]int32, 0, len(s))
	for _, i := range s {
		res = append(res, int32(i))
	}
	return res
}
