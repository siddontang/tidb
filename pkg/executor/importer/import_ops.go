// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	litlog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func (e *LoadDataController) tableVisCols2FieldMappings() ([]*FieldMapping, []string) {
	tableCols := e.Table.VisibleCols()
	mappings := make([]*FieldMapping, 0, len(tableCols))
	names := make([]string, 0, len(tableCols))
	for _, v := range tableCols {
		// Data for generated column is generated from the other rows rather than from the parsed data.
		fieldMapping := &FieldMapping{
			Column: v,
		}
		mappings = append(mappings, fieldMapping)
		names = append(names, v.Name.O)
	}
	return mappings, names
}

// initFieldMappings make a field mapping slice to implicitly map input field to table column or user defined variable
// the slice's order is the same as the order of the input fields.
// Returns a slice of same ordered column names without user defined variable names.
func (e *LoadDataController) initFieldMappings() []string {
	columns := make([]string, 0, len(e.ColumnsAndUserVars)+len(e.ColumnAssignments))
	tableCols := e.Table.VisibleCols()

	if len(e.ColumnsAndUserVars) == 0 {
		e.FieldMappings, columns = e.tableVisCols2FieldMappings()

		return columns
	}

	var column *table.Column

	for _, v := range e.ColumnsAndUserVars {
		if v.ColumnName != nil {
			column = table.FindCol(tableCols, v.ColumnName.Name.O)
			columns = append(columns, v.ColumnName.Name.O)
		} else {
			column = nil
		}

		fieldMapping := &FieldMapping{
			Column:  column,
			UserVar: v.UserVar,
		}
		e.FieldMappings = append(e.FieldMappings, fieldMapping)
	}

	return columns
}

// initLoadColumns sets columns which the input fields loaded to.
func (e *LoadDataController) initLoadColumns(columnNames []string) error {
	var cols []*table.Column
	var missingColName string
	var err error
	tableCols := e.Table.VisibleCols()

	if len(columnNames) != len(tableCols) {
		for _, v := range e.ColumnAssignments {
			columnNames = append(columnNames, v.Column.Name.O)
		}
	}

	cols, missingColName = table.FindCols(tableCols, columnNames, e.Table.Meta().PKIsHandle)
	if missingColName != "" {
		return dbterror.ErrBadField.GenWithStackByArgs(missingColName, "field list")
	}

	e.InsertColumns = append(e.InsertColumns, cols...)

	// e.InsertColumns is appended according to the original tables' column sequence.
	// We have to reorder it to follow the use-specified column order which is shown in the columnNames.
	if err = e.reorderColumns(columnNames); err != nil {
		return err
	}

	// Check column whether is specified only once.
	err = table.CheckOnce(cols)
	if err != nil {
		return err
	}

	return nil
}

// reorderColumns reorder the e.InsertColumns according to the order of columnNames
// Note: We must ensure there must be one-to-one mapping between e.InsertColumns and columnNames in terms of column name.
func (e *LoadDataController) reorderColumns(columnNames []string) error {
	cols := e.InsertColumns

	if len(cols) != len(columnNames) {
		return exeerrors.ErrColumnsNotMatched
	}

	reorderedColumns := make([]*table.Column, len(cols))

	if columnNames == nil {
		return nil
	}

	mapping := make(map[string]int)
	for idx, colName := range columnNames {
		mapping[strings.ToLower(colName)] = idx
	}

	for _, col := range cols {
		idx := mapping[col.Name.L]
		reorderedColumns[idx] = col
	}

	e.InsertColumns = reorderedColumns

	return nil
}

// GetFieldCount get field count.
func (e *LoadDataController) GetFieldCount() int {
	return len(e.FieldMappings)
}

// GenerateCSVConfig generates a CSV config for parser from LoadDataWorker.
func (e *LoadDataController) GenerateCSVConfig() *config.CSVConfig {
	csvConfig := &config.CSVConfig{
		FieldsTerminatedBy: e.FieldsTerminatedBy,
		// ignore optionally enclosed
		FieldsEnclosedBy:   e.FieldsEnclosedBy,
		LinesTerminatedBy:  e.LinesTerminatedBy,
		NotNull:            false,
		FieldNullDefinedBy: e.FieldNullDef,
		Header:             false,
		TrimLastEmptyField: false,
		FieldsEscapedBy:    e.FieldsEscapedBy,
		LinesStartingBy:    e.LinesStartingBy,
	}
	if !e.InImportInto {
		// for load data
		csvConfig.AllowEmptyLine = true
		csvConfig.QuotedNullIsText = !e.NullValueOptEnclosed
		csvConfig.UnescapedQuote = true
	}
	return csvConfig
}

// InitDataStore initializes the data store.
func (e *LoadDataController) InitDataStore(ctx context.Context) error {
	u, err2 := objstore.ParseRawURL(e.Path)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			err2.Error())
	}

	if objstore.IsLocal(u) {
		u.Path = filepath.Dir(e.Path)
	} else {
		u.Path = ""
	}
	s, err := initExternalStore(ctx, u, plannercore.ImportIntoDataSource)
	if err != nil {
		return err
	}
	e.dataStore = s

	if e.IsGlobalSort() {
		store, err3 := GetSortStore(ctx, e.Plan.CloudStorageURI)
		if err3 != nil {
			return err3
		}
		e.globalSortStore = store
	}
	return nil
}

// Close closes all the resources.
func (e *LoadDataController) Close() {
	if e.dataStore != nil {
		e.dataStore.Close()
	}
	if e.globalSortStore != nil {
		e.globalSortStore.Close()
	}
}

// GetSortStore gets the sort store.
func GetSortStore(ctx context.Context, url string) (storeapi.Storage, error) {
	u, err := objstore.ParseRawURL(url)
	target := "cloud storage"
	if err != nil {
		return nil, exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, err.Error())
	}
	return initExternalStore(ctx, u, target)
}

func initExternalStore(ctx context.Context, u *url.URL, target string) (storeapi.Storage, error) {
	b, err2 := objstore.ParseBackendFromURL(u, nil)
	if err2 != nil {
		return nil, exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, errors.GetErrStackMsg(err2))
	}

	s, err := objstore.NewWithDefaultOpt(ctx, b)
	if err != nil {
		return nil, exeerrors.ErrLoadDataCantAccess.GenWithStackByArgs(target, errors.GetErrStackMsg(err))
	}
	return s, nil
}

func estimateCompressionRatio(
	ctx context.Context,
	filePath string,
	fileSize int64,
	tp mydump.SourceType,
	store storeapi.Storage,
) (float64, error) {
	if tp != mydump.SourceTypeParquet {
		return 1.0, nil
	}
	failpoint.Inject("skipEstimateCompressionForParquet", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(2.0, nil)
		}
	})
	rows, rowSize, err := mydump.SampleStatisticsFromParquet(ctx, filePath, store)
	if err != nil {
		return 1.0, err
	}
	// No row in the file, use 2.0 as default compression ratio.
	if rowSize == 0 || rows == 0 {
		return 2.0, nil
	}

	compressionRatio := (rowSize * float64(rows)) / float64(fileSize)
	return compressionRatio, nil
}

// maxSampledCompressedFiles indicates the max number of files we used to sample
// compression ratio for each compression type. Consider the extreme case that
// user data contains all 3 compression types. Then we need to sample about 1,500
// files. Suppose each file costs 0.5 second (for example, cross region access),
// we still can finish in one minute with 16 concurrency.
const maxSampledCompressedFiles = 512

// compressionEstimator estimates compression ratio for different compression types.
// It uses harmonic mean to get the average compression ratio.
type compressionEstimator struct {
	mu      sync.Mutex
	records map[mydump.Compression][]float64
	ratio   sync.Map
}

func newCompressionRecorder() *compressionEstimator {
	return &compressionEstimator{
		records: make(map[mydump.Compression][]float64),
	}
}

func getHarmonicMean(rs []float64) float64 {
	if len(rs) == 0 {
		return 1.0
	}
	var (
		sumInverse float64
		count      int
	)
	for _, r := range rs {
		if r > 0 {
			sumInverse += 1.0 / r
			count++
		}
	}

	if count == 0 {
		return 1.0
	}
	return float64(count) / sumInverse
}

func (r *compressionEstimator) estimate(
	ctx context.Context,
	fileMeta mydump.SourceFileMeta,
	store storeapi.Storage,
) float64 {
	compressTp := mydump.ParseCompressionOnFileExtension(fileMeta.Path)
	if compressTp == mydump.CompressionNone {
		return 1.0
	}
	if v, ok := r.ratio.Load(compressTp); ok {
		return v.(float64)
	}

	compressRatio, err := mydump.SampleFileCompressRatio(ctx, fileMeta, store)
	if err != nil {
		logutil.Logger(ctx).Error("fail to calculate data file compress ratio",
			zap.String("category", "loader"),
			zap.String("path", fileMeta.Path),
			zap.Stringer("type", fileMeta.Type), zap.Error(err),
		)
		return 1.0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.ratio.Load(compressTp); ok {
		return compressRatio
	}

	if r.records[compressTp] == nil {
		r.records[compressTp] = make([]float64, 0, 256)
	}
	if len(r.records[compressTp]) < maxSampledCompressedFiles {
		r.records[compressTp] = append(r.records[compressTp], compressRatio)
	}
	if len(r.records[compressTp]) >= maxSampledCompressedFiles {
		// Using harmonic mean can better handle outlier values.
		compressRatio = getHarmonicMean(r.records[compressTp])
		r.ratio.Store(compressTp, compressRatio)
	}
	return compressRatio
}

// InitDataFiles initializes the data store and files.
// it will call InitDataStore internally.
func (e *LoadDataController) InitDataFiles(ctx context.Context) error {
	u, err2 := objstore.ParseRawURL(e.Path)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			err2.Error())
	}

	var fileNameKey string
	if objstore.IsLocal(u) {
		// LOAD DATA don't support server file.
		if !e.InImportInto {
			return exeerrors.ErrLoadDataFromServerDisk.GenWithStackByArgs(e.Path)
		}

		if !filepath.IsAbs(e.Path) {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
				"file location should be absolute path when import from server disk")
		}
		// we add this check for security, we don't want user import any sensitive system files,
		// most of which is readable text file and don't have a suffix, such as /etc/passwd
		if !slices.Contains(supportedSuffixForServerDisk, strings.ToLower(filepath.Ext(e.Path))) {
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
				"the file suffix is not supported when import from server disk")
		}
		dir := filepath.Dir(e.Path)
		_, err := os.Stat(dir)
		if err != nil {
			// permission denied / file not exist error, etc.
			return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
				err.Error())
		}

		fileNameKey = filepath.Base(e.Path)
	} else {
		fileNameKey = strings.Trim(u.Path, "/")
	}
	// try to find pattern error in advance
	_, err2 = filepath.Match(stringutil.EscapeGlobQuestionMark(fileNameKey), "")
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			"Glob pattern error: "+err2.Error())
	}

	if err2 = e.InitDataStore(ctx); err2 != nil {
		return err2
	}

	s := e.dataStore
	var (
		sourceType mydump.SourceType
		// sizeExpansionRatio is the estimated size expansion for parquet format.
		// For non-parquet format, it's always 1.0.
		sizeExpansionRatio = 1.0
	)
	dataFiles := []*mydump.SourceFileMeta{}
	isAutoDetectingFormat := e.Format == DataFormatAuto
	// check glob pattern is present in filename.
	idx := strings.IndexAny(fileNameKey, "*[")
	// simple path when the path represent one file
	if idx == -1 {
		fileReader, err2 := s.Open(ctx, fileNameKey, nil)
		if err2 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err2), "Please check the file location is correct")
		}
		defer func() {
			terror.Log(fileReader.Close())
		}()
		size, err3 := fileReader.Seek(0, io.SeekEnd)
		if err3 != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err3), "failed to read file size by seek")
		}
		e.detectAndUpdateFormat(fileNameKey)
		sourceType = e.getSourceType()
		compressionRatio, err := estimateCompressionRatio(ctx, fileNameKey, size, sourceType, s)
		if err != nil {
			return errors.Trace(err)
		}
		compressTp := mydump.ParseCompressionOnFileExtension(fileNameKey)
		fileMeta := mydump.SourceFileMeta{
			Path:        fileNameKey,
			FileSize:    size,
			Compression: compressTp,
			Type:        sourceType,
		}
		fileMeta.RealSize = mydump.EstimateRealSizeForFile(ctx, fileMeta, s)
		fileMeta.RealSize = int64(float64(fileMeta.RealSize) * compressionRatio)
		dataFiles = append(dataFiles, &fileMeta)
	} else {
		var commonPrefix string
		if !objstore.IsLocal(u) {
			// for local directory, we're walking the parent directory,
			// so we don't have a common prefix as cloud storage do.
			commonPrefix = fileNameKey[:idx]
		}
		// when import from server disk, all entries in parent directory should have READ
		// access, else walkDir will fail
		// we only support '*', in order to reuse glob library manually escape the path
		escapedPath := stringutil.EscapeGlobQuestionMark(fileNameKey)

		allFiles := make([]mydump.RawFile, 0, 16)
		if err := s.WalkDir(ctx, &storeapi.WalkOption{ObjPrefix: commonPrefix, SkipSubDir: true},
			func(remotePath string, size int64) error {
				allFiles = append(allFiles, mydump.RawFile{Path: remotePath, Size: size})
				return nil
			}); err != nil {
			return exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err), "failed to walk dir")
		}

		var err error
		var processedFiles []*mydump.SourceFileMeta
		var once sync.Once

		ce := newCompressionRecorder()

		if processedFiles, err = mydump.ParallelProcess(ctx, allFiles, e.ThreadCnt*2,
			func(ctx context.Context, f mydump.RawFile) (*mydump.SourceFileMeta, error) {
				// we have checked in LoadDataExec.Next
				//nolint: errcheck
				match, _ := filepath.Match(escapedPath, f.Path)
				if !match {
					return nil, nil
				}
				path, size := f.Path, f.Size
				// pick arbitrary one file to detect the format.
				var err2 error
				once.Do(func() {
					e.detectAndUpdateFormat(path)
					sourceType = e.getSourceType()
					sizeExpansionRatio, err2 = estimateCompressionRatio(ctx, path, size, sourceType, s)
				})
				if err2 != nil {
					return nil, err2
				}
				compressTp := mydump.ParseCompressionOnFileExtension(path)
				fileMeta := mydump.SourceFileMeta{
					Path:        path,
					FileSize:    size,
					Compression: compressTp,
					Type:        sourceType,
				}
				fileMeta.RealSize = int64(ce.estimate(ctx, fileMeta, s) * float64(fileMeta.FileSize))
				fileMeta.RealSize = int64(float64(fileMeta.RealSize) * sizeExpansionRatio)
				return &fileMeta, nil
			}); err != nil {
			return err
		}
		// filter unmatch files
		for _, f := range processedFiles {
			if f != nil {
				dataFiles = append(dataFiles, f)
			}
		}
	}
	if e.InImportInto && isAutoDetectingFormat && e.Format != DataFormatCSV {
		if err2 = e.checkNonCSVFormatOptions(); err2 != nil {
			return err2
		}
	}
	var totalSize, totalRealSize int64
	for _, dfile := range dataFiles {
		totalSize += dfile.FileSize
		realSize := dfile.RealSize
		failpoint.Inject("amplifyRealSize", func(val failpoint.Value) {
			factor := int64(val.(int))
			realSize *= factor
		})
		totalRealSize += realSize
	}

	e.dataFiles = dataFiles
	e.TotalFileSize = totalSize
	e.TotalRealSize = totalRealSize

	return nil
}

// CalResourceParams calculates resource related parameters according to the total
// file size and target node cpu count.
func (e *LoadDataController) CalResourceParams(ctx context.Context, ksCodec []byte) error {
	start := time.Now()
	targetNodeCPUCnt, err := handle.GetCPUCountOfNode(ctx)
	if err != nil {
		return err
	}
	factors, err := handle.GetScheduleTuneFactors(ctx, e.Keyspace)
	if err != nil {
		return err
	}
	totalSize := e.TotalRealSize
	numOfIndexGenKV := GetNumOfIndexGenKV(e.TableInfo)
	var indexSizeRatio float64
	if numOfIndexGenKV > 0 {
		indexSizeRatio, err = e.sampleIndexSizeRatio(ctx, ksCodec)
		if err != nil {
			e.logger.Warn("meet error when sampling index size ratio", zap.Error(err))
		}
	}
	cal := scheduler.NewRCCalc(totalSize, targetNodeCPUCnt, indexSizeRatio, factors)
	e.ThreadCnt = cal.CalcRequiredSlots()
	e.MaxNodeCnt = cal.CalcMaxNodeCountForImportInto()
	e.DistSQLScanConcurrency = scheduler.CalcDistSQLConcurrency(e.ThreadCnt, e.MaxNodeCnt, targetNodeCPUCnt)
	e.logger.Info("auto calculate resource related params",
		zap.Int("thread", e.ThreadCnt),
		zap.Int("maxNode", e.MaxNodeCnt),
		zap.Int("distsqlScanConcurrency", e.DistSQLScanConcurrency),
		zap.Int("targetNodeCPU", targetNodeCPUCnt),
		zap.String("totalFileSize", units.BytesSize(float64(e.TotalFileSize))),
		zap.String("totalRealSize", units.BytesSize(float64(totalSize))),
		zap.Int("fileCount", len(e.dataFiles)),
		zap.Int("numOfIndexGenKV", numOfIndexGenKV),
		zap.Float64("indexSizeRatio", indexSizeRatio),
		zap.Float64("amplifyFactor", factors.AmplifyFactor),
		zap.Duration("costTime", time.Since(start)),
	)
	return nil
}

// update format of the validated file by its extension.
func (e *LoadDataController) detectAndUpdateFormat(path string) {
	if e.Format == DataFormatAuto {
		e.Format = parseFileType(path)
		e.logger.Info("detect and update import plan format based on file extension",
			zap.String("file", path), zap.String("detected format", e.Format))
		e.Parameters.Format = e.Format
	}
}

func parseFileType(path string) string {
	path = strings.ToLower(path)
	ext := filepath.Ext(path)
	// avoid duplicate compress extension
	if ext == ".gz" || ext == ".gzip" || ext == ".zstd" || ext == ".zst" || ext == ".snappy" {
		path = strings.TrimSuffix(path, ext)
		ext = filepath.Ext(path)
	}
	switch ext {
	case ".sql":
		return DataFormatSQL
	case ".parquet":
		return DataFormatParquet
	default:
		// if file do not contain file extension, use ".csv" as default format
		return DataFormatCSV
	}
}

func (e *LoadDataController) getSourceType() mydump.SourceType {
	switch e.Format {
	case DataFormatParquet:
		return mydump.SourceTypeParquet
	case DataFormatDelimitedData, DataFormatCSV:
		return mydump.SourceTypeCSV
	default:
		// DataFormatSQL
		return mydump.SourceTypeSQL
	}
}

// GetLoadDataReaderInfos returns the LoadDataReaderInfo for each data file.
func (e *LoadDataController) GetLoadDataReaderInfos() []LoadDataReaderInfo {
	result := make([]LoadDataReaderInfo, 0, len(e.dataFiles))
	for i := range e.dataFiles {
		f := e.dataFiles[i]
		result = append(result, LoadDataReaderInfo{
			Opener: func(ctx context.Context) (io.ReadSeekCloser, error) {
				fileReader, err2 := mydump.OpenReader(ctx, f, e.dataStore, compressedio.DecompressConfig{})
				if err2 != nil {
					return nil, exeerrors.ErrLoadDataCantRead.GenWithStackByArgs(errors.GetErrStackMsg(err2), "Please check the INFILE path is correct")
				}
				return fileReader, nil
			},
			Remote: f,
		})
	}
	return result
}

// GetParser returns a parser for the data file.
func (e *LoadDataController) GetParser(
	ctx context.Context,
	dataFileInfo LoadDataReaderInfo,
) (parser mydump.Parser, err error) {
	reader, err2 := dataFileInfo.Opener(ctx)
	if err2 != nil {
		return nil, err2
	}
	defer func() {
		if err != nil {
			if err3 := reader.Close(); err3 != nil {
				e.logger.Warn("failed to close reader", zap.Error(err3))
			}
		}
	}()
	switch e.Format {
	case DataFormatDelimitedData, DataFormatCSV:
		var charsetConvertor *mydump.CharsetConvertor
		if e.Charset != nil {
			charsetConvertor, err = mydump.NewCharsetConvertor(*e.Charset, string(utf8.RuneError))
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		parser, err = mydump.NewCSVParser(
			ctx,
			e.GenerateCSVConfig(),
			reader,
			LoadDataReadBlockSize,
			nil,
			false,
			charsetConvertor)
	case DataFormatSQL:
		parser = mydump.NewChunkParser(
			ctx,
			e.SQLMode,
			reader,
			LoadDataReadBlockSize,
			nil,
		)
	case DataFormatParquet:
		parser, err = mydump.NewParquetParser(
			ctx,
			e.dataStore,
			reader,
			dataFileInfo.Remote.Path,
			dataFileInfo.Remote.ParquetMeta,
		)
	}
	if err != nil {
		return nil, exeerrors.ErrLoadDataWrongFormatConfig.GenWithStack(err.Error())
	}
	parser.SetLogger(litlog.Logger{Logger: logutil.Logger(ctx)})

	return parser, nil
}

// HandleSkipNRows skips the first N rows of the data file.
func (e *LoadDataController) HandleSkipNRows(parser mydump.Parser) error {
	// handle IGNORE N LINES
	ignoreOneLineFn := parser.ReadRow
	if csvParser, ok := parser.(*mydump.CSVParser); ok {
		ignoreOneLineFn = func() error {
			_, _, err3 := csvParser.ReadUntilTerminator()
			return err3
		}
	}

	ignoreLineCnt := e.IgnoreLines
	for ignoreLineCnt > 0 {
		err := ignoreOneLineFn()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}
			return err
		}

		ignoreLineCnt--
	}
	return nil
}

func (e *LoadDataController) toMyDumpFiles() []mydump.FileInfo {
	tbl := filter.Table{
		Schema: e.DBName,
		Name:   e.Table.Meta().Name.O,
	}
	res := []mydump.FileInfo{}
	for _, f := range e.dataFiles {
		res = append(res, mydump.FileInfo{
			TableName: tbl,
			FileMeta:  *f,
		})
	}
	return res
}

// IsLocalSort returns true if we sort data on local disk.
func (p *Plan) IsLocalSort() bool {
	return p.CloudStorageURI == ""
}

// IsGlobalSort returns true if we sort data on global storage.
func (p *Plan) IsGlobalSort() bool {
	return !p.IsLocalSort()
}

// non CSV format should not specify CSV only options, we check it again if the
// format is detected automatically.
func (p *Plan) checkNonCSVFormatOptions() error {
	for k := range csvOnlyOptions {
		if _, ok := p.specifiedOptions[k]; ok {
			return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "non-CSV format")
		}
	}
	return nil
}

// CreateColAssignExprs creates the column assignment expressions using session context.
// RewriteAstExpr will write ast node in place(due to xxNode.Accept), but it doesn't change node content,
// so we sync it.
func (e *LoadDataController) CreateColAssignExprs(planCtx planctx.PlanContext) (
	_ []expression.Expression,
	_ []contextutil.SQLWarn,
	retErr error,
) {
	e.colAssignMu.Lock()
	defer e.colAssignMu.Unlock()
	res := make([]expression.Expression, 0, len(e.ColumnAssignments))
	allWarnings := []contextutil.SQLWarn{}
	for _, assign := range e.ColumnAssignments {
		newExpr, err := plannerutil.RewriteAstExprWithPlanCtx(planCtx, assign.Expr, nil, nil, false)
		// col assign expr warnings is static, we should generate it for each row processed.
		// so we save it and clear it here.
		allWarnings = append(allWarnings, planCtx.GetSessionVars().StmtCtx.GetWarnings()...)
		planCtx.GetSessionVars().StmtCtx.SetWarnings(nil)
		if err != nil {
			return nil, nil, err
		}
		res = append(res, newExpr)
	}
	return res, allWarnings, nil
}

// CreateColAssignSimpleExprs creates the column assignment expressions using `expression.BuildContext`.
// This method does not support:
//   - Subquery
//   - System Variables (e.g. `@@tidb_enable_async_commit`)
//   - Window functions
//   - Aggregate functions
//   - Other special functions used in some specified queries such as `GROUPING`, `VALUES` ...
func (e *LoadDataController) CreateColAssignSimpleExprs(ctx expression.BuildContext) (_ []expression.Expression, _ []contextutil.SQLWarn, retErr error) {
	e.colAssignMu.Lock()
	defer e.colAssignMu.Unlock()
	res := make([]expression.Expression, 0, len(e.ColumnAssignments))
	var allWarnings []contextutil.SQLWarn
	for _, assign := range e.ColumnAssignments {
		newExpr, err := expression.BuildSimpleExpr(ctx, assign.Expr)
		// col assign expr warnings is static, we should generate it for each row processed.
		// so we save it and clear it here.
		if ctx.GetEvalCtx().WarningCount() > 0 {
			allWarnings = append(allWarnings, ctx.GetEvalCtx().TruncateWarnings(0)...)
		}
		if err != nil {
			return nil, nil, err
		}
		res = append(res, newExpr)
	}
	return res, allWarnings, nil
}

func (e *LoadDataController) getLocalBackendCfg(keyspace, pdAddr, dataDir string) local.BackendConfig {
	backendConfig := local.BackendConfig{
		PDAddr:                 pdAddr,
		LocalStoreDir:          dataDir,
		MaxConnPerStore:        config.DefaultRangeConcurrency,
		ConnCompressType:       config.CompressionNone,
		WorkerConcurrency:      *atomic.NewInt32(int32(e.ThreadCnt)),
		KVWriteBatchSize:       config.KVWriteBatchSize,
		RegionSplitBatchSize:   config.DefaultRegionSplitBatchSize,
		RegionSplitConcurrency: runtime.GOMAXPROCS(0),
		// enable after we support checkpoint
		CheckpointEnabled:           false,
		MemTableSize:                config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize:     int64(config.DefaultLocalWriterMemCacheSize),
		ShouldCheckTiKV:             true,
		DupeDetectEnabled:           false,
		DuplicateDetectOpt:          common.DupDetectOpt{ReportErrOnDup: false},
		TiKVWorkerURL:               tidb.GetGlobalConfig().TiKVWorkerURL,
		StoreWriteBWLimit:           int(e.MaxWriteSpeed),
		MaxOpenFiles:                int(tidbutil.GenRLimit("table_import")),
		KeyspaceName:                keyspace,
		PausePDSchedulerScope:       config.PausePDSchedulerScopeTable,
		DisableAutomaticCompactions: true,
		BlockSize:                   config.DefaultBlockSize,
	}
	if e.IsRaftKV2 {
		backendConfig.RaftKV2SwitchModeDuration = config.DefaultSwitchTiKVModeInterval
	}
	return backendConfig
}

// FullTableName return FQDN of the table.
func (e *LoadDataController) FullTableName() string {
	return common.UniqueTable(e.DBName, e.Table.Meta().Name.O)
}

func getDataSourceType(p *plannercore.ImportInto) DataSourceType {
	if p.SelectPlan != nil {
		return DataSourceTypeQuery
	}
	return DataSourceTypeFile
}

// GetTargetNodeCPUCnt get cpu count of target node where the import into job will be executed.
// target node is current node if it's server-disk import, import from query or disttask is disabled,
// else it's the node managed by disttask.
// exported for testing.
func GetTargetNodeCPUCnt(ctx context.Context, sourceType DataSourceType, path string) (int, error) {
	if sourceType == DataSourceTypeQuery {
		return cpu.GetCPUCount(), nil
	}

	u, err2 := objstore.ParseRawURL(path)
	if err2 != nil {
		return 0, exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(plannercore.ImportIntoDataSource,
			err2.Error())
	}

	serverDiskImport := objstore.IsLocal(u)
	if serverDiskImport || !vardef.EnableDistTask.Load() {
		return cpu.GetCPUCount(), nil
	}
	return handle.GetCPUCountOfNode(ctx)
}
