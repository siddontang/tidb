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

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

func createInfoSchemaTable(_ autoid.Allocators, _ func() (pools.Resource, error), meta *model.TableInfo) (table.Table, error) {
	columns := make([]*table.Column, len(meta.Columns))
	for i, col := range meta.Columns {
		columns[i] = table.ToColumn(col)
	}
	tp := table.VirtualTable
	if IsClusterTableByName(metadef.InformationSchemaName.L, meta.Name.L) {
		tp = table.ClusterTable
	}
	return &infoschemaTable{meta: meta, cols: columns, tp: tp}, nil
}

type infoschemaTable struct {
	meta *model.TableInfo
	cols []*table.Column
	tp   table.Type
}

// IterRecords implements table.Table IterRecords interface.
func (*infoschemaTable) IterRecords(ctx context.Context, sctx sessionctx.Context, cols []*table.Column, fn table.RecordIterFunc) error {
	return nil
}

// Cols implements table.Table Cols interface.
func (it *infoschemaTable) Cols() []*table.Column {
	return it.cols
}

// VisibleCols implements table.Table VisibleCols interface.
func (it *infoschemaTable) VisibleCols() []*table.Column {
	return it.cols
}

// HiddenCols implements table.Table HiddenCols interface.
func (it *infoschemaTable) HiddenCols() []*table.Column {
	return nil
}

// WritableCols implements table.Table WritableCols interface.
func (it *infoschemaTable) WritableCols() []*table.Column {
	return it.cols
}

// DeletableCols implements table.Table WritableCols interface.
func (it *infoschemaTable) DeletableCols() []*table.Column {
	return it.cols
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (it *infoschemaTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return it.cols
}

// Indices implements table.Table Indices interface.
func (it *infoschemaTable) Indices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (it *infoschemaTable) DeletableIndices() []table.Index {
	return nil
}

// WritableConstraint implements table.Table WritableConstraint interface.
func (it *infoschemaTable) WritableConstraint() []*table.Constraint {
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (it *infoschemaTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (it *infoschemaTable) IndexPrefix() kv.Key {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (it *infoschemaTable) AddRecord(ctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return nil, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (it *infoschemaTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (it *infoschemaTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	return table.ErrUnsupportedOp
}

// Allocators implements table.Table Allocators interface.
func (it *infoschemaTable) Allocators(_ table.AllocatorContext) autoid.Allocators {
	return autoid.Allocators{}
}

// Meta implements table.Table Meta interface.
func (it *infoschemaTable) Meta() *model.TableInfo {
	return it.meta
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (it *infoschemaTable) GetPhysicalID() int64 {
	return it.meta.ID
}

// Type implements table.Table Type interface.
func (it *infoschemaTable) Type() table.Type {
	return it.tp
}

// GetPartitionedTable implements table.Table GetPartitionedTable interface.
func (it *infoschemaTable) GetPartitionedTable() table.PartitionedTable {
	return nil
}

// VirtualTable is a dummy table.Table implementation.
type VirtualTable struct{}

// Cols implements table.Table Cols interface.
func (vt *VirtualTable) Cols() []*table.Column {
	return nil
}

// VisibleCols implements table.Table VisibleCols interface.
func (vt *VirtualTable) VisibleCols() []*table.Column {
	return nil
}

// HiddenCols implements table.Table HiddenCols interface.
func (vt *VirtualTable) HiddenCols() []*table.Column {
	return nil
}

// WritableCols implements table.Table WritableCols interface.
func (vt *VirtualTable) WritableCols() []*table.Column {
	return nil
}

// DeletableCols implements table.Table WritableCols interface.
func (vt *VirtualTable) DeletableCols() []*table.Column {
	return nil
}

// FullHiddenColsAndVisibleCols implements table FullHiddenColsAndVisibleCols interface.
func (vt *VirtualTable) FullHiddenColsAndVisibleCols() []*table.Column {
	return nil
}

// Indices implements table.Table Indices interface.
func (vt *VirtualTable) Indices() []table.Index {
	return nil
}

// DeletableIndices implements table.Table DeletableIndices interface.
func (vt *VirtualTable) DeletableIndices() []table.Index {
	return nil
}

// WritableConstraint implements table.Table WritableConstraint interface.
func (vt *VirtualTable) WritableConstraint() []*table.Constraint {
	return nil
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (vt *VirtualTable) RecordPrefix() kv.Key {
	return nil
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (vt *VirtualTable) IndexPrefix() kv.Key {
	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (vt *VirtualTable) AddRecord(ctx table.MutateContext, txn kv.Transaction, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	return nil, table.ErrUnsupportedOp
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (vt *VirtualTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	return table.ErrUnsupportedOp
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (vt *VirtualTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction, h kv.Handle, oldData, newData []types.Datum, touched []bool, opts ...table.UpdateRecordOption) error {
	return table.ErrUnsupportedOp
}

// Allocators implements table.Table Allocators interface.
func (vt *VirtualTable) Allocators(_ table.AllocatorContext) autoid.Allocators {
	return autoid.Allocators{}
}

// Meta implements table.Table Meta interface.
func (vt *VirtualTable) Meta() *model.TableInfo {
	return nil
}

// GetPhysicalID implements table.Table GetPhysicalID interface.
func (vt *VirtualTable) GetPhysicalID() int64 {
	return 0
}

// Type implements table.Table Type interface.
func (vt *VirtualTable) Type() table.Type {
	return table.VirtualTable
}
