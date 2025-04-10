// Copyright 2022 The Blaze Authors
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

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    io::Cursor,
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::downcast_any;

use crate::{
    agg::{
        acc::{AccBytes, AccColumn, AccColumnRef, AccGenericColumn},
        agg::IdxSelection,
        Agg,
    },
    common::SliceAsRawBytes,
    idx_for_zipped,
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub struct AggFirst {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl AggFirst {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        Ok(Self { child, data_type })
    }
}

impl Debug for AggFirst {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "First({:?})", self.child)
    }
}

impl Agg for AggFirst {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs[0].clone(),
            self.data_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        Box::new(AccFirstColumn {
            values: AccGenericColumn::new(&self.data_type, num_rows),
            flags: AccGenericColumn::new(&DataType::Boolean, num_rows),
        })
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let partial_arg = &partial_args[0];
        let accs = downcast_any!(accs, mut AccFirstColumn).unwrap();
        accs.ensure_size(acc_idx);

        let old_heap_mem_used = accs.values.items_heap_mem_used(acc_idx);

        macro_rules! handle_bytes {
            ($ty:ident) => {{
                type TArray = paste::paste! {[<$ty Array>]};
                let partial_arg = downcast_any!(partial_arg, TArray).unwrap();
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if !accs.flags.prim_valid(acc_idx) {
                            accs.flags.set_prim_valid(acc_idx, true);
                            if partial_arg.is_valid(partial_arg_idx) {
                                accs.values.set_bytes_value(acc_idx, Some(AccBytes::from(partial_arg.value(partial_arg_idx).as_ref())));
                            } else {
                                accs.values.set_bytes_value(acc_idx, None);
                            }
                        }
                    }
                }
            }}
        }

        downcast_primitive_array! {
            partial_arg => {
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if !accs.flags.prim_valid(acc_idx) {
                            accs.flags.set_prim_valid(acc_idx, true);
                            accs.values.set_prim_valid(acc_idx, partial_arg.is_valid(partial_arg_idx));
                            accs.values.set_prim_value(acc_idx, partial_arg.value(partial_arg_idx));
                        }
                    }
                }
            }
            DataType::Utf8 => handle_bytes!(String),
            DataType::Binary => handle_bytes!(Binary),
            _other => {
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if accs.flags.prim_valid(acc_idx) {
                            accs.flags.set_prim_valid(acc_idx, true);
                            accs.values.scalar_values_mut()[acc_idx] = ScalarValue::try_from_array(partial_arg, partial_arg_idx)?;
                        }
                    }
                }
            }
        }

        let new_heap_mem_used = accs.values.items_heap_mem_used(acc_idx);
        accs.values
            .add_heap_mem_used(new_heap_mem_used - old_heap_mem_used);
        Ok(())
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccFirstColumn).unwrap();
        let merging_accs = downcast_any!(merging_accs, mut AccFirstColumn).unwrap();
        accs.ensure_size(acc_idx);

        let old_heap_mem_used = accs.values.items_heap_mem_used(acc_idx);

        // safety: bypass borrow checker
        let accs_values: &mut AccGenericColumn = unsafe { std::mem::transmute(&mut accs.values) };

        match (accs_values, &mut merging_accs.values) {
            (
                AccGenericColumn::Prim {
                    raw,
                    valids,
                    prim_size,
                },
                AccGenericColumn::Prim {
                    raw: other_raw,
                    valids: other_valids,
                    ..
                },
            ) => {
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if !accs.flags.prim_valid(acc_idx) && merging_accs.flags.prim_valid(merging_acc_idx) {
                            let acc_offset = *prim_size * acc_idx;
                            let merging_acc_offset = *prim_size * merging_acc_idx;
                            raw.as_raw_bytes_mut()[acc_offset..][..*prim_size]
                                .copy_from_slice(&other_raw.as_raw_bytes()[merging_acc_offset..][..*prim_size]);
                            valids.set(acc_idx, other_valids[merging_acc_idx]);
                            accs.flags.set_prim_valid(acc_idx, true);
                        }
                    }
                }
            }
            (
                AccGenericColumn::Bytes { items, .. },
                AccGenericColumn::Bytes {
                    items: other_items, ..
                },
            ) => {
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if !accs.flags.prim_valid(acc_idx) && merging_accs.flags.prim_valid(merging_acc_idx) {
                            let item = &mut items[acc_idx];
                            let mut other_item = &mut other_items[merging_acc_idx];
                            *item = std::mem::take(&mut other_item);
                            accs.flags.set_prim_valid(acc_idx, true);
                        }
                    }
                }
            }
            (
                AccGenericColumn::Scalar { items, .. },
                AccGenericColumn::Scalar {
                    items: other_items, ..
                },
            ) => {
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if !accs.flags.prim_valid(acc_idx) && merging_accs.flags.prim_valid(merging_acc_idx) {
                            let item = & mut items[acc_idx];
                            let mut other_item = & mut other_items[merging_acc_idx];
                            * item = std::mem::replace(& mut other_item, ScalarValue::Null);
                            accs.flags.set_prim_valid(acc_idx, true);
                        }
                    }
                }
            }
            _ => unreachable!(),
        }

        let new_heap_mem_used = accs.values.items_heap_mem_used(acc_idx);
        accs.values
            .add_heap_mem_used(new_heap_mem_used - old_heap_mem_used);
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccFirstColumn).unwrap();
        accs.values.to_array(acc_idx, &self.data_type)
    }
}

struct AccFirstColumn {
    values: AccGenericColumn,
    flags: AccGenericColumn,
}

impl AccColumn for AccFirstColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        self.values.resize(len);
        self.flags.resize(len);
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.flags.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.values.num_records()
    }

    fn mem_used(&self) -> usize {
        self.values.mem_used() + self.flags.mem_used()
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        self.values.freeze_to_rows(idx, array)?;
        self.flags.freeze_to_rows(idx, array)?;
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, cursors: &mut [Cursor<&[u8]>]) -> Result<()> {
        self.values.unfreeze_from_rows(cursors)?;
        self.flags.unfreeze_from_rows(cursors)?;
        Ok(())
    }

    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        self.values.spill(idx, w)?;
        self.flags.spill(idx, w)?;
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        self.values.unspill(num_rows, r)?;
        self.flags.unspill(num_rows, r)?;
        Ok(())
    }
}
