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
use datafusion_ext_commons::{downcast_any, scalar_value::compacted_scalar_value_from_array};

use crate::{
    agg::{
        Agg,
        acc::{
            AccBooleanColumn, AccBytes, AccBytesColumn, AccColumn, AccColumnRef, AccPrimColumn,
            AccScalarValueColumn, acc_generic_column_to_array, create_acc_generic_column,
        },
        agg::IdxSelection,
    },
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
            values: create_acc_generic_column(&self.data_type, num_rows),
            flags: AccBooleanColumn::new(num_rows),
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
        let accs = downcast_any!(accs, mut AccFirstColumn)?;
        accs.ensure_size(acc_idx);

        let (value_accs, flag_accs) = accs.inner_mut();

        macro_rules! handle_bytes {
            ($array:expr) => {{
                let value_accs = downcast_any!(value_accs, mut AccBytesColumn)?;
                let partial_arg = $array;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if flag_accs.value(acc_idx).is_none() {
                            if partial_arg.is_valid(partial_arg_idx) {
                                value_accs.set_value(acc_idx, Some(AccBytes::from(partial_arg.value(partial_arg_idx).as_ref())));
                                flag_accs.set_value(acc_idx, Some(true));
                            } else {
                                value_accs.set_value(acc_idx, None);
                                flag_accs.set_value(acc_idx, Some(true));
                            }
                        }
                    }
                }
            }}
        }

        downcast_primitive_array! {
            partial_arg => {
                let value_accs = downcast_any!(value_accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if flag_accs.value(acc_idx).is_none() {
                            if partial_arg.is_valid(partial_arg_idx) {
                                value_accs.set_value(acc_idx, Some(partial_arg.value(partial_arg_idx)));
                                flag_accs.set_value(acc_idx, Some(true));
                            } else {
                                value_accs.set_value(acc_idx, None);
                                flag_accs.set_value(acc_idx, Some(true));
                            }
                        }
                    }
                }
            }
            DataType::Boolean => {
                let value_accs = downcast_any!(value_accs, mut AccBooleanColumn)?;
                let partial_arg = downcast_any!(partial_arg, BooleanArray)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if flag_accs.value(acc_idx).is_none() {
                            if partial_arg.is_valid(partial_arg_idx) {
                                value_accs.set_value(acc_idx, Some(partial_arg.value(partial_arg_idx)));
                                flag_accs.set_value(acc_idx, Some(true));
                            } else {
                                value_accs.set_value(acc_idx, None);
                                flag_accs.set_value(acc_idx, Some(true));
                            }
                        }
                    }
                }
            }
            DataType::Utf8 => handle_bytes!(downcast_any!(partial_arg, StringArray)?),
            DataType::Binary => handle_bytes!(downcast_any!(partial_arg, BinaryArray)?),
            _other => {
                let value_accs = downcast_any!(value_accs, mut AccScalarValueColumn)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if flag_accs.value(acc_idx).is_none() {
                            if partial_arg.is_valid(partial_arg_idx) {
                                value_accs.set_value(acc_idx, compacted_scalar_value_from_array(partial_arg, partial_arg_idx)?);
                                flag_accs.set_value(acc_idx, Some(true));
                            } else {
                                value_accs.set_value(acc_idx, ScalarValue::Null);
                                flag_accs.set_value(acc_idx, Some(true));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccFirstColumn)?;
        let merging_accs = downcast_any!(merging_accs, mut AccFirstColumn)?;
        accs.ensure_size(acc_idx);

        let (value_accs, flag_accs) = accs.inner_mut();
        let (merging_value_accs, merging_flag_accs) = merging_accs.inner_mut();

        macro_rules! handle_primitive {
            ($ty:ty) => {{
                type TNative = <$ty as ArrowPrimitiveType>::Native;
                let value_accs = downcast_any!(value_accs, mut AccPrimColumn<TNative>)?;
                let merging_value_accs = downcast_any!(merging_value_accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if flag_accs.value(acc_idx).is_none()
                            && merging_flag_accs.value(merging_acc_idx).is_some()
                        {
                            value_accs.set_value(acc_idx, merging_value_accs.value(merging_acc_idx));
                            flag_accs.set_value(acc_idx, Some(true));
                        }
                    }
                }
            }}
        }

        macro_rules! handle_boolean {
            () => {{
                let value_accs = downcast_any!(value_accs, mut AccBooleanColumn)?;
                let merging_value_accs = downcast_any!(merging_value_accs, mut AccBooleanColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if flag_accs.value(acc_idx).is_none()
                            && merging_flag_accs.value(merging_acc_idx).is_some()
                        {
                            value_accs.set_value(acc_idx, merging_value_accs.value(merging_acc_idx));
                            flag_accs.set_value(acc_idx, Some(true));
                        }
                    }
                }
            }}
        }

        macro_rules! handle_bytes {
            () => {{
                let value_accs = downcast_any!(value_accs, mut AccBytesColumn)?;
                let merging_value_accs = downcast_any!(merging_value_accs, mut AccBytesColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if flag_accs.value(acc_idx).is_none()
                            && merging_flag_accs.value(merging_acc_idx).is_some()
                        {
                            value_accs.set_value(acc_idx, merging_value_accs.take_value(merging_acc_idx));
                            flag_accs.set_value(acc_idx, Some(true));
                        }
                    }
                }
            }}
        }

        downcast_primitive! {
            (&self.data_type) => (handle_primitive),
            DataType::Boolean => handle_boolean!(),
            DataType::Utf8 | DataType::Binary => handle_bytes!(),
            DataType::Null => {}
            _ => {
                let value_accs = downcast_any!(value_accs, mut AccScalarValueColumn)?;
                let merging_value_accs = downcast_any!(merging_value_accs, mut AccScalarValueColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if flag_accs.value(acc_idx).is_none()
                            && merging_flag_accs.value(merging_acc_idx).is_some()
                        {
                            value_accs.set_value(acc_idx, merging_value_accs.take_value(merging_acc_idx));
                            flag_accs.set_value(acc_idx, Some(true));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccFirstColumn)?;
        acc_generic_column_to_array(&mut accs.values, &self.data_type, acc_idx)
    }
}

struct AccFirstColumn {
    values: AccColumnRef,
    flags: AccBooleanColumn,
}

impl AccFirstColumn {
    fn inner_mut(&mut self) -> (&mut AccColumnRef, &mut AccBooleanColumn) {
        let values = &mut self.values as *mut AccColumnRef;
        let flags = &mut self.flags as *mut AccBooleanColumn;
        unsafe { (&mut *values, &mut *flags) } // safety: bypass borrow checker
    }
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
