// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{downcast_any, scalar_value::compacted_scalar_value_from_array};

use crate::{
    agg::{
        Agg,
        acc::{
            AccBooleanColumn, AccBytes, AccBytesColumn, AccColumnRef, AccPrimColumn,
            AccScalarValueColumn, acc_generic_column_to_array, create_acc_generic_column,
        },
        agg::IdxSelection,
    },
    idx_for_zipped,
};

pub struct AggFirstIgnoresNull {
    child: PhysicalExprRef,
    data_type: DataType,
}

impl AggFirstIgnoresNull {
    pub fn try_new(child: PhysicalExprRef, data_type: DataType) -> Result<Self> {
        Ok(Self { child, data_type })
    }
}

impl Debug for AggFirstIgnoresNull {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FirstIgnoresNull({:?})", self.child)
    }
}

impl Agg for AggFirstIgnoresNull {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Agg>> {
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
        create_acc_generic_column(&self.data_type, num_rows)
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let partial_arg = &partial_args[0];
        accs.ensure_size(acc_idx);

        macro_rules! handle_bytes {
            ($array:expr) => {{
                let accs = downcast_any!(accs, mut AccBytesColumn)?;
                let partial_arg = $array;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if accs.value(acc_idx).is_none() && partial_arg.is_valid(partial_arg_idx) {
                            accs.set_value(acc_idx, Some(AccBytes::from(partial_arg.value(partial_arg_idx).as_ref())));
                        }
                    }
                }
            }}
        }

        downcast_primitive_array! {
            partial_arg => {
                if let Ok(accs) = downcast_any!(accs, mut AccPrimColumn<_>) {
                    idx_for_zipped! {
                        ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                            if accs.value(acc_idx).is_none() && partial_arg.is_valid(partial_arg_idx) {
                                accs.set_value(acc_idx, Some(partial_arg.value(partial_arg_idx)));
                            }
                        }
                    }
                }
            }
            DataType::Boolean => {
                let accs = downcast_any!(accs, mut AccBooleanColumn)?;
                let partial_arg = downcast_any!(partial_arg, BooleanArray)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if accs.value(acc_idx).is_none() && partial_arg.is_valid(partial_arg_idx) {
                            accs.set_value(acc_idx, Some(partial_arg.value(partial_arg_idx)));
                        }
                    }
                }
            }
            DataType::Utf8 => handle_bytes!(downcast_any!(partial_arg, StringArray)?),
            DataType::Binary => handle_bytes!(downcast_any!(partial_arg, BinaryArray)?),
            _other => {
                let accs = downcast_any!(accs, mut AccScalarValueColumn)?;
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if accs.value(acc_idx).is_null() && partial_arg.is_valid(partial_arg_idx) {
                            accs.set_value(acc_idx, compacted_scalar_value_from_array(partial_arg, partial_arg_idx)?);
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
        accs.ensure_size(acc_idx);

        // primitive types
        macro_rules! handle_primitive {
            ($ty:ty) => {{
                type TNative = <$ty as ArrowPrimitiveType>::Native;
                let accs = downcast_any!(accs, mut AccPrimColumn<TNative>)?;
                let merging_accs = downcast_any!(merging_accs, mut AccPrimColumn<_>)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if accs.value(acc_idx).is_none() && merging_accs.value(merging_acc_idx).is_some() {
                            accs.set_value(acc_idx, merging_accs.value(merging_acc_idx));
                        }
                    }
                }
            }}
        }

        macro_rules! handle_boolean {
            () => {{
                let accs = downcast_any!(accs, mut AccBooleanColumn)?;
                let merging_accs = downcast_any!(merging_accs, mut AccBooleanColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if accs.value(acc_idx).is_none() && merging_accs.value(merging_acc_idx).is_some() {
                            accs.set_value(acc_idx, merging_accs.value(merging_acc_idx));
                        }
                    }
                }
            }};
        }

        macro_rules! handle_bytes {
            () => {{
                let accs = downcast_any!(accs, mut AccBytesColumn)?;
                let merging_accs = downcast_any!(merging_accs, mut AccBytesColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if accs.value(acc_idx).is_none() && merging_accs.value(merging_acc_idx).is_some() {
                            accs.set_value(acc_idx, merging_accs.take_value(merging_acc_idx));
                        }
                    }
                }
            }};
        }

        downcast_primitive! {
            (&self.data_type) => (handle_primitive),
            DataType::Boolean => handle_boolean!(),
            DataType::Utf8 | DataType::Binary => handle_bytes!(),
            DataType::Null => {}
            _ => {
                let accs = downcast_any!(accs, mut AccScalarValueColumn)?;
                let merging_accs = downcast_any!(merging_accs, mut AccScalarValueColumn)?;
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if accs.value(acc_idx).is_null() && !merging_accs.value(merging_acc_idx).is_null() {
                            accs.set_value(acc_idx, merging_accs.take_value(merging_acc_idx));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        acc_generic_column_to_array(accs, &self.data_type, acc_idx)
    }
}
