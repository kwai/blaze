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
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use datafusion_ext_commons::{df_unimplemented_err, downcast_any};
use paste::paste;

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef, AccGenericColumn},
        agg::IdxSelection,
        Agg,
    },
    idx_for_zipped,
};

pub struct AggSum {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl AggSum {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        Ok(Self { child, data_type })
    }
}

impl Debug for AggSum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sum({:?})", self.child)
    }
}

impl Agg for AggSum {
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

    fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        // cast arg1 to target data type
        Ok(vec![datafusion_ext_commons::arrow::cast::cast(
            &partial_inputs[0],
            &self.data_type,
        )?])
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        Box::new(AccGenericColumn::new(&self.data_type, num_rows))
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccGenericColumn).unwrap();

        macro_rules! handle {
            ($ty:ident) => {{
                type TArray = paste! {[<$ty Array>]};
                type TType = paste! {[<$ty Type>]};
                type TNative = <TType as ArrowPrimitiveType>::Native;
                let partial_arg = downcast_any!(&partial_args[0], TArray).unwrap();
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if acc_idx >= accs.num_records() {
                            accs.resize(acc_idx + 1);
                        }
                        if partial_arg.is_valid(partial_arg_idx) {
                            let partial_value = partial_arg.value(partial_arg_idx);
                            if !accs.prim_valid(acc_idx) {
                                accs.set_prim_valid(acc_idx, true);
                                accs.set_prim_value(acc_idx, partial_value);
                            } else {
                                accs.update_prim_value::<TNative>(acc_idx, |v| *v += partial_value);
                            }
                        }
                    }
                }
            }};
        }
        match &self.data_type {
            DataType::Null => {}
            DataType::Float32 => handle!(Float32),
            DataType::Float64 => handle!(Float64),
            DataType::Int8 => handle!(Int8),
            DataType::Int16 => handle!(Int16),
            DataType::Int32 => handle!(Int32),
            DataType::Int64 => handle!(Int64),
            DataType::UInt8 => handle!(UInt8),
            DataType::UInt16 => handle!(UInt16),
            DataType::UInt32 => handle!(UInt32),
            DataType::UInt64 => handle!(UInt64),
            DataType::Decimal128(..) => handle!(Decimal128),
            other => df_unimplemented_err!("unsupported data type in sum(): {other}")?,
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
        let accs = downcast_any!(accs, mut AccGenericColumn).unwrap();
        let merging_accs = downcast_any!(merging_accs, mut AccGenericColumn).unwrap();

        macro_rules! handle {
            ($ty:ty) => {{
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if acc_idx >= accs.num_records() {
                            accs.resize(acc_idx + 1);
                        }
                        if merging_accs.prim_valid(merging_acc_idx) {
                            let merging_value = merging_accs.prim_value::<$ty>(merging_acc_idx);
                            if !accs.prim_valid(acc_idx) {
                                accs.set_prim_valid(acc_idx, true);
                                accs.set_prim_value(acc_idx, merging_value);
                            } else {
                                accs.update_prim_value::<$ty>(acc_idx, |v| *v += merging_value);
                            }
                        }
                    }
                }
            }};
        }
        match &self.data_type {
            DataType::Null => {}
            DataType::Float32 => handle!(f32),
            DataType::Float64 => handle!(f64),
            DataType::Int8 => handle!(i8),
            DataType::Int16 => handle!(i16),
            DataType::Int32 => handle!(i32),
            DataType::Int64 => handle!(i64),
            DataType::UInt8 => handle!(u8),
            DataType::UInt16 => handle!(u16),
            DataType::UInt32 => handle!(u32),
            DataType::UInt64 => handle!(u64),
            DataType::Decimal128(..) => handle!(u128),
            other => df_unimplemented_err!("unsupported data type in sum(): {other}")?,
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccGenericColumn).unwrap();
        accs.to_array(acc_idx, &self.data_type)
    }
}
