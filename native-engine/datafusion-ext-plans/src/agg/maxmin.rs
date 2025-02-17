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
    cmp::Ordering,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{common::Result, physical_expr::PhysicalExpr, scalar::ScalarValue};
use datafusion_ext_commons::downcast_any;
use paste::paste;

use crate::{
    agg::{
        acc::{AccBytes, AccColumnRef, AccGenericColumn},
        agg::IdxSelection,
        Agg,
    },
    idx_for_zipped,
};

pub type AggMax = AggMaxMin<AggMaxParams>;
pub type AggMin = AggMaxMin<AggMinParams>;

pub struct AggMaxMin<P: AggMaxMinParams> {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    _phantom: PhantomData<P>,
}

impl<P: AggMaxMinParams> AggMaxMin<P> {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        Ok(Self {
            child,
            data_type,
            _phantom: Default::default(),
        })
    }
}

impl<P: AggMaxMinParams> Debug for AggMaxMin<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", P::NAME, self.child)
    }
}

impl<P: AggMaxMinParams> Agg for AggMaxMin<P> {
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
        Box::new(AccGenericColumn::new(&self.data_type, num_rows))
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
        _batch_schema: SchemaRef,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccGenericColumn).unwrap();
        let old_heap_mem_used = accs.items_heap_mem_used(acc_idx);

        macro_rules! handle_prim {
            ($ty:ident) => {{
                type TArray = paste! {[<$ty Array>]};
                let partial_arg = downcast_any!(&partial_args[0], TArray).unwrap();
                idx_for_zipped! {
                     ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                         if !partial_arg.is_valid(partial_arg_idx) {
                             continue;
                         }
                         let partial_value = partial_arg.value(partial_arg_idx);

                         if !accs.prim_valid(acc_idx) {
                             accs.set_prim_valid(acc_idx, true);
                             accs.set_prim_value(acc_idx, partial_value);
                             continue;
                         }
                         if partial_value.partial_cmp(&accs.prim_value(acc_idx)) == Some(P::ORD) {
                             accs.set_prim_value(acc_idx, partial_value);
                         }
                     }
                }
            }};
        }

        macro_rules! handle_bytes {
            ($ty:ident) => {{
                type TArray = paste::paste! {[<$ty Array>]};
                let partial_arg = downcast_any!(&partial_args[0], TArray).unwrap();
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        if !partial_arg.is_valid(partial_arg_idx) {
                            continue;
                        }
                        let partial_value: &[u8] = partial_arg.value(partial_arg_idx).as_ref();
                        if accs.bytes_value(acc_idx).is_none() {
                            accs.set_bytes_value(acc_idx, Some(AccBytes::from(partial_value)));
                            continue;
                        }
                        let acc_value = accs.bytes_value(acc_idx).unwrap();
                        if partial_value.partial_cmp(acc_value.as_ref()) == Some(P::ORD) {
                            accs.set_bytes_value(acc_idx, Some(AccBytes::from(partial_value)));
                        }
                    }
                }
            }};
        }

        match self.data_type {
            DataType::Null => {}
            DataType::Boolean => handle_prim!(Boolean),
            DataType::Int8 => handle_prim!(Int8),
            DataType::Int16 => handle_prim!(Int16),
            DataType::Int32 => handle_prim!(Int32),
            DataType::Int64 => handle_prim!(Int64),
            DataType::UInt8 => handle_prim!(UInt8),
            DataType::UInt16 => handle_prim!(UInt16),
            DataType::UInt32 => handle_prim!(UInt32),
            DataType::UInt64 => handle_prim!(UInt64),
            DataType::Float32 => handle_prim!(Float32),
            DataType::Float64 => handle_prim!(Float64),
            DataType::Date32 => handle_prim!(Date32),
            DataType::Date64 => handle_prim!(Date64),
            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => handle_prim!(TimestampSecond),
                TimeUnit::Millisecond => handle_prim!(TimestampMillisecond),
                TimeUnit::Microsecond => handle_prim!(TimestampMicrosecond),
                TimeUnit::Nanosecond => handle_prim!(TimestampNanosecond),
            },
            DataType::Decimal128(..) => handle_prim!(Decimal128),
            DataType::Utf8 => handle_bytes!(String),
            DataType::Binary => handle_bytes!(Binary),
            _ => {
                idx_for_zipped! {
                    ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                        let partial_arg_scalar = ScalarValue::try_from_array(&partial_args[0], partial_arg_idx)?;
                        if !partial_arg_scalar.is_null() {
                            let acc_scalar = &mut accs.scalar_values_mut()[acc_idx];
                            if !acc_scalar.is_null() {
                                if partial_arg_scalar.partial_cmp(acc_scalar) == Some(P::ORD) {
                                    *acc_scalar = partial_arg_scalar;
                                }
                            } else {
                                *acc_scalar = partial_arg_scalar;
                            }
                            continue;
                        }
                    }
                }
            }
        }

        let new_heap_mem_used = accs.items_heap_mem_used(acc_idx);
        accs.add_heap_mem_used(new_heap_mem_used - old_heap_mem_used);
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
        let old_mem_used = accs.items_heap_mem_used(acc_idx);

        macro_rules! handle_prim {
            ($ty:ty) => {{
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        if !merging_accs.prim_valid(merging_acc_idx) {
                            continue;
                        }
                        if !accs.prim_valid(acc_idx) {
                            accs.set_prim_valid(acc_idx, true);
                            accs.set_prim_value(acc_idx, merging_accs.prim_value::<$ty>(merging_acc_idx));
                            continue;
                        }
                        if merging_accs.prim_value::<$ty>(merging_acc_idx).partial_cmp(&accs.prim_value::<$ty>(acc_idx)) == Some(P::ORD) {
                            accs.set_prim_value(acc_idx, merging_accs.prim_value::<$ty>(merging_acc_idx));
                        }
                    }
                }
            }}
        }

        match self.data_type {
            DataType::Null => {}
            DataType::Boolean => handle_prim!(bool),
            DataType::Int8 => handle_prim!(i8),
            DataType::Int16 => handle_prim!(i16),
            DataType::Int32 => handle_prim!(i32),
            DataType::Int64 => handle_prim!(i64),
            DataType::UInt8 => handle_prim!(u8),
            DataType::UInt16 => handle_prim!(u16),
            DataType::UInt32 => handle_prim!(u32),
            DataType::UInt64 => handle_prim!(u64),
            DataType::Float32 => handle_prim!(f32),
            DataType::Float64 => handle_prim!(f64),
            DataType::Date32 => handle_prim!(u32),
            DataType::Date64 => handle_prim!(u64),
            DataType::Timestamp(..) => handle_prim!(u64),
            DataType::Decimal128(..) => handle_prim!(u128),
            DataType::Utf8 | DataType::Binary => {
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        let merging_value = merging_accs.take_bytes_value(merging_acc_idx);
                        if merging_value.is_none() {
                            continue;
                        }
                        let merging_value = merging_value.unwrap();
                        let acc_value = accs.bytes_value_mut(acc_idx);
                        if acc_value.is_none() {
                            accs.set_bytes_value(acc_idx, Some(merging_value));
                            continue;
                        }
                        let acc_value = acc_value.unwrap();
                        if merging_value.as_ref().partial_cmp(acc_value.as_ref()) == Some(P::ORD) {
                            accs.set_bytes_value(acc_idx, Some(merging_value));
                        }
                    }
                }
            }
            _ => {
                idx_for_zipped! {
                    ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                        let acc_value = &mut accs.scalar_values_mut()[acc_idx];
                        let merging_acc_value = std::mem::replace(
                            &mut merging_accs.scalar_values_mut()[merging_acc_idx],
                            ScalarValue::Null,
                        );
                        if !merging_acc_value.is_null() {
                            if !acc_value.is_null() {
                                if merging_acc_value.partial_cmp(acc_value) == Some(P::ORD) {
                                    *acc_value = merging_acc_value;
                                }
                            } else {
                                *acc_value = merging_acc_value;
                            }
                        }
                    }
                }
            }
        }

        let new_mem_used = accs.items_heap_mem_used(acc_idx);
        accs.add_heap_mem_used(new_mem_used - old_mem_used);
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccGenericColumn).unwrap();
        accs.to_array(acc_idx, &self.data_type)
    }
}

pub trait AggMaxMinParams: 'static + Send + Sync {
    const NAME: &'static str;
    const ORD: Ordering;
}

pub struct AggMaxParams;
pub struct AggMinParams;

impl AggMaxMinParams for AggMaxParams {
    const NAME: &'static str = "max";
    const ORD: Ordering = Ordering::Greater;
}

impl AggMaxMinParams for AggMinParams {
    const NAME: &'static str = "min";
    const ORD: Ordering = Ordering::Less;
}
