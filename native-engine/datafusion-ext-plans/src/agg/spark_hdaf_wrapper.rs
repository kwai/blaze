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
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use arrow::{
    array::{
        as_struct_array, make_array, Array, ArrayAccessor, ArrayRef, AsArray, BinaryArray, Datum,
        Int32Array, Int32Builder, StructArray,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use blaze_jni_bridge::{
    jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object,
};
use datafusion::{
    common::{DataFusionError, Result},
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::downcast_any;
use jni::objects::{GlobalRef, JObject};
use once_cell::sync::OnceCell;

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef},
        agg::{Agg, IdxSelection},
    },
    idx_for_zipped,
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub struct SparkUDAFWrapper {
    serialized: Vec<u8>,
    pub buffer_schema: SchemaRef,
    pub return_type: DataType,
    child: Vec<Arc<dyn PhysicalExpr>>,
    import_schema: SchemaRef,
    params_schema: OnceCell<SchemaRef>,
    jcontext: OnceCell<GlobalRef>,
}

impl SparkUDAFWrapper {
    pub fn try_new(
        serialized: Vec<u8>,
        buffer_schema: SchemaRef,
        return_type: DataType,
        child: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        Ok(Self {
            serialized,
            buffer_schema,
            return_type: return_type.clone(),
            child,
            import_schema: Arc::new(Schema::new(vec![Field::new("", return_type, true)])),
            params_schema: OnceCell::new(),
            jcontext: OnceCell::new(),
        })
    }

    fn jcontext(&self) -> Result<GlobalRef> {
        self.jcontext
            .get_or_try_init(|| {
                let serialized_buf = jni_new_direct_byte_buffer!(&self.serialized)?;
                let jcontext_local =
                    jni_new_object!(SparkUDAFWrapperContext(serialized_buf.as_obj()))?;
                jni_new_global_ref!(jcontext_local.as_obj())
            })
            .cloned()
    }
}

impl Display for SparkUDAFWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkUDAFWrapper")
    }
}

impl Debug for SparkUDAFWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkUDAFWrapper({:?})", self.child)
    }
}

impl Agg for SparkUDAFWrapper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.child.clone()
    }

    fn data_type(&self) -> &DataType {
        &self.return_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        // num_rows
        let rows = jni_call_static!(
            BlazeUnsafeRowsWrapperUtils.create(num_rows as i32)-> JObject)
        .unwrap();
        let obj = jni_new_global_ref!(rows.as_obj()).unwrap();
        Box::new(AccUnsafeRowsColumn {
            obj,
            num_fields: self.buffer_schema.fields.len(),
        })
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            self.serialized.clone(),
            self.buffer_schema.clone(),
            self.return_type.clone(),
            self.child.clone(),
        )?))
    }

    // todo: implemented prepare_partial_args
    // fn prepare_partial_args(&self, partial_inputs: &[ArrayRef]) ->
    // Result<Vec<ArrayRef>> {     // cast arg1 to target data type
    //     Ok(vec![datafusion_ext_commons::arrow::cast::cast(
    //         &partial_inputs[0],
    //         &self.return_type,
    //     )?])
    // }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
        batch_schema: SchemaRef,
    ) -> Result<()> {
        log::info!("start partial update");
        let accs = downcast_any!(accs, mut AccUnsafeRowsColumn).unwrap();
        log::info!("update before accs.num {}", accs.num_records());

        let params = partial_args.to_vec();
        let params_schema = self
            .params_schema
            .get_or_try_init(|| -> Result<SchemaRef> {
                let mut param_fields = Vec::with_capacity(self.child.len());
                for child in &self.child {
                    param_fields.push(Field::new(
                        "",
                        child.data_type(batch_schema.as_ref())?,
                        child.nullable(batch_schema.as_ref())?,
                    ));
                }
                Ok(Arc::new(Schema::new(param_fields)))
            })?;
        let params_batch = RecordBatch::try_new_with_options(
            params_schema.clone(),
            params.clone(),
            &RecordBatchOptions::new().with_row_count(Some(params[0].len())),
        )?;

        let max_len = std::cmp::max(acc_idx.len(), partial_arg_idx.len());
        let mut acc_idx_builder = Int32Builder::with_capacity(max_len);
        let mut partial_arg_idx_builder = Int32Builder::with_capacity(max_len);
        idx_for_zipped! {
            ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                acc_idx_builder.append_value(acc_idx as i32);
                partial_arg_idx_builder.append_value(partial_arg_idx as i32);
            }
        }
        let acc_idx = acc_idx_builder
            .finish()
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .unwrap();

        let partial_arg_idx = partial_arg_idx_builder
            .finish()
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .unwrap();

        accs.obj = partial_update_udaf(
            self.jcontext()?,
            params_batch,
            accs.obj.clone(),
            acc_idx,
            partial_arg_idx,
        )
        .unwrap();
        log::info!("update after accs.num {}", accs.num_records());
        Ok(())
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        log::info!("start partial merge");
        let accs = downcast_any!(accs, mut AccUnsafeRowsColumn).unwrap();
        let merging_accs = downcast_any!(merging_accs, mut AccUnsafeRowsColumn).unwrap();

        let max_len = std::cmp::max(acc_idx.len(), merging_acc_idx.len());
        let mut acc_idx_builder = Int32Builder::with_capacity(max_len);
        let mut merging_acc_idx_builder = Int32Builder::with_capacity(max_len);
        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                acc_idx_builder.append_value(acc_idx as i32);
                merging_acc_idx_builder.append_value(merging_acc_idx as i32);
            }
        }
        let acc_idx = acc_idx_builder
            .finish()
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .unwrap();

        let merging_acc_idx = merging_acc_idx_builder
            .finish()
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .unwrap();

        accs.obj = partial_merge_udaf(
            self.jcontext()?,
            accs.obj.clone(),
            merging_accs.obj.clone(),
            acc_idx,
            merging_acc_idx,
        )
        .unwrap();

        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        log::info!("start final merge");
        let accs = downcast_any!(accs, mut AccUnsafeRowsColumn).unwrap();
        final_merge_udaf(
            self.jcontext()?,
            accs.obj.clone(),
            acc_idx,
            self.import_schema.clone(),
        )
    }
}

struct AccUnsafeRowsColumn {
    obj: GlobalRef,
    num_fields: usize,
}

impl AccColumn for AccUnsafeRowsColumn {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        let rows = jni_call_static!(
            BlazeUnsafeRowsWrapperUtils.create(len as i32)-> JObject)
        .unwrap();
        self.obj = jni_new_global_ref!(rows.as_obj()).unwrap();
    }

    fn shrink_to_fit(&mut self) {}

    fn num_records(&self) -> usize {
        match jni_call_static!(
        BlazeUnsafeRowsWrapperUtils.num(self.obj.as_obj())
        -> i32)
        {
            Ok(row_num) => row_num as usize,
            Err(_) => 0,
        }
    }

    fn mem_used(&self) -> usize {
        0
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        let field = Arc::new(Field::new("", DataType::Int32, false));
        let idx32 = idx.to_int32_array().into_data();
        let struct_array = StructArray::from(vec![(field, make_array(idx32))]);
        let mut export_ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
        let mut import_ffi_array = FFI_ArrowArray::empty();
        jni_call_static!(
            BlazeUnsafeRowsWrapperUtils.serialize(
                self.obj.as_obj(),
                self.num_fields as i32,
                &mut export_ffi_array as *mut FFI_ArrowArray as i64,
                &mut import_ffi_array as *mut FFI_ArrowArray as i64,)
            -> ())?;
        // import output from context
        let field = Field::new("", DataType::Binary, false);
        let schema = Schema::new(vec![field]);
        let import_ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        let import_struct_array =
            make_array(unsafe { from_ffi(import_ffi_array, &import_ffi_schema)? });
        let result_struct = import_struct_array.as_struct();

        let binary_array = result_struct
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected a BinaryArray".to_string()))?;

        for i in 0..binary_array.len() {
            if binary_array.is_valid(i) {
                let bytes = binary_array.value(i).to_vec();
                array[i].extend_from_slice(&bytes);
            } else {
                log::warn!("AccUnsafeRowsColumn::freeze_to_rows : binary_array null error")
            }
        }
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, array: &[&[u8]], offsets: &mut [usize]) -> Result<()> {
        log::info!("unfreeze array {:?}", array);
        log::info!("unfreeze offsets {:?}", offsets);
        let binary_values = array.iter().map(|&data| data).collect();
        let offsets_i32 = offsets
            .iter()
            .map(|data| *data as i32)
            .collect::<Vec<i32>>();
        let offsets_array = Int32Array::from(offsets_i32)
            .as_any()
            .downcast_ref::<Int32Array>()
            .cloned()
            .unwrap();
        let binary_array = BinaryArray::from_vec(binary_values)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .cloned()
            .unwrap();

        let binary_field = Arc::new(Field::new("", DataType::Binary, false));
        let offsets_field = Arc::new(Field::new("", DataType::Int32, false));

        let struct_array = StructArray::from(vec![
            (binary_field.clone(), make_array(binary_array.into_data())),
            (offsets_field.clone(), make_array(offsets_array.into_data())),
        ]);

        let mut export_ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
        let mut import_ffi_array = FFI_ArrowArray::empty();
        let rows = jni_call_static!(
            BlazeUnsafeRowsWrapperUtils.deserialize(
                self.num_fields as i32,
                &mut export_ffi_array as *mut FFI_ArrowArray as i64,
                &mut import_ffi_array as *mut FFI_ArrowArray as i64,)
            -> JObject)?;
        self.obj = jni_new_global_ref!(rows.as_obj())?;

        // update offsets
        // import output from context
        let field = Field::new("", DataType::Int32, false);
        let schema = Schema::new(vec![field]);
        let import_ffi_schema = FFI_ArrowSchema::try_from(schema)?;
        let import_struct_array =
            make_array(unsafe { from_ffi(import_ffi_array, &import_ffi_schema)? });
        let result_struct = import_struct_array.as_struct();

        let int32array = result_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| DataFusionError::Execution("Expected a Int32Array".to_string()))?;

        assert_eq!(int32array.len(), array.len());

        for i in 0..int32array.len() {
            offsets[i] = int32array.value(i) as usize;
        }

        Ok(())
    }

    fn spill(&self, idx: IdxSelection<'_>, buf: &mut SpillCompressedWriter) -> Result<()> {
        unimplemented!()
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        unimplemented!()
    }
}

fn partial_update_udaf(
    jcontext: GlobalRef,
    params_batch: RecordBatch,
    accs: GlobalRef,
    acc_idx: Int32Array,
    partial_arg_idx: Int32Array,
) -> Result<GlobalRef> {
    let acc_idx_field = Arc::new(Field::new("acc_idx", DataType::Int32, false));
    let partial_arg_idx_field = Arc::new(Field::new("partial_arg_idx", DataType::Int32, false));

    let struct_array = StructArray::from(vec![
        (acc_idx_field.clone(), make_array(acc_idx.into_data())),
        (
            partial_arg_idx_field.clone(),
            make_array(partial_arg_idx.into_data()),
        ),
    ]);
    let mut export_ffi_idx_array = FFI_ArrowArray::new(&struct_array.to_data());

    let struct_array = StructArray::from(params_batch.clone());

    let mut export_ffi_batch_array = FFI_ArrowArray::new(&struct_array.to_data());

    let rows = jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).update(
        accs.as_obj(),
        &mut export_ffi_idx_array as *mut FFI_ArrowArray as i64,
        &mut export_ffi_batch_array as *mut FFI_ArrowArray as i64,
    )-> JObject)?;

    jni_new_global_ref!(rows.as_obj())
}

fn partial_merge_udaf(
    jcontext: GlobalRef,
    accs: GlobalRef,
    merging_accs: GlobalRef,
    acc_idx: Int32Array,
    merging_acc_idx: Int32Array,
) -> Result<GlobalRef> {
    let acc_idx_field = Arc::new(Field::new("acc_idx", DataType::Int32, false));
    let merging_acc_idx_field = Arc::new(Field::new("merging_acc_idx", DataType::Int32, false));

    let struct_array = StructArray::from(vec![
        (acc_idx_field.clone(), make_array(acc_idx.into_data())),
        (
            merging_acc_idx_field.clone(),
            make_array(merging_acc_idx.into_data()),
        ),
    ]);
    let mut export_ffi_idx_array = FFI_ArrowArray::new(&struct_array.to_data());

    let rows = jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).merge(
        accs.as_obj(),
        merging_accs.as_obj(),
        &mut export_ffi_idx_array as *mut FFI_ArrowArray as i64,
    )-> JObject)?;

    jni_new_global_ref!(rows.as_obj())
}

fn final_merge_udaf(
    jcontext: GlobalRef,
    accs: GlobalRef,
    acc_idx: IdxSelection<'_>,
    result_schema: SchemaRef,
) -> Result<ArrayRef> {
    let acc_idx_field = Arc::new(Field::new("acc_idx", DataType::Int32, false));
    let acc_idx = acc_idx.to_int32_array().into_data();
    let struct_array = StructArray::from(vec![(acc_idx_field.clone(), make_array(acc_idx))]);
    let mut export_ffi_idx_array = FFI_ArrowArray::new(&struct_array.to_data());
    let mut import_ffi_array = FFI_ArrowArray::empty();
    let rows = jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).eval(
        accs.as_obj(),
        &mut export_ffi_idx_array as *mut FFI_ArrowArray as i64,
        &mut import_ffi_array as *mut FFI_ArrowArray as i64,
    )-> ())?;

    // import output from context
    let import_ffi_schema = FFI_ArrowSchema::try_from(result_schema.as_ref())?;
    let import_struct_array =
        make_array(unsafe { from_ffi(import_ffi_array, &import_ffi_schema)? });
    let import_array = as_struct_array(&import_struct_array).column(0).clone();
    Ok(import_array)
}
