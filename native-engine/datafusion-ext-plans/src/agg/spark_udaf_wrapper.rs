// Copyright 2022 The Auron Authors
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
    io::{Cursor, Read, Write},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, StructArray, as_struct_array, make_array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use auron_jni_bridge::{
    jni_bridge::LocalRef, jni_call, jni_get_byte_array_len, jni_get_byte_array_region,
    jni_new_direct_byte_buffer, jni_new_global_ref, jni_new_object, jni_new_prim_array,
};
use datafusion::{
    common::{DataFusionError, Result},
    physical_expr::PhysicalExprRef,
};
use datafusion_ext_commons::{
    UninitializedInit, downcast_any,
    io::{read_len, write_len},
};
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
    pub return_type: DataType,
    child: Vec<PhysicalExprRef>,
    import_schema: SchemaRef,
    params_schema: OnceCell<SchemaRef>,
    jcontext: OnceCell<GlobalRef>,
}

impl SparkUDAFWrapper {
    pub fn try_new(
        serialized: Vec<u8>,
        return_type: DataType,
        child: Vec<PhysicalExprRef>,
    ) -> Result<Self> {
        Ok(Self {
            serialized,
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

    pub fn partial_update_with_indices_cache(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
        cache: &OnceCell<LocalRef>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccUDAFBufferRowsColumn)?;

        let params_schema = self.params_schema.get_or_init(|| {
            Arc::new(Schema::new(
                partial_args
                    .iter()
                    .map(|arg| Field::new("", arg.data_type().clone(), true))
                    .collect::<Vec<_>>(),
            ))
        });

        let params_batch_num_rows = match partial_args.get(0) {
            Some(arg) => arg.len(),
            None => 0,
        };
        let params_batch = RecordBatch::try_new_with_options(
            params_schema.clone(),
            partial_args.to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(params_batch_num_rows)),
        )?;
        let batch_struct_array = StructArray::from(params_batch);
        let mut export_ffi_batch_array = FFI_ArrowArray::new(&batch_struct_array.to_data());

        // create zipped indices (using cached indices array)
        let zipped_indices_array = cache.get_or_try_init(move || {
            let max_len = std::cmp::max(acc_idx.len(), partial_arg_idx.len());
            let mut zipped_indices = Vec::with_capacity(max_len);
            idx_for_zipped! {
                ((acc_idx, updating_acc_idx) in (acc_idx, partial_arg_idx)) => {
                    zipped_indices.push((acc_idx as i64) << 32 | updating_acc_idx as i64);
                }
            }
            Ok::<_, DataFusionError>(jni_new_prim_array!(long, &zipped_indices[..])?)
        })?;

        let jcontext = self.jcontext()?;
        jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).update(
            accs.obj.as_obj(),
            &mut export_ffi_batch_array as *mut FFI_ArrowArray as i64,
            zipped_indices_array.as_obj(),
        )-> ())
    }

    pub fn partial_merge_with_indices_cache(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
        cache: &OnceCell<LocalRef>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccUDAFBufferRowsColumn)?;
        let merging_accs = downcast_any!(merging_accs, mut AccUDAFBufferRowsColumn)?;

        // create zipped indices (using cached indices array)
        let zipped_indices_array = cache.get_or_try_init(move || {
            let max_len = std::cmp::max(acc_idx.len(), merging_acc_idx.len());
            let mut zipped_indices = Vec::with_capacity(max_len);
            idx_for_zipped! {
                ((acc_idx, updating_acc_idx) in (acc_idx, merging_acc_idx)) => {
                    zipped_indices.push((acc_idx as i64) << 32 | updating_acc_idx as i64);
                }
            }
            Ok::<_, DataFusionError>(jni_new_prim_array!(long, &zipped_indices[..])?)
        })?;

        let jcontext = self.jcontext()?;
        jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).merge(
            accs.obj.as_obj(),
            merging_accs.obj.as_obj(),
            zipped_indices_array.as_obj(),
        )-> ())
    }

    pub fn final_merge_with_indices_cache(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        cache: &OnceCell<LocalRef>,
    ) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccUDAFBufferRowsColumn)?;
        let acc_indices_array = cache.get_or_try_init(move || {
            let acc_indices = acc_idx.to_int32_vec();
            Ok::<_, DataFusionError>(jni_new_prim_array!(int, &acc_indices[..])?)
        })?;
        let mut import_ffi_array = FFI_ArrowArray::empty();

        let jcontext = self.jcontext()?;
        jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).eval(
            accs.obj.as_obj(),
            acc_indices_array.as_obj(),
            &mut import_ffi_array as *mut FFI_ArrowArray as i64,
        )-> ())?;

        // import output from context
        let import_ffi_schema = FFI_ArrowSchema::try_from(self.import_schema.as_ref())?;
        let import_struct_array =
            make_array(unsafe { from_ffi(import_ffi_array, &import_ffi_schema)? });
        let import_array = as_struct_array(&import_struct_array).column(0).clone();
        Ok(import_array)
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

    fn exprs(&self) -> Vec<PhysicalExprRef> {
        self.child.clone()
    }

    fn data_type(&self) -> &DataType {
        &self.return_type
    }

    fn nullable(&self) -> bool {
        true
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        let jcontext = self.jcontext().unwrap();
        let rows = jni_call!(SparkUDAFWrapperContext(jcontext.as_obj()).initialize(
            num_rows as i32,
        )-> JObject)
        .unwrap();

        let jcontext = self.jcontext().unwrap();
        let obj = jni_new_global_ref!(rows.as_obj()).unwrap();
        Box::new(AccUDAFBufferRowsColumn { obj, jcontext })
    }

    fn with_new_exprs(&self, _exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            self.serialized.clone(),
            self.return_type.clone(),
            self.child.clone(),
        )?))
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        self.partial_update_with_indices_cache(
            accs,
            acc_idx,
            partial_args,
            partial_arg_idx,
            &OnceCell::new(), // without cache
        )
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        self.partial_merge_with_indices_cache(
            accs,
            acc_idx,
            merging_accs,
            merging_acc_idx,
            &OnceCell::new(), // without cache
        )
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        self.final_merge_with_indices_cache(accs, acc_idx, &OnceCell::new())
    }
}

pub struct AccUDAFBufferRowsColumn {
    obj: GlobalRef,
    jcontext: GlobalRef,
}

impl AccUDAFBufferRowsColumn {
    pub fn freeze_to_rows_with_indices_cache(
        &self,
        idx: IdxSelection<'_>,
        array: &mut [Vec<u8>],
        cache: &OnceCell<LocalRef>,
    ) -> Result<()> {
        let idx_array =
            cache.get_or_try_init(move || jni_new_prim_array!(int, &idx.to_int32_vec()[..]))?;
        let serialized = jni_call!(
            SparkUDAFWrapperContext(self.jcontext.as_obj()).serializeRows(
                self.obj.as_obj(),
                idx_array.as_obj(),
            ) -> JObject)?;
        let serialized_len = jni_get_byte_array_len!(serialized.as_obj())?;
        let mut serialized_bytes = Vec::uninitialized_init(serialized_len);
        jni_get_byte_array_region!(serialized.as_obj(), 0, &mut serialized_bytes[..])?;

        // UnsafeRow is serialized with big-endian i32 length prefix
        let mut cursor = Cursor::new(&serialized_bytes);
        for i in 0..array.len() {
            let mut bytes_len_buf = [0; 4];
            cursor.read_exact(&mut bytes_len_buf)?;
            let bytes_len = i32::from_be_bytes(bytes_len_buf) as usize;
            write_len(bytes_len, &mut array[i])?;
            std::io::copy(&mut (&mut cursor).take(bytes_len as u64), &mut array[i])?;
        }
        Ok(())
    }

    pub fn spill_with_indices_cache(
        &self,
        idx: IdxSelection<'_>,
        buf: &mut SpillCompressedWriter,
        spill_idx: usize,
        mem_tracker: &SparkUDAFMemTracker,
        cache: &OnceCell<LocalRef>,
    ) -> Result<()> {
        let idx_array =
            cache.get_or_try_init(move || jni_new_prim_array!(int, &idx.to_int32_vec()[..]))?;
        let spill_block_size = jni_call!(
            SparkUDAFWrapperContext(self.jcontext.as_obj()).spill(
                mem_tracker.as_obj(),
                self.obj.as_obj(),
                idx_array.as_obj(),
                spill_idx as i64,
            ) -> i32)?;
        write_len(spill_block_size as usize, buf)?;
        Ok(())
    }

    pub fn unspill_with_key(
        &mut self,
        num_rows: usize,
        r: &mut SpillCompressedReader,
        mem_tracker: &SparkUDAFMemTracker,
        spill_idx: usize,
    ) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        let spill_block_size = read_len(r)? as i32;
        let rows = jni_call!(SparkUDAFWrapperContext(self.jcontext.as_obj())
            .unspill(mem_tracker.as_obj(), spill_block_size, spill_idx as i64) -> JObject)?;
        self.obj = jni_new_global_ref!(rows.as_obj())?;
        assert_eq!(self.num_records(), num_rows, "unspill rows count mismatch");
        Ok(())
    }
}

impl AccColumn for AccUDAFBufferRowsColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        match jni_call!(SparkUDAFWrapperContext(self.jcontext.as_obj())
            .resize(self.obj.as_obj(), len as i32)-> ())
        {
            Ok(_) => {}
            Err(e) => panic!("SparkUDAFBufferRowsColumn::resize failed: {e:?}"),
        }
    }

    fn shrink_to_fit(&mut self) {}

    fn num_records(&self) -> usize {
        match jni_call!(SparkUDAFWrapperContext(self.jcontext.as_obj())
            .numRecords(self.obj.as_obj()) -> i32)
        {
            Ok(n) => n as usize,
            Err(e) => panic!("SparkUDAFBufferRowsColumn::num_records failed: {e:?}"),
        }
    }

    fn mem_used(&self) -> usize {
        0 // memory is managed in jvm side
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        self.freeze_to_rows_with_indices_cache(idx, array, &OnceCell::new())
    }

    fn unfreeze_from_rows(&mut self, cursors: &mut [Cursor<&[u8]>]) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        let mut data = vec![];
        for cursor in cursors.iter_mut() {
            let bytes_len = read_len(cursor)?;
            data.write_all((bytes_len as i32).to_be_bytes().as_ref())?;
            std::io::copy(&mut cursor.take(bytes_len as u64), &mut data)?;
        }

        let data_buffer = jni_new_direct_byte_buffer!(data)?;
        let rows = jni_call!(SparkUDAFWrapperContext(self.jcontext.as_obj())
            .deserializeRows(data_buffer.as_obj()) -> JObject)?;
        self.obj = jni_new_global_ref!(rows.as_obj())?;
        assert_eq!(
            self.num_records(),
            cursors.len(),
            "unfreeze rows count mismatch"
        );
        Ok(())
    }

    fn spill(&self, _idx: IdxSelection<'_>, _buf: &mut SpillCompressedWriter) -> Result<()> {
        unimplemented!("should call spill_with_indices_cache instead")
    }

    fn unspill(&mut self, _num_rows: usize, _r: &mut SpillCompressedReader) -> Result<()> {
        unimplemented!("should call unspill_with_key instead")
    }
}

pub struct SparkUDAFMemTracker {
    obj: GlobalRef,
}

impl SparkUDAFMemTracker {
    pub fn try_new() -> Result<Self> {
        let obj = jni_new_global_ref!(jni_new_object!(SparkUDAFMemTracker())?.as_obj())?;
        Ok(Self { obj })
    }

    pub fn add_column(&self, column: &AccUDAFBufferRowsColumn) -> Result<()> {
        Ok(jni_call!(SparkUDAFMemTracker(self.obj.as_obj()).addColumn(column.obj.as_obj())-> ())?)
    }

    pub fn reset(&self) -> Result<()> {
        Ok(jni_call!(SparkUDAFMemTracker(self.obj.as_obj()).reset()-> ())?)
    }

    pub fn update_used(&self) -> Result<bool> {
        Ok(jni_call!(SparkUDAFMemTracker(self.obj.as_obj()).updateUsed()-> bool)?)
    }

    pub fn as_obj(&self) -> JObject<'_> {
        self.obj.as_obj()
    }
}

impl Drop for SparkUDAFMemTracker {
    fn drop(&mut self) {
        let _ = jni_call!(SparkUDAFMemTracker(self.obj.as_obj()).reset()-> ());
    }
}
