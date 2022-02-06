use std::sync::Arc;

use datafusion::arrow::array::make_builder;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;

pub struct MutableRecordBatch {
    pub(crate) arrays: Vec<Box<dyn ArrayBuilder>>,
    target_batch_size: usize,
    slots_available: usize,
    schema: Arc<Schema>,
}

impl MutableRecordBatch {
    pub fn new(target_batch_size: usize, schema: Arc<Schema>) -> Self {
        let arrays = new_arrays(&schema, target_batch_size);
        Self {
            arrays,
            target_batch_size,
            slots_available: target_batch_size,
            schema,
        }
    }

    pub fn output_and_reset(&mut self) -> ArrowResult<RecordBatch> {
        let result = self.output();
        let mut new = new_arrays(&self.schema, self.target_batch_size);
        self.arrays.append(&mut new);
        result
    }

    pub fn output(&mut self) -> ArrowResult<RecordBatch> {
        let result = make_batch(self.schema.clone(), self.arrays.drain(..).collect());
        self.slots_available = self.target_batch_size;
        result
    }

    pub fn append(&mut self, size: usize) {
        assert!(size <= self.slots_available);
        self.slots_available -= size;
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.slots_available == 0
    }
}

pub fn new_arrays(schema: &Arc<Schema>, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            make_builder(dt, batch_size)
        })
        .collect::<Vec<_>>()
}

pub fn make_batch(
    schema: Arc<Schema>,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}
