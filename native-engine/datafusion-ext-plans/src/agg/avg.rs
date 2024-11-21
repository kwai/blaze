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
use datafusion::{
    common::{
        cast::{as_decimal128_array, as_int64_array},
        Result,
    },
    physical_expr::PhysicalExpr,
};
use datafusion_ext_commons::downcast_any;

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef},
        agg::IdxSelection,
        count::AggCount,
        sum::AggSum,
        Agg,
    },
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub struct AggAvg {
    child: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    agg_sum: AggSum,
    agg_count: AggCount,
}

impl AggAvg {
    pub fn try_new(child: Arc<dyn PhysicalExpr>, data_type: DataType) -> Result<Self> {
        let agg_sum = AggSum::try_new(child.clone(), data_type.clone())?;
        let agg_count = AggCount::try_new(vec![child.clone()], DataType::Int64)?;
        Ok(Self {
            child,
            data_type,
            agg_sum,
            agg_count,
        })
    }
}

impl Debug for AggAvg {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Avg({:?})", self.child)
    }
}

impl Agg for AggAvg {
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
        Ok(vec![datafusion_ext_commons::cast::cast(
            &partial_inputs[0],
            &self.data_type,
        )?])
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        Box::new(AccAvgColumn {
            sum: self.agg_sum.create_acc_column(num_rows),
            count: self.agg_count.create_acc_column(num_rows),
        })
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccAvgColumn).unwrap();
        self.agg_sum
            .partial_update(&mut accs.sum, acc_idx, partial_args, partial_arg_idx)?;
        self.agg_count
            .partial_update(&mut accs.count, acc_idx, partial_args, partial_arg_idx)?;
        Ok(())
    }

    fn partial_merge(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        merging_accs: &mut AccColumnRef,
        merging_acc_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccAvgColumn).unwrap();
        let merging_accs = downcast_any!(merging_accs, mut AccAvgColumn).unwrap();
        self.agg_sum.partial_merge(
            &mut accs.sum,
            acc_idx,
            &mut merging_accs.sum,
            merging_acc_idx,
        )?;
        self.agg_count.partial_merge(
            &mut accs.count,
            acc_idx,
            &mut merging_accs.count,
            merging_acc_idx,
        )?;
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccAvgColumn).unwrap();
        let sums = self.agg_sum.final_merge(&mut accs.sum, acc_idx)?;
        let counts = self.agg_count.final_merge(&mut accs.count, acc_idx)?;

        let counts_zero_free: Int64Array = as_int64_array(&counts)?.unary_opt(|count| {
            let not_zero = !count.is_zero();
            not_zero.then_some(count)
        });

        if let &DataType::Decimal128(prec, scale) = self.data_type() {
            let sums = as_decimal128_array(&sums)?;
            let counts = counts_zero_free;
            let avgs =
                arrow::compute::binary::<_, _, _, Decimal128Type>(&sums, &counts, |sum, count| {
                    sum.checked_div_euclid(count as i128).unwrap_or_default()
                })?;
            Ok(Arc::new(avgs.with_precision_and_scale(prec, scale)?))
        } else {
            let counts = counts_zero_free;
            Ok(arrow::compute::kernels::numeric::div(
                &arrow::compute::cast(&sums, &DataType::Float64)?,
                &arrow::compute::cast(&counts, &DataType::Float64)?,
            )?)
        }
    }
}

struct AccAvgColumn {
    sum: AccColumnRef,
    count: AccColumnRef,
}

impl AccColumn for AccAvgColumn {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        self.sum.resize(len);
        self.count.resize(len);
    }

    fn shrink_to_fit(&mut self) {
        self.sum.shrink_to_fit();
        self.count.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.sum.num_records()
    }

    fn mem_used(&self) -> usize {
        self.sum.mem_used() + self.count.mem_used()
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        self.sum.freeze_to_rows(idx, array)?;
        self.count.freeze_to_rows(idx, array)?;
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, array: &[&[u8]], offsets: &mut [usize]) -> Result<()> {
        self.sum.unfreeze_from_rows(array, offsets)?;
        self.count.unfreeze_from_rows(array, offsets)?;
        Ok(())
    }

    fn spill(&self, idx: IdxSelection<'_>, buf: &mut SpillCompressedWriter) -> Result<()> {
        self.sum.spill(idx, buf)?;
        self.count.spill(idx, buf)?;
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        self.sum.unspill(num_rows, r)?;
        self.count.unspill(num_rows, r)?;
        Ok(())
    }
}
