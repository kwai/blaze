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
use datafusion::{common::Result, physical_expr::PhysicalExpr};
use datafusion_ext_commons::{
    downcast_any,
    io::{read_len, write_len},
};

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef},
        agg::{Agg, IdxSelection},
    },
    idx_for, idx_for_zipped, idx_with_iter,
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub struct AggCount {
    children: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
}

impl AggCount {
    pub fn try_new(children: Vec<Arc<dyn PhysicalExpr>>, data_type: DataType) -> Result<Self> {
        assert_eq!(data_type, DataType::Int64);
        Ok(Self {
            children,
            data_type,
        })
    }
}

impl Debug for AggCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Count({:?})", self.children)
    }
}

impl Agg for AggCount {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.children.clone()
    }

    fn with_new_exprs(&self, exprs: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn Agg>> {
        Ok(Arc::new(Self::try_new(
            exprs.clone(),
            self.data_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        false
    }

    fn create_acc_column(&self, num_rows: usize) -> Box<dyn AccColumn> {
        Box::new(AccCountColumn {
            values: vec![0; num_rows],
        })
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut AccCountColumn).unwrap();

        if partial_args.is_empty() {
            idx_for_zipped! {
                ((acc_idx, _partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                    if acc_idx >= accs.values.len() {
                        accs.values.push(1);
                    } else {
                        accs.values[acc_idx] += 1;
                    }
                }
            }
        } else {
            idx_for_zipped! {
                ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                    let add = partial_args
                        .iter()
                        .all(|arg| arg.is_valid(partial_arg_idx)) as i64;

                    if acc_idx >= accs.values.len() {
                        accs.values.push(add);
                    } else {
                        accs.values[acc_idx] += add;
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
        let accs = downcast_any!(accs, mut AccCountColumn).unwrap();
        let merging_accs = downcast_any!(merging_accs, mut AccCountColumn).unwrap();

        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                if acc_idx < accs.values.len() {
                    accs.values[acc_idx] += merging_accs.values[merging_acc_idx];
                } else {
                    accs.values.push(merging_accs.values[merging_acc_idx]);
                }
            }
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut AccCountColumn).unwrap();

        idx_with_iter! {
            (acc_idx_iter @ acc_idx) => {
                Ok(Arc::new(Int64Array::from_iter_values(
                    acc_idx_iter.map(|idx| accs.values[idx])
                )))
            }
        }
    }
}

pub struct AccCountColumn {
    pub values: Vec<i64>,
}

impl AccColumn for AccCountColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, num_accs: usize) {
        self.values.resize(num_accs, 0);
    }

    fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.values.len()
    }

    fn mem_used(&self) -> usize {
        self.values.capacity() * 2 * size_of::<i64>()
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        let mut array_idx = 0;

        idx_for! {
            (idx in idx) => {
                write_len(self.values[idx] as usize, &mut array[array_idx])?;
                array_idx += 1;
            }
        }
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, cursors: &mut [Cursor<&[u8]>]) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        for cursor in cursors {
            self.values.push(read_len(cursor)? as i64);
        }
        Ok(())
    }

    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                write_len(self.values[idx] as usize, w)?;
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        for _ in 0..num_rows {
            self.values.push(read_len(r)? as i64);
        }
        Ok(())
    }
}
