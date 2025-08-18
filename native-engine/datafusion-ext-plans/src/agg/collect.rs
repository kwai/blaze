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
    hash::BuildHasher,
    io::{Cursor, Read, Write},
    marker::PhantomData,
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use datafusion::{
    common::{Result, ScalarValue},
    physical_expr::PhysicalExprRef,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any,
    io::{read_bytes_slice, read_len, read_scalar, write_len, write_scalar},
    scalar_value::compacted_scalar_value_from_array,
};
use hashbrown::raw::RawTable;
use smallvec::SmallVec;

use crate::{
    agg::{
        acc::{AccColumn, AccColumnRef},
        agg::{Agg, IdxSelection},
    },
    idx_for, idx_for_zipped,
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub type AggCollectSet = AggGenericCollect<AccSetColumn>;
pub type AggCollectList = AggGenericCollect<AccListColumn>;

pub struct AggGenericCollect<C: AccCollectionColumn> {
    child: PhysicalExprRef,
    data_type: DataType,
    arg_type: DataType,
    return_list_nullable: bool,
    _phantom: PhantomData<C>,
}

impl<C: AccCollectionColumn> AggGenericCollect<C> {
    pub fn try_new(
        child: PhysicalExprRef,
        data_type: DataType,
        arg_type: DataType,
    ) -> Result<Self> {
        let return_list_nullable = match &data_type {
            DataType::List(field) => field.is_nullable(),
            _ => return df_execution_err!("expect DataType::List({arg_type:?}, got {data_type:?}"),
        };
        Ok(Self {
            child,
            arg_type,
            data_type,
            return_list_nullable,
            _phantom: Default::default(),
        })
    }

    pub fn arg_type(&self) -> &DataType {
        &self.arg_type
    }
}

impl<C: AccCollectionColumn> Debug for AggGenericCollect<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Collect({:?})", self.child)
    }
}

impl<C: AccCollectionColumn> Agg for AggGenericCollect<C> {
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
            self.arg_type.clone(),
        )?))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        false
    }

    fn create_acc_column(&self, num_rows: usize) -> AccColumnRef {
        let mut col = Box::new(C::empty(self.arg_type.clone()));
        col.resize(num_rows);
        col
    }

    fn partial_update(
        &self,
        accs: &mut AccColumnRef,
        acc_idx: IdxSelection<'_>,
        partial_args: &[ArrayRef],
        partial_arg_idx: IdxSelection<'_>,
    ) -> Result<()> {
        let accs = downcast_any!(accs, mut C)?;
        accs.ensure_size(acc_idx);

        idx_for_zipped! {
            ((acc_idx, partial_arg_idx) in (acc_idx, partial_arg_idx)) => {
                let scalar = compacted_scalar_value_from_array(&partial_args[0], partial_arg_idx)?;
                if !scalar.is_null() {
                    accs.append_item(acc_idx, &scalar);
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
        let accs = downcast_any!(accs, mut C)?;
        accs.ensure_size(acc_idx);

        let merging_accs = downcast_any!(merging_accs, mut C)?;
        idx_for_zipped! {
            ((acc_idx, merging_acc_idx) in (acc_idx, merging_acc_idx)) => {
                accs.merge_items(acc_idx, merging_accs, merging_acc_idx);
            }
        }
        Ok(())
    }

    fn final_merge(&self, accs: &mut AccColumnRef, acc_idx: IdxSelection<'_>) -> Result<ArrayRef> {
        let accs = downcast_any!(accs, mut C)?;
        let mut list = Vec::with_capacity(accs.num_records());

        idx_for! {
            (acc_idx in acc_idx) => {
                list.push(ScalarValue::List(ScalarValue::new_list(
                    &accs.take_values(acc_idx),
                    &self.arg_type,
                    self.return_list_nullable,
                )));
            }
        }
        ScalarValue::iter_to_array(list)
    }
}

pub trait AccCollectionColumn: AccColumn + Send + Sync + 'static {
    fn empty(dt: DataType) -> Self;
    fn append_item(&mut self, idx: usize, value: &ScalarValue);
    fn merge_items(&mut self, idx: usize, other: &mut Self, other_idx: usize);
    fn save_raw(&self, idx: usize, w: &mut impl Write) -> Result<()>;
    fn load_raw(&mut self, idx: usize, r: &mut impl Read) -> Result<()>;
    fn take_values(&mut self, idx: usize) -> Vec<ScalarValue>;

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        let mut array_idx = 0;

        idx_for! {
            (idx in idx) => {
                self.save_raw(idx, &mut array[array_idx])?;
                array_idx += 1;
            }
        }
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, cursors: &mut [Cursor<&[u8]>]) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        self.resize(cursors.len());

        for (idx, cursor) in cursors.iter_mut().enumerate() {
            self.load_raw(idx, cursor)?;
        }
        Ok(())
    }
}

pub struct AccSetColumn {
    set: Vec<AccSet>,
    dt: DataType,
    mem_used: usize,
}

impl AccCollectionColumn for AccSetColumn {
    fn empty(dt: DataType) -> Self {
        Self {
            set: vec![],
            dt,
            mem_used: 0,
        }
    }

    fn append_item(&mut self, idx: usize, value: &ScalarValue) {
        let old_mem_size = self.set[idx].mem_size();
        self.set[idx].append(value, false);
        self.mem_used += self.set[idx].mem_size() - old_mem_size;
    }

    fn merge_items(&mut self, idx: usize, other: &mut Self, other_idx: usize) {
        let self_value_mem_size = self.set[idx].mem_size();
        let other_value_mem_size = other.set[other_idx].mem_size();
        self.set[idx].merge(&mut other.set[other_idx]);
        self.mem_used += self.set[idx].mem_size() - self_value_mem_size;
        other.mem_used -= other_value_mem_size;
    }

    fn save_raw(&self, idx: usize, w: &mut impl Write) -> Result<()> {
        write_len(self.set[idx].list.raw.len(), w)?;
        w.write_all(&self.set[idx].list.raw)?;
        Ok(())
    }

    fn load_raw(&mut self, idx: usize, r: &mut impl Read) -> Result<()> {
        self.mem_used -= self.set[idx].mem_size();
        self.set[idx] = AccSet::default();

        let len = read_len(r)?;
        let mut cursor = Cursor::new(read_bytes_slice(r, len)?);
        while cursor.position() < len as u64 {
            let scalar = read_scalar(&mut cursor, &self.dt, false)?;
            self.append_item(idx, &scalar);
        }
        self.mem_used += self.set[idx].mem_size();
        Ok(())
    }

    fn take_values(&mut self, idx: usize) -> Vec<ScalarValue> {
        self.mem_used -= self.set[idx].mem_size();
        std::mem::take(&mut self.set[idx])
            .into_values(self.dt.clone(), false)
            .collect()
    }
}

impl AccColumn for AccSetColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        if len < self.set.len() {
            for idx in len..self.set.len() {
                self.mem_used -= self.set[idx].mem_size();
                self.set[idx] = AccSet::default();
            }
        }
        self.set.resize_with(len, || AccSet::default());
    }

    fn shrink_to_fit(&mut self) {
        self.set.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.set.len()
    }

    fn mem_used(&self) -> usize {
        self.mem_used + self.set.capacity() * size_of::<AccSet>()
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        AccCollectionColumn::freeze_to_rows(self, idx, array)
    }

    fn unfreeze_from_rows(&mut self, cursors: &mut [Cursor<&[u8]>]) -> Result<()> {
        AccCollectionColumn::unfreeze_from_rows(self, cursors)
    }

    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                self.save_raw(idx, w)?;
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        self.resize(num_rows);

        for idx in 0..num_rows {
            self.load_raw(idx, r)?;
        }
        Ok(())
    }
}

pub struct AccListColumn {
    list: Vec<AccList>,
    dt: DataType,
    mem_used: usize,
}

impl AccCollectionColumn for AccListColumn {
    fn empty(dt: DataType) -> Self {
        Self {
            list: vec![],
            dt,
            mem_used: 0,
        }
    }

    fn append_item(&mut self, idx: usize, value: &ScalarValue) {
        let old_mem_size = self.list[idx].mem_size();
        self.list[idx].append(value, false);
        self.mem_used += self.list[idx].mem_size() - old_mem_size;
    }

    fn merge_items(&mut self, idx: usize, other: &mut Self, other_idx: usize) {
        let self_value_mem_size = self.list[idx].mem_size();
        let other_value_mem_size = other.list[other_idx].mem_size();
        self.list[idx].merge(&mut other.list[other_idx]);
        self.mem_used += self.list[idx].mem_size() - self_value_mem_size;
        other.mem_used -= other_value_mem_size;
    }

    fn save_raw(&self, idx: usize, w: &mut impl Write) -> Result<()> {
        write_len(self.list[idx].raw.len(), w)?;
        w.write_all(&self.list[idx].raw)?;
        Ok(())
    }

    fn load_raw(&mut self, idx: usize, r: &mut impl Read) -> Result<()> {
        self.mem_used -= self.list[idx].mem_size();
        self.list[idx] = AccList::default();

        let len = read_len(r)?;
        self.list[idx].raw = read_bytes_slice(r, len)?.into();
        self.mem_used += self.list[idx].mem_size();
        Ok(())
    }

    fn take_values(&mut self, idx: usize) -> Vec<ScalarValue> {
        self.mem_used -= self.list[idx].mem_size();
        std::mem::take(&mut self.list[idx])
            .into_values(self.dt.clone(), false)
            .collect()
    }
}

impl AccColumn for AccListColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        if len < self.list.len() {
            for idx in len..self.list.len() {
                self.mem_used -= self.list[idx].mem_size();
                self.list[idx] = AccList::default();
            }
        }
        self.list.resize_with(len, || AccList::default());
    }

    fn shrink_to_fit(&mut self) {
        self.list.shrink_to_fit();
    }

    fn num_records(&self) -> usize {
        self.list.len()
    }

    fn mem_used(&self) -> usize {
        self.mem_used + self.list.capacity() * size_of::<AccList>()
    }

    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()> {
        AccCollectionColumn::freeze_to_rows(self, idx, array)
    }

    fn unfreeze_from_rows(&mut self, cursors: &mut [Cursor<&[u8]>]) -> Result<()> {
        AccCollectionColumn::unfreeze_from_rows(self, cursors)
    }

    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        idx_for! {
            (idx in idx) => {
                self.save_raw(idx, w)?;
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        assert_eq!(self.num_records(), 0, "expect empty AccColumn");
        self.resize(num_rows);
        for idx in 0..num_rows {
            self.load_raw(idx, r)?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct AccList {
    raw: Vec<u8>,
}

impl AccList {
    pub fn mem_size(&self) -> usize {
        self.raw.capacity()
    }

    pub fn append(&mut self, value: &ScalarValue, nullable: bool) {
        write_scalar(&value, nullable, &mut self.raw).unwrap();
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.raw.extend(std::mem::take(&mut other.raw));
    }

    pub fn into_values(self, dt: DataType, nullable: bool) -> impl Iterator<Item = ScalarValue> {
        struct ValuesIterator(Cursor<Vec<u8>>, DataType, bool);
        impl Iterator for ValuesIterator {
            type Item = ScalarValue;

            fn next(&mut self) -> Option<Self::Item> {
                if self.0.position() < self.0.get_ref().len() as u64 {
                    return Some(read_scalar(&mut self.0, &self.1, self.2).unwrap());
                }
                None
            }
        }
        ValuesIterator(Cursor::new(self.raw), dt, nullable)
    }

    fn ref_raw(&self, pos_len: (u32, u32)) -> &[u8] {
        &self.raw[pos_len.0 as usize..][..pos_len.1 as usize]
    }
}

#[derive(Clone, Default)]
struct AccSet {
    list: AccList,
    set: InternalSet,
}

#[derive(Clone)]
enum InternalSet {
    Small(SmallVec<(u32, u32), 4>),
    Huge(RawTable<(u32, u32)>),
}

impl Default for InternalSet {
    fn default() -> Self {
        Self::Small(SmallVec::new())
    }
}

impl InternalSet {
    fn len(&self) -> usize {
        match self {
            InternalSet::Small(s) => s.len(),
            InternalSet::Huge(s) => s.len(),
        }
    }

    fn capacity(&self) -> usize {
        match self {
            InternalSet::Small(s) => s.capacity(),
            InternalSet::Huge(s) => s.capacity(),
        }
    }

    fn into_iter(self) -> impl Iterator<Item = (u32, u32)> {
        let iter: Box<dyn Iterator<Item = (u32, u32)>> = match self {
            InternalSet::Small(s) => Box::new(s.into_iter()),
            InternalSet::Huge(s) => Box::new(s.into_iter()),
        };
        iter
    }

    fn convert_to_huge_if_needed(&mut self, list: &mut AccList) {
        if let Self::Small(s) = self
            && s.len() >= 4
        {
            let mut huge = RawTable::default();

            for &mut pos_len in s {
                let raw = list.ref_raw(pos_len);
                let hash = acc_hash(raw);
                huge.insert(hash, pos_len, |&pos_len| acc_hash(list.ref_raw(pos_len)));
            }
            *self = Self::Huge(huge);
        }
    }
}

impl AccSet {
    pub fn mem_size(&self) -> usize {
        // mem size of internal set is estimated for faster computation
        self.list.mem_size() + self.set.capacity() * size_of::<u128>()
    }

    pub fn append(&mut self, value: &ScalarValue, nullable: bool) {
        let old_raw_len = self.list.raw.len();
        write_scalar(value, nullable, &mut self.list.raw).unwrap();
        self.append_raw_inline(old_raw_len);
    }

    pub fn merge(&mut self, other: &mut Self) {
        if self.set.len() < other.set.len() {
            // ensure the probed set is smaller
            std::mem::swap(self, other);
        }
        for pos_len in std::mem::take(&mut other.set).into_iter() {
            self.append_raw(other.list.ref_raw(pos_len));
        }
    }

    pub fn into_values(self, dt: DataType, nullable: bool) -> impl Iterator<Item = ScalarValue> {
        self.list.into_values(dt, nullable)
    }

    fn append_raw(&mut self, raw: &[u8]) {
        let new_len = raw.len();
        let new_pos_len = (self.list.raw.len() as u32, new_len as u32);

        match &mut self.set {
            InternalSet::Small(s) => {
                let mut found = false;
                for &mut pos_len in &mut *s {
                    if self.list.ref_raw(pos_len) == raw {
                        found = true;
                        break;
                    }
                }
                if !found {
                    s.push(new_pos_len);
                    self.list.raw.extend(raw);
                    self.set.convert_to_huge_if_needed(&mut self.list);
                }
            }
            InternalSet::Huge(s) => {
                let hash = acc_hash(raw);
                match s.find_or_find_insert_slot(
                    hash,
                    |&pos_len| new_len == pos_len.1 as usize && raw == self.list.ref_raw(pos_len),
                    |&pos_len| acc_hash(self.list.ref_raw(pos_len)),
                ) {
                    Ok(_found) => {}
                    Err(slot) => {
                        unsafe {
                            // safety: call unsafe `insert_in_slot` method
                            self.list.raw.extend(raw);
                            s.insert_in_slot(hash, slot, new_pos_len);
                        }
                    }
                }
            }
        }
    }

    fn append_raw_inline(&mut self, raw_start: usize) {
        let new_len = self.list.raw.len() - raw_start;
        let new_pos_len = (raw_start as u32, new_len as u32);
        let mut inserted = true;

        match &mut self.set {
            InternalSet::Small(s) => {
                for &mut pos_len in &mut *s {
                    if self.list.ref_raw(pos_len) == self.list.ref_raw(new_pos_len) {
                        inserted = false;
                        break;
                    }
                }
                if inserted {
                    s.push(new_pos_len);
                    self.set.convert_to_huge_if_needed(&mut self.list);
                }
            }
            InternalSet::Huge(s) => {
                let new_value = self.list.ref_raw(new_pos_len);
                let hash = acc_hash(new_value);
                match s.find_or_find_insert_slot(
                    hash,
                    |&pos_len| {
                        new_len == pos_len.1 as usize && new_value == self.list.ref_raw(pos_len)
                    },
                    |&pos_len| acc_hash(self.list.ref_raw(pos_len)),
                ) {
                    Ok(_found) => {
                        inserted = false;
                    }
                    Err(slot) => {
                        unsafe {
                            // safety: call unsafe `insert_in_slot` method
                            s.insert_in_slot(hash, slot, new_pos_len);
                        }
                    }
                }
            }
        }

        // remove the value from list if not inserted
        if !inserted {
            self.list.raw.truncate(raw_start);
        }
    }
}

#[inline]
fn acc_hash(value: impl AsRef<[u8]>) -> u64 {
    const ACC_HASH_SEED: u32 = 0x7BCB48DA;
    const HASHER: foldhash::fast::FixedState =
        foldhash::fast::FixedState::with_seed(ACC_HASH_SEED as u64);
    HASHER.hash_one(value.as_ref())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;

    use super::*;
    use crate::memmgr::spill::Spill;

    #[test]
    fn test_acc_set_append() {
        let mut acc_set = AccSet::default();
        let value1 = ScalarValue::Int32(Some(1));
        let value2 = ScalarValue::Int32(Some(2));

        acc_set.append(&value1, false);
        acc_set.append(&value2, false);

        assert_eq!(acc_set.list.raw.len(), 8); // 4 bytes for each int32
        assert_eq!(acc_set.set.len(), 2);
    }

    #[test]
    fn test_acc_set_merge() {
        let mut acc_set1 = AccSet::default();
        let mut acc_set2 = AccSet::default();
        let value1 = ScalarValue::Int32(Some(1));
        let value2 = ScalarValue::Int32(Some(2));
        let value3 = ScalarValue::Int32(Some(3));

        acc_set1.append(&value1, false);
        acc_set1.append(&value2, false);
        acc_set2.append(&value2, false);
        acc_set2.append(&value3, false);

        acc_set1.merge(&mut acc_set2);

        assert_eq!(acc_set1.list.raw.len(), 12); // 4 bytes for each int32
        assert_eq!(acc_set1.set.len(), 3);
    }

    #[test]
    fn test_acc_set_into_values() {
        let mut acc_set = AccSet::default();
        let value1 = ScalarValue::Int32(Some(1));
        let value2 = ScalarValue::Int32(Some(2));

        acc_set.append(&value1, false);
        acc_set.append(&value2, false);

        let values: Vec<ScalarValue> = acc_set.into_values(DataType::Int32, false).collect();
        assert_eq!(values, vec![value1, value2]);
    }

    #[test]
    fn test_acc_set_duplicate_append() {
        let mut acc_set = AccSet::default();
        let value1 = ScalarValue::Int32(Some(1));

        acc_set.append(&value1, false);
        acc_set.append(&value1, false);

        assert_eq!(acc_set.list.raw.len(), 4); // 4 bytes for one int32
        assert_eq!(acc_set.set.len(), 1);
    }

    #[test]
    fn test_acc_set_spill() {
        let mut acc_col = AccSetColumn::empty(DataType::Int32);
        acc_col.resize(3);
        acc_col.append_item(1, &ScalarValue::Int32(Some(1)));
        acc_col.append_item(1, &ScalarValue::Int32(Some(2)));
        acc_col.append_item(2, &ScalarValue::Int32(Some(3)));
        acc_col.append_item(2, &ScalarValue::Int32(Some(4)));
        acc_col.append_item(2, &ScalarValue::Int32(Some(5)));
        acc_col.append_item(2, &ScalarValue::Int32(Some(6)));
        acc_col.append_item(2, &ScalarValue::Int32(Some(7)));

        let mut spill: Box<dyn Spill> = Box::new(vec![]);
        let mut spill_writer = spill.get_compressed_writer();
        acc_col
            .spill(IdxSelection::Range(0, 3), &mut spill_writer)
            .unwrap();
        spill_writer.finish().unwrap();

        let mut acc_col_unspill = AccSetColumn::empty(DataType::Int32);
        acc_col_unspill
            .unspill(3, &mut spill.get_compressed_reader())
            .unwrap();

        assert_eq!(acc_col.take_values(0), acc_col_unspill.take_values(0));
        assert_eq!(acc_col.take_values(1), acc_col_unspill.take_values(1));
        assert_eq!(acc_col.take_values(2), acc_col_unspill.take_values(2));
    }
}
