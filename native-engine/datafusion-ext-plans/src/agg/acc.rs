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
    io::{Cursor, Read, Write},
    sync::Arc,
};

use arrow::{
    array::*,
    datatypes::{DataType, *},
};
use bitvec::vec::BitVec;
use byteorder::{ReadBytesExt, WriteBytesExt};
use datafusion::{
    common::{Result, ScalarValue},
    error::DataFusionError,
};
use datafusion_ext_commons::{
    assume,
    io::{read_bytes_into_vec, read_bytes_slice, read_len, read_scalar, write_len, write_scalar},
    unchecked,
};
use smallvec::SmallVec;

use crate::{
    agg::agg::IdxSelection,
    common::SliceAsRawBytes,
    idx_for,
    memmgr::spill::{SpillCompressedReader, SpillCompressedWriter},
};

pub trait AccColumn: Send {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn resize(&mut self, len: usize);
    fn shrink_to_fit(&mut self);
    fn num_records(&self) -> usize;
    fn mem_used(&self) -> usize;
    fn freeze_to_rows(&self, idx: IdxSelection<'_>, array: &mut [Vec<u8>]) -> Result<()>;
    fn unfreeze_from_rows(&mut self, array: &[&[u8]], offsets: &mut [usize]) -> Result<()>;
    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()>;
    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()>;
}

pub type AccColumnRef = Box<dyn AccColumn>;

pub type AccBytes = SmallVec<u8, 24>;
const _ACC_BYTES_SIZE_CHECKER: [(); 32] = [(); size_of::<AccBytes>()];

pub struct AccTable {
    cols: Vec<AccColumnRef>,
}

impl AccTable {
    pub fn new(cols: Vec<Box<dyn AccColumn>>, num_records: usize) -> Self {
        assert!(cols.iter().all(|c| c.num_records() == num_records));
        Self { cols }
    }

    pub fn cols(&self) -> &[AccColumnRef] {
        &self.cols
    }

    pub fn cols_mut(&mut self) -> &mut [AccColumnRef] {
        &mut self.cols
    }

    pub fn resize(&mut self, num_records: usize) {
        self.cols.iter_mut().for_each(|c| c.resize(num_records));
    }

    pub fn shrink_to_fit(&mut self) {
        self.cols.iter_mut().for_each(|c| c.shrink_to_fit());
    }

    pub fn mem_size(&self) -> usize {
        self.cols.iter().map(|c| c.mem_used()).sum()
    }
}

pub enum AccGenericColumn {
    Prim {
        raw: Vec<i128>, // for minimum alignment of Decimal128
        valids: BitVec,
        prim_size: usize,
    },
    Bytes {
        items: Vec<Option<AccBytes>>,
        heap_mem_used: usize,
    },
    Scalar {
        items: Vec<ScalarValue>,
        dt: DataType,
        heap_mem_used: usize,
    },
}

impl AccGenericColumn {
    pub fn new(dt: &DataType, num_rows: usize) -> Self {
        let mut new = Self::new_empty(dt);
        new.resize(num_rows);
        new
    }

    pub fn new_empty(dt: &DataType) -> Self {
        match dt {
            _ if dt == &DataType::Null || dt == &DataType::Boolean || dt.is_primitive() => {
                AccGenericColumn::Prim {
                    raw: Default::default(),
                    valids: Default::default(),
                    prim_size: match dt {
                        DataType::Null => 0,
                        DataType::Boolean => 1,
                        primitive => primitive.primitive_width().unwrap(),
                    },
                }
            }
            DataType::Utf8 | DataType::Binary => AccGenericColumn::Bytes {
                items: Default::default(),
                heap_mem_used: 0,
            },
            _ => AccGenericColumn::Scalar {
                items: Default::default(),
                dt: dt.clone(),
                heap_mem_used: 0,
            },
        }
    }

    pub fn prim_valid(&self, idx: usize) -> bool {
        assume!(matches!(self, AccGenericColumn::Prim { .. }));
        match self {
            AccGenericColumn::Prim { valids, .. } => unsafe {
                // safety: performance critical, disable bounds checking
                *valids.get_unchecked(idx)
            },
            _ => unreachable!("expect primitive values"),
        }
    }

    pub fn set_prim_valid(&mut self, idx: usize, valid: bool) {
        assume!(matches!(self, AccGenericColumn::Prim { .. }));
        match self {
            AccGenericColumn::Prim { valids, .. } => unsafe {
                // safety: performance critical, disable bounds checking
                valids.set_unchecked(idx, valid)
            },
            _ => unreachable!("expect primitive values"),
        }
    }

    pub fn prim_value<T: Copy>(&self, idx: usize) -> T {
        assume!(matches!(self, AccGenericColumn::Prim { .. }));
        match self {
            AccGenericColumn::Prim { raw, .. } => unsafe {
                // safety: ptr is aligned
                let ptr_base = raw.as_ptr() as *mut T;
                *ptr_base.wrapping_add(idx)
            },
            _ => panic!("expect primitive values"),
        }
    }

    pub fn set_prim_value<T: Copy>(&mut self, idx: usize, v: T) {
        assume!(matches!(self, AccGenericColumn::Prim { .. }));
        match self {
            AccGenericColumn::Prim { raw, .. } => unsafe {
                // safety: ptr is aligned
                let ptr_base = raw.as_mut_ptr() as *mut T;
                *ptr_base.wrapping_add(idx) = v;
            },
            _ => panic!("expect primitive values"),
        }
    }

    pub fn update_prim_value<T: Copy>(&mut self, idx: usize, f: impl FnOnce(&mut T)) {
        assume!(matches!(self, AccGenericColumn::Prim { .. }));
        match self {
            AccGenericColumn::Prim { raw, .. } => unsafe {
                // safety: ptr is aligned
                let ptr_base = raw.as_mut_ptr() as *mut T;
                f(&mut *ptr_base.wrapping_add(idx))
            },
            _ => panic!("expect primitive values"),
        }
    }

    pub fn bytes_value(&self, idx: usize) -> Option<&AccBytes> {
        assume!(matches!(self, AccGenericColumn::Bytes { .. }));
        match self {
            AccGenericColumn::Bytes { items, .. } => unsafe {
                // safety: performance critical, disable bounds checking
                items.get_unchecked(idx).as_ref()
            },
            _ => panic!("expect bytes values"),
        }
    }

    pub fn bytes_value_mut(&mut self, idx: usize) -> Option<&mut AccBytes> {
        assume!(matches!(self, AccGenericColumn::Bytes { .. }));
        match self {
            AccGenericColumn::Bytes { items, .. } => unsafe {
                // safety: performance critical, disable bounds checking
                items.get_unchecked_mut(idx).as_mut()
            },
            _ => panic!("expect bytes values"),
        }
    }

    pub fn set_bytes_value(&mut self, idx: usize, v: Option<AccBytes>) {
        assume!(matches!(self, AccGenericColumn::Bytes { .. }));
        match self {
            AccGenericColumn::Bytes { items, .. } => unchecked!(items)[idx] = v,
            _ => panic!("expect bytes values"),
        }
    }

    pub fn take_bytes_value(&mut self, idx: usize) -> Option<AccBytes> {
        assume!(matches!(self, AccGenericColumn::Bytes { .. }));
        match self {
            AccGenericColumn::Bytes { items, .. } => std::mem::take(&mut unchecked!(items)[idx]),
            _ => panic!("expect bytes values"),
        }
    }

    pub fn add_bytes_mem_used(&mut self, mem_used: usize) {
        match self {
            AccGenericColumn::Bytes { heap_mem_used, .. } => *heap_mem_used += mem_used,
            _ => panic!("expect bytes values"),
        }
    }

    pub fn scalar_values(&self) -> &[ScalarValue] {
        match self {
            AccGenericColumn::Scalar { items, .. } => items,
            _ => panic!("expect scalar values"),
        }
    }

    pub fn scalar_values_mut(&mut self) -> &mut [ScalarValue] {
        match self {
            AccGenericColumn::Scalar { items, .. } => items,
            _ => panic!("expect scalar values"),
        }
    }

    pub fn add_scalar_mem_used(&mut self, mem_used: usize) {
        match self {
            AccGenericColumn::Scalar { heap_mem_used, .. } => *heap_mem_used += mem_used,
            _ => panic!("expect scalar values"),
        }
    }

    pub fn to_array(&mut self, idx: IdxSelection, dt: &DataType) -> Result<ArrayRef> {
        macro_rules! handle_prim {
            ($ty:ident) => {{
                type TBuilder = paste::paste! {[<$ty Builder>]};
                let mut builder = TBuilder::with_capacity(idx.len());
                idx_for! {
                    (idx in idx) => {
                        builder.append_option(self
                            .prim_valid(idx)
                            .then(|| self.prim_value(idx)));
                    }
                }
                Ok::<ArrayRef, DataFusionError>(Arc::new(builder.finish()))
            }};
        }

        match self {
            AccGenericColumn::Prim { .. } => match dt {
                DataType::Null => Ok(Arc::new(NullArray::new(idx.len()))),
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
                DataType::Time32(time_unit) => match time_unit {
                    TimeUnit::Second => handle_prim!(Time32Second),
                    TimeUnit::Millisecond => handle_prim!(Time32Millisecond),
                    _ => panic!("unsupported data type: {dt:?}"),
                },
                DataType::Time64(time_unit) => match time_unit {
                    TimeUnit::Microsecond => handle_prim!(Time64Microsecond),
                    TimeUnit::Nanosecond => handle_prim!(Time64Nanosecond),
                    _ => panic!("unsupported data type: {dt:?}"),
                },
                DataType::Decimal128(precision, scale) => Ok(Arc::new(
                    handle_prim!(Decimal128)?
                        .as_primitive::<Decimal128Type>()
                        .clone()
                        .with_precision_and_scale(*precision, *scale)?,
                )),
                _ => panic!("unsupported data type: {dt:?}"),
            },
            AccGenericColumn::Bytes { items, .. } => match dt {
                DataType::Utf8 => {
                    let mut string_builder = StringBuilder::with_capacity(idx.len(), 0);
                    idx_for! {
                        (idx in idx) => {
                            match &items[idx] {
                                Some(bytes) => {
                                    string_builder.append_value(unsafe {
                                        std::str::from_utf8_unchecked(bytes.as_ref())
                                    })
                                }
                                None => string_builder.append_null(),
                            }
                        }
                    }
                    Ok(Arc::new(string_builder.finish()))
                }
                DataType::Binary => {
                    let mut binary_builder = BinaryBuilder::with_capacity(idx.len(), 0);
                    idx_for! {
                        (idx in idx) => {
                            match &items[idx] {
                                Some(bytes) => binary_builder.append_value(bytes.as_ref()),
                                None => binary_builder.append_null(),
                            }
                        }
                    }
                    Ok(Arc::new(binary_builder.finish()))
                }
                _ => panic!("unsupported data type: {dt:?}"),
            },
            AccGenericColumn::Scalar { items, .. } => {
                let mut scalars = Vec::with_capacity(idx.len());
                idx_for! {
                    (idx in idx) => {
                        scalars.push(std::mem::replace(&mut items[idx], ScalarValue::Null));
                    }
                }
                Ok(ScalarValue::iter_to_array(scalars).expect("unsupported data type: {dt:?}"))
            }
        }
    }

    pub fn items_heap_mem_used(&self, idx: IdxSelection) -> usize {
        let mut heap_mem_used = 0;
        match self {
            AccGenericColumn::Prim { .. } => {}
            AccGenericColumn::Bytes { items, .. } => {
                idx_for! {
                    (idx in idx) => {
                        if let Some(item) = &items[idx] && item.spilled() {
                            heap_mem_used += item.len();
                        }
                    }
                }
            }
            AccGenericColumn::Scalar { items, .. } => {
                idx_for! {
                    (idx in idx) => {
                        heap_mem_used += items[idx].size() - size_of::<ScalarValue>();
                    }
                }
            }
        }
        heap_mem_used
    }

    pub fn add_heap_mem_used(&mut self, heap_mem_used: usize) {
        match self {
            AccGenericColumn::Prim { .. } => {}
            AccGenericColumn::Bytes {
                heap_mem_used: heap_mem_used_ref,
                ..
            } => {
                *heap_mem_used_ref += heap_mem_used;
            }
            AccGenericColumn::Scalar {
                heap_mem_used: heap_mem_used_ref,
                ..
            } => {
                *heap_mem_used_ref += heap_mem_used;
            }
        }
    }
}

impl AccColumn for AccGenericColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn resize(&mut self, len: usize) {
        match self {
            &mut AccGenericColumn::Prim {
                ref mut raw,
                ref mut valids,
                prim_size,
            } => {
                unsafe {
                    // resize without initialization to zeroes
                    let new_len = (prim_size * len + 15) / 16;
                    if new_len > raw.len() {
                        raw.reserve(new_len - raw.len());
                        raw.set_len(new_len);
                    } else {
                        raw.truncate(new_len);
                        raw.set_len(new_len);
                    }
                }
                valids.resize(len, false);
            }
            AccGenericColumn::Bytes {
                items,
                heap_mem_used,
            } => {
                for idx in len..items.len() {
                    if let Some(bytes) = &items[idx]
                        && bytes.spilled()
                    {
                        *heap_mem_used -= bytes.len();
                    }
                }
                items.resize(len, None);
            }
            AccGenericColumn::Scalar {
                items,
                dt,
                heap_mem_used,
            } => {
                for idx in len..items.len() {
                    *heap_mem_used -= items[idx].size() - size_of::<ScalarValue>();
                }
                items.resize_with(len, || {
                    ScalarValue::try_from(&*dt).expect("unsupported data type: {dt:?}")
                });
            }
        }
    }

    fn shrink_to_fit(&mut self) {
        match self {
            AccGenericColumn::Prim { raw, valids, .. } => {
                raw.shrink_to_fit();
                valids.shrink_to_fit();
            }
            AccGenericColumn::Bytes { items, .. } => items.shrink_to_fit(),
            AccGenericColumn::Scalar { items, .. } => items.shrink_to_fit(),
        }
    }

    fn num_records(&self) -> usize {
        match self {
            AccGenericColumn::Prim { valids, .. } => valids.len(),
            AccGenericColumn::Bytes { items, .. } => items.len(),
            AccGenericColumn::Scalar { items, .. } => items.len(),
        }
    }

    fn mem_used(&self) -> usize {
        match self {
            AccGenericColumn::Prim { raw, valids, .. } => raw.capacity() + valids.capacity() / 8,
            AccGenericColumn::Bytes {
                items,
                heap_mem_used,
                ..
            } => heap_mem_used + items.capacity() * size_of::<Option<AccBytes>>(),
            AccGenericColumn::Scalar {
                items,
                heap_mem_used,
                ..
            } => heap_mem_used + items.capacity() * size_of::<ScalarValue>(),
        }
    }

    fn freeze_to_rows(&self, idx: IdxSelection, array: &mut [Vec<u8>]) -> Result<()> {
        let mut array_idx = 0;

        match self {
            &AccGenericColumn::Prim {
                ref raw,
                ref valids,
                prim_size,
            } => {
                idx_for! {
                    (i in idx) => {
                        let w = &mut array[array_idx];
                        if valids[i] {
                            w.write_u8(1)?;
                            w.write_all(&raw.as_raw_bytes()[prim_size * i..][..prim_size])?;
                        } else {
                            w.write_u8(0)?;
                        }
                        array_idx += 1;
                    }
                }
            }
            AccGenericColumn::Bytes { items, .. } => {
                idx_for! {
                    (i in idx) => {
                        let w = &mut array[array_idx];
                        if let Some(v) = &items[i] {
                            write_len(1 + v.len(), w)?;
                            w.write_all(v)?;
                        } else {
                            w.write_u8(0)?;
                        }
                        array_idx += 1;
                    }
                }
            }
            AccGenericColumn::Scalar { items, .. } => {
                idx_for! {
                    (i in idx) => {
                        let w = &mut array[array_idx];
                        write_scalar(&items[i], true, w)?;
                        array_idx += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn unfreeze_from_rows(&mut self, array: &[&[u8]], offsets: &mut [usize]) -> Result<()> {
        let mut idx = self.num_records();
        self.resize(idx + array.len());

        match self {
            &mut AccGenericColumn::Prim {
                ref mut raw,
                ref mut valids,
                prim_size,
            } => {
                for (data, offset) in array.iter().zip(offsets) {
                    let mut r = Cursor::new(data);
                    r.set_position(*offset as u64);

                    let valid = r.read_u8()?;
                    if valid == 1 {
                        r.read_exact(&mut raw.as_raw_bytes_mut()[prim_size * idx..][..prim_size])?;
                        valids.set(idx, true);
                    } else {
                        valids.set(idx, false);
                    }
                    *offset = r.position() as usize;
                    idx += 1;
                }
            }
            AccGenericColumn::Bytes {
                items,
                heap_mem_used,
            } => {
                for (data, offset) in array.iter().zip(offsets) {
                    let mut r = Cursor::new(data);
                    r.set_position(*offset as u64);

                    let len = read_len(&mut r)?;
                    if len > 0 {
                        let len = len - 1;
                        let bytes = AccBytes::from_vec({
                            let vec: Vec<u8> = read_bytes_slice(&mut r, len)?.into();
                            vec
                        });
                        if bytes.spilled() {
                            *heap_mem_used += bytes.len();
                        }
                        items[idx] = Some(bytes);
                    } else {
                        items[idx] = None;
                    }
                    *offset = r.position() as usize;
                    idx += 1;
                }
            }
            AccGenericColumn::Scalar {
                items,
                dt,
                heap_mem_used,
            } => {
                for (data, offset) in array.iter().zip(offsets) {
                    let mut r = Cursor::new(data);
                    r.set_position(*offset as u64);

                    items[idx] = read_scalar(&mut r, dt, true)?;
                    *heap_mem_used += items[idx].size() - size_of::<ScalarValue>();
                    *offset = r.position() as usize;
                    idx += 1;
                }
            }
        }
        Ok(())
    }

    fn spill(&self, idx: IdxSelection<'_>, w: &mut SpillCompressedWriter) -> Result<()> {
        match self {
            &AccGenericColumn::Prim {
                ref raw,
                ref valids,
                prim_size,
            } => {
                // write valids
                let mut bits: BitVec<u8> = BitVec::with_capacity(idx.len());
                idx_for! {
                    (idx in idx) => {
                        bits.push(valids[idx]);
                    }
                }
                w.write_all(bits.as_raw_slice())?;

                // write raw data
                idx_for! {
                    (idx in idx) => {
                        if valids[idx] {
                            w.write_all(&raw.as_raw_bytes()[prim_size * idx..][..prim_size])?;
                        }
                    }
                }
            }
            AccGenericColumn::Bytes { items, .. } => {
                idx_for! {
                    (i in idx) => {
                        if let Some(v) = &items[i] {
                            write_len(1 + v.len(), w)?;
                            w.write_all(v)?;
                        } else {
                            w.write_u8(0)?;
                        }
                    }
                }
            }
            AccGenericColumn::Scalar { items, .. } => {
                idx_for! {
                    (i in idx) => {
                        write_scalar(&items[i], true, w)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn unspill(&mut self, num_rows: usize, r: &mut SpillCompressedReader) -> Result<()> {
        let idx = self.num_records();
        self.resize(idx + num_rows);

        match self {
            &mut AccGenericColumn::Prim {
                ref mut raw,
                ref mut valids,
                prim_size,
            } => {
                let mut valid_buf = vec![];
                let valid_len = (num_rows + 7) / 8;
                read_bytes_into_vec(r, &mut valid_buf, valid_len)?;
                let unfreezed_valids = BitVec::<u8>::from_vec(valid_buf);
                valids.truncate(idx);
                valids.extend_from_bitslice(unfreezed_valids.as_bitslice());

                for i in idx..idx + num_rows {
                    if valids[i] {
                        r.read_exact(&mut raw.as_raw_bytes_mut()[prim_size * i..][..prim_size])?;
                    }
                }
            }
            AccGenericColumn::Bytes {
                items,
                heap_mem_used,
            } => {
                for i in idx..idx + num_rows {
                    let len = read_len(r)?;
                    if len > 0 {
                        let len = len - 1;
                        let bytes = read_bytes_slice(r, len)?.into();
                        items[i] = Some(AccBytes::from_vec(bytes));
                        *heap_mem_used += len;
                    } else {
                        items[i] = None;
                    }
                }
            }
            AccGenericColumn::Scalar {
                items,
                dt,
                heap_mem_used,
            } => {
                for i in idx..idx + num_rows {
                    items[i] = read_scalar(r, dt, true)?;
                    *heap_mem_used += items[i].size() - size_of::<ScalarValue>();
                }
            }
        }
        Ok(())
    }
}
