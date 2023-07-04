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

use datafusion::common::{Result, ScalarValue};
use datafusion_ext_commons::io::{read_array, read_bytes_slice, read_data_type, read_len, write_array, write_data_type, write_len, write_u8};
use std::any::Any;
use std::collections::HashSet;
use std::io::{Cursor, Read, Write};
use std::mem::{size_of, size_of_val};
use arrow::array::Array;

#[derive(Eq, PartialEq)]
pub struct AggBuf {
    fixed: Box<[u8]>,
    dyns: Box<[Box<dyn AggDynValue>]>,
}

impl Clone for AggBuf {
    fn clone(&self) -> Self {
        Self {
            fixed: self.fixed.clone(),
            dyns: self.dyns.iter().map(|v| v.clone_boxed()).collect(),
        }
    }
}

#[allow(clippy::borrowed_box)]
impl AggBuf {
    pub fn mem_size(&self) -> usize {
        size_of::<Self>()
            + self.fixed.len()
            + self.dyns.iter().map(|v| size_of_val(v) + v.mem_size()).sum::<usize>()
    }

    pub fn is_fixed_valid(&self, addr: u64) -> bool {
        let idx = get_fixed_addr_valid_idx(addr);
        self.fixed[self.fixed.len() - 1 - idx / 8] & (1 << (idx % 8)) != 0
    }

    pub fn set_fixed_valid(&mut self, addr: u64, valid: bool) {
        let idx = get_fixed_addr_valid_idx(addr);
        self.fixed[self.fixed.len() - 1 - idx / 8] |= (valid as u8) << (idx % 8);
    }

    pub fn fixed_value<T: Sized + Copy>(&self, addr: u64) -> T {
        let offset = get_fixed_addr_offset(addr);
        let tptr = self.fixed[offset..][..size_of::<T>()].as_ptr() as *const T;
        unsafe {
            std::ptr::read_unaligned(tptr)
        }
    }

    pub fn set_fixed_value<T: Sized + Copy>(&mut self, addr: u64, v: T) {
        let offset = get_fixed_addr_offset(addr);
        let tptr = self.fixed[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe {
            std::ptr::write_unaligned(tptr, v);
        }
    }

    pub fn update_fixed_value<T: Sized + Copy>(&mut self, addr: u64, updater: impl Fn(T) -> T) {
        let offset = get_fixed_addr_offset(addr);
        let tptr = self.fixed[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe {
            std::ptr::write_unaligned(tptr, updater(std::ptr::read_unaligned(tptr)))
        }
    }

    pub fn dyn_value(&mut self, addr: u64) -> &Box<dyn AggDynValue> {
        &self.dyns[get_dyn_addr_idx(addr)]
    }

    pub fn dyn_value_mut(&mut self, addr: u64) -> &mut Box<dyn AggDynValue> {
        &mut self.dyns[get_dyn_addr_idx(addr)]
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        r.read_exact(&mut self.fixed)?;
        if !self.dyns.is_empty() {
            let mut boxed: Box<dyn Read> = Box::new(&mut r);
            for v in self.dyns.as_mut() {
                v.load(&mut boxed)?;
            }
        }
        Ok(())
    }

    pub fn load_from_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.load(Cursor::new(bytes))
    }

    pub fn save(&mut self, mut w: impl Write) -> Result<()> {
        w.write_all(&self.fixed)?;
        if !self.dyns.is_empty() {
            let mut w: Box<dyn Write> = Box::new(&mut w);
            for v in self.dyns.as_mut() {
                v.save(&mut w)?;
            }
        }
        Ok(())
    }

    pub fn save_to_bytes(&mut self) -> Result<Box<[u8]>> {
        let mut bytes = vec![];
        let mut write: Box<dyn Write> = Box::new(Cursor::new(&mut bytes));
        self.save(&mut write)?;
        drop(write);
        Ok(bytes.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccumInitialValue {
    Scalar(ScalarValue),
    DynList,
    DynSet,
}

pub fn create_agg_buf_from_initial_value(values: &[AccumInitialValue]) -> Result<(AggBuf, Box<[u64]>)> {
    let mut fixed_count = 0;
    let mut fixed_valids = vec![];
    let mut fixed: Vec<u8> = vec![];
    let mut dyns: Vec<Box<dyn AggDynValue>> = vec![];
    let mut addrs: Vec<u64> = vec![];

    macro_rules! handle_fixed {
        ($v:expr, $nbytes:expr) => {{
            addrs.push(make_fixed_addr(fixed_count, fixed.len()));
            if fixed_count % 8 == 0 {
                fixed_valids.push(0);
            }
            match $v {
                Some(v) => {
                    fixed_valids[fixed_count / 8] |= 1 << (fixed_count % 8);
                    fixed.extend(v.to_ne_bytes());
                }
                None => {
                    fixed.extend(&[0; $nbytes]);
                }
            }
            fixed_count += 1;
        }};
    }
    for value in values {
        match value {
            AccumInitialValue::Scalar(scalar) => match scalar {
                ScalarValue::Null => handle_fixed!(None::<u8>, 0),
                ScalarValue::Boolean(v) => handle_fixed!(v.map(|x| x as u8), 1),
                ScalarValue::Float32(v) => handle_fixed!(v, 4),
                ScalarValue::Float64(v) => handle_fixed!(v, 8),
                ScalarValue::Decimal128(v, _, _) => handle_fixed!(v, 16),
                ScalarValue::Int8(v) => handle_fixed!(v, 1),
                ScalarValue::Int16(v) => handle_fixed!(v, 2),
                ScalarValue::Int32(v) => handle_fixed!(v, 4),
                ScalarValue::Int64(v) => handle_fixed!(v, 8),
                ScalarValue::UInt8(v) => handle_fixed!(v, 1),
                ScalarValue::UInt16(v) => handle_fixed!(v, 2),
                ScalarValue::UInt32(v) => handle_fixed!(v, 4),
                ScalarValue::UInt64(v) => handle_fixed!(v, 8),
                ScalarValue::Date32(v) => handle_fixed!(v, 4),
                ScalarValue::Date64(v) => handle_fixed!(v, 8),
                ScalarValue::TimestampSecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::TimestampMillisecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::TimestampMicrosecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::TimestampNanosecond(v, _) => handle_fixed!(v, 8),
                ScalarValue::Utf8(v) => {
                    addrs.push(make_dyn_addr(dyns.len()));
                    dyns.push(Box::new(AggDynStr::new(v.clone().map(|v| v.into()))));
                }
                ScalarValue::Binary(v) => {
                    addrs.push(make_dyn_addr(dyns.len()));
                    dyns.push(Box::new(AggDynBinary::new(v.clone().map(|v| v.into()))));
                }
                other => {
                    addrs.push(make_dyn_addr(dyns.len()));
                    dyns.push(Box::new(AggDynScalar::new(other.clone())));
                }
            }
            AccumInitialValue::DynList => {
                addrs.push(make_dyn_addr(dyns.len()));
                dyns.push(Box::new(AggDynList::default()));
            }
            AccumInitialValue::DynSet => {
                addrs.push(make_dyn_addr(dyns.len()));
                dyns.push(Box::new(AggDynSet::default()));
            }
        }
    }

    // reverse fixed_valids and append it to fixed, so no need to change addrs
    fixed_valids.reverse();
    fixed.extend(fixed_valids);

    let agg_buf = AggBuf {
        fixed: fixed.into(),
        dyns: dyns.into(),
    };
    Ok((agg_buf, addrs.into()))
}

#[allow(clippy::borrowed_box)]
pub trait AggDynValue: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn mem_size(&self) -> usize;
    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool;
    fn default_boxed(&self) -> Box<dyn AggDynValue>;
    fn clone_boxed(&self) -> Box<dyn AggDynValue>;
}

impl Eq for Box<dyn AggDynValue> {}
impl PartialEq for Box<dyn AggDynValue> {
    fn eq(&self, other: &Self) -> bool {
        self.eq_boxed(other)
    }
}

impl dyn AggDynValue {
    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        macro_rules! handle_dyn_type {
            ($ty:ty) => {{
                if let Some(d) = self.as_any_mut().downcast_mut::<$ty>() {
                    return d.load(&mut r);
                }
            }};
        }
        handle_dyn_type!(AggDynScalar);
        handle_dyn_type!(AggDynBinary);
        handle_dyn_type!(AggDynStr);
        handle_dyn_type!(AggDynList);
        handle_dyn_type!(AggDynSet);
        unreachable!("unknown dyn value")
    }

    pub fn save(&mut self, mut w: impl Write) -> Result<()> {
        macro_rules! handle_dyn_type {
            ($ty:ty) => {{
                if let Some(d) = self.as_any_mut().downcast_mut::<$ty>() {
                    return d.save(&mut w);
                }
            }};
        }
        handle_dyn_type!(AggDynScalar);
        handle_dyn_type!(AggDynBinary);
        handle_dyn_type!(AggDynStr);
        handle_dyn_type!(AggDynList);
        handle_dyn_type!(AggDynSet);
        unreachable!("unknown dyn value")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynScalar {
    pub value: ScalarValue,
}

#[allow(clippy::borrowed_box)]
impl AggDynScalar {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }

    pub fn value(value: &Box<dyn AggDynValue>) -> &ScalarValue {
        &value.as_any().downcast_ref::<Self>().unwrap().value
    }

    pub fn value_mut(value: &mut Box<dyn AggDynValue>) -> &mut ScalarValue {
        &mut value.as_any_mut().downcast_mut::<Self>().unwrap().value
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let col = read_array(&mut r, &self.value.get_datatype(), 1)?;
        self.value = ScalarValue::try_from_array(&col, 0)?;
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        let col = self.value.to_array();
        write_array(&col, &mut w)?;
        Ok(())
    }
}

impl AggDynValue for AggDynScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value.size()
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(Self { value: ScalarValue::Null })
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynBinary {
    pub value: Option<Box<[u8]>>,
}

#[allow(clippy::borrowed_box)]
impl AggDynBinary {
    pub fn new(value: Option<Box<[u8]>>) -> Self {
        Self { value }
    }

    pub fn value(value: &Box<dyn AggDynValue>) -> &Option<Box<[u8]>> {
        &value.as_any().downcast_ref::<Self>().unwrap().value
    }

    pub fn value_mut(value: &mut Box<dyn AggDynValue>) -> &mut Option<Box<[u8]>> {
        &mut value.as_any_mut().downcast_mut::<Self>().unwrap().value
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let len = read_len(&mut r)?;
        if len > 0 {
            let len = len - 1;
            self.value = Some(read_bytes_slice(&mut r, len)?);
        } else {
            self.value = None;
        }
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        match &self.value {
            Some(v) => {
                write_len(1 + v.len(), &mut w)?;
                w.write_all(&v)?;
            }
            None => {
                write_u8(0, &mut w)?;
            }
        }
        Ok(())
    }
}

impl AggDynValue for AggDynBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(Self { value: None })
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynStr {
    pub value: Option<Box<str>>,
}

#[allow(clippy::borrowed_box)]
impl AggDynStr {
    pub fn new(value: Option<Box<str>>) -> Self {
        Self { value }
    }

    pub fn value(value: &Box<dyn AggDynValue>) -> &Option<Box<str>> {
        &value.as_any().downcast_ref::<Self>().unwrap().value
    }

    pub fn value_mut(value: &mut Box<dyn AggDynValue>) -> &mut Option<Box<str>> {
        &mut value.as_any_mut().downcast_mut::<Self>().unwrap().value
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let len = read_len(&mut r)?;
        if len > 0 {
            let len = len - 1;
            let value_buf = read_bytes_slice(&mut r, len)?;
            let value = String::from_utf8_lossy(&value_buf);
            self.value = Some(value.into());
        } else {
            self.value = None;
        }
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        match &self.value {
            Some(v) => {
                write_len(1 + v.len(), &mut w)?;
                w.write_all(v.as_bytes())?;
            }
            None => {
                write_u8(0, &mut w)?;
            }
        }
        Ok(())
    }
}

impl AggDynValue for AggDynStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(Self { value: None })
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynList {
    pub values: Vec<ScalarValue>,
}

impl AggDynList {
    pub fn append(&mut self, value: ScalarValue) {
        self.values.push(value);
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.values.append(other.values.as_mut());
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let array_len = read_len(&mut r)?;
        if array_len > 0 {
            let dt = read_data_type(&mut r)?;
            let array = read_array(&mut r, &dt, array_len)?;
            self.values = (0..array.len())
                .map(|i| ScalarValue::try_from_array(&array, i))
                .collect::<Result<_>>()?;
        } else {
            self.values.clear();
        }
        Ok(())
    }

    pub fn save(&mut self, mut w: impl Write) -> Result<()> {
        if self.values.is_empty() {
            write_len(0, &mut w)?;
        } else {
            let array = ScalarValue::iter_to_array(std::mem::take(&mut self.values).into_iter())?;
            write_len(array.len(), &mut w)?;
            write_data_type(array.data_type(), &mut w)?;
            write_array(&array, &mut w)?;
        }
        Ok(())
    }
}

impl AggDynValue for AggDynList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + ScalarValue::size_of_vec(&self.values)
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::<AggDynList>::default()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynSet {
    pub values: HashSet<ScalarValue>,
}

impl AggDynSet {
    pub fn append(&mut self, value: ScalarValue) {
        self.values.insert(value);
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.values.extend(std::mem::take(other).values.into_iter());
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let array_len = read_len(&mut r)?;
        if array_len > 0 {
            let dt = read_data_type(&mut r)?;
            let array = read_array(&mut r, &dt, array_len)?;
            self.values = (0..array.len())
                .map(|i| ScalarValue::try_from_array(&array, i))
                .collect::<Result<_>>()?;
        } else {
            self.values.clear();
        }
        Ok(())
    }

    pub fn save(&mut self, mut w: impl Write) -> Result<()> {
        if self.values.is_empty() {
            write_len(0, &mut w)?;
        } else {
            let array = ScalarValue::iter_to_array(std::mem::take(&mut self.values).into_iter())?;
            write_len(array.len(), &mut w)?;
            write_data_type(array.data_type(), &mut w)?;
            write_array(&array, &mut w)?;
        }
        Ok(())
    }
}

impl AggDynValue for AggDynSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + ScalarValue::size_of_hashset(&self.values)
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::<AggDynSet>::default()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[inline]
fn get_fixed_addr_offset(addr: u64) -> usize {
    (addr & 0x0000_0000_ffff_ffff) as usize
}

#[inline]
fn get_fixed_addr_valid_idx(addr: u64) -> usize {
    ((addr & 0x7fff_ffff_0000_0000) >> 32) as usize
}

#[inline]
fn get_dyn_addr_idx(addr: u64) -> usize {
    (addr & 0x7fff_ffff_ffff_ffff) as usize
}

#[inline]
fn make_fixed_addr(valid_idx: usize, offset: usize) -> u64 {
    (valid_idx as u64) << 32 | (offset as u64)
}

#[inline]
fn make_dyn_addr(idx: usize) -> u64 {
    (idx as u64) | 0x8000_0000_0000_0000
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::io::Cursor;
    use arrow::datatypes::DataType;
    use datafusion::common::{Result, ScalarValue};
    use crate::agg::agg_buf::{create_agg_buf_from_initial_value, AggDynList, AggDynSet, AggDynStr, AccumInitialValue};

    #[test]
    fn test_dyn_list() {
        let mut dyn_list = AggDynList::default();
        dyn_list.append(ScalarValue::from(1i32));
        dyn_list.append(ScalarValue::from(2i32));
        dyn_list.append(ScalarValue::from(3i32));

        let mut buf = vec![];
        dyn_list.save(&mut Cursor::new(&mut buf)).unwrap();

        let mut dyn_list = AggDynList::default();
        dyn_list.load(&mut Cursor::new(&mut buf)).unwrap();
        assert_eq!(dyn_list.values, vec![
            ScalarValue::from(1i32),
            ScalarValue::from(2i32),
            ScalarValue::from(3i32),
        ]);
    }

    #[test]
    fn test_dyn_set() {
        let mut dyn_set = AggDynSet::default();
        dyn_set.append(ScalarValue::from(1i32));
        dyn_set.append(ScalarValue::from(2i32));
        dyn_set.append(ScalarValue::from(3i32));
        dyn_set.append(ScalarValue::from(2i32));

        let mut buf = vec![];
        dyn_set.save(&mut Cursor::new(&mut buf)).unwrap();

        let mut dyn_set = AggDynSet::default();
        dyn_set.load(&mut Cursor::new(&mut buf)).unwrap();

        assert_eq!(dyn_set.values, HashSet::from_iter(vec![
            ScalarValue::from(1i32),
            ScalarValue::from(2i32),
            ScalarValue::from(3i32),
        ].into_iter()));
    }

    #[test]
    fn test_agg_buf() {
        let data_types = vec![
            DataType::Null,
            DataType::Int32,
            DataType::Int64,
            DataType::Utf8,
        ];
        let scalars = data_types
            .iter()
            .map(|dt: &DataType| Ok(AccumInitialValue::Scalar(dt.clone().try_into()?)))
            .collect::<Result<Vec<AccumInitialValue>>>()
            .unwrap();

        let (mut agg_buf, addrs) = create_agg_buf_from_initial_value(&scalars).unwrap();
        assert!(!agg_buf.is_fixed_valid(addrs[0]));
        assert!(!agg_buf.is_fixed_valid(addrs[1]));
        assert!(!agg_buf.is_fixed_valid(addrs[2]));

        // set values
        let mut agg_buf_valued = agg_buf.clone();
        agg_buf_valued.set_fixed_value(addrs[1], 123456789_i32);
        agg_buf_valued.set_fixed_value(addrs[2], 1234567890123456789_i64);
        agg_buf_valued.set_fixed_valid(addrs[1], true);
        agg_buf_valued.set_fixed_valid(addrs[2], true);
        *AggDynStr::value_mut(agg_buf_valued.dyn_value_mut(addrs[3])) =
            Some("test".to_string().into());

        // save + load
        let bytes = agg_buf_valued.save_to_bytes().unwrap();
        agg_buf.load_from_bytes(&bytes).unwrap();

        assert!(agg_buf_valued == agg_buf);
        assert!(!agg_buf.is_fixed_valid(addrs[0]));
        assert!(agg_buf.is_fixed_valid(addrs[1]));
        assert!(agg_buf.is_fixed_valid(addrs[2]));
        assert_eq!(agg_buf.fixed_value::<i32>(addrs[1]), 123456789_i32);
        assert_eq!(agg_buf.fixed_value::<i64>(addrs[2]), 1234567890123456789_i64);
        assert_eq!(
            *AggDynStr::value(agg_buf.dyn_value(addrs[3])),
            Some("test".to_string().into()),
        );
    }
}
