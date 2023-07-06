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

use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion_ext_commons::io::{read_bytes_slice, read_len, write_len, write_u8};
use hashbrown::HashSet;
use std::any::Any;
use std::hash::Hash;
use std::io::{Cursor, Read, Write};
use std::mem::size_of;

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
        std::mem::size_of::<Self>()
            + self.fixed.len()
            + self
                .dyns
                .iter()
                .map(|v| std::mem::size_of_val(v) + v.mem_size())
                .sum::<usize>()
    }

    pub fn is_fixed_valid(&self, addr: u64) -> bool {
        let idx = get_fixed_addr_valid_idx(addr);
        self.fixed[self.fixed.len() - 1 - idx / 8] & (1 << (idx % 8)) != 0
    }

    pub fn set_fixed_valid(&mut self, addr: u64, valid: bool) {
        let idx = get_fixed_addr_valid_idx(addr);
        self.fixed[self.fixed.len() - 1 - idx / 8] |= (valid as u8) << (idx % 8);
    }

    pub fn fixed_value<T: Sized + Copy>(&self, addr: u64) -> &T {
        let offset = get_fixed_addr_offset(addr);
        let tptr = self.fixed[offset..][..std::mem::size_of::<T>()].as_ptr() as *const T;
        unsafe { &*tptr }
    }

    pub fn fixed_value_mut<T: Sized + Copy>(&mut self, addr: u64) -> &mut T {
        let offset = get_fixed_addr_offset(addr);
        let tptr = self.fixed[offset..][..std::mem::size_of::<T>()].as_ptr() as *mut T;
        unsafe { &mut *tptr }
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

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        w.write_all(&self.fixed)?;
        if !self.dyns.is_empty() {
            let mut w: Box<dyn Write> = Box::new(&mut w);
            for v in self.dyns.as_ref() {
                v.save(&mut w)?;
            }
        }
        Ok(())
    }

    pub fn save_to_bytes(&self) -> Result<Box<[u8]>> {
        let mut bytes = vec![];
        let mut write: Box<dyn Write> = Box::new(Cursor::new(&mut bytes));
        self.save(&mut write)?;
        drop(write);
        Ok(bytes.into())
    }
}

pub fn create_agg_buf_from_scalar(values: &[ScalarValue]) -> Result<(AggBuf, Box<[u64]>)> {
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
                match v {
                    Some(v) => {
                        dyns.push(Box::new(AggDynStr::new(Some(v.clone().into()))));
                    }
                    None => {
                        dyns.push(Box::new(AggDynStr::new(None)));
                    }
                }
            }
            ScalarValue::List(_, field) => {
                macro_rules! handle_fixed_list {
                    ($ty:ty) => {{
                        dyns.push(match field.name().as_str() {
                            "collect_list" => Box::<AggDynList<$ty>>::default(),
                            "collect_set" => Box::<AggDynSet<$ty>>::default(),
                            _ => unreachable!(),
                        });
                    }};
                }
                macro_rules! handle_fixed_float_list {
                    ($ty:ty) => {{
                        dyns.push(match field.name().as_str() {
                            "collect_list" => Box::<AggDynList<$ty>>::default(),
                            "collect_set" => Box::<AggDynSet<[u8; size_of::<$ty>()]>>::default(),
                            _ => unreachable!(),
                        });
                    }};
                }
                addrs.push(make_dyn_addr(dyns.len()));
                match field.data_type() {
                    DataType::Int8 => handle_fixed_list!(i8),
                    DataType::Int16 => handle_fixed_list!(i16),
                    DataType::Int32 => handle_fixed_list!(i32),
                    DataType::Int64 => handle_fixed_list!(i64),
                    DataType::UInt8 => handle_fixed_list!(u8),
                    DataType::UInt16 => handle_fixed_list!(u16),
                    DataType::UInt32 => handle_fixed_list!(u32),
                    DataType::UInt64 => handle_fixed_list!(u64),
                    DataType::Float32 => handle_fixed_float_list!(f32),
                    DataType::Float64 => handle_fixed_float_list!(f64),
                    DataType::Date32 => handle_fixed_list!(i32),
                    DataType::Date64 => handle_fixed_list!(i64),
                    DataType::Timestamp(_, _) => handle_fixed_list!(i64),
                    DataType::Decimal128(_, _) => handle_fixed_list!(i128),
                    DataType::Utf8 => dyns.push(match field.name().as_str() {
                        "collect_list" => Box::<AggDynStrList>::default(),
                        "collect_set" => Box::<AggDynStrSet>::default(),
                        _ => unreachable!(),
                    }),
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "AggDynList now do not support type: {:?}",
                            field.data_type()
                        )));
                    }
                }
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported agg data type: {:?}",
                    other
                )));
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

        handle_dyn_type!(AggDynStr);
        handle_dyn_type!(AggDynStrList);
        handle_dyn_type!(AggDynStrSet);

        handle_dyn_type!(AggDynList<i8>);
        handle_dyn_type!(AggDynList<i16>);
        handle_dyn_type!(AggDynList<i32>);
        handle_dyn_type!(AggDynList<i64>);
        handle_dyn_type!(AggDynList<i128>);
        handle_dyn_type!(AggDynList<u8>);
        handle_dyn_type!(AggDynList<u16>);
        handle_dyn_type!(AggDynList<u32>);
        handle_dyn_type!(AggDynList<u64>);
        handle_dyn_type!(AggDynList<u128>);
        handle_dyn_type!(AggDynList<f32>);
        handle_dyn_type!(AggDynList<f64>);

        handle_dyn_type!(AggDynSet<i8>);
        handle_dyn_type!(AggDynSet<i16>);
        handle_dyn_type!(AggDynSet<i32>);
        handle_dyn_type!(AggDynSet<i64>);
        handle_dyn_type!(AggDynSet<i128>);
        handle_dyn_type!(AggDynSet<u8>);
        handle_dyn_type!(AggDynSet<u16>);
        handle_dyn_type!(AggDynSet<u32>);
        handle_dyn_type!(AggDynSet<u64>);
        handle_dyn_type!(AggDynSet<u128>);
        handle_dyn_type!(AggDynSet<[u8; 4]>);
        handle_dyn_type!(AggDynSet<[u8; 8]>);

        unreachable!("unknown dyn value")
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        macro_rules! handle_dyn_type {
            ($ty:ty) => {{
                if let Some(d) = self.as_any().downcast_ref::<$ty>() {
                    return d.save(&mut w);
                }
            }};
        }

        handle_dyn_type!(AggDynStr);
        handle_dyn_type!(AggDynStrList);
        handle_dyn_type!(AggDynStrSet);

        handle_dyn_type!(AggDynList<i8>);
        handle_dyn_type!(AggDynList<i16>);
        handle_dyn_type!(AggDynList<i32>);
        handle_dyn_type!(AggDynList<i64>);
        handle_dyn_type!(AggDynList<i128>);
        handle_dyn_type!(AggDynList<u8>);
        handle_dyn_type!(AggDynList<u16>);
        handle_dyn_type!(AggDynList<u32>);
        handle_dyn_type!(AggDynList<u64>);
        handle_dyn_type!(AggDynList<u128>);
        handle_dyn_type!(AggDynList<f32>);
        handle_dyn_type!(AggDynList<f64>);

        handle_dyn_type!(AggDynSet<i8>);
        handle_dyn_type!(AggDynSet<i16>);
        handle_dyn_type!(AggDynSet<i32>);
        handle_dyn_type!(AggDynSet<i64>);
        handle_dyn_type!(AggDynSet<i128>);
        handle_dyn_type!(AggDynSet<u8>);
        handle_dyn_type!(AggDynSet<u16>);
        handle_dyn_type!(AggDynSet<u32>);
        handle_dyn_type!(AggDynSet<u64>);
        handle_dyn_type!(AggDynSet<u128>);
        handle_dyn_type!(AggDynSet<[u8; 4]>);
        handle_dyn_type!(AggDynSet<[u8; 8]>);

        unreachable!("unknown dyn value")
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
        std::mem::size_of::<Self>() + self.value.as_ref().map(|s| s.len()).unwrap_or(0)
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(AggDynStr { value: None })
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynList<T: PartialEq + Clone + Copy + Default> {
    pub values: Vec<T>,
}

impl<T: PartialEq + Clone + Copy + Default + 'static> AggDynList<T> {
    pub fn append(&mut self, value: T) {
        self.values.push(value);
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.values.append(other.values.as_mut());
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let len = read_len(&mut r)?;
        if len > 0 {
            let bytes_length = (len - 1) * size_of::<T>();
            let bytes = read_bytes_slice(&mut r, bytes_length)?;
            let ptr = bytes.as_ptr() as *const T;
            let num_buf = unsafe { std::slice::from_raw_parts(ptr, len - 1) };
            self.values = num_buf.to_vec();
        } else {
            self.values = Vec::<T>::new();
        }
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        match &self.values.len() {
            0 => {
                write_u8(0, &mut w)?;
            }
            _ => {
                let ptr = self.values.as_ptr() as *const u8;
                let bytes = unsafe { std::slice::from_raw_parts(ptr, self.values.len() * size_of::<T>()) };

                write_len(self.values.len() + 1, &mut w)?;
                w.write_all(bytes)?;
            }
        }
        Ok(())
    }
}

impl<T: PartialEq + Copy + Clone + Default + Send + Sync + 'static> AggDynValue for AggDynList<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.values.len() * size_of::<T>()
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::<AggDynList<T>>::default()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynStrList {
    pub strs: String,
    pub lens: Vec<u32>,
}

impl AggDynStrList {
    pub fn append(&mut self, value: &str) {
        self.lens.push(value.len() as u32);
        self.strs.push_str(value);
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.lens.append(&mut other.lens);
        self.strs.extend(other.strs.drain(..));
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let prefix = read_len(&mut r)?;
        if prefix > 0 {
            let num_bytes = prefix - 1;
            let bytes = read_bytes_slice(&mut r, num_bytes)?;
            let strs = String::from_utf8_lossy(&bytes).to_string();

            // last len is not saved because it can be calculated from strs.len()
            let num_items = read_len(&mut r)?;
            let mut lens = Vec::with_capacity(num_items);
            for _ in 0..num_items - 1 {
                lens.push(read_len(&mut r)? as u32);
            }
            lens.push(strs.len() as u32 - lens.iter().sum::<u32>());

            self.strs = strs;
            self.lens = lens;
        } else {
            self.strs = String::new();
            self.lens.clear();
        }
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        match &self.lens.len() {
            0 => {
                write_len(0, &mut w)?;
            }
            _ => {
                write_len(self.strs.as_bytes().len() + 1, &mut w)?;
                w.write_all(self.strs.as_bytes())?;

                // last len is not saved because it can be calculated from strs.len()
                write_len(self.lens.len(), &mut w)?;
                for &len in &self.lens[..self.lens.len() - 1] {
                    write_len(len as usize, &mut w)?;
                }
            }
        }
        Ok(())
    }
}

impl AggDynValue for AggDynStrList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        let mut len = size_of::<Self>();
        len += self.strs.as_bytes().len() + self.lens.len() * 4;
        len
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::<AggDynStrList>::default()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynSet<T: PartialEq + Eq + Clone + Copy + Default + Hash + Send + Sync> {
    pub values: HashSet<T>,
}

impl<T: PartialEq + Eq + Copy + Clone + Default + Hash + Send + Sync + 'static> AggDynSet<T> {
    pub fn append(&mut self, value: T) {
        self.values.insert(value);
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.values.extend(std::mem::take(other).values.into_iter());
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let prefix = read_len(&mut r)?;
        if prefix > 0 {
            let num_items = prefix - 1;
            let bytes = read_bytes_slice(&mut r, num_items * size_of::<T>())?;
            let ptr = bytes.as_ptr() as *const T;
            let items = unsafe { std::slice::from_raw_parts(ptr, num_items) };
            self.values = HashSet::<T>::from_iter(items.iter().cloned());
        } else {
            self.values = HashSet::<T>::new();
        }
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        match &self.values.len() {
            0 => {
                write_len(0, &mut w)?;
            }
            _ => {
                write_len(self.values.len() + 1, &mut w)?;
                for value in &self.values {
                    let ptr = value as *const T as *const u8;
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, size_of::<T>()) };
                    w.write_all(bytes)?;
                }
            }
        }
        Ok(())
    }
}

impl<T: PartialEq + Eq + Copy + Clone + Default + Hash + Send + Sync + 'static> AggDynValue
    for AggDynSet<T>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>()
            + self
                .values.len() * size_of::<T>()
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::<AggDynSet<T>>::default()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct AggDynStrSet {
    pub strs: HashSet<Box<str>>,
    pub data_len: usize,
}

impl AggDynStrSet {
    pub fn append(&mut self, value: &str) {
        self.data_len += value.as_bytes().len();
        self.strs.insert(value.into());
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.data_len += other.data_len;
        self.strs.extend(std::mem::take(&mut other.strs).into_iter());
    }

    pub fn load(&mut self, mut r: impl Read) -> Result<()> {
        let prefix = read_len(&mut r)?;
        if prefix > 0 {
            self.data_len = 0;
            let num_items = prefix - 1;
            let mut set = HashSet::new();
            for _ in 0..num_items {
                let num_bytes = read_len(&mut r)?;
                let str_bytes = read_bytes_slice(&mut r, num_bytes)?;
                set.insert(String::from_utf8_lossy(&str_bytes).into());
            }
            self.strs = set;
        } else {
            self.data_len = 0;
            self.strs = HashSet::new();
        }
        Ok(())
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        match &self.strs.len() {
            0 => {
                write_len(0, &mut w)?;
            }
            _ => {
                write_len(self.strs.len() + 1, &mut w)?;
                for str in &self.strs {
                    write_len(str.len(), &mut w)?;
                    w.write_all(str.as_bytes())?;
                }
            }
        }
        Ok(())
    }
}

impl AggDynValue for AggDynStrSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        let mut len = size_of::<Self>();
        len += self.strs.capacity() * (size_of::<Box<str>>() + 1);
        len += self.data_len;
        len
    }

    fn eq_boxed(&self, that: &Box<dyn AggDynValue>) -> bool {
        match that.as_any().downcast_ref() {
            Some(that) => self.eq(that),
            None => false,
        }
    }

    fn default_boxed(&self) -> Box<dyn AggDynValue> {
        Box::<AggDynStrSet>::default()
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
    use crate::agg::agg_buf::{
        create_agg_buf_from_scalar, AggDynList, AggDynSet, AggDynStr, AggDynStrList, AggDynStrSet,
    };
    use arrow::datatypes::DataType;
    use datafusion::common::{Result, ScalarValue};
    use hashbrown::HashSet;
    use std::io::Cursor;

    #[test]
    fn test_dyn_list() {
        let mut dyn_list = AggDynList::<i32>::default();
        dyn_list.append(1);
        dyn_list.append(2);
        dyn_list.append(3);

        let mut buf = vec![];
        dyn_list.save(&mut Cursor::new(&mut buf)).unwrap();

        let mut dyn_list = AggDynList::<i32>::default();
        dyn_list.load(&mut Cursor::new(&mut buf)).unwrap();
        assert_eq!(dyn_list.values, Some(vec![1, 2, 3]));

        let mut dyn_list = AggDynStrList::default();
        dyn_list.append("Hello");
        dyn_list.append("World");
        dyn_list.append("你好");

        let mut buf = vec![];
        dyn_list.save(&mut Cursor::new(&mut buf)).unwrap();

        let mut dyn_list = AggDynStrList::default();
        dyn_list.load(&mut Cursor::new(&mut buf)).unwrap();
        assert_eq!(dyn_list.strs, Some("HelloWorld你好".to_owned()));
        assert_eq!(dyn_list.lens, vec![5, 5, 6]);
    }

    #[test]
    fn test_dyn_set() {
        let mut dyn_set = AggDynSet::<i32>::default();
        dyn_set.append(1);
        dyn_set.append(2);
        dyn_set.append(3);
        dyn_set.append(2);

        let mut buf = vec![];
        dyn_set.save(&mut Cursor::new(&mut buf)).unwrap();

        let mut dyn_set = AggDynSet::<i32>::default();
        dyn_set.load(&mut Cursor::new(&mut buf)).unwrap();
        assert_eq!(
            dyn_set.values,
            Some(HashSet::from_iter(vec![1, 2, 3].into_iter()))
        );

        let mut dyn_set = AggDynStrSet::default();
        dyn_set.append("Hello");
        dyn_set.append("你好");
        dyn_set.append("World");
        dyn_set.append("你好");

        let mut buf = vec![];
        dyn_set.save(&mut Cursor::new(&mut buf)).unwrap();

        let mut dyn_set = AggDynStrSet::default();
        dyn_set.load(&mut Cursor::new(&mut buf)).unwrap();
        assert_eq!(
            dyn_set.strs,
            Some(HashSet::from_iter(vec![
                "Hello".to_owned().into(),
                "World".to_owned().into(),
                "你好".to_owned().into(),
            ]))
        );
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
            .map(|dt: &DataType| dt.clone().try_into())
            .collect::<Result<Vec<ScalarValue>>>()
            .unwrap();

        let (mut agg_buf, addrs) = create_agg_buf_from_scalar(&scalars).unwrap();
        assert!(!agg_buf.is_fixed_valid(addrs[0]));
        assert!(!agg_buf.is_fixed_valid(addrs[1]));
        assert!(!agg_buf.is_fixed_valid(addrs[2]));

        // set values
        let mut agg_buf_valued = agg_buf.clone();
        *agg_buf_valued.fixed_value_mut::<i32>(addrs[1]) = 123456789;
        *agg_buf_valued.fixed_value_mut::<i64>(addrs[2]) = 1234567890123456789;
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
        assert_eq!(*agg_buf.fixed_value::<i32>(addrs[1]), 123456789);
        assert_eq!(*agg_buf.fixed_value::<i64>(addrs[2]), 1234567890123456789);
        assert_eq!(
            *AggDynStr::value(agg_buf.dyn_value(addrs[3])),
            Some("test".to_string().into()),
        );
    }
}
