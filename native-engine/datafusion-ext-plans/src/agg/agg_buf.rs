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

use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion_ext_commons::io::{read_bytes_slice, read_len, write_len, write_u8};
use std::any::Any;
use std::io::{Cursor, Read, Write};

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

impl AggBuf {
    pub fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.fixed.len()
            + self.dyns.iter().map(|v| {
                std::mem::size_of_val(v) + v.mem_size()
            }).sum::<usize>()
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

pub fn create_agg_buf_from_scalar(
    values: &[ScalarValue],
) -> Result<(AggBuf, Box<[u64]>)> {
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
            ScalarValue::Date32(v) => handle_fixed!(v, 4),
            ScalarValue::Date64(v) => handle_fixed!(v, 8),
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
        if let Some(dyn_str) = self.as_any_mut().downcast_mut::<AggDynStr>() {
            let len = read_len(&mut r)?;
            if len > 0 {
                let len = len - 1;
                let value_buf = read_bytes_slice(&mut r, len)?;
                let value = String::from_utf8_lossy(&value_buf);
                dyn_str.value = Some(value.into());
            } else {
                dyn_str.value = None;
            }
            Ok(())
        } else {
            unreachable!()
        }
    }

    pub fn save(&self, mut w: impl Write) -> Result<()> {
        if let Some(dyn_str) = self.as_any().downcast_ref::<AggDynStr>() {
            match &dyn_str.value {
                Some(v) => {
                    write_len(1 + v.len(), &mut w)?;
                    w.write_all(v.as_bytes())?;
                }
                None => {
                    write_u8(0, &mut w)?;
                }
            }
            Ok(())
        } else {
            unreachable!()
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynStr {
    pub value: Option<Box<str>>,
}

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
}

impl AggDynValue for AggDynStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mem_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.value.as_ref().map(|s| s.len()).unwrap_or(0)
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
    use crate::agg::agg_buf::{create_agg_buf_from_scalar, AggDynStr};
    use arrow::datatypes::DataType;
    use datafusion::common::{Result, ScalarValue};

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
        assert_eq!(agg_buf.is_fixed_valid(addrs[0]), false);
        assert_eq!(agg_buf.is_fixed_valid(addrs[1]), false);
        assert_eq!(agg_buf.is_fixed_valid(addrs[2]), false);

        // set values
        let mut agg_buf_valued = agg_buf.clone();
        *agg_buf_valued.fixed_value_mut::<i32>(addrs[1]) = 123456789;
        *agg_buf_valued.fixed_value_mut::<i64>(addrs[2]) = 1234567890123456789;
        agg_buf_valued.set_fixed_valid(addrs[1], true);
        agg_buf_valued.set_fixed_valid(addrs[2], true);
        *AggDynStr::value_mut(agg_buf_valued.dyn_value_mut(addrs[3])) =
            Some("test".to_string());

        // save + load
        let bytes = agg_buf_valued.save_to_bytes().unwrap();
        agg_buf.load_from_bytes(&bytes).unwrap();

        assert!(agg_buf_valued == agg_buf);
        assert_eq!(agg_buf.is_fixed_valid(addrs[0]), false);
        assert_eq!(agg_buf.is_fixed_valid(addrs[1]), true);
        assert_eq!(agg_buf.is_fixed_valid(addrs[2]), true);
        assert_eq!(*agg_buf.fixed_value::<i32>(addrs[1]), 123456789);
        assert_eq!(*agg_buf.fixed_value::<i64>(addrs[2]), 1234567890123456789);
        assert_eq!(
            *AggDynStr::value(agg_buf.dyn_value(addrs[3])),
            Some("test".to_string()),
        );
    }
}
