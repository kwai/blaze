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
    collections::HashSet,
    io::{Cursor, Read, Write},
    mem::{size_of, size_of_val},
};

use arrow::{
    array::{Array, NullArray},
    datatypes::DataType,
};
use datafusion::{
    common::{Result, ScalarValue},
    parquet::data_type::AsBytes,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any,
    io::{read_array, read_bytes_slice, read_len, write_array, write_len},
    slim_bytes::SlimBytes,
};
use slimmer_box::SlimmerBox;

pub type DynVal = Option<Box<dyn AggDynValue>>;

pub struct AccumStateRow {
    fixed: SlimBytes,
    dyns: Option<Box<[DynVal]>>,
}

impl Clone for AccumStateRow {
    fn clone(&self) -> Self {
        Self {
            fixed: self.fixed.clone(),
            dyns: self.dyns.as_ref().map(|dyns| {
                dyns.iter()
                    .map(|v| v.as_ref().map(|x| x.clone_boxed()))
                    .collect::<Box<[DynVal]>>()
            }),
        }
    }
}

#[allow(clippy::borrowed_box)]
impl AccumStateRow {
    pub fn mem_size(&self) -> usize {
        let dyns_mem_size = self
            .dyns
            .as_ref()
            .map(|dyns| {
                dyns.iter()
                    .map(|v| size_of_val(v) + v.as_ref().map(|x| x.mem_size()).unwrap_or_default())
                    .sum::<usize>()
            })
            .unwrap_or_default();
        size_of::<Self>() + self.fixed.len() + dyns_mem_size
    }

    pub fn is_fixed_valid(&self, addr: AccumStateValAddr) -> bool {
        let idx = addr.fixed_valid_idx();
        self.fixed[self.fixed.len() - 1 - idx / 8] & (1 << (idx % 8)) != 0
    }

    pub fn set_fixed_valid(&mut self, addr: AccumStateValAddr, valid: bool) {
        let idx = addr.fixed_valid_idx();
        let fixed_len = self.fixed.len();
        self.fixed[fixed_len - 1 - idx / 8] |= (valid as u8) << (idx % 8);
    }

    pub fn fixed_value<T: Sized + Copy>(&self, addr: AccumStateValAddr) -> T {
        let offset = addr.fixed_offset();
        let tptr = self.fixed[offset..][..size_of::<T>()].as_ptr() as *const T;
        unsafe { std::ptr::read_unaligned(tptr) }
    }

    pub fn set_fixed_value<T: Sized + Copy>(&mut self, addr: AccumStateValAddr, v: T) {
        let offset = addr.fixed_offset();
        let tptr = self.fixed[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe {
            std::ptr::write_unaligned(tptr, v);
        }
    }

    pub fn update_fixed_value<T: Sized + Copy>(
        &mut self,
        addr: AccumStateValAddr,
        updater: impl Fn(T) -> T,
    ) {
        let offset = addr.fixed_offset();
        let tptr = self.fixed[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe { std::ptr::write_unaligned(tptr, updater(std::ptr::read_unaligned(tptr))) }
    }

    pub fn dyn_value(&mut self, addr: AccumStateValAddr) -> &DynVal {
        &self.dyns.as_ref().unwrap()[addr.dyn_idx()]
    }

    pub fn dyn_value_mut(&mut self, addr: AccumStateValAddr) -> &mut DynVal {
        &mut self.dyns.as_mut().unwrap()[addr.dyn_idx()]
    }

    pub fn load(&mut self, mut r: impl Read, dyn_laders: &[LoadFn]) -> Result<()> {
        r.read_exact(&mut self.fixed)?;
        if self.dyns.is_some() {
            let mut reader = LoadReader(Box::new(r));
            for (v, load) in self.dyns.as_mut().unwrap().iter_mut().zip(dyn_laders) {
                *v = load(&mut reader)?;
            }
        }
        Ok(())
    }

    pub fn load_from_bytes(&mut self, bytes: &[u8], dyn_loaders: &[LoadFn]) -> Result<()> {
        self.load(Cursor::new(bytes), dyn_loaders)
    }

    pub fn save(&mut self, mut w: impl Write, dyn_savers: &[SaveFn]) -> Result<()> {
        w.write_all(&self.fixed)?;
        if self.dyns.is_some() {
            let mut writer = SaveWriter(Box::new(&mut w));
            for (v, save) in self.dyns.as_mut().unwrap().iter_mut().zip(dyn_savers) {
                save(&mut writer, std::mem::take(v))?;
            }
        }
        Ok(())
    }

    pub fn save_to_bytes(&mut self, dyn_savers: &[SaveFn]) -> Result<SlimBytes> {
        let mut bytes = vec![];
        self.save(&mut bytes, dyn_savers)?;
        Ok(bytes.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccumInitialValue {
    Scalar(ScalarValue),
    DynList(DataType),
    DynSet(DataType),
}

pub fn create_acc_from_initial_value(
    values: &[AccumInitialValue],
) -> Result<(AccumStateRow, Box<[AccumStateValAddr]>)> {
    let mut fixed_count = 0;
    let mut fixed_valids = vec![];
    let mut fixed: Vec<u8> = vec![];
    let mut dyns: Vec<DynVal> = vec![];
    let mut addrs: Vec<AccumStateValAddr> = vec![];

    macro_rules! handle_fixed {
        ($v:expr, $nbytes:expr) => {{
            addrs.push(AccumStateValAddr::new_fixed(fixed_count, fixed.len()));
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
                ScalarValue::Decimal128(v, ..) => handle_fixed!(v, 16),
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
                    addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                    dyns.push(v.as_ref().map(|s| {
                        addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                        let v: Box<dyn AggDynValue> = Box::new(AggDynStr::from_str(s));
                        v
                    }));
                }
                ScalarValue::Binary(v) => {
                    addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                    dyns.push(v.as_ref().map(|s| {
                        addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                        let v: Box<dyn AggDynValue> = Box::new(AggDynBinary::from_slice(s));
                        v
                    }));
                }
                other => {
                    addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                    dyns.push(match other {
                        v if v.is_null() => None,
                        v => Some(Box::new(AggDynScalar::new(v.clone()))),
                    });
                }
            },
            AccumInitialValue::DynList(_dt) => {
                addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                dyns.push(Some(Box::new(AggDynList::default())));
            }
            AccumInitialValue::DynSet(_dt) => {
                addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                dyns.push(Some(Box::new(AggDynSet::default())));
            }
        }
    }

    // reverse fixed_valids and append it to fixed, so no need to change addrs
    fixed_valids.reverse();
    fixed.extend(fixed_valids);

    let acc = AccumStateRow {
        fixed: fixed.into(),
        dyns: if !dyns.is_empty() {
            Some(dyns.into())
        } else {
            None
        },
    };
    Ok((acc, addrs.into()))
}

pub struct LoadReader<'a>(pub Box<dyn Read + 'a>);
pub struct SaveWriter<'a>(pub Box<dyn Write + 'a>);
pub type LoadFn = Box<dyn Fn(&mut LoadReader) -> Result<DynVal> + Send + Sync>;
pub type SaveFn = Box<dyn Fn(&mut SaveWriter, DynVal) -> Result<()> + Send + Sync>;

pub fn create_dyn_loaders_from_initial_value(values: &[AccumInitialValue]) -> Result<Vec<LoadFn>> {
    let mut loaders: Vec<LoadFn> = vec![];
    for value in values {
        let loader: LoadFn = match value {
            AccumInitialValue::Scalar(scalar) => match scalar {
                ScalarValue::Null => continue,
                ScalarValue::Boolean(_) => continue,
                ScalarValue::Float32(_) => continue,
                ScalarValue::Float64(_) => continue,
                ScalarValue::Decimal128(_, ..) => continue,
                ScalarValue::Int8(_) => continue,
                ScalarValue::Int16(_) => continue,
                ScalarValue::Int32(_) => continue,
                ScalarValue::Int64(_) => continue,
                ScalarValue::UInt8(_) => continue,
                ScalarValue::UInt16(_) => continue,
                ScalarValue::UInt32(_) => continue,
                ScalarValue::UInt64(_) => continue,
                ScalarValue::Date32(_) => continue,
                ScalarValue::Date64(_) => continue,
                ScalarValue::TimestampSecond(..) => continue,
                ScalarValue::TimestampMillisecond(..) => continue,
                ScalarValue::TimestampMicrosecond(..) => continue,
                ScalarValue::TimestampNanosecond(..) => continue,
                ScalarValue::Utf8(_) => Box::new(|r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        n => {
                            let s = read_bytes_slice(&mut r.0, n - 1)?;
                            let v = String::from_utf8_lossy(&s);
                            Some(Box::new(AggDynStr::from_str(&v)))
                        }
                    })
                }),
                ScalarValue::Binary(_) => Box::new(|r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        n => {
                            let v = read_bytes_slice(&mut r.0, n - 1)?;
                            Some(Box::new(AggDynBinary::new(SlimBytes::from(v))))
                        }
                    })
                }),
                other => {
                    let dt = other.get_datatype();
                    Box::new(move |r: &mut LoadReader| {
                        let col = read_array(&mut r.0, &dt, 1)?;
                        Ok(match ScalarValue::try_from_array(&col, 0)? {
                            v if v.is_null() => None,
                            v => Some(Box::new(AggDynScalar::new(v))),
                        })
                    })
                }
            },
            AccumInitialValue::DynList(dt) => {
                let dt = dt.clone();
                Box::new(move |r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        1 => Some(Box::new(AggDynList::default())),
                        n => {
                            let array = read_array(&mut r.0, &dt, n - 2)?;
                            let mut dyn_list = AggDynList::default();
                            dyn_list.values = (0..array.len())
                                .map(|i| ScalarValue::try_from_array(&array, i))
                                .collect::<Result<_>>()?;
                            Some(Box::new(dyn_list))
                        }
                    })
                })
            }
            AccumInitialValue::DynSet(dt) => {
                let dt = dt.clone();
                Box::new(move |r: &mut LoadReader| {
                    Ok(match read_len(&mut r.0)? {
                        0 => None,
                        1 => Some(Box::new(AggDynSet::default())),
                        n => {
                            let array = read_array(&mut r.0, &dt, n - 2)?;
                            let mut dyn_set = AggDynSet::default();
                            dyn_set.values = (0..array.len())
                                .map(|i| ScalarValue::try_from_array(&array, i))
                                .collect::<Result<_>>()?;
                            Some(Box::new(dyn_set))
                        }
                    })
                })
            }
        };
        loaders.push(loader);
    }
    Ok(loaders)
}

pub fn create_dyn_savers_from_initial_value(values: &[AccumInitialValue]) -> Result<Vec<SaveFn>> {
    let mut savers: Vec<SaveFn> = vec![];
    for value in values {
        let saver = match value {
            AccumInitialValue::Scalar(scalar) => match scalar {
                ScalarValue::Null => continue,
                ScalarValue::Boolean(_) => continue,
                ScalarValue::Float32(_) => continue,
                ScalarValue::Float64(_) => continue,
                ScalarValue::Decimal128(_, ..) => continue,
                ScalarValue::Int8(_) => continue,
                ScalarValue::Int16(_) => continue,
                ScalarValue::Int32(_) => continue,
                ScalarValue::Int64(_) => continue,
                ScalarValue::UInt8(_) => continue,
                ScalarValue::UInt16(_) => continue,
                ScalarValue::UInt32(_) => continue,
                ScalarValue::UInt64(_) => continue,
                ScalarValue::Date32(_) => continue,
                ScalarValue::Date64(_) => continue,
                ScalarValue::TimestampSecond(..) => continue,
                ScalarValue::TimestampMillisecond(..) => continue,
                ScalarValue::TimestampMicrosecond(..) => continue,
                ScalarValue::TimestampNanosecond(..) => continue,
                ScalarValue::Utf8(_) => {
                    fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                        match v {
                            None => write_len(0, &mut w.0)?,
                            Some(v) => {
                                let s = downcast_any!(v, AggDynStr)?;
                                write_len(s.value().as_bytes().len() + 1, &mut w.0)?;
                                w.0.write_all(s.value().as_bytes())?;
                            }
                        }
                        Ok(())
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
                ScalarValue::Binary(_) => {
                    fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                        match v {
                            None => write_len(0, &mut w.0)?,
                            Some(v) => {
                                let s = downcast_any!(v, AggDynBinary)?;
                                write_len(s.value().as_bytes().len() + 1, &mut w.0)?;
                                w.0.write_all(s.value().as_bytes())?;
                            }
                        }
                        Ok(())
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
                _other => {
                    fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                        match v {
                            None => write_array(&NullArray::new(1), &mut w.0)?,
                            Some(v) => {
                                let v = downcast_any!(v, AggDynScalar)?.value();
                                write_array(&v.to_array(), &mut w.0)?;
                            }
                        };
                        Ok(())
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
            },
            AccumInitialValue::DynList(_dt) => {
                fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                    match v {
                        None => write_len(0, &mut w.0)?,
                        Some(v) => {
                            let list = v
                                .as_any_boxed()
                                .downcast::<AggDynList>()
                                .or_else(|_| df_execution_err!("error downcasting to AggDynList"))?
                                .into_values();
                            if list.is_empty() {
                                write_len(1, &mut w.0)?;
                            } else {
                                let array = ScalarValue::iter_to_array(list.into_iter())?;
                                write_len(array.len() + 2, &mut w.0)?;
                                write_array(&array, &mut w.0)?;
                            }
                        }
                    }
                    Ok(())
                }
                let f: SaveFn = Box::new(f);
                f
            }
            AccumInitialValue::DynSet(_dt) => {
                fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                    match v {
                        None => write_len(0, &mut w.0)?,
                        Some(v) => {
                            let set = v
                                .as_any_boxed()
                                .downcast::<AggDynSet>()
                                .or_else(|_| df_execution_err!("error downcasting to AggDynList"))?
                                .into_values();
                            if set.is_empty() {
                                write_len(1, &mut w.0)?;
                            } else {
                                let array = ScalarValue::iter_to_array(set.into_iter())?;
                                write_len(array.len() + 2, &mut w.0)?;
                                write_array(&array, &mut w.0)?;
                            }
                        }
                    }
                    Ok(())
                }
                let f: SaveFn = Box::new(f);
                f
            }
        };
        savers.push(saver);
    }
    Ok(savers)
}

#[allow(clippy::borrowed_box)]
pub trait AggDynValue: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any>;
    fn mem_size(&self) -> usize;
    fn clone_boxed(&self) -> Box<dyn AggDynValue>;
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

    pub fn value(&self) -> &ScalarValue {
        &self.value
    }

    pub fn into_value(self) -> ScalarValue {
        self.value
    }
}

impl AggDynValue for AggDynScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value.size()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynBinary {
    pub value: SlimBytes,
}

#[allow(clippy::borrowed_box)]
impl AggDynBinary {
    pub fn new(value: SlimBytes) -> Self {
        Self { value }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        Self::new(SlimBytes::from(slice))
    }

    pub fn value(&self) -> &[u8] {
        self.value.as_ref()
    }

    pub fn into_value(self) -> SlimBytes {
        self.value
    }
}

impl AggDynValue for AggDynBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value().as_bytes().len()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AggDynStr {
    value: SlimmerBox<str>,
}

#[allow(clippy::borrowed_box)]
impl AggDynStr {
    pub fn new(value: SlimmerBox<str>) -> Self {
        Self { value }
    }

    pub fn from_str(v: &str) -> Self {
        Self::new(SlimmerBox::from_box(v.to_owned().into()))
    }

    pub fn value(&self) -> &str {
        self.value.as_ref()
    }

    pub fn into_value(self) -> SlimmerBox<str> {
        self.value
    }
}

impl AggDynValue for AggDynStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + self.value().as_bytes().len()
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

    pub fn values(&self) -> &[ScalarValue] {
        self.values.as_slice()
    }

    pub fn into_values(self) -> Vec<ScalarValue> {
        self.values
    }
}

impl AggDynValue for AggDynList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + ScalarValue::size_of_vec(&self.values)
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

    pub fn values(&self) -> &HashSet<ScalarValue> {
        &self.values
    }

    pub fn into_values(self) -> HashSet<ScalarValue> {
        self.values
    }
}

impl AggDynValue for AggDynSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_boxed(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn mem_size(&self) -> usize {
        size_of::<Self>() + ScalarValue::size_of_hashset(&self.values)
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Default, Clone, Copy)]
pub struct AccumStateValAddr(u64);

impl AccumStateValAddr {
    #[inline]
    fn new_fixed(valid_idx: usize, offset: usize) -> Self {
        Self((valid_idx as u64) << 32 | (offset as u64))
    }

    #[inline]
    fn new_dyn(idx: usize) -> Self {
        Self((idx as u64) | 0x8000_0000_0000_0000)
    }
    #[inline]
    fn fixed_offset(&self) -> usize {
        (self.0 & 0x0000_0000_ffff_ffff) as usize
    }

    #[inline]
    fn fixed_valid_idx(&self) -> usize {
        ((self.0 & 0x7fff_ffff_0000_0000) >> 32) as usize
    }

    #[inline]
    fn dyn_idx(&self) -> usize {
        (self.0 & 0x7fff_ffff_ffff_ffff) as usize
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, io::Cursor};

    use arrow::datatypes::DataType;
    use datafusion::common::{Result, ScalarValue};
    use datafusion_ext_commons::downcast_any;

    use crate::agg::acc::{
        create_acc_from_initial_value, create_dyn_loaders_from_initial_value,
        create_dyn_savers_from_initial_value, AccumInitialValue, AggDynList, AggDynSet, AggDynStr,
        LoadReader, SaveWriter,
    };

    #[test]
    fn test_dyn_list() {
        let loaders =
            create_dyn_loaders_from_initial_value(&[AccumInitialValue::DynList(DataType::Int32)])
                .unwrap();
        let savers =
            create_dyn_savers_from_initial_value(&[AccumInitialValue::DynList(DataType::Int32)])
                .unwrap();
        let mut dyn_list = AggDynList::default();
        dyn_list.append(ScalarValue::from(1i32));
        dyn_list.append(ScalarValue::from(2i32));
        dyn_list.append(ScalarValue::from(3i32));

        let mut buf = vec![];
        savers[0](
            &mut SaveWriter(Box::new(&mut buf)),
            Some(Box::new(dyn_list)),
        )
        .unwrap();

        let dyn_list = loaders[0](&mut LoadReader(Box::new(Cursor::new(&buf)))).unwrap();
        assert_eq!(
            downcast_any!(dyn_list.unwrap(), AggDynList)
                .unwrap()
                .values(),
            &[
                ScalarValue::from(1i32),
                ScalarValue::from(2i32),
                ScalarValue::from(3i32),
            ]
        );
    }

    #[test]
    fn test_dyn_set() {
        let loaders =
            create_dyn_loaders_from_initial_value(&[AccumInitialValue::DynSet(DataType::Int32)])
                .unwrap();
        let savers =
            create_dyn_savers_from_initial_value(&[AccumInitialValue::DynSet(DataType::Int32)])
                .unwrap();
        let mut dyn_set = AggDynSet::default();
        dyn_set.append(ScalarValue::from(1i32));
        dyn_set.append(ScalarValue::from(2i32));
        dyn_set.append(ScalarValue::from(3i32));
        dyn_set.append(ScalarValue::from(2i32));

        let mut buf = vec![];
        savers[0](&mut SaveWriter(Box::new(&mut buf)), Some(Box::new(dyn_set))).unwrap();

        let dyn_set = loaders[0](&mut LoadReader(Box::new(Cursor::new(&buf)))).unwrap();
        assert_eq!(
            downcast_any!(dyn_set.unwrap(), AggDynSet).unwrap().values(),
            &HashSet::from_iter(
                vec![
                    ScalarValue::from(1i32),
                    ScalarValue::from(2i32),
                    ScalarValue::from(3i32),
                ]
                .into_iter()
            )
        );
    }

    #[test]
    fn test_acc() {
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

        let (mut acc, addrs) = create_acc_from_initial_value(&scalars).unwrap();
        let dyn_loaders = create_dyn_loaders_from_initial_value(&scalars).unwrap();
        let dyn_savers = create_dyn_savers_from_initial_value(&scalars).unwrap();
        assert!(!acc.is_fixed_valid(addrs[0]));
        assert!(!acc.is_fixed_valid(addrs[1]));
        assert!(!acc.is_fixed_valid(addrs[2]));

        // set values
        let mut acc_valued = acc.clone();
        acc_valued.set_fixed_value(addrs[1], 123456789_i32);
        acc_valued.set_fixed_value(addrs[2], 1234567890123456789_i64);
        acc_valued.set_fixed_valid(addrs[1], true);
        acc_valued.set_fixed_valid(addrs[2], true);
        *acc_valued.dyn_value_mut(addrs[3]) = Some(Box::new(AggDynStr::from_str("test")));

        // save + load
        let bytes = acc_valued.save_to_bytes(&dyn_savers).unwrap();
        acc.load_from_bytes(&bytes, &dyn_loaders).unwrap();

        assert!(!acc.is_fixed_valid(addrs[0]));
        assert!(acc.is_fixed_valid(addrs[1]));
        assert!(acc.is_fixed_valid(addrs[2]));
        assert_eq!(acc.fixed_value::<i32>(addrs[1]), 123456789_i32);
        assert_eq!(acc.fixed_value::<i64>(addrs[2]), 1234567890123456789_i64);
        assert_eq!(
            downcast_any!(acc.dyn_value(addrs[3]).as_ref().unwrap(), AggDynStr)
                .unwrap()
                .value(),
            "test",
        );
    }
}
