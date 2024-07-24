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
    mem::{size_of, size_of_val},
};

use arrow::datatypes::DataType;
use datafusion::{
    common::{Result, ScalarValue},
    parquet::data_type::AsBytes,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any,
    io::{read_bytes_slice, read_len, read_scalar, read_u8, write_len, write_scalar, write_u8},
    slim_bytes::SlimBytes,
    spark_bloom_filter::SparkBloomFilter,
};
use hashbrown::raw::RawTable;
use itertools::Itertools;
use slimmer_box::SlimmerBox;
use smallvec::SmallVec;

use crate::agg::agg_table::gx_hash;

pub type DynVal = Option<Box<dyn AggDynValue>>;

const ACC_STORE_BLOCK_SIZE: usize = 65536;

pub struct AccStore {
    initial: OwnedAccumStateRow,
    num_accs: usize,
    fixed_store: Vec<Vec<u8>>,
    dyn_store: Vec<Vec<DynVal>>,
}

impl AccStore {
    pub fn new(initial: OwnedAccumStateRow) -> Self {
        Self {
            initial,
            num_accs: 0,
            fixed_store: vec![],
            dyn_store: vec![],
        }
    }

    pub fn clear(&mut self) {
        self.num_accs = 0;
        self.fixed_store.iter_mut().for_each(|s| s.clear());
        self.dyn_store.iter_mut().for_each(|s| s.clear());
    }

    pub fn clear_and_free(&mut self) {
        self.num_accs = 0;
        self.fixed_store = vec![];
        self.dyn_store = vec![];
    }

    pub fn mem_size(&self) -> usize {
        self.num_accs * (self.fixed_len() + self.dyns_len() * size_of::<DynVal>() + 32)
    }

    pub fn new_acc(&mut self) -> u32 {
        let initial = unsafe {
            // safety: ignore borrow checker
            std::mem::transmute::<_, &OwnedAccumStateRow>(&self.initial)
        };
        self.new_acc_from(initial)
    }

    pub fn new_acc_from(&mut self, acc: &impl AccumStateRow) -> u32 {
        let idx = self.num_accs;
        self.num_accs += 1;
        if self.num_required_blocks() >= self.fixed_store.len() {
            // add a new block
            // reserve a whole block to avoid reallocation
            self.fixed_store
                .push(Vec::with_capacity(ACC_STORE_BLOCK_SIZE * self.fixed_len()));
            self.dyn_store
                .push(Vec::with_capacity(ACC_STORE_BLOCK_SIZE * self.dyns_len()));
        }
        let idx1 = idx / ACC_STORE_BLOCK_SIZE;
        self.fixed_store[idx1].extend_from_slice(acc.fixed());
        self.dyn_store[idx1].extend(
            acc.dyns()
                .iter()
                .map(|v| v.as_ref().map(|v| v.clone_boxed())),
        );
        idx as u32
    }

    pub fn get(&self, idx: u32) -> RefAccumStateRow {
        let idx1 = idx as usize / ACC_STORE_BLOCK_SIZE;
        let idx2 = idx as usize % ACC_STORE_BLOCK_SIZE;
        let fixed_ptr = self.fixed_store[idx1][idx2 * self.fixed_len()..].as_ptr() as *mut u8;
        let dyns_ptr = self.dyn_store[idx1][idx2 * self.dyns_len()..].as_ptr() as *mut DynVal;
        unsafe {
            // safety: skip borrow/mutable checking
            RefAccumStateRow {
                fixed: std::slice::from_raw_parts_mut(fixed_ptr, self.fixed_len()),
                dyns: std::slice::from_raw_parts_mut(dyns_ptr, self.dyns_len()),
            }
        }
    }

    fn num_required_blocks(&self) -> usize {
        (self.num_accs + ACC_STORE_BLOCK_SIZE - 1) / ACC_STORE_BLOCK_SIZE
    }

    fn fixed_len(&self) -> usize {
        self.initial.fixed.len()
    }

    fn dyns_len(&self) -> usize {
        self.initial.dyns.len()
    }
}

pub struct OwnedAccumStateRow {
    fixed: SlimBytes,
    dyns: SlimmerBox<[DynVal]>,
}

impl OwnedAccumStateRow {
    pub fn as_mut<'a>(&'a mut self) -> RefAccumStateRow<'a> {
        RefAccumStateRow {
            fixed: &mut self.fixed,
            dyns: &mut self.dyns,
        }
    }
}

impl Clone for OwnedAccumStateRow {
    fn clone(&self) -> Self {
        Self {
            fixed: self.fixed.clone(),
            dyns: SlimmerBox::from_box(
                self.dyns
                    .iter()
                    .map(|v| v.as_ref().map(|x| x.clone_boxed()))
                    .collect::<Box<[DynVal]>>(),
            ),
        }
    }
}

impl AccumStateRow for OwnedAccumStateRow {
    fn fixed(&self) -> &[u8] {
        &self.fixed
    }

    fn fixed_mut(&mut self) -> &mut [u8] {
        &mut self.fixed
    }

    fn dyns(&self) -> &[DynVal] {
        &self.dyns
    }

    fn dyns_mut(&mut self) -> &mut [DynVal] {
        &mut self.dyns
    }
}

pub struct RefAccumStateRow<'a> {
    fixed: &'a mut [u8],
    dyns: &'a mut [DynVal],
}

impl<'a> AccumStateRow for RefAccumStateRow<'a> {
    fn fixed(&self) -> &[u8] {
        self.fixed
    }

    fn fixed_mut(&mut self) -> &mut [u8] {
        self.fixed
    }

    fn dyns(&self) -> &[DynVal] {
        self.dyns
    }

    fn dyns_mut(&mut self) -> &mut [DynVal] {
        self.dyns
    }
}

pub trait AccumStateRow {
    fn fixed(&self) -> &[u8];
    fn fixed_mut(&mut self) -> &mut [u8];
    fn dyns(&self) -> &[DynVal];
    fn dyns_mut(&mut self) -> &mut [DynVal];

    fn mem_size(&self) -> usize {
        let dyns_mem_size = self
            .dyns()
            .iter()
            .map(|v| size_of_val(v) + v.as_ref().map(|x| x.mem_size()).unwrap_or_default())
            .sum::<usize>();
        self.fixed().len() + dyns_mem_size
    }

    fn is_fixed_valid(&self, addr: AccumStateValAddr) -> bool {
        let idx = addr.fixed_valid_idx();
        self.fixed()[self.fixed().len() - 1 - idx / 8] & (1 << (idx % 8)) != 0
    }

    fn set_fixed_valid(&mut self, addr: AccumStateValAddr, valid: bool) {
        let idx = addr.fixed_valid_idx();
        let fixed_len = self.fixed().len();
        self.fixed_mut()[fixed_len - 1 - idx / 8] |= (valid as u8) << (idx % 8);
    }

    fn fixed_value<T: Sized + Copy>(&self, addr: AccumStateValAddr) -> T {
        let offset = addr.fixed_offset();
        let tptr = self.fixed()[offset..][..size_of::<T>()].as_ptr() as *const T;
        unsafe { std::ptr::read_unaligned(tptr) }
    }

    fn set_fixed_value<T: Sized + Copy>(&mut self, addr: AccumStateValAddr, v: T) {
        let offset = addr.fixed_offset();
        let tptr = self.fixed_mut()[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe {
            std::ptr::write_unaligned(tptr, v);
        }
    }

    fn update_fixed_value<T: Sized + Copy>(
        &mut self,
        addr: AccumStateValAddr,
        updater: impl Fn(T) -> T,
    ) {
        let offset = addr.fixed_offset();
        let tptr = self.fixed_mut()[offset..][..size_of::<T>()].as_ptr() as *mut T;
        unsafe { std::ptr::write_unaligned(tptr, updater(std::ptr::read_unaligned(tptr))) }
    }

    fn dyn_value(&mut self, addr: AccumStateValAddr) -> &DynVal {
        &self.dyns_mut()[addr.dyn_idx()]
    }

    fn dyn_value_mut(&mut self, addr: AccumStateValAddr) -> &mut DynVal {
        &mut self.dyns_mut()[addr.dyn_idx()]
    }

    fn load(&mut self, mut r: impl Read, dyn_loders: &[LoadFn]) -> Result<()> {
        r.read_exact(&mut self.fixed_mut())?;
        let dyns = self.dyns_mut();
        if !dyns.is_empty() {
            let mut reader = LoadReader(Box::new(r));
            for (v, load) in dyns.iter_mut().zip(dyn_loders) {
                *v = load(&mut reader)?;
            }
        }
        Ok(())
    }

    fn load_from_bytes(&mut self, bytes: &[u8], dyn_loaders: &[LoadFn]) -> Result<()> {
        self.load(Cursor::new(bytes), dyn_loaders)
    }

    fn save(&mut self, mut w: impl Write, dyn_savers: &[SaveFn]) -> Result<()> {
        w.write_all(&self.fixed())?;
        let dyns = self.dyns_mut();
        if !dyns.is_empty() {
            let mut writer = SaveWriter(Box::new(&mut w));
            for (v, save) in dyns.iter_mut().zip(dyn_savers) {
                save(&mut writer, std::mem::take(v))?;
            }
        }
        Ok(())
    }

    fn save_to_bytes(&mut self, dyn_savers: &[SaveFn]) -> Result<SlimBytes> {
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
    BloomFilter {
        estimated_num_items: usize,
        num_bits: usize,
    },
}

pub fn create_acc_from_initial_value(
    values: &[AccumInitialValue],
) -> Result<(OwnedAccumStateRow, Box<[AccumStateValAddr]>)> {
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
            AccumInitialValue::BloomFilter {
                estimated_num_items,
                num_bits,
            } => {
                addrs.push(AccumStateValAddr::new_dyn(dyns.len()));
                dyns.push(Some(Box::new(
                    SparkBloomFilter::new_with_expected_num_items(*estimated_num_items, *num_bits),
                )));
            }
        }
    }

    // reverse fixed_valids and append it to fixed, so no need to change addrs
    fixed_valids.reverse();
    fixed.extend(fixed_valids);

    let acc = OwnedAccumStateRow {
        fixed: fixed.into(),
        dyns: SlimmerBox::from_box(dyns.into()),
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
                    let dt = other.data_type();
                    Box::new(move |r: &mut LoadReader| {
                        let valid = read_u8(&mut r.0)? != 0;
                        if valid {
                            let scalar = read_scalar(&mut r.0, &dt, false)?;
                            Ok(Some(Box::new(AggDynScalar::new(scalar))))
                        } else {
                            Ok(None)
                        }
                    })
                }
            },
            AccumInitialValue::DynList(_dt) => Box::new(move |r: &mut LoadReader| {
                Ok(match read_len(&mut r.0)? {
                    0 => None,
                    n => {
                        let data_len = n - 1;
                        let raw = read_bytes_slice(&mut r.0, data_len)?.into_vec();
                        Some(Box::new(AggDynList { raw }))
                    }
                })
            }),
            AccumInitialValue::DynSet(_dt) => Box::new(move |r: &mut LoadReader| {
                Ok(match read_len(&mut r.0)? {
                    0 => None,
                    n => {
                        let data_len = n - 1;
                        let raw = read_bytes_slice(&mut r.0, data_len)?.into_vec();
                        let num_items = read_len(&mut r.0)?;

                        let list = AggDynList { raw };
                        let mut internal_set = if num_items <= 4 {
                            InternalSet::Small(SmallVec::new())
                        } else {
                            InternalSet::Huge(RawTable::with_capacity(num_items))
                        };

                        let mut pos = 0;
                        for _ in 0..num_items {
                            let pos_len = (pos, read_len(&mut r.0)? as u32);
                            pos += pos_len.1;

                            match &mut internal_set {
                                InternalSet::Small(s) => s.push(pos_len),
                                InternalSet::Huge(s) => {
                                    let raw = list.ref_raw(pos_len);
                                    let hash = gx_hash::<AGG_DYN_SET_HASH_SEED>(raw);
                                    s.insert(hash, pos_len, |&pos_len| {
                                        gx_hash::<AGG_DYN_SET_HASH_SEED>(list.ref_raw(pos_len))
                                    });
                                }
                            }
                        }
                        Some(Box::new(AggDynSet {
                            list,
                            set: internal_set,
                        }))
                    }
                })
            }),
            AccumInitialValue::BloomFilter { .. } => Box::new(move |r: &mut LoadReader| {
                Ok(match read_len(&mut r.0)? {
                    0 => None,
                    _ => Some(Box::new(SparkBloomFilter::read_from(&mut r.0)?)),
                })
            }),
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
                        if let Some(v) = v {
                            let scalar = &downcast_any!(v, AggDynScalar)?.value;
                            if !scalar.is_null() {
                                write_u8(1, &mut w.0)?;
                                return write_scalar(scalar, false, &mut w.0);
                            }
                        }
                        return Ok(write_u8(0, &mut w.0)?);
                    }
                    let f: SaveFn = Box::new(f);
                    f
                }
            },
            AccumInitialValue::DynList(_dt) => {
                fn f(w: &mut SaveWriter, v: DynVal) -> Result<()> {
                    if let Some(v) = v {
                        let list = v
                            .as_any_boxed()
                            .downcast::<AggDynList>()
                            .or_else(|_| df_execution_err!("error downcasting to AggDynList"))?;
                        write_len(list.raw.len() + 1, &mut w.0)?;
                        w.0.write_all(&list.raw)?;
                    } else {
                        write_len(0, &mut w.0)?;
                    }
                    Ok(())
                }
                let f: SaveFn = Box::new(f);
                f
            }
            AccumInitialValue::DynSet(_dt) => {
                let f: SaveFn = Box::new(move |w: &mut SaveWriter, v: DynVal| -> Result<()> {
                    if let Some(v) = v {
                        let mut set = v
                            .as_any_boxed()
                            .downcast::<AggDynSet>()
                            .or_else(|_| df_execution_err!("error downcasting to AggDynSet"))?;
                        write_len(set.list.raw.len() + 1, &mut w.0)?;
                        w.0.write_all(&set.list.raw)?;

                        write_len(set.set.len(), &mut w.0)?;
                        for len in std::mem::take(&mut set.set)
                            .into_iter()
                            .sorted()
                            .map(|pos_len| pos_len.1)
                        {
                            write_len(len as usize, &mut w.0)?;
                        }
                    } else {
                        write_len(0, &mut w.0)?;
                    }
                    Ok(())
                });
                f
            }
            AccumInitialValue::BloomFilter { .. } => {
                let f: SaveFn = Box::new(move |w: &mut SaveWriter, v: DynVal| -> Result<()> {
                    if let Some(v) = v {
                        let bloom_filter = v
                            .as_any_boxed()
                            .downcast::<SparkBloomFilter>()
                            .or_else(|_| {
                                df_execution_err!("error downcasting to AggDynBloomFilter")
                            })?;
                        write_len(1, &mut w.0)?;
                        bloom_filter.write_to(&mut w.0)?;
                    } else {
                        write_len(0, &mut w.0)?;
                    }
                    Ok(())
                });
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
        size_of::<Self>() + self.value.size() - size_of_val(&self.value)
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

#[derive(Clone, Default)]
pub struct AggDynList {
    pub raw: Vec<u8>,
}

impl AggDynList {
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
        size_of::<Self>() + self.raw.capacity()
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Default)]
pub struct AggDynSet {
    list: AggDynList,
    set: InternalSet,
}

#[derive(Clone)]
enum InternalSet {
    Small(SmallVec<[(u32, u32); 4]>),
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

    fn into_iter(self) -> impl Iterator<Item = (u32, u32)> {
        let iter: Box<dyn Iterator<Item = (u32, u32)>> = match self {
            InternalSet::Small(s) => Box::new(s.into_iter()),
            InternalSet::Huge(s) => Box::new(s.into_iter()),
        };
        iter
    }

    fn convert_to_huge_if_needed(&mut self, list: &mut AggDynList) {
        if let Self::Small(s) = self {
            let mut huge = RawTable::default();

            for &mut pos_len in s {
                let raw = list.ref_raw(pos_len);
                let hash = gx_hash::<AGG_DYN_SET_HASH_SEED>(raw);
                huge.insert(hash, pos_len, |&pos_len| {
                    gx_hash::<AGG_DYN_SET_HASH_SEED>(list.ref_raw(pos_len))
                });
            }
            *self = Self::Huge(huge);
        }
    }
}

const AGG_DYN_SET_HASH_SEED: i64 = 0x7BCB48DA4C72B4F2;

impl AggDynSet {
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
                let hash = gx_hash::<AGG_DYN_SET_HASH_SEED>(raw);
                match s.find_or_find_insert_slot(
                    hash,
                    |&pos_len| new_len == pos_len.1 as usize && raw == self.list.ref_raw(pos_len),
                    |&pos_len| gx_hash::<AGG_DYN_SET_HASH_SEED>(self.list.ref_raw(pos_len)),
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
                let hash = gx_hash::<AGG_DYN_SET_HASH_SEED>(new_value);
                match s.find_or_find_insert_slot(
                    hash,
                    |&pos_len| {
                        new_len == pos_len.1 as usize && new_value == self.list.ref_raw(pos_len)
                    },
                    |&pos_len| gx_hash::<AGG_DYN_SET_HASH_SEED>(self.list.ref_raw(pos_len)),
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
        size_of::<Self>()
            + self.list.raw.capacity()
            + match &self.set {
                InternalSet::Small(_) => 0,
                InternalSet::Huge(s) => s.capacity() * size_of::<(u32, u32, u8)>(),
            }
    }

    fn clone_boxed(&self) -> Box<dyn AggDynValue> {
        Box::new(self.clone())
    }
}

impl AggDynValue for SparkBloomFilter {
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
        SparkBloomFilter::mem_size(self)
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
        create_dyn_savers_from_initial_value, AccumInitialValue, AccumStateRow, AggDynSet,
        AggDynStr, LoadReader, SaveWriter,
    };

    #[test]
    fn test_dyn_set() {
        let mut dyn_set = AggDynSet::default();
        dyn_set.append(&ScalarValue::from("Hello"), false);
        dyn_set.append(&ScalarValue::from("Wooden"), false);
        dyn_set.append(&ScalarValue::from("Bird"), false);
        dyn_set.append(&ScalarValue::from("Snake"), false);
        dyn_set.append(&ScalarValue::from("Wooden"), false);
        dyn_set.append(&ScalarValue::from("Bird"), false);

        // test merge
        let mut dyn_set2 = AggDynSet::default();
        dyn_set2.append(&ScalarValue::from("Hello"), false);
        dyn_set2.append(&ScalarValue::from("Batman"), false);
        dyn_set2.append(&ScalarValue::from("Candy"), false);
        dyn_set.merge(&mut dyn_set2);

        // test save
        let mut buf = vec![];
        let mut save_writer = SaveWriter(Box::new(Cursor::new(&mut buf)));
        let savers =
            create_dyn_savers_from_initial_value(&[AccumInitialValue::DynSet(DataType::Utf8)])
                .unwrap();
        savers[0](&mut save_writer, Some(Box::new(dyn_set))).unwrap();
        drop(save_writer);

        // test load
        let mut load_reader = LoadReader(Box::new(Cursor::new(&buf)));
        let loaders =
            create_dyn_loaders_from_initial_value(&[AccumInitialValue::DynSet(DataType::Utf8)])
                .unwrap();
        let dyn_set = loaders[0](&mut load_reader)
            .unwrap()
            .unwrap()
            .as_any_boxed()
            .downcast::<AggDynSet>()
            .unwrap();
        drop(load_reader);

        let actual_set: HashSet<ScalarValue> = dyn_set.into_values(DataType::Utf8, false).collect();
        assert_eq!(actual_set.len(), 6);
        assert!(actual_set.contains(&ScalarValue::from("Hello")));
        assert!(actual_set.contains(&ScalarValue::from("Wooden")));
        assert!(actual_set.contains(&ScalarValue::from("Bird")));
        assert!(actual_set.contains(&ScalarValue::from("Snake")));
        assert!(actual_set.contains(&ScalarValue::from("Batman")));
        assert!(actual_set.contains(&ScalarValue::from("Candy")));
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
