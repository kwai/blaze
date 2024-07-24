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
    fmt::{Debug, Formatter},
    io::Write,
};

use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use datafusion::common::Result;

use crate::{
    df_execution_err,
    hash::mur::{spark_compatible_murmur3_hash, spark_compatible_murmur3_hash_long},
    spark_bit_array::SparkBitArray,
};

#[derive(Default, Clone)]
pub struct SparkBloomFilter {
    bits: SparkBitArray,
    num_hash_functions: usize,
}

impl Debug for SparkBloomFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkBloomFilter")
            .field("bit_size", &self.bits.bit_size())
            .field("num_hash_functions", &self.num_hash_functions)
            .field("mem_size", &self.mem_size())
            .finish()
    }
}

impl SparkBloomFilter {
    pub fn new_with_expected_num_items(expected_num_items: usize, num_bits: usize) -> Self {
        let num_hash_functions = Self::optimal_num_of_hash_functions(expected_num_items, num_bits);
        Self::new_with_num_hash_functions(num_hash_functions, num_bits)
    }

    pub fn new_with_num_hash_functions(num_hash_functions: usize, num_bits: usize) -> Self {
        let bits = SparkBitArray::new_with_num_bits(num_bits);
        Self {
            bits,
            num_hash_functions,
        }
    }

    pub fn read_from(mut r: impl std::io::Read) -> Result<Self> {
        let version = r.read_i32::<BE>()?;
        if version != 1 {
            return df_execution_err!("unsupported version: {}", version);
        }
        let num_hash_functions = r.read_i32::<BE>()? as usize;
        let bits = SparkBitArray::read_from(r)?;
        Ok(Self {
            bits,
            num_hash_functions,
        })
    }

    pub fn write_to(&self, mut w: impl Write) -> Result<()> {
        w.write_i32::<BE>(1)?; // version number
        w.write_i32::<BE>(self.num_hash_functions as i32)?;
        self.bits.write_to(w)?;
        Ok(())
    }

    pub fn mem_size(&self) -> usize {
        self.bits.mem_size() + size_of_val(&self.num_hash_functions)
    }

    #[inline]
    pub fn put_long(&mut self, item: i64) {
        let h1 = spark_compatible_murmur3_hash_long(item, 0);
        let h2 = spark_compatible_murmur3_hash_long(item, h1);
        let bit_size = self.bits.bit_size() as i32;

        for i in 1..=self.num_hash_functions as i32 {
            let mut combined_hash = h1 + i * h2;
            // flip all the bits if it's negative (guaranteed positive number)
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            self.bits.set((combined_hash % bit_size) as usize);
        }
    }

    #[inline]
    pub fn put_binary<T: AsRef<[u8]>>(&mut self, item: T) {
        let item = item.as_ref();
        let h1 = spark_compatible_murmur3_hash(item, 0);
        let h2 = spark_compatible_murmur3_hash(item, h1);
        let bit_size = self.bits.bit_size() as i32;

        for i in 1..=self.num_hash_functions as i32 {
            let mut combined_hash = h1 + i * h2;
            // flip all the bits if it's negative (guaranteed positive number)
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            self.bits.set((combined_hash % bit_size) as usize);
        }
    }

    #[inline]
    pub fn might_contain_long(&self, item: i64) -> bool {
        let h1 = spark_compatible_murmur3_hash_long(item, 0);
        let h2 = spark_compatible_murmur3_hash_long(item, h1);
        let bit_size = self.bits.bit_size() as i32;
        for i in 1..=self.num_hash_functions as i32 {
            let mut combined_hash = h1 + i * h2;
            // flip all the bits if it's negative (guaranteed positive number)
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            if !self.bits.get((combined_hash % bit_size) as usize) {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn might_contain_binary<T: AsRef<[u8]>>(&self, item: T) -> bool {
        let item = item.as_ref();
        let h1 = spark_compatible_murmur3_hash(item, 0);
        let h2 = spark_compatible_murmur3_hash(item, h1);
        let bit_size = self.bits.bit_size() as i32;
        for i in 1..=self.num_hash_functions as i32 {
            let mut combined_hash = h1 + i * h2;
            // flip all the bits if it's negative (guaranteed positive number)
            if combined_hash < 0 {
                combined_hash = !combined_hash;
            }
            if !self.bits.get((combined_hash % bit_size) as usize) {
                return false;
            }
        }
        true
    }

    pub fn put_all(&mut self, other: &Self) {
        assert_eq!(self.num_hash_functions, other.num_hash_functions);
        self.bits.put_all(&other.bits);
    }

    fn optimal_num_of_hash_functions(n: usize, m: usize) -> usize {
        let result = (m as f64 / n as f64 * 2.0_f64.ln()).round() as usize;
        result.max(1)
    }
}
