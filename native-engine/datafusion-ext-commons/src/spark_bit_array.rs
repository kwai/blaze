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

use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt, BE};
use datafusion::common::Result;

// native implementation of org.apache.spark.util.sketch.BitArray
#[derive(Default, Clone)]
pub struct SparkBitArray {
    data: Vec<u64>,
}

impl SparkBitArray {
    pub fn new(data: Vec<u64>) -> Self {
        Self { data }
    }

    pub fn new_with_num_bits(num_bits: usize) -> Self {
        assert!(
            num_bits > 0,
            "num_bits must be positive, but got {num_bits}"
        );
        assert!(
            num_bits <= i32::MAX as usize,
            "cannot allocate enough space for {num_bits} bits"
        );
        let data_len = (num_bits + 63) / 64;
        Self::new(vec![0; data_len])
    }

    pub fn read_from(mut r: impl Read) -> Result<Self> {
        let data_len = r.read_i32::<BE>()? as usize;
        let mut data = vec![0; data_len];
        for datum in &mut data {
            *datum = r.read_i64::<BE>()? as u64;
        }
        Ok(Self::new(data))
    }

    pub fn write_to(&self, mut w: impl Write) -> Result<()> {
        w.write_i32::<BE>(self.data.len() as i32)?;
        for &datum in &self.data {
            w.write_i64::<BE>(datum as i64)?;
        }
        Ok(())
    }

    pub fn set(&mut self, index: usize) {
        let data_offset = index >> 6;
        let bit_offset = index & 0b00111111;
        self.data[data_offset] |= 1 << bit_offset;
    }

    pub fn get(&self, index: usize) -> bool {
        let data_offset = index >> 6;
        let bit_offset = index & 0b00111111;
        let datum = &self.data[data_offset];
        (datum >> bit_offset) & 1 == 1
    }

    /// Combines the two BitArrays using bitwise OR
    pub fn put_all(&mut self, bit_array: &SparkBitArray) {
        assert_eq!(self.data.len(), bit_array.data.len());

        for (datum, bit_array_datum) in self.data.iter_mut().zip(&bit_array.data) {
            *datum |= *bit_array_datum;
        }
    }

    pub fn and(&mut self, bit_array: &SparkBitArray) {
        assert_eq!(self.data.len(), bit_array.data.len());

        for (datum, bit_array_datum) in self.data.iter_mut().zip(&bit_array.data) {
            *datum &= *bit_array_datum;
        }
    }

    pub fn mem_size(&self) -> usize {
        self.data.len() * 8
    }

    pub fn bit_size(&self) -> usize {
        self.data.len() * 64
    }
}

#[cfg(test)]
mod tests {
    // these test cases come from org.apache.spark.util.sketch.BitArraySuite

    use itertools::Itertools;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    #[should_panic]
    fn test_bit_array_error1() {
        SparkBitArray::new_with_num_bits(0);
    }

    #[test]
    #[should_panic]
    fn test_bit_array_error2() {
        SparkBitArray::new_with_num_bits(64 * i32::MAX as usize + 1);
    }

    #[test]
    fn test_bit_size() {
        assert_eq!(SparkBitArray::new_with_num_bits(64).bit_size(), 64);
        assert_eq!(SparkBitArray::new_with_num_bits(65).bit_size(), 128);
        assert_eq!(SparkBitArray::new_with_num_bits(127).bit_size(), 128);
        assert_eq!(SparkBitArray::new_with_num_bits(128).bit_size(), 128);
    }

    #[test]
    fn test_normal_operation() {
        // use a fixed seed to make the test predictable.
        let mut r = rand::rngs::StdRng::seed_from_u64(37);

        let mut bit_array = SparkBitArray::new_with_num_bits(320);
        let indexes: Vec<usize> = (1..=100)
            .map(|_| r.gen_range(0..320) as usize)
            .unique()
            .collect();

        indexes.iter().for_each(|&i| {
            bit_array.set(i);
        });
        indexes.iter().for_each(|&i| assert!(bit_array.get(i)));
    }

    #[test]
    fn test_merge() {
        // use a fixed seed to make the test predictable.
        let mut r = rand::rngs::StdRng::seed_from_u64(37);

        let mut bit_array1 = SparkBitArray::new_with_num_bits(64 * 6);
        let mut bit_array2 = SparkBitArray::new_with_num_bits(64 * 6);

        let indexes1: Vec<usize> = (1..=100)
            .map(|_| r.gen_range(0..64 * 6) as usize)
            .unique()
            .collect();
        let indexes2: Vec<usize> = (1..=100)
            .map(|_| r.gen_range(0..64 * 6) as usize)
            .unique()
            .collect();

        indexes1.iter().for_each(|&i| {
            bit_array1.set(i);
        });
        indexes2.iter().for_each(|&i| {
            bit_array2.set(i);
        });

        bit_array1.put_all(&bit_array2);
        indexes1.iter().for_each(|&i| assert!(bit_array1.get(i)));
        indexes2.iter().for_each(|&i| assert!(bit_array1.get(i)));
    }
}
