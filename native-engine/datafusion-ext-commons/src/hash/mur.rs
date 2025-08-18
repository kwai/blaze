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

use crate::hash::read32;

#[inline]
pub fn spark_compatible_murmur3_hash<T: AsRef<[u8]>>(data: T, seed: i32) -> i32 {
    let data = data.as_ref();
    let len = data.len();
    let (data_aligned, data_trailing) = data.split_at(len - len % 4);

    let mut h1 = hash_bytes_by_int(data_aligned, seed);
    for &b in data_trailing {
        let half_word = b as i8 as i32;
        h1 = mix_h1(h1, mix_k1(half_word));
    }
    fmix(h1, len as i32)
}

#[inline]
pub fn spark_compatible_murmur3_hash_long(value: i64, seed: i32) -> i32 {
    hash_long(value, seed)
}

#[inline]
fn mix_k1(mut k1: i32) -> i32 {
    k1 *= 0xcc9e2d51u32 as i32;
    k1 = k1.rotate_left(15);
    k1 *= 0x1b873593u32 as i32;
    k1
}

#[inline]
fn mix_h1(mut h1: i32, k1: i32) -> i32 {
    h1 ^= k1;
    h1 = h1.rotate_left(13);
    h1 = h1 * 5 + 0xe6546b64u32 as i32;
    h1
}

#[inline]
fn fmix(mut h1: i32, len: i32) -> i32 {
    h1 ^= len;
    h1 ^= ((h1 as u32) >> 16) as i32;
    h1 *= 0x85ebca6bu32 as i32;
    h1 ^= ((h1 as u32) >> 13) as i32;
    h1 *= 0xc2b2ae35u32 as i32;
    h1 ^= ((h1 as u32) >> 16) as i32;
    h1
}

#[inline]
fn hash_bytes_by_int(data: &[u8], seed: i32) -> i32 {
    // safety: data length must be aligned to 4 bytes
    let mut h1 = seed;
    for i in (0..data.len()).step_by(4) {
        let half_word = read32(data, i) as i32;
        h1 = mix_h1(h1, mix_k1(half_word));
    }
    h1
}

#[inline]
fn hash_long(input: i64, seed: i32) -> i32 {
    let low = input as i32;
    let high = (input >> 32) as i32;

    let k1 = mix_k1(low);
    let h1 = mix_h1(seed, k1);

    let k1 = mix_k1(high);
    let h1 = mix_h1(h1, k1);

    fmix(h1, 8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur3() {
        let _hashes = ["", "a", "ab", "abc", "abcd", "abcde"]
            .into_iter()
            .map(|s| spark_compatible_murmur3_hash(s.as_bytes(), 42))
            .collect::<Vec<_>>();
        let _expected = vec![
            142593372, 1485273170, -97053317, 1322437556, -396302900, 814637928,
        ];
        assert_eq!(_hashes, _expected)
    }
}
