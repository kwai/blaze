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

use crate::hash::{read32, read64};

const PRIME64_1: u64 = 0x9E3779B185EBCA87u64;
const PRIME64_2: u64 = 0xC2B2AE3D27D4EB4Fu64;
const PRIME64_3: u64 = 0x165667B19E3779F9u64;
const PRIME64_4: u64 = 0x85EBCA77C2B2AE63u64;
const PRIME64_5: u64 = 0x27D4EB2F165667C5u64;

#[inline]
pub fn spark_compatible_xxhash64_hash<T: AsRef<[u8]>>(data: T, seed: i64) -> i64 {
    xxhash64(data.as_ref(), seed as u64) as i64
}

#[inline]
fn xxhash64(input: &[u8], seed: u64) -> u64 {
    let mut hash;
    let mut remaining = input.len();
    let mut offset = 0;

    if remaining >= 32 {
        let mut acc1 = seed + PRIME64_1 + PRIME64_2;
        let mut acc2 = seed + PRIME64_2;
        let mut acc3 = seed + 0;
        let mut acc4 = seed - PRIME64_1;

        while remaining >= 32 {
            acc1 = xxh64_round(acc1, read64(input, offset));
            offset += 8;
            acc2 = xxh64_round(acc2, read64(input, offset));
            offset += 8;
            acc3 = xxh64_round(acc3, read64(input, offset));
            offset += 8;
            acc4 = xxh64_round(acc4, read64(input, offset));
            offset += 8;
            remaining -= 32;
        }
        hash =
            xxh_rotl64(acc1, 1) + xxh_rotl64(acc2, 7) + xxh_rotl64(acc3, 12) + xxh_rotl64(acc4, 18);
        hash = xxh64_merge_round(hash, acc1);
        hash = xxh64_merge_round(hash, acc2);
        hash = xxh64_merge_round(hash, acc3);
        hash = xxh64_merge_round(hash, acc4);
    } else {
        hash = seed + PRIME64_5;
    }
    hash += input.len() as u64;

    while remaining >= 8 {
        hash ^= xxh64_round(0, read64(input, offset));
        hash = xxh_rotl64(hash, 27);
        hash *= PRIME64_1;
        hash += PRIME64_4;
        offset += 8;
        remaining -= 8;
    }

    if remaining >= 4 {
        hash ^= read32(input, offset) as u64 * PRIME64_1;
        hash = xxh_rotl64(hash, 23);
        hash *= PRIME64_2;
        hash += PRIME64_3;
        offset += 4;
        remaining -= 4;
    }

    while remaining != 0 {
        hash ^= input[offset] as u64 * PRIME64_5;
        hash = xxh_rotl64(hash, 11);
        hash *= PRIME64_1;
        offset += 1;
        remaining -= 1;
    }
    xxh64_avalanche(hash)
}

#[inline]
fn xxh64_merge_round(mut hash: u64, acc: u64) -> u64 {
    hash ^= xxh64_round(0, acc);
    hash *= PRIME64_1;
    hash += PRIME64_4;
    hash
}

#[inline]
fn xxh_rotl64(value: u64, amt: i32) -> u64 {
    (value << (amt % 64)) | (value >> (64 - amt % 64))
}

#[inline]
fn xxh64_round(mut acc: u64, input: u64) -> u64 {
    acc += input * PRIME64_2;
    acc = xxh_rotl64(acc, 31);
    acc *= PRIME64_1;
    acc
}

#[inline]
fn xxh64_avalanche(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash *= PRIME64_2;
    hash ^= hash >> 29;
    hash *= PRIME64_3;
    hash ^= hash >> 32;
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xxhash64() {
        let _hashes = [
            "",
            "a",
            "ab",
            "abc",
            "abcd",
            "abcde",
            "abcdefghijklmnopqrstuvwxyz",
        ]
        .into_iter()
        .map(|s| spark_compatible_xxhash64_hash(s.as_bytes(), 42))
        .collect::<Vec<_>>();
        let _expected = vec![
            -7444071767201028348,
            -8582455328737087284,
            2710560539726725091,
            1423657621850124518,
            -6810745876291105281,
            -990457398947679591,
            -3265757659154784300,
        ];
        assert_eq!(_hashes, _expected)
    }
}
