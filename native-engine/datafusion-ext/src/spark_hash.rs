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

//! Functionality used both on logical and physical plans

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit,
};
use datafusion::error::{DataFusionError, Result};

#[inline]
fn spark_compatible_murmur3_hash<T: AsRef<[u8]>>(data: T, seed: u32) -> u32 {
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
        h1 ^= (h1 as u32 >> 16) as i32;
        h1 *= 0x85ebca6bu32 as i32;
        h1 ^= (h1 as u32 >> 13) as i32;
        h1 *= 0xc2b2ae35u32 as i32;
        h1 ^= (h1 as u32 >> 16) as i32;
        h1
    }

    #[inline]
    unsafe fn hash_bytes_by_int(data: &[u8], seed: u32) -> i32 {
        // safety: data length must be aligned to 4 bytes
        let mut h1 = seed as i32;
        for i in (0..data.len()).step_by(4) {
            let mut half_word = *(data.as_ptr().add(i) as *const i32);
            if cfg!(target_endian = "big") {
                half_word = half_word.reverse_bits();
            }
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        h1
    }
    let data = data.as_ref();
    let len = data.len();
    let len_aligned = len - len % 4;

    // safety:
    // avoid boundary checking in performance critical codes.
    // all operations are garenteed to be safe
    unsafe {
        let mut h1 = hash_bytes_by_int(
            std::slice::from_raw_parts(data.get_unchecked(0), len_aligned),
            seed,
        );

        for i in len_aligned..len {
            let half_word = *data.get_unchecked(i) as i8 as i32;
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        fmix(h1, len as i32) as u32
    }
}

#[test]
fn test_murmur3() {
    let _hashes = ["", "a", "ab", "abc", "abcd", "abcde"]
        .into_iter()
        .map(|s| spark_compatible_murmur3_hash(s.as_bytes(), 42) as i32)
        .collect::<Vec<_>>();
    let _expected = vec![
        142593372, 1485273170, -97053317, 1322437556, -396302900, 814637928,
    ];
}

macro_rules! hash_array {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = spark_compatible_murmur3_hash(&array.value(i), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(&array.value(i), *hash);
                }
            }
        }
    };
}

macro_rules! hash_array_primitive_i32 {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                *hash =
                    spark_compatible_murmur3_hash((*value as i32).to_le_bytes(), *hash);
            }
        } else {
            for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        (*value as i32).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
    };
}

macro_rules! hash_array_primitive_i64 {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                *hash =
                    spark_compatible_murmur3_hash((*value as i64).to_le_bytes(), *hash);
            }
        } else {
            for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        (*value as i64).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
    };
}

macro_rules! hash_array_decimal {
    ($array_type:ident, $column: ident, $hashes: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = spark_compatible_murmur3_hash(
                    array.value(i).as_i128().to_le_bytes(),
                    *hash,
                );
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).as_i128().to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
    };
}

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u32],
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = Arc::clone(dict_array.values());
    let mut dict_hashes = vec![0; dict_values.len()];
    create_hashes(&[dict_values], &mut dict_hashes)?;

    for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
        if let Some(key) = key {
            let idx = key.to_usize().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Can not convert key value {:?} to usize in dictionary of type {:?}",
                    key,
                    dict_array.data_type()
                ))
            })?;
            *hash = dict_hashes[idx]
        } // no update for Null, consistent with other hashes
    }
    Ok(())
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut Vec<u32>,
) -> Result<&'a mut Vec<u32>> {
    for col in arrays {
        match col.data_type() {
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                        *hash = spark_compatible_murmur3_hash(
                            ((if array.value(i) { 1 } else { 0 }) as i32).to_le_bytes(),
                            *hash,
                        );
                    }
                } else {
                    for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = spark_compatible_murmur3_hash(
                                ((if array.value(i) { 1 } else { 0 }) as i32)
                                    .to_le_bytes(),
                                *hash,
                            );
                        }
                    }
                }
            }
            DataType::Int8 => {
                hash_array_primitive_i32!(Int8Array, col, i8, hashes_buffer);
            }
            DataType::Int16 => {
                hash_array_primitive_i32!(Int16Array, col, i16, hashes_buffer);
            }
            DataType::Int32 => {
                hash_array_primitive_i32!(Int32Array, col, i32, hashes_buffer);
            }
            DataType::Int64 => {
                hash_array_primitive_i64!(Int64Array, col, i64, hashes_buffer);
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                hash_array_primitive_i64!(TimestampSecondArray, col, i64, hashes_buffer);
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                hash_array_primitive_i64!(
                    TimestampMillisecondArray,
                    col,
                    i64,
                    hashes_buffer
                );
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_array_primitive_i64!(
                    TimestampMicrosecondArray,
                    col,
                    i64,
                    hashes_buffer
                );
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                hash_array_primitive_i64!(
                    TimestampNanosecondArray,
                    col,
                    i64,
                    hashes_buffer
                );
            }
            DataType::Date32 => {
                hash_array_primitive_i32!(Date32Array, col, i32, hashes_buffer);
            }
            DataType::Date64 => {
                hash_array_primitive_i64!(Date64Array, col, i64, hashes_buffer);
            }
            DataType::Utf8 => {
                hash_array!(StringArray, col, str, hashes_buffer);
            }
            DataType::LargeUtf8 => {
                hash_array!(LargeStringArray, col, str, hashes_buffer);
            }
            DataType::Decimal(_, _) => {
                hash_array_decimal!(DecimalArray, col, hashes_buffer);
            }
            DataType::Dictionary(index_type, _) => match **index_type {
                DataType::Int8 => {
                    create_hashes_dictionary::<Int8Type>(col, hashes_buffer)?;
                }
                DataType::Int16 => {
                    create_hashes_dictionary::<Int16Type>(col, hashes_buffer)?;
                }
                DataType::Int32 => {
                    create_hashes_dictionary::<Int32Type>(col, hashes_buffer)?;
                }
                DataType::Int64 => {
                    create_hashes_dictionary::<Int64Type>(col, hashes_buffer)?;
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported dictionary type in hasher hashing: {}",
                        col.data_type(),
                    )))
                }
            },
            _ => {
                // This is internal because we should have caught this before.
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type in hasher: {}",
                    col.data_type()
                )));
            }
        }
    }
    Ok(hashes_buffer)
}

pub(crate) fn pmod(hash: u32, n: usize) -> usize {
    let hash = hash as i32;
    let n = n as i32;
    let r = hash % n;
    let result = if r < 0 { (r + n) % n } else { r };
    result as usize
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, Int32Array, Int64Array, Int8Array, StringArray,
    };
    use datafusion::from_slice::FromSlice;

    use crate::spark_hash::{create_hashes, pmod};

    #[test]
    fn test_i8() {
        let i = Arc::new(Int8Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i8::MAX),
            Some(i8::MIN),
        ])) as ArrayRef;
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Spark Murmur3_x86_32
        let expected = vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x43b4d8ed, 0x422a1365];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_i32() {
        let i = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i32::MAX),
            Some(i32::MIN),
        ])) as ArrayRef;
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Spark Murmur3_x86_32
        let expected = vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x07fb67e7, 0x2b1f0fc6];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_i64() {
        let i = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
        ])) as ArrayRef;
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Spark Murmur3_x86_32
        let expected = vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_str() {
        let i = Arc::new(StringArray::from_slice(&["hello", "bar", "", "😁", "天地"]));
        let mut hashes = vec![42; 5];
        create_hashes(&[i], &mut hashes).unwrap();

        // generated with Murmur3Hash(Seq(Literal("")), 42).eval() since Spark is tested against this as well
        let expected = vec![3286402344, 2486176763, 142593372, 885025535, 2395000894];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_pmod() {
        let i: Vec<u32> =
            vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        let result = i.into_iter().map(|i| pmod(i, 200)).collect::<Vec<usize>>();

        // expected partition from Spark with n=200
        let expected = vec![69, 5, 193, 171, 115];
        assert_eq!(result, expected);
    }
}
