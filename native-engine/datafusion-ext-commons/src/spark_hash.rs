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

//! Functionality used both on logical and physical plans

use arrow::{
    array::*,
    datatypes::{
        ArrowDictionaryKeyType, ArrowNativeType, DataType, Int8Type, Int16Type, Int32Type,
        Int64Type, TimeUnit,
    },
};

use crate::hash::{mur::spark_compatible_murmur3_hash, xxhash::spark_compatible_xxhash64_hash};

pub fn create_murmur3_hashes(len: usize, arrays: &[ArrayRef], seed: i32) -> Vec<i32> {
    create_hashes(len, arrays, seed, |data: &[u8], seed: i32| {
        spark_compatible_murmur3_hash(data, seed)
    })
}

pub fn create_xxhash64_hashes(len: usize, arrays: &[ArrayRef], seed: i64) -> Vec<i64> {
    create_hashes(len, arrays, seed, |data: &[u8], seed: i64| {
        spark_compatible_xxhash64_hash(data, seed)
    })
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[inline]
pub fn create_hashes<T: num::PrimInt>(
    len: usize,
    arrays: &[ArrayRef],
    seed: T,
    h: impl Fn(&[u8], T) -> T + Copy,
) -> Vec<T> {
    let mut hash_buffer = vec![seed; len];
    for col in arrays.iter() {
        hash_array(col, &mut hash_buffer, h);
    }
    hash_buffer
}

#[inline]
fn hash_array<T: num::PrimInt>(
    array: &ArrayRef,
    hashes_buffer: &mut [T],
    h: impl Fn(&[u8], T) -> T + Copy,
) {
    assert_eq!(array.len(), hashes_buffer.len());

    macro_rules! hash_array {
        ($array_type:ident, $column:ident, $hashes:ident, $h:expr) => {
            let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
            if array.null_count() == 0 {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $h(&array.value(i).as_ref(), *hash);
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $h(&array.value(i).as_ref(), *hash);
                    }
                }
            }
        };
    }

    macro_rules! hash_array_primitive {
        ($array_type:ident, $column:ident, $ty:ident, $hashes:ident, $h:expr) => {
            let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
            let values = array.values();

            if array.null_count() == 0 {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $h((*value as $ty).to_le_bytes().as_ref(), *hash);
                }
            } else {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = $h((*value as $ty).to_le_bytes().as_ref(), *hash);
                    }
                }
            }
        };
    }

    macro_rules! hash_array_decimal {
        ($array_type:ident, $column:ident, $hashes:ident, $h:expr) => {
            let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

            if array.null_count() == 0 {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $h(array.value(i).to_le_bytes().as_ref(), *hash);
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $h(array.value(i).to_le_bytes().as_ref(), *hash);
                    }
                }
            }
        };
    }

    match array.data_type() {
        DataType::Null => {}
        DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if array.null_count() == 0 {
                for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                    *hash = h(
                        (if array.value(i) { 1u32 } else { 0u32 })
                            .to_le_bytes()
                            .as_ref(),
                        *hash,
                    );
                }
            } else {
                for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = h(
                            (if array.value(i) { 1u32 } else { 0u32 })
                                .to_le_bytes()
                                .as_ref(),
                            *hash,
                        );
                    }
                }
            }
        }
        DataType::Int8 => {
            hash_array_primitive!(Int8Array, array, i32, hashes_buffer, h);
        }
        DataType::Int16 => {
            hash_array_primitive!(Int16Array, array, i32, hashes_buffer, h);
        }
        DataType::Int32 => {
            hash_array_primitive!(Int32Array, array, i32, hashes_buffer, h);
        }
        DataType::Int64 => {
            hash_array_primitive!(Int64Array, array, i64, hashes_buffer, h);
        }
        DataType::Float32 => {
            hash_array_primitive!(Float32Array, array, f32, hashes_buffer, h);
        }
        DataType::Float64 => {
            hash_array_primitive!(Float64Array, array, f64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            hash_array_primitive!(TimestampSecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            hash_array_primitive!(TimestampMillisecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            hash_array_primitive!(TimestampMicrosecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            hash_array_primitive!(TimestampNanosecondArray, array, i64, hashes_buffer, h);
        }
        DataType::Date32 => {
            hash_array_primitive!(Date32Array, array, i32, hashes_buffer, h);
        }
        DataType::Date64 => {
            hash_array_primitive!(Date64Array, array, i64, hashes_buffer, h);
        }
        DataType::Binary => {
            hash_array!(BinaryArray, array, hashes_buffer, h);
        }
        DataType::LargeBinary => {
            hash_array!(LargeBinaryArray, array, hashes_buffer, h);
        }
        DataType::Utf8 => {
            hash_array!(StringArray, array, hashes_buffer, h);
        }
        DataType::LargeUtf8 => {
            hash_array!(LargeStringArray, array, hashes_buffer, h);
        }
        DataType::Decimal128(..) => {
            hash_array_decimal!(Decimal128Array, array, hashes_buffer, h);
        }
        DataType::Dictionary(index_type, _) => match index_type.as_ref() {
            DataType::Int8 => create_hashes_dictionary::<Int8Type, _>(array, hashes_buffer, h),
            DataType::Int16 => create_hashes_dictionary::<Int16Type, _>(array, hashes_buffer, h),
            DataType::Int32 => create_hashes_dictionary::<Int32Type, _>(array, hashes_buffer, h),
            DataType::Int64 => create_hashes_dictionary::<Int64Type, _>(array, hashes_buffer, h),
            other => panic!("Unsupported dictionary type in hasher hashing: {other}"),
        },
        _ => {
            for idx in 0..array.len() {
                hash_one(array, idx, &mut hashes_buffer[idx], h);
            }
        }
    }
}

/// Hash the values in a dictionary array
#[inline]
fn create_hashes_dictionary<K: ArrowDictionaryKeyType, T: num::PrimInt>(
    array: &ArrayRef,
    hashes_buffer: &mut [T],
    h: impl Fn(&[u8], T) -> T + Copy,
) {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = dict_array.values();
    for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
        if let Some(key) = key {
            hash_one(dict_values, key.as_usize(), hash, h);
        }
    }
}

fn hash_one<T: num::PrimInt>(
    col: &ArrayRef,
    idx: usize,
    hash: &mut T,
    h: impl Fn(&[u8], T) -> T + Copy,
) {
    macro_rules! hash_one_primitive {
        ($array_type:ident, $column:ident, $ty:ident, $hash:ident, $idx:ident, $h:expr) => {
            let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
            *$hash = $h(
                (array.value($idx as usize) as $ty).to_le_bytes().as_ref(),
                *$hash,
            );
        };
    }

    macro_rules! hash_one_binary {
        ($array_type:ident, $column:ident, $hash:ident, $idx:ident, $h:expr) => {
            let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
            *$hash = $h(&array.value($idx as usize).as_ref(), *$hash);
        };
    }

    macro_rules! hash_one_decimal {
        ($array_type:ident, $column:ident, $hash:ident, $idx:ident, $h:expr) => {
            let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
            *$hash = $h(array.value($idx as usize).to_le_bytes().as_ref(), *$hash);
        };
    }

    if col.is_valid(idx) {
        match col.data_type() {
            DataType::Null => {}
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                *hash = h(
                    (if array.value(idx) { 1u32 } else { 0u32 })
                        .to_le_bytes()
                        .as_ref(),
                    *hash,
                );
            }
            DataType::Int8 => {
                hash_one_primitive!(Int8Array, col, i32, hash, idx, h);
            }
            DataType::Int16 => {
                hash_one_primitive!(Int16Array, col, i32, hash, idx, h);
            }
            DataType::Int32 => {
                hash_one_primitive!(Int32Array, col, i32, hash, idx, h);
            }
            DataType::Int64 => {
                hash_one_primitive!(Int64Array, col, i64, hash, idx, h);
            }
            DataType::Float32 => {
                hash_one_primitive!(Float32Array, col, f32, hash, idx, h);
            }
            DataType::Float64 => {
                hash_one_primitive!(Float64Array, col, f64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                hash_one_primitive!(TimestampSecondArray, col, i64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                hash_one_primitive!(TimestampMillisecondArray, col, i64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_one_primitive!(TimestampMicrosecondArray, col, i64, hash, idx, h);
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                hash_one_primitive!(TimestampNanosecondArray, col, i64, hash, idx, h);
            }
            DataType::Date32 => {
                hash_one_primitive!(Date32Array, col, i32, hash, idx, h);
            }
            DataType::Date64 => {
                hash_one_primitive!(Date64Array, col, i64, hash, idx, h);
            }
            DataType::Binary => {
                hash_one_binary!(BinaryArray, col, hash, idx, h);
            }
            DataType::LargeBinary => {
                hash_one_binary!(LargeBinaryArray, col, hash, idx, h);
            }
            DataType::Utf8 => {
                hash_one_binary!(StringArray, col, hash, idx, h);
            }
            DataType::LargeUtf8 => {
                hash_one_binary!(LargeStringArray, col, hash, idx, h);
            }
            DataType::Decimal128(..) => {
                hash_one_decimal!(Decimal128Array, col, hash, idx, h);
            }
            DataType::List(..) => {
                let list_array = col.as_any().downcast_ref::<ListArray>().unwrap();
                let value_array = list_array.value(idx);
                for i in 0..value_array.len() {
                    hash_one(&value_array, i, hash, h);
                }
            }
            DataType::Map(..) => {
                let map_array = col.as_any().downcast_ref::<MapArray>().unwrap();
                let kv_array = map_array.value(idx);
                let key_array = kv_array.column(0);
                let value_array = kv_array.column(1);
                for i in 0..kv_array.len() {
                    hash_one(key_array, i, hash, h);
                    hash_one(value_array, i, hash, h);
                }
            }
            DataType::Struct(_) => {
                let struct_array = col.as_any().downcast_ref::<StructArray>().unwrap();
                for col in struct_array.columns() {
                    hash_one(col, idx, hash, h);
                }
            }
            other => panic!("Unsupported data type in hasher: {other}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, ArrayData, ArrayRef, Int8Array, Int32Array, Int64Array, MapArray, StringArray,
            StructArray, UInt32Array, make_array,
        },
        buffer::Buffer,
        datatypes::{DataType, Field, ToByteSlice},
    };

    use super::*;

    #[test]
    fn test_list() {
        let mut hashes_buffer = vec![42; 4];
        for hash in hashes_buffer.iter_mut() {
            *hash = spark_compatible_murmur3_hash(5_i32.to_le_bytes(), *hash);
        }
    }

    #[test]
    fn test_i8() {
        let i = Arc::new(Int8Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i8::MAX),
            Some(i8::MIN),
        ])) as ArrayRef;
        let hashes = create_murmur3_hashes(5, &[i], 42);

        // generated with Spark Murmur3_x86_32
        let expected: Vec<i32> = [
            0xdea578e3_u32,
            0x379fae8f,
            0xa0590e3d,
            0x43b4d8ed,
            0x422a1365,
        ]
        .into_iter()
        .map(|v| v as i32)
        .collect();
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_i32() {
        let i = Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[i], 42);
        assert_eq!(hashes, vec![-559580957]);

        let j = Arc::new(Int32Array::from(vec![Some(2)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[j], 42);
        assert_eq!(hashes, vec![1765031574]);

        let m = Arc::new(Int32Array::from(vec![Some(3)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[m], 42);
        assert_eq!(hashes, vec![-1823081949]);

        let n = Arc::new(Int32Array::from(vec![Some(4)])) as ArrayRef;
        let hashes = create_murmur3_hashes(1, &[n], 42);
        assert_eq!(hashes, vec![-397064898]);
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

        // generated with Murmur3Hash(Seq(Literal(1L)), 42).eval() since Spark is tested
        let hashes = create_murmur3_hashes(5, &[i.clone()], 42);
        let expected: Vec<i32> = [
            0x99f0149d_u32,
            0x9c67b85d,
            0xc8008529,
            0xa05b5d7b,
            0xcd1e64fb,
        ]
        .into_iter()
        .map(|v| v as i32)
        .collect();
        assert_eq!(hashes, expected);

        // generated with XxHash64(Seq(Literal(1L)), 42).eval() since Spark is tested
        // against this as well
        let hashes = create_xxhash64_hashes(5, &[i.clone()], 42);
        let expected = vec![
            -7001672635703045582,
            -5252525462095825812,
            3858142552250413010,
            -3246596055638297850,
            -8619748838626508300,
        ];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_str() {
        let i = Arc::new(StringArray::from(vec!["hello", "bar", "", "😁", "天地"]));

        // generated with Murmur3Hash(Seq(Literal("")), 42).eval() since Spark is tested
        // against this as well
        let hashes = create_murmur3_hashes(5, &[i.clone()], 42);
        let expected: Vec<i32> = [3286402344_u32, 2486176763, 142593372, 885025535, 2395000894]
            .into_iter()
            .map(|v| v as i32)
            .collect();
        assert_eq!(hashes, expected);

        // generated with XxHash64(Seq(Literal("")), 42).eval() since Spark is tested
        // against this as well
        let hashes = create_xxhash64_hashes(5, &[i.clone()], 42);
        let expected = vec![
            -4367754540140381902,
            -1798770879548125814,
            -7444071767201028348,
            -6337236088984028203,
            -235771157374669727,
        ];
        assert_eq!(hashes, expected);
    }

    #[test]
    fn test_list_array() {
        // Create inner array data: [1, 2, 3, 4, 5, 6]
        let value_data = ArrayData::builder(DataType::Int32)
            .len(6)
            .add_buffer(Buffer::from_slice_ref(
                &[1i32, 2, 3, 4, 5, 6].to_byte_slice(),
            ))
            .build()
            .unwrap();

        // Create offset array to define list boundaries: [[1, 2], [3, 4, 5], [6]]
        let list_data_type = DataType::new_list(DataType::Int32, false);
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(Buffer::from_slice_ref(&[0i32, 2, 5, 6].to_byte_slice()))
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListArray::from(list_data);
        let array_ref = Arc::new(list_array) as ArrayRef;

        // Test Murmur3 hash
        let hashes = create_murmur3_hashes(3, &[array_ref.clone()], 42);
        assert_eq!(hashes, vec![-222940379, -374492525, -331964951]);
    }

    #[test]
    fn test_map_array() {
        // Construct key and values
        let key_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(
                &[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice(),
            ))
            .build()
            .unwrap();
        let value_data = ArrayData::builder(DataType::UInt32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref(
                &[0u32, 10, 20, 0, 40, 0, 60, 70].to_byte_slice(),
            ))
            .null_bit_buffer(Some(Buffer::from(&[0b11010110])))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let entry_offsets = Buffer::from_slice_ref(&[0, 3, 6, 8].to_byte_slice());

        let keys_field = Arc::new(Field::new("keys", DataType::Int32, false));
        let values_field = Arc::new(Field::new("values", DataType::UInt32, true));
        let entry_struct = StructArray::from(vec![
            (keys_field.clone(), make_array(key_data)),
            (values_field.clone(), make_array(value_data.clone())),
        ]);

        // Construct a map array from the above two
        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                true,
            )),
            false,
        );
        let map_data = ArrayData::builder(map_data_type)
            .len(3)
            .add_buffer(entry_offsets)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        assert_eq!(&value_data, &map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(3, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[2]);
        assert_eq!(2, map_array.value_length(2));

        let key_array = Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef;
        let value_array =
            Arc::new(UInt32Array::from(vec![None, Some(10u32), Some(20)])) as ArrayRef;
        let struct_array = StructArray::from(vec![
            (keys_field.clone(), key_array),
            (values_field.clone(), value_array),
        ]);
        assert_eq!(
            struct_array,
            StructArray::from(map_array.value(0).into_data())
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        for i in 0..3 {
            assert!(map_array.is_valid(i));
            assert!(!map_array.is_null(i));
        }

        // Now test with a non-zero offset
        let map_data = ArrayData::builder(map_array.data_type().clone())
            .len(2)
            .offset(1)
            .add_buffer(map_array.to_data().buffers()[0].clone())
            .add_child_data(map_array.to_data().child_data()[0].clone())
            .build()
            .unwrap();
        let map_array = MapArray::from(map_data);

        assert_eq!(&value_data, &map_array.values().to_data());
        assert_eq!(&DataType::UInt32, map_array.value_type());
        assert_eq!(2, map_array.len());
        assert_eq!(0, map_array.null_count());
        assert_eq!(6, map_array.value_offsets()[1]);
        assert_eq!(2, map_array.value_length(1));

        let key_array = Arc::new(Int32Array::from(vec![3, 4, 5])) as ArrayRef;
        let value_array = Arc::new(UInt32Array::from(vec![None, Some(40), None])) as ArrayRef;
        let struct_array =
            StructArray::from(vec![(keys_field, key_array), (values_field, value_array)]);
        assert_eq!(
            &struct_array,
            map_array
                .value(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
        assert_eq!(
            &struct_array,
            unsafe { map_array.value_unchecked(0) }
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        );
    }
}
