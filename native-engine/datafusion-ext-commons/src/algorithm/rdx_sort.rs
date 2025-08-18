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

use crate::unchecked;

/// Perform radix sort on a single array
///
/// - array: the array to be sorted
/// - counts: the counters to be used for counting, must be initialized to 0.
///   will be filled with the number of elements in each bucket after sorting.
/// - key: a function to extract the key from the array element
pub fn radix_sort_by_key<T>(array: &mut [T], counts: &mut [usize], key: impl Fn(&T) -> usize) {
    #[derive(Default, Clone, Copy)]
    struct Part {
        cur: usize,
        end: usize,
    }

    let num_keys = counts.len();
    let mut counts = unchecked!(counts);
    let mut parts = unchecked!(vec![Part::default(); num_keys]);

    // count
    array.iter().for_each(|item| counts[key(item)] += 1);

    // construct parts
    let mut beg = 0;
    for (idx, count) in counts.iter().enumerate() {
        if *count > 0 {
            parts[idx] = Part {
                cur: beg,
                end: beg + count,
            };
            beg += count;
        }
    }

    // reorganize each partition
    let mut inexhausted_part_indices = unchecked!(vec![0; num_keys]);
    for i in 0..num_keys {
        inexhausted_part_indices[i] = i;
    }
    while {
        inexhausted_part_indices.retain(|&i| parts[i].cur < parts[i].end);
        inexhausted_part_indices.len() > 1
    } {
        for &part_idx in inexhausted_part_indices.iter() {
            let cur_part = &parts[part_idx];
            let cur = cur_part.cur;
            let end = cur_part.end;
            for item_idx in cur..end {
                let target_part_idx = key(&array[item_idx]);
                let target_part = &mut parts[target_part_idx];
                unsafe {
                    // safety: skip bound check
                    array.swap_unchecked(item_idx, target_part.cur);
                }
                target_part.cur += 1;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use super::*;

    #[test]
    fn fuzzytest_u16_small() {
        for n in 0..1000 {
            let mut array = vec![];
            for _ in 0..n {
                array.push(rand::rng().random::<u16>());
            }

            let mut array1 = array.clone();
            radix_sort_by_key(&mut array1, &mut [0; 65536], |key| *key as usize);

            let mut array2 = array.clone();
            array2.sort_unstable();

            assert_eq!(array1, array2);
        }
    }

    #[test]
    fn fuzzytest_u16_1m() {
        let mut array = vec![];
        for _ in 0..1000000 {
            array.push(rand::rng().random::<u16>());
        }

        let mut array1 = array.clone();
        radix_sort_by_key(&mut array1, &mut [0; 65536], |key| *key as usize);

        let mut array2 = array.clone();
        array2.sort_unstable();

        assert_eq!(array1, array2);
    }
}
