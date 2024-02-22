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

#[inline]
pub fn radix_sort_u16_by<T>(array: &mut [T], key: impl Fn(&T) -> u16) -> Vec<usize> {
    radix_sort_u16_ranged_by(array, 65536, key)
}

#[inline]
pub fn radix_sort_u16_ranged_by<T>(
    array: &mut [T],
    num_keys: usize,
    key: impl Fn(&T) -> u16,
) -> Vec<usize> {
    // performance critical
    unsafe {
        // count
        let mut counts = vec![0; num_keys];
        for item in array.iter() {
            *counts.get_unchecked_mut(key(item) as usize) += 1;
        }

        // construct parts
        #[derive(Default, Clone, Copy)]
        struct Part {
            cur: usize,
            end: usize,
        }
        let mut parts = vec![Part::default(); num_keys];
        let mut beg = 0;
        for (idx, count) in counts.iter().enumerate() {
            if *count > 0 {
                *parts.get_unchecked_mut(idx) = Part {
                    cur: beg,
                    end: beg + count,
                };
                beg += count;
            }
        }

        // reorganize each partition
        let mut inexhausted_part_indices = vec![0; num_keys];
        for i in 0..num_keys {
            inexhausted_part_indices[i] = i;
        }
        while {
            inexhausted_part_indices.retain(|&i| {
                let part = parts.get_unchecked(i);
                part.cur < part.end
            });
            inexhausted_part_indices.len() > 1
        } {
            for &part_idx in &inexhausted_part_indices {
                let cur_part = parts.get_unchecked(part_idx);
                let cur = cur_part.cur;
                let end = cur_part.end;
                for item_idx in cur..end {
                    let target_part_idx = key(array.get_unchecked(item_idx)) as usize;
                    let target_part = parts.get_unchecked_mut(target_part_idx);
                    array.swap_unchecked(item_idx, target_part.cur);
                    target_part.cur += 1;
                }
            }
        }

        // returns counts of each bucket
        counts
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use crate::rdxsort::radix_sort_u16_by;

    #[test]
    fn fuzzytest_u16_small() {
        for n in 0..1000 {
            let mut array = vec![];
            for _ in 0..n {
                array.push(rand::thread_rng().gen::<u16>());
            }

            let mut array1 = array.clone();
            radix_sort_u16_by(&mut array1, |key| *key);

            let mut array2 = array.clone();
            array2.sort_unstable();

            assert_eq!(array1, array2);
        }
    }

    #[test]
    fn fuzzytest_u16_1m() {
        let mut array = vec![];
        for _ in 0..1000000 {
            array.push(rand::thread_rng().gen::<u16>());
        }

        let mut array1 = array.clone();
        radix_sort_u16_by(&mut array1, |key| *key);

        let mut array2 = array.clone();
        array2.sort_unstable();

        assert_eq!(array1, array2);
    }
}
