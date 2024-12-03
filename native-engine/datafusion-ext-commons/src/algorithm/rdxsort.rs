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

use std::vec::IntoIter;

use radsort::Key;

const STD_SORT_LIMIT: usize = 4096;

pub fn radix_sort_unstable(array: &mut [impl Key + Ord]) {
    radix_sort_unstable_by_key(array, |v| *v);
}

pub fn radix_sort_unstable_by_key<T, K: Key + Ord>(array: &mut [T], key: impl Fn(&T) -> K) {
    if array.len() < STD_SORT_LIMIT {
        array.sort_unstable_by_key(key);
    } else {
        radsort::sort_by_key(array, key);
    }
}

pub trait RadixSortIterExt: Iterator {
    fn radix_sorted_unstable(self) -> IntoIter<Self::Item>
    where
        Self: Sized,
        Self::Item: Key + Ord,
    {
        let mut vec: Vec<Self::Item> = self.collect();
        radix_sort_unstable(&mut vec);
        vec.into_iter()
    }

    fn radix_sorted_unstable_by_key<K: Key + Ord>(
        self,
        key: impl Fn(&Self::Item) -> K,
    ) -> IntoIter<Self::Item>
    where
        Self: Sized,
    {
        let mut vec: Vec<Self::Item> = self.collect();
        radix_sort_unstable_by_key(&mut vec, key);
        vec.into_iter()
    }
}

impl<T, I: Iterator<Item = T>> RadixSortIterExt for I {}
