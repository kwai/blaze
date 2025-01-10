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

use crate::joins::Idx;

pub mod existence_join;
pub mod full_join;
pub mod semi_join;

#[derive(Default)]
struct IdxVec {
    vec: Vec<Idx>,
    smallest: Option<Idx>,
}

impl IdxVec {
    pub fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    pub fn push(&mut self, idx: Idx) {
        if idx != (0, 0) {
            if self.smallest.is_none() || idx < self.smallest.unwrap() {
                self.smallest = Some(idx);
            }
        }
        self.vec.push(idx);
    }

    pub fn take_vec(&mut self) -> Vec<Idx> {
        let vec = std::mem::take(&mut self.vec);
        self.smallest = None;
        vec
    }

    pub fn smallest(&self) -> Option<Idx> {
        self.smallest
    }
}
