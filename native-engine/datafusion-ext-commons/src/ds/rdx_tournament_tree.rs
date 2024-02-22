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

use std::ops::{Deref, DerefMut};

use unchecked_index::UncheckedIndex;

pub trait KeyForRadixTournamentTree {
    fn rdx(&self) -> usize;
}

/// An implementation of the radix tournament tree
/// with time complexity of sorting all values: O(n + K)
pub struct RadixTournamentTree<T> {
    num_keys: usize,
    cur_rdx: usize,
    values: UncheckedIndex<Vec<T>>,
    entries: UncheckedIndex<Vec<usize>>,
    node_nexts: UncheckedIndex<Vec<usize>>,
}

#[allow(clippy::len_without_is_empty)]
impl<T: KeyForRadixTournamentTree> RadixTournamentTree<T> {
    pub fn new(values: Vec<T>, num_keys: usize) -> Self {
        let num_values = values.len();
        let mut tree = unsafe {
            // safety:
            // this component is performance critical,  use unchecked index
            // to avoid boundary checking.
            Self {
                num_keys,
                cur_rdx: 0,
                values: unchecked_index::unchecked_index(values),
                entries: unchecked_index::unchecked_index(vec![usize::MAX; num_keys]),
                node_nexts: unchecked_index::unchecked_index(vec![usize::MAX; num_values]),
            }
        };
        tree.init_tree();
        tree
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut [T] {
        &mut self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn peek(&self) -> &T {
        &self.values[self.entries.get(self.cur_rdx).cloned().unwrap_or_default()]
    }

    pub fn peek_mut(&mut self) -> RadixTournamentTreePeekMut<T> {
        RadixTournamentTreePeekMut {
            tree: self,
            dirty: false,
        }
    }

    fn init_tree(&mut self) {
        let mut min_rdx = usize::MAX;
        for (i, v) in self.values.iter().enumerate() {
            let rdx = v.rdx();
            if rdx < self.num_keys {
                self.node_nexts[i] = self.entries[rdx];
                self.entries[rdx] = i;
            }
            min_rdx = min_rdx.min(rdx);
        }
        self.cur_rdx = min_rdx;
    }

    fn adjust_tree(&mut self) {
        let old_rdx = self.cur_rdx;
        if old_rdx < self.num_keys {
            let i = self.entries[old_rdx];
            let new_rdx = self.values[i].rdx();

            // move current node to the correct bucket
            if new_rdx > old_rdx {
                // unlink from old bucket
                self.entries[old_rdx] = self.node_nexts[i];

                // link to new bucket
                if new_rdx < self.num_keys {
                    self.node_nexts[i] = self.entries[new_rdx];
                    self.entries[new_rdx] = i;
                }

                // forward cur_rdx if current bucket is exhausted
                let mut next_rdx = old_rdx;
                while next_rdx < self.num_keys && self.entries[next_rdx] == usize::MAX {
                    next_rdx += 1;
                }
                self.cur_rdx = next_rdx;
            }
        }
    }
}

/// A PeekMut structure to the loser tree, used to get smallest value and auto
/// adjusting after dropped.
pub struct RadixTournamentTreePeekMut<'a, T: KeyForRadixTournamentTree> {
    tree: &'a mut RadixTournamentTree<T>,
    dirty: bool,
}

impl<T: KeyForRadixTournamentTree> Deref for RadixTournamentTreePeekMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.tree.peek()
    }
}

impl<T: KeyForRadixTournamentTree> DerefMut for RadixTournamentTreePeekMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.tree.values[self
            .tree
            .entries
            .get(self.tree.cur_rdx)
            .cloned()
            .unwrap_or_default()]
    }
}

impl<T: KeyForRadixTournamentTree> Drop for RadixTournamentTreePeekMut<'_, T> {
    fn drop(&mut self) {
        if self.dirty {
            self.tree.adjust_tree();
        }
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use rand::Rng;

    use crate::ds::rdx_tournament_tree::{KeyForRadixTournamentTree, RadixTournamentTree};

    #[test]
    fn fuzztest() {
        for _ in 0..10 {
            let num_nodes = rand::thread_rng().gen_range(1..=999);
            let mut nodes = vec![];
            for _ in 0..num_nodes {
                let node_len = rand::thread_rng().gen_range(1..=999);
                let mut node = vec![];
                for _ in 0..node_len {
                    node.push(rand::thread_rng().gen_range(1000..=9999));
                }
                nodes.push(node);
            }

            // expected
            let expected = nodes
                .clone()
                .into_iter()
                .flatten()
                .sorted_unstable()
                .collect_vec();

            // actual
            struct Cursor {
                row_idx: usize,
                values: Vec<u64>,
            }
            impl KeyForRadixTournamentTree for Cursor {
                fn rdx(&self) -> usize {
                    self.values.get(self.row_idx).cloned().unwrap_or(u64::MAX) as usize
                }
            }
            let mut loser_tree = RadixTournamentTree::new(
                nodes
                    .into_iter()
                    .map(|node| Cursor {
                        row_idx: 0,
                        values: node.into_iter().sorted_unstable().collect_vec(),
                    })
                    .collect_vec(),
                10000,
            );

            let mut actual = vec![];
            loop {
                let mut min = loser_tree.peek_mut();
                if let Some(v) = min.values.get(min.row_idx) {
                    actual.push(*v);
                    min.row_idx += 1;
                } else {
                    break;
                }
            }

            for cursor in loser_tree.values() {
                assert_eq!(cursor.row_idx, cursor.values.len());
            }
            assert_eq!(actual, expected);
        }
    }
}
