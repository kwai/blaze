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

pub trait ComparableForLoserTree {
    fn lt(&self, other: &Self) -> bool;
}

/// An implementation of the tournament loser tree data structure.
pub struct LoserTree<T> {
    losers: UncheckedIndex<Vec<usize>>,
    values: UncheckedIndex<Vec<T>>,
}

#[allow(clippy::len_without_is_empty)]
impl<T: ComparableForLoserTree> LoserTree<T> {
    pub fn new(values: Vec<T>) -> Self {
        let mut tree = unsafe {
            // safety:
            // this component is performance critical,  use unchecked index
            // to avoid boundary checking.
            Self {
                losers: unchecked_index::unchecked_index(vec![]),
                values: unchecked_index::unchecked_index(values),
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
        &self.values[0]
    }

    pub fn peek_mut(&mut self) -> LoserTreePeekMut<T> {
        LoserTreePeekMut {
            tree: self,
            dirty: false,
        }
    }

    fn init_tree(&mut self) {
        self.losers.resize(self.values.len(), usize::MAX);
        for i in 0..self.values.len() {
            let mut winner = i;
            let mut cmp_node = (self.values.len() + i) / 2;
            while cmp_node != 0 && self.losers[cmp_node] != usize::MAX {
                let challenger = self.losers[cmp_node];
                if self.values[challenger].lt(&self.values[winner]) {
                    self.losers[cmp_node] = winner;
                    winner = challenger;
                } else {
                    self.losers[cmp_node] = challenger;
                }
                cmp_node /= 2;
            }
            self.losers[cmp_node] = winner;
        }
    }

    fn adjust_tree(&mut self) {
        let mut winner = self.losers[0];
        let mut cmp_node = (self.values.len() + winner) / 2;
        while cmp_node != 0 {
            let challenger = self.losers[cmp_node];
            if self.values[challenger].lt(&self.values[winner]) {
                self.losers[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node /= 2;
        }
        self.losers[0] = winner;
    }
}

/// A PeekMut structure to the loser tree, used to get smallest value and auto
/// adjusting after dropped.
pub struct LoserTreePeekMut<'a, T: ComparableForLoserTree> {
    tree: &'a mut LoserTree<T>,
    dirty: bool,
}

impl<T: ComparableForLoserTree> LoserTreePeekMut<'_, T> {
    pub fn adjust(&mut self) {
        if self.dirty {
            self.tree.adjust_tree();
            self.dirty = false;
        }
    }
}

impl<T: ComparableForLoserTree> Deref for LoserTreePeekMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.tree.values[self.tree.losers[0]]
    }
}

impl<T: ComparableForLoserTree> DerefMut for LoserTreePeekMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.tree.values[self.tree.losers[0]]
    }
}

impl<T: ComparableForLoserTree> Drop for LoserTreePeekMut<'_, T> {
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

    use crate::ds::loser_tree::{ComparableForLoserTree, LoserTree};

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
            impl ComparableForLoserTree for Cursor {
                fn lt(&self, other: &Self) -> bool {
                    match (
                        self.values.get(self.row_idx),
                        other.values.get(other.row_idx),
                    ) {
                        (Some(v1), Some(v2)) => v1 < v2,
                        (None, _) => false,
                        (_, None) => true,
                    }
                }
            }
            let mut loser_tree = LoserTree::new(
                nodes
                    .into_iter()
                    .map(|node| Cursor {
                        row_idx: 0,
                        values: node.into_iter().sorted_unstable().collect_vec(),
                    })
                    .collect_vec(),
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
