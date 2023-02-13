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

/// An implementation of the tournament loser tree data structure.
pub struct LoserTree<T> {
    nodes: Vec<usize>,
    values: Vec<T>,
    lt: fn(&T, &T) -> bool,
}

impl<T> LoserTree<T> {
    pub fn new_by(values: Vec<T>, lt: fn(&T, &T) -> bool) -> Self {
        let mut tree = Self {
            nodes: vec![],
            values,
            lt,
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

    pub fn peek_mut(&mut self) -> LoserTreePeekMut<T> {
        LoserTreePeekMut {
            tree: self,
            dirty: false,
        }
    }

    fn init_tree(&mut self) {
        self.nodes = vec![usize::MAX; self.values.len()];

        for i in 0..self.values.len() {
            let mut winner = i;
            let mut cmp_node = (self.values.len() + i) / 2;
            while cmp_node != 0 && self.nodes[cmp_node] != usize::MAX {
                let challenger = self.nodes[cmp_node];
                if (self.lt)(&self.values[challenger], &self.values[winner]) {
                    self.nodes[cmp_node] = winner;
                    winner = challenger;
                } else {
                    self.nodes[cmp_node] = challenger;
                }
                cmp_node /= 2;
            }
            self.nodes[cmp_node] = winner;
        }
    }

    fn adjust_tree(&mut self) {
        let mut winner = self.nodes[0];
        let mut cmp_node = (self.values.len() + winner) / 2;
        while cmp_node != 0 {
            let challenger = self.nodes[cmp_node];
            if (self.lt)(&self.values[challenger], &self.values[winner]) {
                self.nodes[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node /= 2;
        }
        self.nodes[0] = winner;
    }
}

/// A PeekMut structure to the loser tree, used to get smallest value and auto
/// adjusting after dropped.
pub struct LoserTreePeekMut<'a, T> {
    tree: &'a mut LoserTree<T>,
    dirty: bool,
}

impl<T> Deref for LoserTreePeekMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.tree.values[self.tree.nodes[0]]
    }
}

impl<T> DerefMut for LoserTreePeekMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.tree.values[self.tree.nodes[0]]
    }
}

impl<T> Drop for LoserTreePeekMut<'_, T> {
    fn drop(&mut self) {
        if self.dirty {
            self.tree.adjust_tree();
        }
    }
}
