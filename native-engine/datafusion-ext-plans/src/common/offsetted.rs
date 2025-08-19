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

use std::{marker::PhantomData, ops::Range};

use datafusion::common::Result;
use datafusion_ext_commons::algorithm::rdx_queue::{KeyForRadixQueue, RadixQueue};
use num::PrimInt;

pub struct Offsetted<O, T> {
    offsets: Vec<O>,
    data: T,
}

impl<O: PrimInt, T> Offsetted<O, T> {
    pub fn new(offsets: Vec<O>, data: T) -> Self {
        Self { offsets, data }
    }

    pub fn offsets(&self) -> &[O] {
        &self.offsets
    }

    pub fn offset(&self, i: usize) -> Range<O> {
        self.offsets[i]..self.offsets[i + 1]
    }

    pub fn data(&self) -> &T {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut T {
        &mut self.data
    }

    pub fn map_data<U>(self, f: impl FnOnce(T) -> U) -> Offsetted<O, U> {
        Offsetted {
            offsets: self.offsets,
            data: f(self.data),
        }
    }

    pub fn try_map_data<U>(self, f: impl FnOnce(T) -> Result<U>) -> Result<Offsetted<O, U>> {
        Ok(Offsetted {
            offsets: self.offsets,
            data: f(self.data)?,
        })
    }
}

pub struct OffsettedCursor<O, T> {
    offsetted: Offsetted<O, T>,
    cur: usize,
}

impl<O: PrimInt, T> KeyForRadixQueue for OffsettedCursor<O, T> {
    fn rdx(&self) -> usize {
        self.cur
    }
}

impl<O: PrimInt, T> OffsettedCursor<O, T> {
    pub fn new(offsetted: Offsetted<O, T>) -> Self {
        let mut new = Self { offsetted, cur: 0 };
        new.skip_empty_partitions();
        new
    }

    pub fn skip_empty_partitions(&mut self) {
        let offsets = self.offsetted.offsets();
        while self.cur + 1 < offsets.len() && offsets[self.cur + 1] == offsets[self.cur] {
            self.cur += 1;
        }
    }
}

pub struct OffsettedMergeIterator<'a, O, T> {
    num_partitions: usize,
    cursors: RadixQueue<OffsettedCursor<O, T>>,
    cur_partition_id: usize,
    cur_offset: O,
    merged_offsets: Vec<O>,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, O: PrimInt + 'a, T: 'a> OffsettedMergeIterator<'a, O, T> {
    pub fn new(num_partitions: usize, offsets: Vec<Offsetted<O, T>>) -> Self {
        assert!(
            !offsets.is_empty(),
            "OffsettedSpillsMergeIterator got no spills"
        );

        let cursors = RadixQueue::new(
            offsets
                .into_iter()
                .map(|offsetted| OffsettedCursor::new(offsetted))
                .collect(),
            num_partitions,
        );
        let cur_partition_id = cursors.peek().cur;

        Self {
            num_partitions,
            cursors,
            cur_partition_id,
            cur_offset: O::zero(),
            merged_offsets: Default::default(),
            _phantom: Default::default(),
        }
    }

    pub fn peek_next_partition_id(&self) -> usize {
        self.cursors.peek().cur
    }

    pub fn merged_offsets(&self) -> &[O] {
        &self.merged_offsets
    }

    pub fn next_partition_chunk<'z>(
        &'z mut self,
    ) -> Option<(usize, OffsettedMergePartitionChunkIterator<'a, 'z, O, T>)> {
        let chunk_partition_id = self.peek_next_partition_id();
        if chunk_partition_id < self.num_partitions {
            let chunk_iter = OffsettedMergePartitionChunkIterator {
                merge_iter: self,
                chunk_partition_id,
            };
            return Some((chunk_partition_id, chunk_iter));
        }
        None
    }
}

impl<'a, O: PrimInt + 'a, T: 'a> Iterator for OffsettedMergeIterator<'a, O, T> {
    type Item = (usize, &'a mut T, Range<O>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut min_cursor = self.cursors.peek_mut();
        self.cur_partition_id = min_cursor.cur;
        self.merged_offsets
            .resize(self.cur_partition_id + 1, self.cur_offset);

        if min_cursor.cur >= self.num_partitions {
            return None; // no more partitions
        }

        let range = min_cursor.offsetted.offset(self.cur_partition_id);
        let data = unsafe {
            // safety: bypass lifetime checker
            std::mem::transmute(min_cursor.offsetted.data_mut())
        };

        // forward partition id in min_spill
        self.cur_offset = self.cur_offset + range.end - range.start;
        min_cursor.cur += 1;
        min_cursor.skip_empty_partitions();

        // return current reader
        Some((self.cur_partition_id, data, range))
    }
}

pub struct OffsettedMergePartitionChunkIterator<'a, 'z, O, T> {
    merge_iter: &'z mut OffsettedMergeIterator<'a, O, T>,
    chunk_partition_id: usize,
}

impl<'a, O: PrimInt + 'a, T: 'a> Iterator for OffsettedMergePartitionChunkIterator<'a, '_, O, T> {
    type Item = (&'a mut T, Range<O>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.merge_iter.peek_next_partition_id() == self.chunk_partition_id {
            return self.merge_iter.next().map(|(_, data, range)| (data, range));
        }
        None
    }
}

pub type OffsettedMergePartitionChunkIteratorBypassLifetimeCheck<O, T> =
    OffsettedMergePartitionChunkIterator<'static, 'static, O, T>;
