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

use arrow::row::Row;
use slimmer_box::SlimmerBox;

#[derive(Debug, Clone, Hash, PartialOrd, PartialEq, Ord, Eq)]
pub struct SlimBytes(SlimmerBox<[u8]>);

// safety: types inside SlimmerBox are threads-safely
unsafe impl Send for SlimBytes {}
unsafe impl Sync for SlimBytes {}

impl SlimBytes {
    pub fn into_vec(self) -> Vec<u8> {
        SlimmerBox::into_box(self.0).into_vec()
    }
}

impl Default for SlimBytes {
    fn default() -> Self {
        SlimBytes(SlimmerBox::new(&[]))
    }
}

impl From<Box<[u8]>> for SlimBytes {
    fn from(value: Box<[u8]>) -> Self {
        SlimBytes(SlimmerBox::from_box(value))
    }
}

impl From<Vec<u8>> for SlimBytes {
    fn from(value: Vec<u8>) -> Self {
        SlimBytes(SlimmerBox::from_box(value.into()))
    }
}

impl From<&[u8]> for SlimBytes {
    fn from(value: &[u8]) -> Self {
        SlimBytes(SlimmerBox::new(value))
    }
}

impl From<&Row<'_>> for SlimBytes {
    fn from(value: &Row) -> Self {
        SlimBytes(SlimmerBox::new(value.as_ref()))
    }
}

impl AsRef<[u8]> for SlimBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for SlimBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl DerefMut for SlimBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut()
    }
}
