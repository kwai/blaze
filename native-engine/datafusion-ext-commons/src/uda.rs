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

use std::{any::Any, fmt::Debug, slice::Iter, sync::Arc};

use arrow::{
    array::{Array, ArrayData, ArrayRef, BooleanArray},
    buffer::NullBuffer,
    datatypes::DataType,
    error::Result,
};

#[derive(Debug, Clone)]
pub struct UserDefinedArray {
    data_type: DataType,
    items: Arc<Vec<Option<Arc<dyn Any + Send + Sync + 'static>>>>,
    offset: usize,
    length: usize,
}

impl UserDefinedArray {
    pub fn from_items(
        fake_data_type: DataType,
        items: Vec<Option<Arc<dyn Any + Send + Sync + 'static>>>,
    ) -> Self {
        Self {
            offset: 0,
            length: items.len(),
            data_type: fake_data_type,
            items: Arc::new(items),
        }
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, Option<Arc<dyn Any + Send + Sync + 'static>>> {
        self.items[self.offset..][..self.length].iter()
    }

    pub fn filter(&self, cond: &BooleanArray) -> Result<Self> {
        assert_eq!(cond.len(), self.len());

        let mut filtered_items = Vec::with_capacity(self.length);
        for (item, cond) in self.iter().zip(cond.iter()) {
            if cond.unwrap_or(false) {
                filtered_items.push(item.clone());
            }
        }
        filtered_items.shrink_to_fit();
        Ok(Self::from_items(self.data_type.clone(), filtered_items))
    }

    pub fn scatter(&self, mask: &BooleanArray) -> Result<Self> {
        let mut scattered_items = vec![];
        let mut iter = self.iter();

        for cond in mask.iter() {
            if cond.unwrap_or(false) {
                scattered_items.push(
                    iter.next()
                        .cloned()
                        .expect("scatter with incorrect truty value count"),
                );
            } else {
                scattered_items.push(None);
            }
        }
        scattered_items.shrink_to_fit();
        Ok(Self::from_items(self.data_type.clone(), scattered_items))
    }
}

impl Array for UserDefinedArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        unimplemented!("UserDefinedArray.to_data() not implemented")
    }

    fn into_data(self) -> ArrayData {
        unimplemented!("UserDefinedArray.into_data() not implemented")
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        let offset = self.offset + offset;
        Arc::new(Self {
            data_type: self.data_type.clone(),
            items: self.items.clone(),
            offset,
            length,
        })
    }

    fn len(&self) -> usize {
        self.length
    }

    fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    fn is_null(&self, index: usize) -> bool {
        self.items[self.offset + index].is_none()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.items[self.offset + index].is_some()
    }

    fn null_count(&self) -> usize {
        self.iter().filter(|item| item.is_some()).count()
    }

    fn get_buffer_memory_size(&self) -> usize {
        64 * self.items.len() // not precise
    }

    fn get_array_memory_size(&self) -> usize {
        64 * self.items.len() + size_of::<Self>() // not precise
    }
}
