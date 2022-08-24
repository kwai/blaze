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

use datafusion::arrow::datatypes::*;
use datafusion::arrow::array::*;

///  Array builder for [`FixedSizeListArray`]
#[derive(Debug)]
pub struct FixedSizeListBuilder<T: ArrayBuilder> {
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    list_len: i32,
    field: Option<Field>,
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T> {
    /// Creates a new [`FixedSizeListBuilder`] from a given values array builder
    /// `value_length` is the number of values within each array
    pub fn new(values_builder: T, value_length: i32) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, value_length, capacity)
    }

    /// Creates a new [`FixedSizeListBuilder`] from a given values array builder and field
    /// `value_length` is the number of values within each array
    pub fn new_with_field(values_builder: T, value_length: i32, field: Field) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, value_length, capacity)
    }

    /// Creates a new [`FixedSizeListBuilder`] from a given values array builder
    /// `value_length` is the number of values within each array
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, value_length: i32, capacity: usize) -> Self {
        Self {
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            list_len: value_length,
            field: None,
        }
    }

    /// Creates a new [`FixedSizeListBuilder`] from a given values array builder and field
    /// `value_length` is the number of values within each array
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity_and_field(
        values_builder: T,
        value_length: i32,
        capacity: usize,
        field: Field,
    ) -> Self {
        let mut builder = Self::with_capacity(values_builder, value_length, capacity);
        builder.field = Some(field);
        builder
    }
}

impl<T: ArrayBuilder> ArrayBuilder for FixedSizeListBuilder<T>
where
    T: 'static,
{
    /// Returns the builder as a non-mutable `Any` reference.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    /// Returns the number of array slots in the builder
    fn len(&self) -> usize {
        self.bitmap_builder.len()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.bitmap_builder.is_empty()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<T: ArrayBuilder> FixedSizeListBuilder<T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call [`append`](#method.append) to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    pub fn value_length(&self) -> i32 {
        self.list_len
    }

    /// Finish the current fixed-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.bitmap_builder.append(is_valid);
        Ok(())
    }

    /// Builds the [`FixedSizeListBuilder`] and reset this builder.
    pub fn finish(&mut self) -> FixedSizeListArray {
        let len = self.len();
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        assert!(
            values_data.len() == len * self.list_len as usize,
            "Length of the child array ({}) must be the multiple of the value length ({}) and the array length ({}).",
            values_data.len(),
            self.list_len,
            len,
        );

        let null_bit_buffer = self.bitmap_builder.finish();
        let array_data = ArrayData::builder(DataType::FixedSizeList(
            Box::new(
                if let Some(field) = self.field {
                    // build with given field
                    field.clone()
                } else {
                    // build with default field
                    Field::new(
                        "item",
                        values_data.data_type().clone(),
                        true, // TODO: find a consistent way of getting this
                    )
                }
            ),
            self.list_len,
        ))
        .len(len)
        .add_child_data(values_data.clone())
        .null_bit_buffer(Some(null_bit_buffer));

        let array_data = unsafe { array_data.build_unchecked() };

        FixedSizeListArray::from(array_data)
    }
}

