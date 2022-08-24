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

///  Array builder for `ListArray`
///  Originally copied from arrow-rs, with custom field supports
#[derive(Debug)]
pub struct GenericListBuilder<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> {
    offsets_builder: BufferBuilder<OffsetSize>,
    bitmap_builder: BooleanBufferBuilder,
    values_builder: T,
    len: OffsetSize,
    field: Option<Field>,
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T> {
    /// Creates a new `ListArrayBuilder` from a given values array builder
    pub fn new(values_builder: T) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity(values_builder, capacity)
    }

    /// Creates a new `ListArrayBuilder` from a given values array builder and field
    pub fn new_with_field(values_builder: T, field: Field) -> Self {
        let capacity = values_builder.len();
        Self::with_capacity_and_field(values_builder, capacity, field)
    }

    /// Creates a new `ListArrayBuilder` from a given values array builder
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity(values_builder: T, capacity: usize) -> Self {
        let mut offsets_builder = BufferBuilder::<OffsetSize>::new(capacity + 1);
        let len = OffsetSize::zero();
        offsets_builder.append(len);
        Self {
            field: None,
            offsets_builder,
            bitmap_builder: BooleanBufferBuilder::new(capacity),
            values_builder,
            len,
        }
    }

    /// Creates a new `ListArrayBuilder` from a given values array builder and field
    /// `capacity` is the number of items to pre-allocate space for in this builder
    pub fn with_capacity_and_field(
        values_builder: T,
        capacity: usize,
        field: Field,
    ) -> Self {
        let mut builder = Self::with_capacity(values_builder, capacity);
        builder.field = Some(field);
        builder
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> ArrayBuilder
    for GenericListBuilder<OffsetSize, T>
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
        self.len.to_usize().unwrap()
    }

    /// Returns whether the number of array slots is zero
    fn is_empty(&self) -> bool {
        self.len == OffsetSize::zero()
    }

    /// Builds the array and reset this builder.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<OffsetSize: OffsetSizeTrait, T: ArrayBuilder> GenericListBuilder<OffsetSize, T>
where
    T: 'static,
{
    /// Returns the child array builder as a mutable reference.
    ///
    /// This mutable reference can be used to append values into the child array builder,
    /// but you must call `append` to delimit each distinct list value.
    pub fn values(&mut self) -> &mut T {
        &mut self.values_builder
    }

    /// Returns the child array builder as an immutable reference
    pub fn values_ref(&self) -> &T {
        &self.values_builder
    }

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) -> Result<()> {
        self.offsets_builder
            .append(OffsetSize::from_usize(self.values_builder.len()).unwrap());
        self.bitmap_builder.append(is_valid);
        self.len += OffsetSize::one();
        Ok(())
    }

    /// Builds the `ListArray` and reset this builder.
    pub fn finish(&mut self) -> GenericListArray<OffsetSize> {
        let len = self.len();
        self.len = OffsetSize::zero();
        let values_arr = self
            .values_builder
            .as_any_mut()
            .downcast_mut::<T>()
            .unwrap()
            .finish();
        let values_data = values_arr.data();

        let offset_buffer = self.offsets_builder.finish();
        let null_bit_buffer = self.bitmap_builder.finish();
        self.offsets_builder.append(self.len);
        let field = Box::new(
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
        );
        let data_type = if OffsetSize::IS_LARGE {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        };
        let array_data = ArrayData::builder(data_type)
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(values_data.clone())
            .null_bit_buffer(Some(null_bit_buffer));

        let array_data = unsafe { array_data.build_unchecked() };

        GenericListArray::<OffsetSize>::from(array_data)
    }

    /// Returns the current offsets buffer as a slice
    pub fn offsets_slice(&self) -> &[OffsetSize] {
        self.offsets_builder.as_slice()
    }
}
