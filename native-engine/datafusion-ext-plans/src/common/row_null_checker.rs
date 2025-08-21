// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::{buffer::NullBuffer, row::Rows};
use arrow_schema::{DataType, Field, SchemaRef, SortOptions};

/// Utility class for checking if a Row contains null values
pub struct RowNullChecker {
    field_configs: Vec<FieldConfig>,
}

impl RowNullChecker {
    /// Create a new RowNullChecker from data types and sort options
    ///
    /// # Parameters
    /// * `fields` - Array of tuples containing DataType and SortOptions
    ///
    /// # Returns
    /// * A new RowNullChecker instance configured for the given fields
    pub fn new(fields: &[(DataType, SortOptions)]) -> Self {
        let mut field_configs = Vec::new();

        for (data_type, sort_options) in fields {
            let field_config = Self::create_field_config_from_data_type(data_type, *sort_options);
            field_configs.push(field_config);
        }

        Self { field_configs }
    }

    /// Check if multiple rows contain null values and return a NullBuffer
    ///
    /// # Parameters
    /// * `rows` - Collection of row data to check
    ///
    /// # Returns
    /// * `NullBuffer` - Buffer indicating which rows contain null values
    ///   - `false` bits indicate rows that contain at least one null value
    ///   - `true` bits indicate rows where all fields are non-null
    pub fn has_nulls(&self, rows: &Rows) -> NullBuffer {
        // Create NullBuffer from the collected bits
        NullBuffer::from_iter((0..rows.num_rows()).map(|row_index| {
            let row_data = rows.row(row_index);
            // Check if this row has any null values
            let has_null = self.has_null(row_data.as_ref());
            // NullBuffer uses true for valid (non-null) and false for null
            // So we need to invert the result since has_null returns true for "has nulls"
            !has_null
        }))
    }

    /// Create a FieldConfig from DataType and SortOptions
    fn create_field_config_from_data_type(
        data_type: &DataType,
        sort_options: SortOptions,
    ) -> FieldConfig {
        match data_type {
            DataType::Null => FieldConfig {
                data_type: DataTypeInfo::Null,
                sort_options,
                encoded_length: 0,
            },
            DataType::Boolean => FieldConfig::new_boolean(sort_options),
            dt if dt.is_primitive() => {
                FieldConfig::new_primitive(sort_options, 1 + dt.primitive_width().unwrap())
            }
            // DataType::Int8 => FieldConfig::new_primitive(sort_options, 2), // 1 byte null flag +
            // // 1 byte value
            // DataType::Int16 => FieldConfig::new_primitive(sort_options, 3), /* 1 byte null flag +
            // 2 bytes value */ DataType::Int32 =>
            // FieldConfig::new_primitive(sort_options, 5), /* 1 byte null flag + 4 bytes value */
            // DataType::Date32 => FieldConfig::new_primitive(sort_options, 5),
            // DataType::Int64 => FieldConfig::new_primitive(sort_options, 9), /* 1 byte null flag +
            // 8 bytes value */ DataType::Date64 =>
            // FieldConfig::new_primitive(sort_options, 9), DataType::UInt8 =>
            // FieldConfig::new_primitive(sort_options, 2), DataType::UInt16 =>
            // FieldConfig::new_primitive(sort_options, 3), DataType::UInt32 =>
            // FieldConfig::new_primitive(sort_options, 5), DataType::UInt64 =>
            // FieldConfig::new_primitive(sort_options, 9), DataType::Float32 =>
            // FieldConfig::new_primitive(sort_options, 5), // 1 byte null // flag + 4
            // bytes // value
            // DataType::Float64 => FieldConfig::new_primitive(sort_options, 9), // 1 byte null
            // // flag + 8 bytes
            // // value
            // DataType::Decimal128(..) => FieldConfig::new_primitive(sort_options, 17),
            DataType::Utf8 | DataType::LargeUtf8 => FieldConfig::new_variable(sort_options),
            DataType::Binary | DataType::LargeBinary => FieldConfig::new_variable(sort_options),
            DataType::FixedSizeBinary(size) => {
                FieldConfig::new_fixed_size_binary(sort_options, *size as usize)
            }
            DataType::List(_) | DataType::LargeList(_) => FieldConfig::new_list(sort_options),
            DataType::Struct(_) => {
                // For struct types, we need to calculate the total encoded length
                // This is a simplified implementation
                let estimated_length = 32; // Placeholder for struct encoding length
                FieldConfig::new_struct(sort_options, estimated_length)
            }
            DataType::Dictionary(_, value_type) => {
                // Dictionary encoding typically uses the key type for encoding
                // Simplified to use a fixed length based on common key types
                let key_length = match value_type.as_ref() {
                    DataType::Int8 | DataType::UInt8 => 2,
                    DataType::Int16 | DataType::UInt16 => 3,
                    DataType::Int32 | DataType::UInt32 => 5,
                    DataType::Int64 | DataType::UInt64 => 9,
                    _ => 5, // Default to 32-bit key
                };
                FieldConfig::new_dictionary(sort_options, key_length)
            }
            other => {
                // For unsupported types, panic
                panic!("unsupported data type in RowNullChecker: {:?}", other)
            }
        }
    }

    /// Helper method to create RowNullChecker from schema (for backward
    /// compatibility)
    pub fn from_schema(schema: &SchemaRef) -> Self {
        let mut field_configs = Vec::new();

        for field in schema.fields() {
            let sort_options = SortOptions::default(); // Use default sort options
            let field_config = Self::create_field_config_from_arrow_field(field, sort_options);
            field_configs.push(field_config);
        }

        Self { field_configs }
    }

    /// Create a FieldConfig from an Arrow Field (for backward compatibility)
    fn create_field_config_from_arrow_field(
        field: &Field,
        sort_options: SortOptions,
    ) -> FieldConfig {
        Self::create_field_config_from_data_type(field.data_type(), sort_options)
    }

    /// Check if the given row data contains null values using the configured
    /// schema
    ///
    /// # Parameters
    /// * `row_data` - Row byte data
    ///
    /// # Returns
    /// * `true` - If the row contains at least one null value
    /// * `false` - If all fields in the row are not null
    pub fn has_null(&self, row_data: &[u8]) -> bool {
        Self::has_null_internal(row_data, &self.field_configs)
    }

    /// Check if the given row data contains null values
    ///
    /// # Parameters
    /// * `row_data` - Row byte data
    /// * `field_configs` - Configuration information for each field, including
    ///   data type and sort options
    ///
    /// # Returns
    /// * `true` - If the row contains at least one null value
    /// * `false` - If all fields in the row are not null
    fn has_null_internal(row_data: &[u8], field_configs: &[FieldConfig]) -> bool {
        let mut offset = 0;

        for field_config in field_configs {
            if offset >= row_data.len() {
                return true; // Incomplete data, considered as having null
            }

            let has_null = Self::check_field_null(&row_data[offset..], field_config);
            if has_null {
                return true;
            }

            // Move to next field
            offset += Self::calculate_field_length(&row_data[offset..], field_config);
        }

        false
    }

    /// Check if a single field is null
    fn check_field_null(data: &[u8], field_config: &FieldConfig) -> bool {
        if data.is_empty() {
            return true;
        }

        match &field_config.data_type {
            DataTypeInfo::Null => true, // Null type is always null
            DataTypeInfo::Primitive => {
                // Null check for primitive type: first byte 0 means null, 1 means non-null
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
            DataTypeInfo::Boolean => {
                // Boolean type null check, encoding same as primitive type
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
            DataTypeInfo::Variable => {
                // Variable length type null check: first byte as null_sentinel means null
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
            DataTypeInfo::FixedSizeBinary => {
                // Fixed-size binary null check
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
            DataTypeInfo::Struct => {
                // Struct type null check: first byte as null_sentinel means entire struct is
                // null
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
            DataTypeInfo::List => {
                // List type uses variable-length encoding rules
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
            DataTypeInfo::Dictionary => {
                // Dictionary type: check if key is null (using underlying value type encoding)
                // Simplified handling here, assuming primitive type encoding
                let null_sentinel = Self::null_sentinel(field_config.sort_options);
                data[0] == null_sentinel
            }
        }
    }

    /// Calculate field length in row
    fn calculate_field_length(data: &[u8], field_config: &FieldConfig) -> usize {
        if data.is_empty() {
            return 0;
        }

        match &field_config.data_type {
            DataTypeInfo::Null => 0, // Null type takes no space
            DataTypeInfo::Primitive => field_config.encoded_length,
            DataTypeInfo::Boolean => 2, // 1 byte null flag + 1 byte value
            DataTypeInfo::Variable => {
                // Variable length type needs to parse entire encoding block to determine length
                Self::calculate_variable_length(data, field_config.sort_options)
            }
            DataTypeInfo::FixedSizeBinary => field_config.encoded_length,
            DataTypeInfo::Struct => {
                // Struct type: 1 byte null flag + actual data length
                if Self::check_field_null(data, field_config) {
                    field_config.encoded_length // Fixed length for null struct
                } else {
                    // Non-null struct needs to parse sub-fields (simplified as fixed length here)
                    field_config.encoded_length
                }
            }
            DataTypeInfo::List => {
                // List type uses variable-length encoding
                Self::calculate_variable_length(data, field_config.sort_options)
            }
            DataTypeInfo::Dictionary => field_config.encoded_length,
        }
    }

    /// Calculate encoded length of variable-length field
    fn calculate_variable_length(data: &[u8], sort_options: SortOptions) -> usize {
        if data.is_empty() {
            return 0;
        }

        let null_sentinel = Self::null_sentinel(sort_options);

        // Check null
        if data[0] == null_sentinel {
            return 1;
        }

        // Check empty value
        let empty_sentinel = match sort_options.descending {
            true => !1u8, // EMPTY_SENTINEL = 1, inverted for descending order
            false => 1u8,
        };

        if data[0] == empty_sentinel {
            return 1;
        }

        // Non-empty value, need to parse block structure
        Self::decode_variable_blocks_length(data, sort_options)
    }

    /// Parse total length of variable-length encoding blocks
    fn decode_variable_blocks_length(data: &[u8], sort_options: SortOptions) -> usize {
        let non_empty_sentinel = match sort_options.descending {
            true => !2u8, // NON_EMPTY_SENTINEL = 2
            false => 2u8,
        };

        if data[0] != non_empty_sentinel {
            return 1; // Empty value or null
        }

        let continuation = match sort_options.descending {
            true => !0xFFu8, // BLOCK_CONTINUATION = 0xFF
            false => 0xFFu8,
        };

        let mut idx = 1;
        const MINI_BLOCK_SIZE: usize = 8; // 32/4
        const MINI_BLOCK_COUNT: usize = 4;
        const BLOCK_SIZE: usize = 32;

        // Process mini blocks
        for _ in 0..MINI_BLOCK_COUNT {
            if idx + MINI_BLOCK_SIZE >= data.len() {
                return data.len(); // Incomplete data
            }

            let sentinel = data[idx + MINI_BLOCK_SIZE];
            if sentinel != continuation {
                return idx + MINI_BLOCK_SIZE + 1;
            }
            idx += MINI_BLOCK_SIZE + 1;
        }

        // Process full blocks
        loop {
            if idx + BLOCK_SIZE >= data.len() {
                return data.len(); // Incomplete data
            }

            let sentinel = data[idx + BLOCK_SIZE];
            if sentinel != continuation {
                return idx + BLOCK_SIZE + 1;
            }
            idx += BLOCK_SIZE + 1;
        }
    }

    /// Get null sentinel value based on sort options
    fn null_sentinel(options: SortOptions) -> u8 {
        match options.nulls_first {
            true => 0,
            false => 0xFF,
        }
    }
}

/// Field configuration information
#[derive(Debug, Clone)]
struct FieldConfig {
    data_type: DataTypeInfo,
    sort_options: SortOptions,
    encoded_length: usize, // For fixed-length types, represents encoded byte length
}

/// Data type information
#[derive(Debug, Clone)]
enum DataTypeInfo {
    Null,
    Primitive, // Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, etc.
    Boolean,
    Variable,        // Variable-length types like String, Binary
    FixedSizeBinary, // Fixed-length binary data
    Struct,          // Struct type
    List,            // List type
    Dictionary,      // Dictionary type
}

impl FieldConfig {
    /// Create configuration for primitive numeric types
    fn new_primitive(sort_options: SortOptions, encoded_length: usize) -> Self {
        Self {
            data_type: DataTypeInfo::Primitive,
            sort_options,
            encoded_length,
        }
    }

    /// Create configuration for Boolean type
    fn new_boolean(sort_options: SortOptions) -> Self {
        Self {
            data_type: DataTypeInfo::Boolean,
            sort_options,
            encoded_length: 2, // 1 byte null flag + 1 byte value
        }
    }

    /// Create configuration for variable-length types
    fn new_variable(sort_options: SortOptions) -> Self {
        Self {
            data_type: DataTypeInfo::Variable,
            sort_options,
            encoded_length: 0, // Variable-length types don't have fixed length
        }
    }

    /// Create configuration for fixed-size binary type
    fn new_fixed_size_binary(sort_options: SortOptions, size: usize) -> Self {
        Self {
            data_type: DataTypeInfo::FixedSizeBinary,
            sort_options,
            encoded_length: 1 + size, // 1 byte null flag + size bytes data
        }
    }

    /// Create configuration for Struct type
    fn new_struct(sort_options: SortOptions, encoded_length: usize) -> Self {
        Self {
            data_type: DataTypeInfo::Struct,
            sort_options,
            encoded_length,
        }
    }

    /// Create configuration for List type
    fn new_list(sort_options: SortOptions) -> Self {
        Self {
            data_type: DataTypeInfo::List,
            sort_options,
            encoded_length: 0, // Variable-length type
        }
    }

    /// Create configuration for Dictionary type
    fn new_dictionary(sort_options: SortOptions, encoded_length: usize) -> Self {
        Self {
            data_type: DataTypeInfo::Dictionary,
            sort_options,
            encoded_length,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray},
        row::{RowConverter, SortField},
    };
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_primitive_null_detection() {
        // Create a SortField with Int32 data type
        let checker = RowNullChecker::new(&[(DataType::Int32, SortOptions::default())]);
        // Null value (first byte = 0)
        let null_data = vec![0u8, 0, 0, 0, 0];
        assert!(checker.has_null(&null_data));

        // Non-null value (first byte = 1)
        let non_null_data = vec![1u8, 0x80, 0, 0, 1]; // encoded i32 value
        assert!(!checker.has_null(&non_null_data));
    }

    #[test]
    fn test_variable_null_detection() {
        // Create a SortField with String data type
        let checker = RowNullChecker::new(&[(DataType::Utf8, SortOptions::default())]);
        // Null value
        let null_data = vec![0u8];
        assert!(checker.has_null(&null_data));

        // Empty string
        let empty_data = vec![1u8];
        assert!(!checker.has_null(&empty_data));

        // Non-empty string (simplified)
        let non_empty_data = vec![2u8, b'h', b'e', b'l', b'l', b'o', 0, 0, 0, 5];
        assert!(!checker.has_null(&non_empty_data));
    }

    #[test]
    fn test_multiple_fields() {
        // Create SortFields with Int32 and String data types
        let checker = RowNullChecker::new(&[
            (DataType::Int32, SortOptions::default()),
            (DataType::Utf8, SortOptions::default()),
        ]);
        // Both fields non-null
        let data = vec![
            1u8, 0x80, 0, 0, 1,   // non-null i32
            1u8, // empty string (non-null)
        ];
        assert!(!checker.has_null(&data));

        // First field null
        let data = vec![
            0u8, 0, 0, 0, 0,   // null i32
            1u8, // empty string (non-null)
        ];
        assert!(checker.has_null(&data));

        // Second field null
        let data = vec![
            1u8, 0x80, 0, 0, 1,   // non-null i32
            0u8, // null string
        ];
        assert!(checker.has_null(&data));
    }

    #[test]
    fn test_descending_null_sentinel() {
        // Create SortField with Int32 data type and nulls_first = false
        let checker = RowNullChecker::new(&[(
            DataType::Int32,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )]);

        // Null value with nulls_first = false (0xFF is the null sentinel)
        let null_data = vec![0xFFu8, 0, 0, 0, 0];
        assert!(checker.has_null(&null_data));

        // Non-null value
        let non_null_data = vec![1u8, 0x80, 0, 0, 1];
        assert!(!checker.has_null(&non_null_data));

        // Test that 0x00 is not treated as null with nulls_first = false
        let data_00 = vec![0x00u8, 0x80, 0, 0, 1];
        assert!(!checker.has_null(&data_00));
    }

    #[test]
    fn test_roundtrip_with_record_batch() {
        // Create a schema with multiple data types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        // Create arrays with some null values
        let id_array = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let name_array = StringArray::from(vec![
            Some("Alice"),
            None, // null value
            Some("Bob"),
            Some("Charlie"),
        ]);
        let active_array = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None, // null value
            Some(true),
        ]);

        // Create RecordBatch
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        )
        .unwrap();

        // Create RowNullChecker
        let checker = RowNullChecker::new(
            &schema
                .fields()
                .iter()
                .map(|f| (f.data_type().clone(), SortOptions::default()))
                .collect::<Vec<_>>(),
        );

        // Simulate row data for each row in the RecordBatch
        // Row 0: (1, "Alice", true) - no nulls
        let row0_data = vec![
            1u8, 0x80, 0, 0, 1, // non-null Int32 value 1
            2u8, b'A', b'l', b'i', b'c', b'e', 0, 0, 5, // non-null String "Alice"
            1u8, 1u8, // non-null Boolean true
        ];
        assert!(!checker.has_null(&row0_data));

        // Row 1: (2, null, false) - has null in name field
        let row1_data = vec![
            1u8, 0x80, 0, 0, 2,   // non-null Int32 value 2
            0u8, // null String
            1u8, 0u8, // non-null Boolean false
        ];
        assert!(checker.has_null(&row1_data));

        // Row 2: (3, "Bob", null) - has null in active field
        let row2_data = vec![
            1u8, 0x80, 0, 0, 3, // non-null Int32 value 3
            2u8, b'B', b'o', b'b', 0, 0, 0, 3, // non-null String "Bob"
            0u8, 0u8, // null Boolean
        ];
        assert!(checker.has_null(&row2_data));

        // Row 3: (4, "Charlie", true) - no nulls
        let row3_data = vec![
            1u8, 0x80, 0, 0, 4, // non-null Int32 value 4
            2u8, b'C', b'h', b'a', b'r', b'l', b'i', b'e', 0, 7, // non-null String "Charlie"
            1u8, 1u8, // non-null Boolean true
        ];
        assert!(!checker.has_null(&row3_data));

        // Verify that row count matches RecordBatch
        assert_eq!(record_batch.num_rows(), 4);
        assert_eq!(record_batch.num_columns(), 3);
    }

    #[test]
    fn test_different_sort_options() {
        // Test with different sort options
        let checker = RowNullChecker::new(&[
            (
                DataType::Int32,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
            (
                DataType::Int32,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
            (
                DataType::Int32,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
            (
                DataType::Int32,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            ),
        ]);
        // Test data with nulls in specific positions
        let data = vec![
            0u8, 0, 0, 0, 0, // null field1 (nulls_first = true, sentinel = 0)
            0xFFu8, 0, 0, 0, 0, // null field2 (nulls_first = false, sentinel = 0xFF)
            0u8, 0, 0, 0, 0, // null field3 (nulls_first = true, sentinel = 0)
            0xFFu8, 0, 0, 0, 0, // null field4 (nulls_first = false, sentinel = 0xFF)
        ];

        assert!(checker.has_null(&data));
    }

    #[test]
    fn test_backward_compatibility() {
        // Test backward compatibility with schema-based creation
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create from schema using the backward compatibility method
        let checker = RowNullChecker::from_schema(&schema);

        // Both fields non-null
        let data = vec![
            1u8, 0x80, 0, 0, 1,   // non-null i32
            1u8, // empty string (non-null)
        ];
        assert!(!checker.has_null(&data));

        // Second field null
        let data = vec![
            1u8, 0x80, 0, 0, 1,   // non-null i32
            0u8, // null string
        ];
        assert!(checker.has_null(&data));
    }

    #[test]
    fn test_has_nulls_with_rows() {
        use arrow::{array::ArrayRef, row::RowConverter};

        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create test data arrays
        let id_array = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let name_array =
            StringArray::from(vec![Some("Alice"), None, Some("Charlie"), Some("David")]);

        let columns: Vec<ArrayRef> = vec![Arc::new(id_array), Arc::new(name_array)];

        // Create RecordBatch
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        // Create sort fields for RowConverter
        let sort_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|field| {
                SortField::new_with_options(field.data_type().clone(), SortOptions::default())
            })
            .collect();

        // Convert RecordBatch to Rows
        let converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();

        // Create field configs for RowNullChecker
        let field_configs: Vec<(DataType, SortOptions)> = schema
            .fields()
            .iter()
            .map(|field| (field.data_type().clone(), SortOptions::default()))
            .collect();

        // Create RowNullChecker
        let checker = RowNullChecker::new(&field_configs);

        // Test has_nulls method
        let null_buffer = checker.has_nulls(&rows);

        // Verify results
        // Row 0: (1, "Alice") - no nulls -> should be true (valid)
        // Row 1: (2, null) - has null -> should be false (invalid)
        // Row 2: (null, "Charlie") - has null -> should be false (invalid)
        // Row 3: (4, "David") - no nulls -> should be true (valid)
        assert_eq!(null_buffer.len(), 4);
        assert_eq!(null_buffer.is_valid(0), true); // No nulls
        assert_eq!(null_buffer.is_valid(1), false); // Has null in name
        assert_eq!(null_buffer.is_valid(2), false); // Has null in id
        assert_eq!(null_buffer.is_valid(3), true); // No nulls
    }

    #[test]
    fn test_has_nulls_empty_rows() {
        // Test with empty rows
        let field_configs = vec![(DataType::Int32, SortOptions::default())];
        let checker = RowNullChecker::new(&field_configs);

        // Create empty rows (this is a bit tricky to create directly)
        // For this test, we'll create a minimal test case
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

        let id_array = Int32Array::from(Vec::<Option<i32>>::new());
        let columns: Vec<ArrayRef> = vec![Arc::new(id_array)];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let sort_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|field| {
                SortField::new_with_options(field.data_type().clone(), SortOptions::default())
            })
            .collect();

        let converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();

        let null_buffer = checker.has_nulls(&rows);
        assert_eq!(null_buffer.len(), 0);
    }

    #[test]
    fn test_has_nulls_all_nulls() {
        // Test with all rows containing nulls
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

        let id_array = Int32Array::from(vec![None, None, None]);
        let columns: Vec<ArrayRef> = vec![Arc::new(id_array)];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let sort_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|field| {
                SortField::new_with_options(field.data_type().clone(), SortOptions::default())
            })
            .collect();

        let converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();

        let field_configs: Vec<(DataType, SortOptions)> = schema
            .fields()
            .iter()
            .map(|field| (field.data_type().clone(), SortOptions::default()))
            .collect();

        let checker = RowNullChecker::new(&field_configs);
        let null_buffer = checker.has_nulls(&rows);

        // All rows should be invalid (false) since they all contain nulls
        assert_eq!(null_buffer.len(), 3);
        for i in 0..3 {
            assert_eq!(null_buffer.is_valid(i), false);
        }
    }

    #[test]
    fn test_has_nulls_no_nulls() {
        // Test with no nulls in any row
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), Some("Charlie")]);
        let columns: Vec<ArrayRef> = vec![Arc::new(id_array), Arc::new(name_array)];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let sort_fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|field| {
                SortField::new_with_options(field.data_type().clone(), SortOptions::default())
            })
            .collect();

        let converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();

        let field_configs: Vec<(DataType, SortOptions)> = schema
            .fields()
            .iter()
            .map(|field| (field.data_type().clone(), SortOptions::default()))
            .collect();

        let checker = RowNullChecker::new(&field_configs);
        let null_buffer = checker.has_nulls(&rows);

        // All rows should be valid (true) since none contain nulls
        assert_eq!(null_buffer.len(), 3);
        for i in 0..3 {
            assert_eq!(null_buffer.is_valid(i), true);
        }
    }
}
