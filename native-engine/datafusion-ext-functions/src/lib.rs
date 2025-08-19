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

use std::sync::Arc;

use datafusion::{common::Result, logical_expr::ScalarFunctionImplementation};
use datafusion_ext_commons::df_unimplemented_err;

mod brickhouse;
mod spark_check_overflow;
mod spark_dates;
pub mod spark_get_json_object;
mod spark_hash;
mod spark_make_array;
mod spark_make_decimal;
mod spark_normalize_nan_and_zero;
mod spark_null_if;
mod spark_sha2;
mod spark_strings;
mod spark_unscaled_value;

pub fn create_spark_ext_function(name: &str) -> Result<ScalarFunctionImplementation> {
    Ok(match name {
        "Placeholder" => Arc::new(|_| panic!("placeholder() should never be called")),
        "NullIf" => Arc::new(spark_null_if::spark_null_if),
        "NullIfZero" => Arc::new(spark_null_if::spark_null_if_zero),
        "UnscaledValue" => Arc::new(spark_unscaled_value::spark_unscaled_value),
        "MakeDecimal" => Arc::new(spark_make_decimal::spark_make_decimal),
        "CheckOverflow" => Arc::new(spark_check_overflow::spark_check_overflow),
        "Murmur3Hash" => Arc::new(spark_hash::spark_murmur3_hash),
        "XxHash64" => Arc::new(spark_hash::spark_xxhash64),
        "Sha224" => Arc::new(spark_sha2::spark_sha224),
        "Sha256" => Arc::new(spark_sha2::spark_sha256),
        "Sha384" => Arc::new(spark_sha2::spark_sha384),
        "Sha512" => Arc::new(spark_sha2::spark_sha512),
        "GetJsonObject" => Arc::new(spark_get_json_object::spark_get_json_object),
        "GetParsedJsonObject" => Arc::new(spark_get_json_object::spark_get_parsed_json_object),
        "ParseJson" => Arc::new(spark_get_json_object::spark_parse_json),
        "MakeArray" => Arc::new(spark_make_array::array),
        "StringSpace" => Arc::new(spark_strings::string_space),
        "StringRepeat" => Arc::new(spark_strings::string_repeat),
        "StringSplit" => Arc::new(spark_strings::string_split),
        "StringConcat" => Arc::new(spark_strings::string_concat),
        "StringConcatWs" => Arc::new(spark_strings::string_concat_ws),
        "StringLower" => Arc::new(spark_strings::string_lower),
        "StringUpper" => Arc::new(spark_strings::string_upper),
        "Year" => Arc::new(spark_dates::spark_year),
        "Month" => Arc::new(spark_dates::spark_month),
        "Day" => Arc::new(spark_dates::spark_day),
        "BrickhouseArrayUnion" => Arc::new(brickhouse::array_union::array_union),
        "NormalizeNanAndZero" => {
            Arc::new(spark_normalize_nan_and_zero::spark_normalize_nan_and_zero)
        }
        _ => df_unimplemented_err!("spark ext function not implemented: {name}")?,
    })
}
