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

use std::any::Any;

use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};

pub mod bloom_filter_might_contain;
pub mod cast;
pub mod get_indexed_field;
pub mod get_map_value;
pub mod named_struct;
pub mod row_num;
pub mod spark_scalar_subquery_wrapper;
pub mod spark_udf_wrapper;
pub mod string_contains;
pub mod string_ends_with;
pub mod string_starts_with;

fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<PhysicalExprRef>() {
        any.downcast_ref::<PhysicalExprRef>().unwrap().as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}
