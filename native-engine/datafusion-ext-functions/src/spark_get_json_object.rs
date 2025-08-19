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

use std::{any::Any, borrow::Cow, fmt::Debug, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, AsArray, StringArray, StringBuilder, StructArray, make_array,
        new_null_array,
    },
    datatypes::{DataType, Field, Fields},
    ffi::{FFI_ArrowArray, from_ffi_and_data_type, to_ffi},
};
use auron_jni_bridge::{conf, conf::BooleanConf, jni_call, jni_new_object, jni_new_string};
use datafusion::{
    common::{Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::{downcast_any, uda::UserDefinedArray};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

/// implement hive/spark's UDFGetJson
/// get_json_object(str, path) == get_parsed_json_object(parse_json(str), path)
pub fn spark_get_json_object(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let json_string_array = match &args[0] {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let json_strings = json_string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let path_string = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => match str {
            Some(path) => path,
            None => {
                return Ok(ColumnarValue::Array(new_null_array(
                    &DataType::Utf8,
                    json_strings.len(),
                )));
            }
        },
        _ => unreachable!("path should be ScalarValue"),
    };

    let mut evaluator = match HiveGetJsonObjectEvaluator::try_new(path_string) {
        Ok(evaluator) => evaluator,
        Err(_) => {
            return Ok(ColumnarValue::Array(new_null_array(
                &DataType::Utf8,
                json_strings.len(),
            )));
        }
    };

    let output = json_strings
        .iter()
        .map(|json_string| {
            json_string.and_then(|s| match evaluator.evaluate(s) {
                Ok(Some(matched)) => Some(matched),
                _ => None,
            })
        })
        .collect::<StringArray>();
    Ok(ColumnarValue::Array(Arc::new(output)))
}

pub fn spark_parse_json(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let json_string_array = match &args[0] {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
    };

    let json_strings = json_string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let fallback_enabled = conf::PARSE_JSON_ERROR_FALLBACK.value().unwrap_or(false);

    let json_values: Vec<Option<Arc<dyn Any + Send + Sync + 'static>>> = json_strings
        .iter()
        .map(|s| {
            s.and_then(|s| {
                // first try parsing with sonic-rs and fail-backing to serde-json
                if s.is_empty() {
                    None
                } else if let Ok(v) = sonic_rs::from_str::<sonic_rs::Value>(s) {
                    let v: Arc<dyn Any + Send + Sync> = Arc::new(ParsedJsonValue::Sonic(v));
                    Some(v)
                } else if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                    let v: Arc<dyn Any + Send + Sync> = Arc::new(ParsedJsonValue::SerdeJson(v));
                    Some(v)
                } else if fallback_enabled {
                    let v: Arc<dyn Any + Send + Sync> =
                        Arc::new(ParsedJsonValue::Fallback(s.to_owned()));
                    Some(v)
                } else {
                    None
                }
            })
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(
        UserDefinedArray::from_items(DataType::Binary, json_values),
    )))
}

pub fn spark_get_parsed_json_object(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let json_array = match &args[0] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<UserDefinedArray>().unwrap(),
        ColumnarValue::Scalar(_) => unreachable!(),
    };

    let path_string = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => match str {
            Some(path) => path,
            None => {
                return Ok(ColumnarValue::Array(new_null_array(
                    &DataType::Utf8,
                    json_array.len(),
                )));
            }
        },
        _ => unreachable!("path should be ScalarValue"),
    };

    let mut evaluator = match HiveGetJsonObjectEvaluator::try_new(path_string) {
        Ok(evaluator) => evaluator,
        Err(_) => {
            return Ok(ColumnarValue::Array(new_null_array(
                &DataType::Utf8,
                json_array.len(),
            )));
        }
    };

    let fallback_results = parse_fallback(&path_string, json_array)?;
    let mut fallback_results_iter = fallback_results.iter();

    let output = StringArray::from_iter(json_array.iter().map(|value| {
        value.as_ref().and_then(|value| -> Option<Cow<str>> {
            let json_value = value.downcast_ref::<ParsedJsonValue>().unwrap();
            match json_value {
                ParsedJsonValue::SerdeJson(v) => evaluator
                    .evaluate_with_value_serde_json(v)
                    .ok()
                    .unwrap_or(None)
                    .map(Cow::from),
                ParsedJsonValue::Sonic(v) => evaluator
                    .evaluate_with_value_sonic(v)
                    .ok()
                    .unwrap_or(None)
                    .map(Cow::from),
                ParsedJsonValue::Fallback(_) => {
                    fallback_results_iter.next().unwrap().map(Cow::from)
                }
            }
        })
    }));
    Ok(ColumnarValue::Array(Arc::new(output)))
}

// this function is for json_tuple() usage, which can parse only top-level json
// fields
pub fn spark_get_parsed_json_simple_field(
    parsed_json_array: &ArrayRef,
    field: &String,
) -> Result<ArrayRef> {
    let json_array = downcast_any!(parsed_json_array, UserDefinedArray)?;
    let fallback_results = parse_fallback(&field, json_array)?;
    let mut fallback_results_iter = fallback_results.iter();

    let output = StringArray::from_iter(json_array.iter().map(|value| {
        value.as_ref().and_then(|value| {
            let json_value = value.downcast_ref::<ParsedJsonValue>().unwrap();
            match json_value {
                ParsedJsonValue::SerdeJson(v) => v
                    .as_object()
                    .and_then(|object| object.get(field))
                    .and_then(|v| serde_json_value_to_string(v).unwrap_or_default())
                    .map(Cow::from),
                ParsedJsonValue::Sonic(v) => v
                    .as_object()
                    .and_then(|object| object.get(field))
                    .and_then(|v| sonic_value_to_string(v).unwrap_or_default())
                    .map(Cow::from),
                ParsedJsonValue::Fallback(_) => {
                    fallback_results_iter.next().unwrap().map(Cow::from)
                }
            }
        })
    }));
    Ok(Arc::new(output))
}

fn parse_fallback(json_path: &str, json_array: &UserDefinedArray) -> Result<StringArray> {
    let fallback_results = if !conf::PARSE_JSON_ERROR_FALLBACK.value().unwrap_or(false) {
        StringArray::new_null(0)
    } else {
        let mut fallback_jsons_array = StringBuilder::new();
        for json in json_array.iter().flat_map(|value| {
            value.as_ref().and_then(|value| -> Option<&str> {
                let json_value = value.downcast_ref::<ParsedJsonValue>().unwrap();
                if let ParsedJsonValue::Fallback(json) = json_value {
                    return Some(json.as_ref());
                }
                None
            })
        }) {
            fallback_jsons_array.append_value(json);
        }
        let fallback_jsons_array = fallback_jsons_array.finish();

        if fallback_jsons_array.is_empty() {
            StringArray::new_null(0)
        } else {
            let single_field = Fields::from(vec![Field::new("", DataType::Utf8, false)]);
            let json_path = jni_new_string!(json_path)?;
            let fallback_wrapper = jni_new_object!(AuronJsonFallbackWrapper(json_path.as_obj()))?;
            let export_array = StructArray::new(
                single_field.clone(),
                vec![Arc::new(fallback_jsons_array)],
                None,
            );
            let (mut ffi_export_array, _) = to_ffi(&export_array.to_data())?;
            let mut ffi_import_array = FFI_ArrowArray::empty();
            jni_call!(AuronJsonFallbackWrapper(fallback_wrapper.as_obj()).parseJsons(
                &mut ffi_export_array as *mut FFI_ArrowArray as i64,
                &mut ffi_import_array as *mut FFI_ArrowArray as i64,
            ) -> ())?;
            let import_array = unsafe {
                // safety: use arrow-ffi
                from_ffi_and_data_type(ffi_import_array, DataType::Struct(single_field.clone()))?
            };
            let result_array = make_array(import_array.child_data()[0].clone());
            result_array.as_string().clone()
        }
    };
    Ok(fallback_results)
}

#[derive(Debug)]
enum ParsedJsonValue {
    SerdeJson(serde_json::Value),
    Sonic(sonic_rs::Value),
    Fallback(String),
}

#[derive(Debug)]
enum HiveGetJsonObjectError {
    InvalidJsonPath,
    InvalidInput,
}

struct HiveGetJsonObjectEvaluator {
    matchers: Vec<HiveGetJsonObjectMatcher>,
}

impl HiveGetJsonObjectEvaluator {
    fn try_new(json_path: &str) -> std::result::Result<Self, HiveGetJsonObjectError> {
        let mut evaluator = Self { matchers: vec![] };
        let chars = json_path.chars();
        let mut peekable = chars.peekable();

        while let Some(matcher) = HiveGetJsonObjectMatcher::parse(&mut peekable)? {
            evaluator.matchers.push(matcher);
        }
        if evaluator.matchers.first() != Some(&HiveGetJsonObjectMatcher::Root) {
            return Err(HiveGetJsonObjectError::InvalidJsonPath);
        }
        evaluator.matchers.remove(0); // remove root matcher
        if evaluator.matchers.contains(&HiveGetJsonObjectMatcher::Root) {
            return Err(HiveGetJsonObjectError::InvalidJsonPath);
        }
        Ok(evaluator)
    }

    fn evaluate(
        &mut self,
        json_str: &str,
    ) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
        // first try parsing with sonic-rs and fail-backing to serde-json
        if let Ok(root_value) = sonic_rs::from_str::<sonic_rs::Value>(json_str) {
            if let Ok(v) = self.evaluate_with_value_sonic(&root_value) {
                return Ok(v);
            }
        }
        if let Ok(root_value) = serde_json::from_str::<serde_json::Value>(json_str) {
            if let Ok(v) = self.evaluate_with_value_serde_json(&root_value) {
                return Ok(v);
            }
        }
        Err(HiveGetJsonObjectError::InvalidInput)
    }

    fn evaluate_with_value_serde_json(
        &mut self,
        root_value: &serde_json::Value,
    ) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
        let mut root_value: Cow<serde_json::Value> = Cow::Borrowed(root_value);
        let mut value = root_value.as_ref();
        for matcher in &self.matchers {
            match matcher.evaluate_serde_json(value) {
                Cow::Borrowed(evaluated) => {
                    value = evaluated;
                }
                Cow::Owned(evaluated) => {
                    root_value = Cow::Owned(evaluated);
                    value = root_value.as_ref();
                }
            }
        }
        serde_json_value_to_string(value)
    }

    fn evaluate_with_value_sonic(
        &mut self,
        root_value: &sonic_rs::Value,
    ) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
        let mut root_value: Cow<sonic_rs::Value> = Cow::Borrowed(root_value);
        let mut value = root_value.as_ref();
        for matcher in &self.matchers {
            match matcher.evaluate_sonic(value) {
                Cow::Borrowed(evaluated) => {
                    value = evaluated;
                }
                Cow::Owned(evaluated) => {
                    root_value = Cow::Owned(evaluated);
                    value = root_value.as_ref();
                }
            }
        }
        sonic_value_to_string(value)
    }
}

fn serde_json_value_to_string(
    value: &serde_json::Value,
) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(string) => Ok(Some(string.to_string())),
        serde_json::Value::Number(number) => Ok(Some(number.to_string())),
        serde_json::Value::Bool(b) => Ok(Some(b.to_string())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => serde_json::to_string(value)
            .map(Some)
            .map_err(|_| HiveGetJsonObjectError::InvalidInput),
    }
}

fn sonic_value_to_string(
    value: &sonic_rs::Value,
) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
    match value.get_type() {
        sonic_rs::JsonType::Null => Ok(None),
        sonic_rs::JsonType::String => Ok(value.as_str().map(|v| v.to_string())),
        sonic_rs::JsonType::Number => Ok(value.as_number().map(|v| v.to_string())),
        sonic_rs::JsonType::Boolean => Ok(value.as_bool().map(|v| v.to_string())),
        _ => sonic_rs::to_string(value)
            .map(Some)
            .map_err(|_| HiveGetJsonObjectError::InvalidInput),
    }
}

#[derive(Debug, PartialEq)]
enum HiveGetJsonObjectMatcher {
    Root,
    Child(String),
    Subscript(usize),
    SubscriptAll,
}

impl HiveGetJsonObjectMatcher {
    fn parse(
        chars: &mut std::iter::Peekable<std::str::Chars>,
    ) -> std::result::Result<Option<Self>, HiveGetJsonObjectError> {
        match chars.peek() {
            None => Ok(None),
            Some('$') => {
                chars.next();
                Ok(Some(Self::Root))
            }
            Some('.') => {
                chars.next();

                loop {
                    match chars.peek() {
                        Some(' ') => {
                            chars.next();
                        }
                        Some(_) | None => {
                            break;
                        }
                    }
                }

                if chars.peek().cloned() == Some('[') {
                    return Self::parse(chars); // handle special case like
                    // $.aaa.[0].xxx
                }
                let mut child_name = String::new();
                loop {
                    match chars.peek() {
                        Some('.') | Some('[') | None => {
                            break;
                        }
                        Some(c) => {
                            child_name.push(*c);
                            chars.next();
                        }
                    }
                }
                if child_name.is_empty() {
                    return Err(HiveGetJsonObjectError::InvalidJsonPath);
                }
                Ok(Some(Self::Child(child_name)))
            }
            Some('[') => {
                chars.next();
                let mut index_str = String::new();
                loop {
                    match chars.peek() {
                        Some(']') => {
                            chars.next();
                            break;
                        }
                        Some(c) => {
                            index_str.push(*c);
                            chars.next();
                        }
                        None => {
                            return Err(HiveGetJsonObjectError::InvalidJsonPath);
                        }
                    }
                }
                if index_str.is_empty() || index_str == "*" {
                    return Ok(Some(Self::SubscriptAll));
                }
                let index = str::parse::<usize>(&index_str)
                    .map_err(|_| HiveGetJsonObjectError::InvalidJsonPath)?;
                Ok(Some(Self::Subscript(index)))
            }
            Some(_) => Err(HiveGetJsonObjectError::InvalidJsonPath),
        }
    }

    fn evaluate_serde_json<'a>(&self, value: &'a serde_json::Value) -> Cow<'a, serde_json::Value> {
        match self {
            HiveGetJsonObjectMatcher::Root => {
                return Cow::Borrowed(value);
            }
            HiveGetJsonObjectMatcher::Child(child) => {
                if let serde_json::Value::Object(object) = value {
                    return match object.get(child) {
                        Some(child) => Cow::Borrowed(child),
                        None => Cow::Owned(serde_json::Value::Null),
                    };
                } else if let serde_json::Value::Array(array) = value {
                    let vs = array
                        .into_iter()
                        .map(|item| {
                            if let serde_json::Value::Object(object) = item {
                                match object.get(child) {
                                    Some(v) => v.clone(),
                                    None => serde_json::Value::Null,
                                }
                            } else {
                                serde_json::Value::Null
                            }
                        })
                        .filter(|r| !r.is_null())
                        .flat_map(|r| {
                            // keep consistent with hive UDFJson
                            let iter: Box<dyn Iterator<Item = serde_json::Value>> = match r {
                                serde_json::Value::Array(array) => Box::new(array.into_iter()),
                                other => Box::new(std::iter::once(other)),
                            };
                            iter
                        })
                        .collect::<Vec<_>>();

                    if vs.is_empty() {
                        return Cow::Owned(serde_json::Value::Null);
                    }
                    return Cow::Owned(serde_json::Value::Array(vs));
                }
            }
            HiveGetJsonObjectMatcher::Subscript(index) => {
                if let serde_json::Value::Array(array) = value {
                    return match array.get(*index) {
                        Some(v) => Cow::Borrowed(v),
                        None => Cow::Owned(serde_json::Value::Null),
                    };
                }
            }
            HiveGetJsonObjectMatcher::SubscriptAll => {
                if let serde_json::Value::Array(_) = value {
                    return Cow::Borrowed(value);
                }
            }
        }
        Cow::Owned(serde_json::Value::Null)
    }

    fn evaluate_sonic<'a>(&self, value: &'a sonic_rs::Value) -> Cow<'a, sonic_rs::Value> {
        match self {
            HiveGetJsonObjectMatcher::Root => {
                return Cow::Borrowed(value);
            }
            HiveGetJsonObjectMatcher::Child(child) => {
                if let Some(object) = value.as_object() {
                    return match object.get(child) {
                        Some(child) => Cow::Borrowed(child),
                        None => Cow::Owned(sonic_rs::Value::default()),
                    };
                } else if let Some(array) = value.as_array() {
                    let vs = array
                        .into_iter()
                        .map(|item| {
                            if let Some(object) = item.as_object() {
                                match object.get(child) {
                                    Some(v) => v.clone(),
                                    None => sonic_rs::Value::default(),
                                }
                            } else {
                                sonic_rs::Value::default()
                            }
                        })
                        .filter(|r| !r.is_null())
                        .flat_map(|r| {
                            // keep consistent with hive UDFJson
                            let iter: Box<dyn Iterator<Item = sonic_rs::Value>> = match r {
                                v if v.is_array() => Box::new(v.into_array().unwrap().into_iter()),
                                other => Box::new(std::iter::once(other)),
                            };
                            iter
                        })
                        .collect::<Vec<_>>();

                    if vs.is_empty() {
                        return Cow::Owned(sonic_rs::Value::default());
                    }
                    return Cow::Owned(sonic_rs::Value::from(vs));
                }
            }
            HiveGetJsonObjectMatcher::Subscript(index) => {
                if let Some(array) = value.as_array() {
                    return match array.get(*index) {
                        Some(v) => Cow::Borrowed(v),
                        None => Cow::Owned(sonic_rs::Value::default()),
                    };
                }
            }
            HiveGetJsonObjectMatcher::SubscriptAll => {
                if let Some(_array) = value.as_array() {
                    return Cow::Borrowed(value);
                }
            }
        }
        Cow::Owned(sonic_rs::Value::default())
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::array::{AsArray, StringArray};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_get_json_object::{
        HiveGetJsonObjectEvaluator, spark_get_parsed_json_object, spark_parse_json,
    };

    #[test]
    fn test_hive_demo() -> Result<(), Box<dyn Error>> {
        let input = r#"
            {
                "store": {
                    "fruit": [
                        {
                            "weight": 8,
                            "type": "apple"
                        },
                        {
                            "weight": 9,
                            "type": "pear"
                        }
                    ],
                    "bicycle": {
                        "price": 19.95,
                        "color": "red"
                    }
                },
                "email": "amy@only_for_json_udf_test.net",
                "owner": "amy"
            }"#;

        let path = "$.owner";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("amy".to_owned())
        );

        let path = "$.  owner";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("amy".to_owned())
        );

        let path = "$.store.bicycle.price";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("19.95".to_owned())
        );

        let path = "$.  store.  bicycle.  price";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("19.95".to_owned())
        );

        let path = "$.store.fruit[0]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some(r#"{"weight":8,"type":"apple"}"#.to_owned())
        );

        let path = "$. store.  fruit[0]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some(r#"{"weight":8,"type":"apple"}"#.to_owned())
        );

        let path = "$.store.fruit[1].weight";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("9".to_owned())
        );

        let path = "$.store.fruit[*]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some(r#"[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}]"#.to_owned())
        );

        let path = "$. store.  fruit[*]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some(r#"[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}]"#.to_owned())
        );

        let path = "$.store.fruit.[1].type";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("pear".to_owned())
        );

        let path = "$. store.  fruit.  [1]. type";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            Some("pear".to_owned())
        );

        let path = "$.non_exist_key";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path)
                .unwrap()
                .evaluate(input)
                .unwrap(),
            None
        );
        Ok(())
    }

    #[test]
    fn test_2() -> Result<(), Box<dyn Error>> {
        let input = r#"
            {
                "ID": 121,
                "message": {
                    "name": "
                        非法换行
                        非法转义\哈
                        特殊字符😍
                        Asher
                    ",
                    "location": [
                        {
                            "county": "浦东",
                            "city": "1.234"
                        },
                        {
                            "county": "西直门",
                            "city": 1.234
                        },
                        {
                            "other": "invalid"
                        }
                    ]
                }
            }"#;
        let input_array = Arc::new(StringArray::from(vec![input]));
        let parsed = spark_parse_json(&[ColumnarValue::Array(input_array)]).unwrap();

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location.county"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"["浦东","西直门"]"#));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location.NOT_EXISTED"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, None);

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.name"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert!(v.unwrap().contains("Asher"));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location.city"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"["1.234",1.234]"#));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location[0]"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"{"city":"1.234","county":"浦东"}"#));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location[].county"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"["浦东","西直门"]"#));
        Ok(())
    }

    #[test]
    fn test_3() -> Result<(), Box<dyn Error>> {
        let input = r#"
            {
                "i1": [
                    {
                        "j1": 100,
                        "j2": [
                            200,
                            300
                        ]
                    }, {
                        "j1": 300,
                        "j2": [
                            400,
                            500
                        ]
                    }, {
                        "j1": 300,
                        "j2": null
                    }, {
                        "j1": 300,
                        "j2": "other"
                    }
                ]
            }
        "#;
        let input_array = Arc::new(StringArray::from(vec![input]));
        let parsed = spark_parse_json(&[ColumnarValue::Array(input_array)])?;

        let path = ColumnarValue::Scalar(ScalarValue::from("$.i1.j2"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])?.into_array(1)?;
        let v = r.as_string::<i32>().iter().next().unwrap();

        // NOTE:
        // standard jsonpath should output [[200,300],[400, 500],null,"other"]
        // but we have to keep consistent with hive UDFJson
        assert_eq!(v, Some(r#"[200,300,400,500,"other"]"#));
        Ok(())
    }
}
