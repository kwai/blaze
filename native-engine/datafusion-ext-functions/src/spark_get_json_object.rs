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

use std::{any::Any, borrow::Cow, fmt::Debug, sync::Arc};

use arrow::{
    array::{new_null_array, Array, StringArray},
    datatypes::DataType,
};
use datafusion::{
    common::{Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::uda::UserDefinedArray;

/// implement hive/spark's UDFGetJson
/// get_json_object(str, path) == get_parsed_json_object(parse_json(str), path)
pub fn spark_get_json_object(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let json_string_array = match &args[0] {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1),
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
                )))
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
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1),
    };

    let json_strings = json_string_array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let json_values: Vec<Option<Arc<dyn Any + Send + Sync + 'static>>> = json_strings
        .iter()
        .map(|s| {
            s.and_then(|s| {
                serde_json::from_str::<serde_json::Value>(s)
                    .map(|v| {
                        let any: Arc<dyn Any + Send + Sync + 'static> = Arc::new(v);
                        any
                    })
                    .ok()
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
                )))
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

    let output = StringArray::from(
        json_array
            .iter()
            .map(|value| {
                value.as_ref().and_then(|value| {
                    let json_value = value.downcast_ref::<serde_json::Value>().unwrap();
                    evaluator
                        .evaluate_with_value(json_value)
                        .ok()
                        .unwrap_or(None)
                })
            })
            .collect::<Vec<_>>(),
    );
    Ok(ColumnarValue::Array(Arc::new(output)))
}

#[derive(Debug)]
enum HiveGetJsonObjectError {
    InvalidJsonPath(String),
    InvalidInput(String),
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
            return Err(HiveGetJsonObjectError::InvalidJsonPath(
                "json path missing root".to_string(),
            ));
        }
        evaluator.matchers.remove(0); // remove root matcher
        if evaluator.matchers.contains(&HiveGetJsonObjectMatcher::Root) {
            return Err(HiveGetJsonObjectError::InvalidJsonPath(
                "json path has more than one root".to_string(),
            ));
        }
        Ok(evaluator)
    }

    fn evaluate(
        &mut self,
        json_str: &str,
    ) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
        let root_value: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|_| HiveGetJsonObjectError::InvalidInput("invalid json string".to_string()))?;
        self.evaluate_with_value(&root_value)
    }

    fn evaluate_with_value(
        &mut self,
        root_value: &serde_json::Value,
    ) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
        let mut root_value: Cow<serde_json::Value> = Cow::Borrowed(root_value);
        let mut value = root_value.as_ref();
        for matcher in &self.matchers {
            match matcher.evaluate(value) {
                Cow::Borrowed(evaluated) => {
                    value = evaluated;
                }
                Cow::Owned(evaluated) => {
                    root_value = Cow::Owned(evaluated);
                    value = root_value.as_ref();
                }
            }
        }

        let ret = match value {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(string) => Ok(Some(string.to_string())),
            serde_json::Value::Number(number) => Ok(Some(number.to_string())),
            serde_json::Value::Bool(b) => Ok(Some(b.to_string())),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                serde_json::to_string(value).map(Some).map_err(|_| {
                    HiveGetJsonObjectError::InvalidInput("array to json error".to_string())
                })
            }
        };
        ret
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
                    return Err(HiveGetJsonObjectError::InvalidJsonPath(
                        "empty child name".to_string(),
                    ));
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
                            return Err(HiveGetJsonObjectError::InvalidJsonPath(
                                "unterminated subscript".to_string(),
                            ));
                        }
                    }
                }
                if index_str == "*" {
                    return Ok(Some(Self::SubscriptAll));
                }
                let index = str::parse::<usize>(&index_str).map_err(|_| {
                    HiveGetJsonObjectError::InvalidJsonPath("invalid subscript index".to_string())
                })?;
                Ok(Some(Self::Subscript(index)))
            }
            Some(c) => Err(HiveGetJsonObjectError::InvalidJsonPath(format!(
                "unexpected char in json path: {}",
                c
            ))),
        }
    }

    fn evaluate<'a>(&self, value: &'a serde_json::Value) -> Cow<'a, serde_json::Value> {
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
                    return Cow::Owned(serde_json::Value::Array(
                        array
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
                            .collect(),
                    ));
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
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{AsArray, StringArray};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_get_json_object::{
        spark_get_parsed_json_object, spark_parse_json, HiveGetJsonObjectEvaluator,
    };

    #[test]
    fn test_hive_demo() {
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

        let path = "$.store.bicycle.price";
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
            Some(r#"{"type":"apple","weight":8}"#.to_owned())
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
            Some(r#"[{"type":"apple","weight":8},{"type":"pear","weight":9}]"#.to_owned())
        );

        let path = "$.store.fruit.[1].type";
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
    }

    #[test]
    fn test_2() {
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

        // let path = ColumnarValue::Scalar(ScalarValue::from("$.NOT_EXISTED"));
        // let r = spark_get_parsed_json_object(&[parsed.clone(),
        // path]).unwrap().into_array(1); let v = r.as_string::<i32>().iter().
        // next().unwrap(); assert_eq!(v, None);

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location.county"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])
            .unwrap()
            .into_array(1);
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"["浦东","西直门"]"#));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location.NOT_EXISTED"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])
            .unwrap()
            .into_array(1);
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"[]"#));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.name"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])
            .unwrap()
            .into_array(1);
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert!(v.unwrap().contains("Asher"));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location.city"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])
            .unwrap()
            .into_array(1);
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"["1.234",1.234]"#));

        let path = ColumnarValue::Scalar(ScalarValue::from("$.message.location[0]"));
        let r = spark_get_parsed_json_object(&[parsed.clone(), path])
            .unwrap()
            .into_array(1);
        let v = r.as_string::<i32>().iter().next().unwrap();
        assert_eq!(v, Some(r#"{"city":"1.234","county":"浦东"}"#));
    }
}
