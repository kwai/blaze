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

use std::sync::Arc;
use arrow::array::{Array, new_null_array};
use arrow::datatypes::DataType;
use arrow::array::StringArray;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_plan::ColumnarValue;

pub fn spark_get_json_object(args: &[ColumnarValue]) ->Result<ColumnarValue> {
    let json_string_array = match &args[0] {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => {
            scalar.to_array_of_size(1)
        }
    };

    let json_strings = json_string_array.as_any().downcast_ref::<StringArray>().unwrap();
    let path_string = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(str)) => match str {
            Some(path) => path,
            None => {
                return Ok(ColumnarValue::Array(new_null_array(&DataType::Utf8, json_strings.len())))
            }
        }
        _ => unreachable!("path should be ScalarValue"),
    };

    let mut evaluator = match HiveGetJsonObjectEvaluator::try_new(path_string) {
        Ok(evaluator) => evaluator,
        Err(_) => {
            return Ok(ColumnarValue::Array(new_null_array(&DataType::Utf8, json_strings.len())));
        }
    };

    let output = json_strings
        .iter()
        .map(|json_string| {
            json_string.and_then(|s| match evaluator.evaluate(s) {
                Ok(Some(matched)) => Some(matched),
                _ => None
            })
        })
        .collect::<StringArray>();
    Ok(ColumnarValue::Array(Arc::new(output)))
}

#[derive(Debug)]
enum HiveGetJsonObjectError {
    InvalidJsonPath(String),
    InvalidInput(String),
}

struct HiveGetJsonObjectEvaluator {
    matchers: Vec<HiveGetJsonObjectMatcher>,
    buffer: Vec<Box<serde_json::Value>>,
}

impl HiveGetJsonObjectEvaluator {
    fn try_new(json_path: &str) -> std::result::Result<Self, HiveGetJsonObjectError> {
        let mut evaluator = Self {
            matchers: vec![],
            buffer: vec![],
        };
        let chars = json_path.chars();
        let mut peekable = chars.peekable();

        while let Some(matcher) = HiveGetJsonObjectMatcher::parse(&mut peekable)? {
            evaluator.matchers.push(matcher);
        }
        if evaluator.matchers.first() != Some(&HiveGetJsonObjectMatcher::Root) {
            return Err(HiveGetJsonObjectError::InvalidJsonPath(format!("json path missing root")));
        }
        evaluator.matchers.remove(0); // remove root matcher
        if evaluator.matchers.contains(&HiveGetJsonObjectMatcher::Root) {
            return Err(HiveGetJsonObjectError::InvalidJsonPath(format!("json path has more than one root")));
        }
        Ok(evaluator)
    }

    fn evaluate(&mut self, json_str: &str) -> std::result::Result<Option<String>, HiveGetJsonObjectError> {
        let value: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|_| HiveGetJsonObjectError::InvalidInput(format!("invalid json string")))?;
        let mut current = &value;

        for matcher in &self.matchers {
            current = matcher.evaluate(current, unsafe {
                // safety - bypass rust borrow checker, as items in buffer are never
                // dropped in matcher.evaluate().
                &mut *(&self.buffer
                    as *const Vec<Box<serde_json::Value>>
                    as *mut Vec<Box<serde_json::Value>>
                )
            });
        }
        let ret = match current {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(string) => Ok(Some(string.to_string())),
            serde_json::Value::Number(number) => Ok(Some(number.to_string())),
            serde_json::Value::Bool(b) => Ok(Some(b.to_string())),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                serde_json::to_string(current)
                    .map(|string| Some(string.to_string()))
                    .map_err(|_| HiveGetJsonObjectError::InvalidInput(
                        format!("array to json error")
                    ))
            }
        };
        self.buffer.clear();
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
        chars: &mut std::iter::Peekable<std::str::Chars>
    ) -> std::result::Result<Option<Self>, HiveGetJsonObjectError> {

        match chars.peek() {
            None => {
                Ok(None)
            }
            Some('$') => {
                chars.next();
                Ok(Some(Self::Root))
            }
            Some('.') => {
                chars.next();
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
                        format!("empty child name")
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
                                format!("unterminated subscript")
                            ));
                        }
                    }
                }
                if index_str == "*" {
                    return Ok(Some(Self::SubscriptAll));
                }
                let index = str::parse::<usize>(&index_str)
                    .map_err(|_| HiveGetJsonObjectError::InvalidJsonPath(
                        format!("invalid subscript index")
                    ))?;
                Ok(Some(Self::Subscript(index)))
            }
            Some(c) => {
                return Err(HiveGetJsonObjectError::InvalidJsonPath(
                    format!("unexpected char in json path: {}", c)
                ));
            }
        }
    }

    fn evaluate<'a>(
        &self,
        value: &'a serde_json::Value,
        buffer: &'a mut Vec<Box<serde_json::Value>>,
    ) -> &'a serde_json::Value {

        match self {
            HiveGetJsonObjectMatcher::Root => {
                return value;
            }
            HiveGetJsonObjectMatcher::Child(child) => {
                if let serde_json::Value::Object(object) = &value {
                    if let Some(child) = object.get(child) {
                        return &child;
                    }
                }
                if let serde_json::Value::Array(array) = &value {
                    buffer.push(Box::new(serde_json::Value::Array(array
                        .iter()
                        .map(|item| {
                            if let serde_json::Value::Object(object) = item {
                                object.get(child)
                                    .map(|child| child.clone())
                                    .unwrap_or_default()
                            } else {
                                serde_json::Value::Null
                            }
                        })
                        .collect()
                    )));
                    return buffer.last().unwrap().as_ref();
                }
            }
            HiveGetJsonObjectMatcher::Subscript(index) => {
                if let serde_json::Value::Array(array) = &value {
                    if let Some(child) = array.get(*index) {
                        return &child;
                    }
                }
            }
            HiveGetJsonObjectMatcher::SubscriptAll => {
                if let serde_json::Value::Array(_) = &value {
                    return value;
                }
            }
        }
        &serde_json::Value::Null
    }
}

#[cfg(test)]
mod test {
    use crate::spark_get_json_object::HiveGetJsonObjectEvaluator;

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
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some("amy".to_owned())
        );

        let path = "$.store.bicycle.price";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some("19.95".to_owned())
        );

        let path = "$.store.fruit[0]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some(r#"{"type":"apple","weight":8}"#.to_owned())
        );

        let path = "$.store.fruit[1].weight";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some("9".to_owned())
        );

        let path = "$.store.fruit[*]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some(r#"[{"type":"apple","weight":8},{"type":"pear","weight":9}]"#.to_owned())
        );

        let path = "$.non_exist_key";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            None
        );
    }

    #[test]
    fn test_2() {
        let input = r#"
            {
                "ID": 121,
                "message": {
                    "name": "Asher",
                    "location": [
                        {
                            "county": "浦东",
                            "city": "1.234"
                        },
                        {
                            "county": "西直门",
                            "city": 1.234
                        }
                    ]
                }
            }"#;

        let path = "$.message.name";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some("Asher".to_owned())
        );

        let path = "$.message.location.city";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some(r#"["1.234",1.234]"#.to_owned())
        );

        let path = "$.message.location[0]";
        assert_eq!(
            HiveGetJsonObjectEvaluator::try_new(path).unwrap().evaluate(input).unwrap(),
            Some(r#"{"city":"1.234","county":"浦东"}"#.to_owned())
        );
    }
}