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

use arrow::{
    array::{Array, ArrayRef, AsArray, ListArray, ListBuilder, StringArray, StringBuilder},
    datatypes::DataType,
};
use datafusion::{
    common::{
        cast::{as_int32_array, as_list_array, as_string_array},
        Result, ScalarValue,
    },
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

pub fn string_lower(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            as_string_array(array)?
                .into_iter()
                .map(|s| s.map(|s| s.to_lowercase())),
        )))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(str))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(str.to_lowercase())),
        )),
        _ => df_execution_err!("string_lower only supports literal utf8"),
    }
}

pub fn string_upper(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            as_string_array(array)?
                .into_iter()
                .map(|s| s.map(|s| s.to_uppercase())),
        )))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(str))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(str.to_uppercase())),
        )),
        _ => df_execution_err!("string_lower only supports literal utf8"),
    }
}

pub fn string_space(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let n_array = args[0].clone().into_array(1)?;
    let repeated_string_array = Arc::new(StringArray::from_iter(
        as_int32_array(&n_array)?
            .into_iter()
            .map(|n| n.map(|n| " ".repeat(n.max(0) as usize))),
    ));
    Ok(ColumnarValue::Array(repeated_string_array))
}

pub fn string_repeat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let string_array = args[0].clone().into_array(1)?;
    let n = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) => (*n).max(0),
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        _ => df_execution_err!("string_repeat n only supports literal int32")?,
    };

    let repeated_string_array: ArrayRef = Arc::new(StringArray::from_iter(
        as_string_array(&string_array)?
            .into_iter()
            .map(|s| s.map(|s| s.repeat(n as usize))),
    ));
    Ok(ColumnarValue::Array(repeated_string_array))
}

pub fn string_split(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let string_array = args[0].clone().into_array(1)?;
    let pat = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(pat))) if !pat.is_empty() => pat,
        _ => df_execution_err!("string_split pattern only supports non-empty literal string")?,
    };

    let mut splitted_builder = ListBuilder::new(StringBuilder::new());
    for s in as_string_array(&string_array)? {
        match s {
            Some(s) => {
                for segment in s.split(pat) {
                    splitted_builder.values().append_value(segment);
                }
                splitted_builder.append(true);
            }
            None => {
                splitted_builder.append_null();
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(splitted_builder.finish())))
}

/// concat() function compatible with spark (returns null if any param is null)
/// concat('abcde', 2, 22) = 'abcde222
/// concat('abcde', 2, NULL, 22) = NULL
pub fn string_concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        df_execution_err!(
            "concat was called with {} arguments. It requires at least 1.",
            args.len(),
        )?;
    }

    // first, decide whether to return a scalar or a vector.
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });
    if let Some(size) = return_array.next() {
        let result = (0..size)
            .map(|index| {
                let mut owned_string: String = "".to_owned();
                let mut is_not_null = true;
                for arg in args {
                    #[allow(clippy::collapsible_match)]
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                            if let Some(value) = maybe_value {
                                owned_string.push_str(value);
                            } else {
                                is_not_null = false;
                                break;
                            }
                        }
                        ColumnarValue::Array(v) => {
                            if v.is_valid(index) {
                                let v = as_string_array(v).unwrap();
                                owned_string.push_str(v.value(index));
                            } else {
                                is_not_null = false;
                                break;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                is_not_null.then_some(owned_string)
            })
            .collect::<StringArray>();

        Ok(ColumnarValue::Array(Arc::new(result)))
    } else {
        // short avenue with only scalars
        // returns null if args contains null
        let is_not_null = args.iter().all(|arg| match arg {
            ColumnarValue::Scalar(scalar) if scalar.is_null() => false,
            _ => true,
        });
        if !is_not_null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }

        // concat
        let initial = Some("".to_string());
        let result = args.iter().fold(initial, |mut acc, rhs| {
            if let Some(ref mut inner) = acc {
                match rhs {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => {
                        inner.push_str(v);
                    }
                    _ => unreachable!(""),
                };
            };
            acc
        });
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }
}

pub fn string_concat_ws(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let sep = match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(pat))) => pat,
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        _ => df_execution_err!("string_concat_ws separator only supports literal string")?,
    };

    #[derive(Clone)]
    enum Arg<'a> {
        Literal(&'a str),
        LiteralList(Vec<&'a str>),
        Array(StringArray),
        List(ListArray),
        Ignore,
    }
    let mut args = args[1..]
        .iter()
        .map(|arg| {
            match arg {
                ColumnarValue::Array(array) => {
                    if let Ok(s) = as_string_array(&array).cloned() {
                        return Ok(Arg::Array(s));
                    }
                    if let Ok(l) = as_list_array(&array).cloned() {
                        if l.value_type() == DataType::Utf8 {
                            return Ok(Arg::List(l));
                        }
                    }
                }
                ColumnarValue::Scalar(scalar) => {
                    if let ScalarValue::Utf8(s) = scalar {
                        match s {
                            Some(s) => return Ok(Arg::Literal(&s)),
                            None => return Ok(Arg::Ignore),
                        }
                    }
                    if let ScalarValue::List(l) = scalar
                        && l.data_type() == &DataType::Utf8
                    {
                        if l.is_null(0) {
                            return Ok(Arg::Ignore);
                        }
                        let list = l.value(0);
                        let str_list = list.as_string::<i32>();
                        let mut strs = vec![];
                        for s in str_list.iter() {
                            if let Some(s) = s {
                                strs.push(unsafe {
                                    // safety: bypass "returning temporary value" check
                                    std::mem::transmute::<_, &str>(s)
                                });
                            }
                        }
                        return Ok(Arg::LiteralList(strs));
                    }
                }
            }
            df_execution_err!("concat_ws args must be string or array<string>")
        })
        .collect::<Result<Vec<_>>>()?;

    args.retain(|arg| !matches!(arg, Arg::Ignore));

    // fast path when all args are literals
    if args
        .iter()
        .all(|arg| matches!(arg, Arg::Literal(_) | Arg::LiteralList(_)))
    {
        let mut segments = vec![];
        for arg in &args {
            match arg {
                Arg::Literal(s) => segments.push(*s),
                Arg::LiteralList(l) => segments.extend(l),
                _ => unreachable!(),
            }
        }
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            segments.join(sep),
        ))));
    }

    let mut array_len = 1;
    for arg in &args {
        match arg {
            Arg::Array(strings) => array_len = strings.len(),
            Arg::List(list) => array_len = list.len(),
            _ => continue,
        }
    }

    let mut segments = vec![];
    let concatenated_string_array: ArrayRef =
        Arc::new(StringArray::from_iter((0..array_len).map(|i| {
            segments.clear();
            for arg in &args {
                match &arg {
                    Arg::Literal(s) => segments.push(*s),
                    Arg::LiteralList(l) => segments.extend(l),
                    Arg::Array(strings) => {
                        if strings.is_valid(i) {
                            segments.push(strings.value(i));
                        }
                    }
                    Arg::List(list) => {
                        if list.is_valid(i) {
                            let strings = as_string_array(list.values()).unwrap();
                            let offsets = list.value_offsets();
                            let l = offsets[i] as usize;
                            let r = offsets[i + 1] as usize;
                            for i in l..r {
                                if strings.is_valid(i) {
                                    segments.push(strings.value(i));
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Some(segments.join(sep))
        })));
    Ok(ColumnarValue::Array(concatenated_string_array))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int32Array, ListBuilder, StringArray, StringBuilder};
    use datafusion::{
        common::{
            cast::{as_list_array, as_string_array},
            Result, ScalarValue,
        },
        physical_plan::ColumnarValue,
    };

    use crate::spark_strings::{
        string_concat, string_concat_ws, string_lower, string_repeat, string_space, string_split,
        string_upper,
    };

    #[test]
    fn test_string_space() -> Result<()> {
        // positive case
        let r = string_space(&vec![ColumnarValue::Array(Arc::new(
            Int32Array::from_iter(vec![Some(3), Some(0), Some(-100), None]),
        ))])?;
        let s = r.into_array(4)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("   "), Some(""), Some(""), None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_upper() -> Result<()> {
        let r = string_upper(&vec![ColumnarValue::Array(Arc::new(
            StringArray::from_iter(vec![Some("{123}"), Some("A'asd'"), None]),
        ))])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("{123}"), Some("A'ASD'"), None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_lower() -> Result<()> {
        let r = string_lower(&vec![ColumnarValue::Array(Arc::new(
            StringArray::from_iter(vec![Some("{123}"), Some("A'asd'"), None]),
        ))])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("{123}"), Some("a'asd'"), None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_repeat() -> Result<()> {
        // positive case
        let r = string_repeat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("123")),
                Some(format!("a")),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(3_i32)),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("123123123"), Some("aaa"), None,]
        );

        // repeat with n < 0
        let r = string_repeat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("123")),
                Some(format!("a")),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(-1_i32)),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(""), Some(""), None,]
        );

        // repeat with n = null
        let r = string_repeat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("123")),
                Some(format!("a")),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![None, None, None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_split() -> Result<()> {
        // positive case
        let r = string_split(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("123,456,,,789,")),
                Some(format!("123")),
                Some(format!("")),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(",")),
        ])?;
        let list = r.into_array(4)?;
        let list_offsets = as_list_array(&list)?.value_offsets();
        let list_values = as_list_array(&list)?.values();

        assert_eq!(list_offsets, vec![0, 6, 7, 8, 8]);
        assert_eq!(
            as_string_array(list_values)?
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                Some("123"),
                Some("456"),
                Some(""),
                Some(""),
                Some("789"),
                Some(""),
                Some("123"),
                Some(""),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_string_concat() -> Result<()> {
        // positive case
        let r = string_concat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("123")),
                None,
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("444")),
                Some(format!("456")),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("")),
                Some(format!("")),
            ]))),
            ColumnarValue::Scalar(ScalarValue::from("SomeScalar")),
        ])?;
        let s = r.into_array(2)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("123444SomeScalar"), None,]
        );
        Ok(())
    }
    #[test]
    fn test_string_concat_ws() -> Result<()> {
        // positive case
        let r = string_concat_ws(&vec![
            ColumnarValue::Scalar(ScalarValue::from("||")),
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("123")),
                None,
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                None,
                Some(format!("456")),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some(format!("")),
                Some(format!("")),
            ]))),
            ColumnarValue::Array(Arc::new({
                let mut list_builder = ListBuilder::new(StringBuilder::new());
                list_builder.values().append_value("XX");
                list_builder.values().append_value("YY");
                list_builder.values().append_null();
                list_builder.append(true);
                list_builder.append_null();
                list_builder.finish()
            })),
            ColumnarValue::Scalar(ScalarValue::from("SomeScalar")),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ])?;
        let s = r.into_array(2)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("123||||XX||YY||SomeScalar"), Some("456||||SomeScalar"),]
        );
        Ok(())
    }
}
