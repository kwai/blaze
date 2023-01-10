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

use crate::down_cast_any_ref;
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Computes If() with short circuiting
#[derive(Debug)]
pub struct IIfExpr {
    condition_expr: Arc<dyn PhysicalExpr>,
    truthy_expr: Arc<dyn PhysicalExpr>,
    falsy_expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl PartialEq<dyn Any> for IIfExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.condition_expr.eq(&x.condition_expr)
                    && self.truthy_expr.eq(&x.truthy_expr)
                    && self.falsy_expr.eq(&x.falsy_expr)
                    && self.data_type == x.data_type
            })
            .unwrap_or(false)
    }
}

impl IIfExpr {
    pub fn new(
        condition_expr: Arc<dyn PhysicalExpr>,
        truthy_expr: Arc<dyn PhysicalExpr>,
        falsy_expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
    ) -> Self {
        Self {
            condition_expr,
            truthy_expr,
            falsy_expr,
            data_type,
        }
    }
}

impl Display for IIfExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "If({}, {}, {})",
            self.condition_expr, self.truthy_expr, self.falsy_expr
        )
    }
}

impl PhysicalExpr for IIfExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let true_nullable = self.truthy_expr.nullable(input_schema)?;
        let false_nullable = self.falsy_expr.nullable(input_schema)?;
        Ok(true_nullable || false_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let condition = self.condition_expr.evaluate(batch)?;
        match condition {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                self.truthy_expr.evaluate(batch)
            }
            ColumnarValue::Scalar(_) => self.falsy_expr.evaluate(batch),
            ColumnarValue::Array(conditions) => Ok(ColumnarValue::Array(compute_iif(
                batch,
                as_boolean_array(&conditions),
                &self.truthy_expr,
                &self.falsy_expr,
            )?)),
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![
            self.condition_expr.clone(),
            self.truthy_expr.clone(),
            self.falsy_expr.clone(),
        ]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            children[2].clone(),
            self.data_type.clone(),
        )))
    }
}

fn compute_iif(
    batch: &RecordBatch,
    conditions: &BooleanArray,
    truthy_expr: &Arc<dyn PhysicalExpr>,
    falsy_expr: &Arc<dyn PhysicalExpr>,
) -> Result<ArrayRef> {
    let mut conditions_truthy = conditions;
    let tmp1;
    if conditions.null_count() > 0 {
        tmp1 = compute::prep_null_mask_filter(conditions);
        conditions_truthy = &tmp1;
    };

    let tmp2 = compute::not(conditions_truthy)?;
    let conditions_falsy = &tmp2;

    let (truthy_values, falsy_values) = (
        truthy_expr
            .evaluate_selection(batch, conditions_truthy)?
            .into_array(conditions.len()),
        falsy_expr
            .evaluate_selection(batch, conditions_falsy)?
            .into_array(conditions.len()),
    );
    Ok(compute::kernels::zip::zip(
        conditions_truthy,
        &truthy_values,
        &falsy_values,
    )?)
}

#[cfg(test)]
mod test {
    use crate::iif::IIfExpr;
    use arrow::array::*;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::Operator::Gt;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_expr::PhysicalExpr;
    use std::sync::Arc;

    #[test]
    fn test() {
        let arg1: ArrayRef = Arc::new(Int32Array::from_iter(&[
            None,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));
        let arg2: ArrayRef = Arc::new(Int32Array::from_iter(&[
            Some(5),
            Some(4),
            Some(3),
            Some(2),
            Some(1),
            None,
        ]));
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("a", arg1, true),
            ("b", arg2, true),
        ])
        .unwrap();

        let a = Arc::new(Column::new("a", 0));
        let b = Arc::new(Column::new("b", 1));
        let condition_expr = Arc::new(BinaryExpr::new(a.clone(), Gt, b.clone()));
        let if_expr = Arc::new(IIfExpr::new(condition_expr, a, b, DataType::Int32));

        let result = if_expr.evaluate(&batch).unwrap().into_array(6);
        let result: &Int32Array = as_primitive_array(&result);
        assert_eq!(
            result.into_iter().collect::<Vec<_>>(),
            vec![Some(5), Some(4), Some(3), Some(3), Some(4), None,]
        );
    }
}
