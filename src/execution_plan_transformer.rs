use datafusion::logical_plan::Operator;
use datafusion::physical_plan::expressions::{
    BinaryExpr, CaseExpr, CastExpr, InListExpr, IsNotNullExpr, IsNullExpr, NegativeExpr,
    NotExpr, PhysicalSortExpr, TryCastExpr, DEFAULT_DATAFUSION_CAST_OPTIONS,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::functions::ScalarFunctionExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_ext::shuffle_reader_exec::BlazeShuffleReaderExec;
use plan_serde::execution_plans::ShuffleReaderExec;
use std::sync::Arc;

pub fn replace_shuffle_reader(
    plan: Arc<dyn ExecutionPlan>,
    job_id: &str,
) -> Arc<dyn ExecutionPlan> {
    transform_up_execution_plan(plan, &|plan| {
        if let Some(exec) = plan.as_any().downcast_ref::<ShuffleReaderExec>() {
            return Arc::new(BlazeShuffleReaderExec {
                job_id: job_id.to_owned(),
                schema: exec.schema(),
            });
        }
        plan
    })
}

pub fn replace_blaze_extension_exprs(
    plan: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    transform_up_execution_plan(plan, &|plan| {
        if let Some(exec) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let expr = exec
                .expr()
                .iter()
                .map(|(expr, name)| {
                    (convert_blaze_extension_expr(expr.clone()), name.clone())
                })
                .collect::<Vec<_>>();
            return Arc::new(
                ProjectionExec::try_new(expr, exec.input().clone()).unwrap(),
            );
        }
        if let Some(exec) = plan.as_any().downcast_ref::<FilterExec>() {
            let predicate = convert_blaze_extension_expr(exec.predicate().clone());
            return Arc::new(
                FilterExec::try_new(predicate, exec.input().clone()).unwrap(),
            );
        }
        if let Some(exec) = plan.as_any().downcast_ref::<SortExec>() {
            let expr = exec
                .expr()
                .iter()
                .map(|expr| PhysicalSortExpr {
                    expr: convert_blaze_extension_expr(expr.expr.clone()),
                    options: expr.options,
                })
                .collect::<Vec<_>>();
            return Arc::new(SortExec::new_with_partitioning(
                expr,
                exec.input().clone(),
                true,
            ));
        }
        plan
    })
}

fn convert_blaze_extension_expr(expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    transform_up_physical_expr(expr, &|expr| {
        if let Some(f) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
            if f.name() == "blaze-extension-expr:Remainder" {
                return Arc::new(BinaryExpr::new(
                    f.args()[0].clone(),
                    Operator::Modulo,
                    f.args()[1].clone(),
                ));
            }
        }
        expr
    })
}

fn transform_up_execution_plan(
    plan: Arc<dyn ExecutionPlan>,
    transform: &impl Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let children = plan
        .children()
        .iter()
        .map(|child| transform_up_execution_plan(child.clone(), transform))
        .collect::<Vec<_>>();

    return transform(match children.is_empty() {
        false => plan.with_new_children(children).unwrap(),
        true => plan,
    });
}

fn transform_up_physical_expr(
    expr: Arc<dyn PhysicalExpr>,
    transform: &impl Fn(Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    let recursion = |expr| transform_up_physical_expr(expr, transform);
    let e = expr.as_any();

    if let Some(e) = e.downcast_ref::<BinaryExpr>() {
        return transform(Arc::new(BinaryExpr::new(
            recursion(e.left().clone()),
            *e.op(),
            recursion(e.right().clone()),
        )));
    }
    if let Some(e) = e.downcast_ref::<CaseExpr>() {
        return transform(Arc::new(
            CaseExpr::try_new(
                e.expr().as_ref().map(|expr| recursion((*expr).clone())),
                e.when_then_expr()
                    .iter()
                    .map(|(expr1, expr2)| {
                        (recursion(expr1.clone()), recursion(expr2.clone()))
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                e.else_expr()
                    .as_ref()
                    .map(|expr| recursion((*expr).clone())),
            )
            .unwrap(),
        ));
    }
    if let Some(e) = e.downcast_ref::<NotExpr>() {
        return transform(Arc::new(NotExpr::new(recursion(e.arg().clone()))));
    }
    if let Some(e) = e.downcast_ref::<IsNullExpr>() {
        return transform(Arc::new(IsNullExpr::new(recursion(e.arg().clone()))));
    }
    if let Some(e) = e.downcast_ref::<IsNotNullExpr>() {
        return transform(Arc::new(IsNotNullExpr::new(recursion(e.arg().clone()))));
    }
    if let Some(e) = e.downcast_ref::<InListExpr>() {
        return transform(Arc::new(InListExpr::new(
            recursion(e.expr().clone()),
            e.list()
                .iter()
                .map(|expr| recursion(expr.clone()))
                .collect::<Vec<_>>(),
            e.negated(),
        )));
    }
    if let Some(e) = e.downcast_ref::<NegativeExpr>() {
        return transform(Arc::new(NegativeExpr::new(recursion(e.arg().clone()))));
    }
    if let Some(e) = e.downcast_ref::<CastExpr>() {
        return transform(Arc::new(CastExpr::new(
            recursion(e.expr().clone()),
            e.cast_type().clone(),
            DEFAULT_DATAFUSION_CAST_OPTIONS,
        )));
    }
    if let Some(e) = e.downcast_ref::<TryCastExpr>() {
        return transform(Arc::new(TryCastExpr::new(
            recursion(e.expr().clone()),
            e.cast_type().clone(),
        )));
    }
    if let Some(e) = e.downcast_ref::<ScalarFunctionExpr>() {
        return transform(Arc::new(ScalarFunctionExpr::new(
            e.name(),
            e.fun().clone(),
            e.args()
                .iter()
                .map(|expr| recursion(expr.clone()))
                .collect::<Vec<_>>(),
            e.return_type(),
        )));
    }
    transform(expr.clone()) // transform leaf expr
}
