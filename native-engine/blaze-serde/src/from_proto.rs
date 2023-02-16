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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::FileRange;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::TypeSignature::VariadicEqual;
use datafusion::logical_expr::{BuiltinScalarFunction, Case, Cast};
use datafusion::logical_expr::{
    Expr, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    Volatility,
};
use datafusion::physical_expr::{functions, ScalarFunctionExpr};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::sorts::sort::SortOptions;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    expressions::{
        BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
        Literal, NegativeExpr, NotExpr, PhysicalSortExpr,
        DEFAULT_DATAFUSION_CAST_OPTIONS,
    },
    projection::ProjectionExec,
    Partitioning,
};
use datafusion::physical_plan::{
    ColumnStatistics, ExecutionPlan, PhysicalExpr, Statistics,
};
use datafusion::scalar::ScalarValue;
use datafusion_ext_commons::streams::ipc_stream::IpcReadMode;
use datafusion_ext_file_formats::{
    FileScanConfig, ObjectMeta, ParquetExec, PartitionedFile,
};
use datafusion_ext_plans::agg::{
    create_agg, AggExecMode, AggExpr, AggFunction, AggMode, GroupingExpr,
};
use datafusion_ext_plans::agg_exec::AggExec;
use datafusion_ext_plans::broadcast_hash_join_exec::BroadcastHashJoinExec;
use datafusion_ext_plans::debug_exec::DebugExec;
use datafusion_ext_plans::empty_partitions_exec::EmptyPartitionsExec;
use datafusion_ext_plans::expand_exec::ExpandExec;
use datafusion_ext_plans::ffi_reader_exec::FFIReaderExec;
use datafusion_ext_plans::filter_exec::FilterExec;
use datafusion_ext_plans::ipc_reader_exec::IpcReaderExec;
use datafusion_ext_plans::ipc_writer_exec::IpcWriterExec;
use datafusion_ext_plans::limit_exec::LimitExec;
use datafusion_ext_plans::rename_columns_exec::RenameColumnsExec;
use datafusion_ext_plans::rss_shuffle_writer_exec::RssShuffleWriterExec;
use datafusion_ext_plans::shuffle_writer_exec::ShuffleWriterExec;
use datafusion_ext_plans::sort_exec::SortExec;
use datafusion_ext_plans::sort_merge_join_exec::SortMergeJoinExec;

use crate::error::PlanSerDeError;
use crate::protobuf::physical_expr_node::ExprType;
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::{
    convert_box_required, convert_required, into_required, protobuf, DataType, Schema,
};
use crate::{from_proto_binary_op, proto_error};
use datafusion::physical_plan::expressions::GetIndexedFieldExpr;
use datafusion_ext_exprs::cast::TryCastExpr;
use datafusion_ext_exprs::get_indexed_field::FixedSizeListGetIndexedFieldExpr;
use datafusion_ext_exprs::iif::IIfExpr;
use datafusion_ext_exprs::spark_expression_wrapper::SparkExpressionWrapperExpr;
use datafusion_ext_exprs::spark_logical::{SparkLogicalExpr, SparkLogicalOp};
use datafusion_ext_exprs::string_contains::StringContainsExpr;
use datafusion_ext_exprs::string_ends_with::StringEndsWithExpr;
use datafusion_ext_exprs::string_starts_with::StringStartsWithExpr;

fn bind(
    expr_in: Arc<dyn PhysicalExpr>,
    input_schema: &Arc<Schema>,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let expr = expr_in.as_any();

    if let Some(expr) = expr.downcast_ref::<Column>() {
        Ok(Arc::new(Column::new_with_schema(
            expr.name(),
            input_schema,
        )?))
    } else {
        let new_children = expr_in
            .children()
            .iter()
            .map(|child_expr| bind(child_expr.clone(), input_schema))
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        Ok(expr_in.with_new_children(new_children)?)
    }
}

pub fn convert_physical_expr_to_logical_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Expr, DataFusionError> {
    let expr = expr.as_any();

    if let Some(expr) = expr.downcast_ref::<Column>() {
        Ok(Expr::Column(datafusion::common::Column::from_name(
            expr.name(),
        )))
    } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        Ok(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
            left: Box::new(convert_physical_expr_to_logical_expr(expr.left())?),
            op: *expr.op(),
            right: Box::new(convert_physical_expr_to_logical_expr(expr.right())?),
        }))
    } else if let Some(expr) = expr.downcast_ref::<CaseExpr>() {
        Ok(Expr::Case(Case {
            expr: expr
                .expr()
                .as_ref()
                .map(|expr| convert_physical_expr_to_logical_expr(expr).map(Box::new))
                .transpose()?,
            when_then_expr: expr
                .when_then_expr()
                .iter()
                .map(|wt| -> Result<_, DataFusionError> {
                    Ok((
                        Box::new(convert_physical_expr_to_logical_expr(&wt.0)?),
                        Box::new(convert_physical_expr_to_logical_expr(&wt.1)?),
                    ))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            else_expr: expr
                .else_expr()
                .map(|else_expr| {
                    convert_physical_expr_to_logical_expr(else_expr).map(Box::new)
                })
                .transpose()?,
        }))
    } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
        Ok(Expr::Not(Box::new(convert_physical_expr_to_logical_expr(
            expr.arg(),
        )?)))
    } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
        Ok(Expr::IsNull(Box::new(
            convert_physical_expr_to_logical_expr(expr.arg())?,
        )))
    } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
        Ok(Expr::IsNotNull(Box::new(
            convert_physical_expr_to_logical_expr(expr.arg())?,
        )))
    } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
        Ok(Expr::InList {
            expr: Box::new(convert_physical_expr_to_logical_expr(expr.expr())?),
            list: expr
                .list()
                .iter()
                .map(convert_physical_expr_to_logical_expr)
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            negated: expr.negated(),
        })
    } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
        Ok(Expr::Negative(Box::new(
            convert_physical_expr_to_logical_expr(expr.arg())?,
        )))
    } else if let Some(expr) = expr.downcast_ref::<Literal>() {
        Ok(Expr::Literal(expr.value().clone()))
    } else if let Some(expr) = expr.downcast_ref::<CastExpr>() {
        Ok(Expr::Cast(Cast {
            expr: Box::new(convert_physical_expr_to_logical_expr(expr.expr())?),
            data_type: expr.cast_type().clone(),
        }))
    } else if let Some(expr) = expr.downcast_ref::<TryCastExpr>() {
        Ok(Expr::TryCast {
            expr: Box::new(convert_physical_expr_to_logical_expr(&expr.expr)?),
            data_type: expr.cast_type.clone(),
        })
    } else if let Some(expr1) = expr.downcast_ref::<StringStartsWithExpr>() {
        let mut args = Vec::new();
        args.push(convert_physical_expr_to_logical_expr(expr1.expr())?);
        args.push(Expr::Literal(ScalarValue::Utf8(Some(
            expr1.prefix().to_string(),
        ))));

        let func: ScalarFunctionImplementation = Arc::new(|_| panic!("placeholder"));
        let return_type: ReturnTypeFunction =
            Arc::new(|_| Ok(Arc::new(DataType::Boolean)));
        Ok(Expr::ScalarUDF {
            fun: Arc::from(ScalarUDF::new(
                "string_starts_with",
                &Signature::new(VariadicEqual, Volatility::Immutable),
                &return_type,
                &func,
            )),
            args,
        })
    } else if expr.downcast_ref::<ScalarFunctionExpr>().is_some() {
        Err(DataFusionError::Plan(
            "converting physical ScalarFunctionExpr to logical is not supported"
                .to_string(),
        ))
    } else if expr.downcast_ref::<SparkExpressionWrapperExpr>().is_some() {
        Err(DataFusionError::Plan(
            "converting physical SparkExpressionWrapperExpr to logical is not supported"
                .to_string(),
        ))
    } else if expr.downcast_ref::<GetIndexedFieldExpr>().is_some() {
        Err(DataFusionError::Plan(
            "converting physical GetIndexedFieldExpr to logical is not supported"
                .to_string(),
        ))
    } else if expr
        .downcast_ref::<FixedSizeListGetIndexedFieldExpr>()
        .is_some()
    {
        Err(DataFusionError::Plan(
            "converting physical FixedSizeListGetIndexedFieldExpr to logical is not supported".to_string()
        ))
    } else {
        Err(DataFusionError::Plan(
            "Expression binding not implemented yet".to_string(),
        ))
    }
}

impl TryInto<Arc<dyn ExecutionPlan>> for &protobuf::PhysicalPlanNode {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<Arc<dyn ExecutionPlan>, Self::Error> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(projection.input)?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| {
                        Ok((
                            bind(
                                try_parse_physical_expr(expr, &input.schema())?,
                                &input.schema(),
                            )?,
                            name.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, Self::Error>>(
                    )?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicate =
                    try_parse_physical_expr_required(&filter.expr, &input.schema())?;
                Ok(Arc::new(FilterExec::try_new(
                    bind(predicate, &input.schema())?,
                    input,
                )?))
            }
            PhysicalPlanType::ParquetScan(scan) => {
                let conf: FileScanConfig = scan.base_conf.as_ref().unwrap().try_into()?;
                let predicate = scan.pruning_predicates
                    .iter()
                    .filter_map(|predicate| {
                        try_parse_physical_expr(
                            predicate,
                            &conf.file_schema,
                        ).and_then(|expr| {
                            convert_physical_expr_to_logical_expr(&expr).map_err(|err| {
                                log::warn!("ignore unsupported predicate pruning expr: {:?}: {}",
                                    expr,
                                    err.to_string(),
                                );
                                PlanSerDeError::DataFusionError(err)
                            })
                        })
                            .ok()
                    })
                    .fold(Expr::Literal(ScalarValue::from(true)), |a, b| {
                        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                            op: Operator::And,
                            left: Box::new(a),
                            right: Box::new(b),
                        })
                    });
                Ok(Arc::new(ParquetExec::new(
                    conf,
                    scan.fs_resource_id.clone(),
                    Some(predicate),
                )))
            }
            PhysicalPlanType::SortMergeJoin(sort_merge_join) => {
                let left: Arc<dyn ExecutionPlan> =
                    convert_box_required!(sort_merge_join.left)?;
                let right: Arc<dyn ExecutionPlan> =
                    convert_box_required!(sort_merge_join.right)?;
                let on: Vec<(Column, Column)> = sort_merge_join
                    .on
                    .iter()
                    .map(|col| {
                        let left_col: Column = into_required!(col.left)?;
                        let left_col_binded: Column =
                            Column::new_with_schema(left_col.name(), &left.schema())?;
                        let right_col: Column = into_required!(col.right)?;
                        let right_col_binded: Column =
                            Column::new_with_schema(right_col.name(), &right.schema())?;
                        Ok((left_col_binded, right_col_binded))
                    })
                    .collect::<Result<_, Self::Error>>()?;

                let sort_options = sort_merge_join
                    .sort_options
                    .iter()
                    .map(|sort_options| SortOptions {
                        descending: !sort_options.asc,
                        nulls_first: sort_options.nulls_first,
                    })
                    .collect::<Vec<_>>();

                let join_type = protobuf::JoinType::from_i32(sort_merge_join.join_type)
                    .ok_or_else(|| {
                    proto_error(format!(
                        "Received a SortMergeJoinNode message with unknown JoinType {}",
                        sort_merge_join.join_type
                    ))
                })?;

                Ok(Arc::new(SortMergeJoinExec::try_new(
                    left,
                    right,
                    on,
                    join_type.into(),
                    sort_options,
                )?))
            }
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(shuffle_writer.input)?;

                let output_partitioning = parse_protobuf_hash_partitioning(
                    input.clone(),
                    shuffle_writer.output_partitioning.as_ref(),
                )?;

                Ok(Arc::new(ShuffleWriterExec::try_new(
                    input,
                    output_partitioning.unwrap(),
                    shuffle_writer.output_data_file.clone(),
                    shuffle_writer.output_index_file.clone(),
                )?))
            }
            PhysicalPlanType::RssShuffleWriter(rss_shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(rss_shuffle_writer.input)?;

                let output_partitioning = parse_protobuf_hash_partitioning(
                    input.clone(),
                    rss_shuffle_writer.output_partitioning.as_ref(),
                )?;
                Ok(Arc::new(RssShuffleWriterExec::try_new(
                    input,
                    output_partitioning.unwrap(),
                    rss_shuffle_writer.rss_partition_writer_resource_id.clone(),
                )?))
            }
            PhysicalPlanType::IpcWriter(ipc_writer) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(ipc_writer.input)?;

                Ok(Arc::new(IpcWriterExec::new(
                    input,
                    ipc_writer.ipc_consumer_resource_id.clone(),
                )))
            }
            PhysicalPlanType::IpcReader(ipc_reader) => {
                let schema = Arc::new(convert_required!(ipc_reader.schema)?);
                let mode = match protobuf::IpcReadMode::from_i32(ipc_reader.mode).unwrap()
                {
                    protobuf::IpcReadMode::ChannelUncompressed => {
                        IpcReadMode::ChannelUncompressed
                    }
                    protobuf::IpcReadMode::Channel => IpcReadMode::Channel,
                    protobuf::IpcReadMode::ChannelAndFileSegment => {
                        IpcReadMode::ChannelAndFileSegment
                    }
                };
                Ok(Arc::new(IpcReaderExec::new(
                    ipc_reader.num_partitions as usize,
                    ipc_reader.ipc_provider_resource_id.clone(),
                    schema,
                    mode,
                )))
            }
            PhysicalPlanType::Debug(debug) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(debug.input)?;
                Ok(Arc::new(DebugExec::new(input, debug.debug_id.clone())))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: bind(try_parse_physical_expr(expr, &input.schema())?, &input.schema())?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(PlanSerDeError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                // always preserve partitioning
                Ok(Arc::new(SortExec::new(
                    input,
                    exprs,
                    sort.fetch_limit.map(|limit| limit as usize),
                )))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(hashjoin.left)?;
                let right: Arc<dyn ExecutionPlan> =
                    convert_box_required!(hashjoin.right)?;
                let on: Vec<(Column, Column)> = hashjoin
                    .on
                    .iter()
                    .map(|col| {
                        let left_col: Column = into_required!(col.left)?;
                        let left_col_binded: Column =
                            Column::new_with_schema(left_col.name(), &left.schema())?;
                        let right_col: Column = into_required!(col.right)?;
                        let right_col_binded: Column =
                            Column::new_with_schema(right_col.name(), &right.schema())?;
                        Ok((left_col_binded, right_col_binded))
                    })
                    .collect::<Result<_, Self::Error>>()?;

                let join_type = protobuf::JoinType::from_i32(hashjoin.join_type)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown JoinType {}",
                            hashjoin.join_type
                        ))
                    })?;
                let filter = hashjoin
                    .filter
                    .as_ref()
                    .map(|f| {
                        let schema = Arc::new(convert_required!(f.schema)?);
                        let expression = try_parse_physical_expr_required(
                            &f.expression,
                            &schema,
                        )?;
                        let column_indices = f.column_indices
                            .iter()
                            .map(|i| {
                                let side = protobuf::JoinSide::from_i32(i.side)
                                    .ok_or_else(|| proto_error(format!(
                                        "Received a HashJoinNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex {
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>, PlanSerDeError>>()?;

                        Ok(JoinFilter::new(
                            bind(expression, &schema)?,
                            column_indices,
                            schema.as_ref().clone()
                        ))
                    })
                    .map_or(Ok(None), |v: Result<_, PlanSerDeError>| v.map(Some))?;

                let partition_mode =
                    protobuf::PartitionMode::from_i32(hashjoin.partition_mode)
                        .ok_or_else(|| {
                            proto_error(format!(
                        "Received a HashJoinNode message with unknown PartitionMode {}",
                        hashjoin.partition_mode
                    ))
                        })?;
                let partition_mode = match partition_mode {
                    protobuf::PartitionMode::CollectLeft => PartitionMode::CollectLeft,
                    protobuf::PartitionMode::Partitioned => PartitionMode::Partitioned,
                };
                Ok(Arc::new(BroadcastHashJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    &join_type.into(),
                    partition_mode,
                    &hashjoin.null_equals_null,
                )?))
            }
            PhysicalPlanType::Union(union) => {
                let inputs: Vec<Arc<dyn ExecutionPlan>> = union
                    .children
                    .iter()
                    .map(|i| i.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(UnionExec::new(inputs)))
            }
            PhysicalPlanType::EmptyPartitions(empty_partitions) => {
                let schema = Arc::new(convert_required!(empty_partitions.schema)?);
                Ok(Arc::new(EmptyPartitionsExec::new(
                    schema,
                    empty_partitions.num_partitions as usize,
                )))
            }
            PhysicalPlanType::RenameColumns(rename_columns) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(rename_columns.input)?;
                Ok(Arc::new(RenameColumnsExec::try_new(
                    input,
                    rename_columns.renamed_column_names.clone(),
                )?))
            }
            PhysicalPlanType::Agg(agg) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(agg.input)?;
                let input_schema = input.schema();

                let exec_mode = protobuf::AggExecMode::from_i32(agg.exec_mode)
                    .ok_or_else(|| {
                        proto_error(format!("invalid AggExecMode {}", agg.exec_mode))
                    })
                    .map(|exec_mode| match exec_mode {
                        protobuf::AggExecMode::HashAgg => AggExecMode::HashAgg,
                        protobuf::AggExecMode::SortAgg => AggExecMode::SortAgg,
                    })?;

                let agg_modes = agg
                    .mode
                    .iter()
                    .map(|&mode| {
                        protobuf::AggMode::from_i32(mode)
                            .ok_or_else(|| {
                                proto_error(format!("invalid AggMode {}", mode))
                            })
                            .map(|mode| match mode {
                                protobuf::AggMode::Partial => AggMode::Partial,
                                protobuf::AggMode::PartialMerge => AggMode::PartialMerge,
                                protobuf::AggMode::Final => AggMode::Final,
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let physical_groupings: Vec<GroupingExpr> = agg
                    .grouping_expr
                    .iter()
                    .zip(agg.grouping_expr_name.iter())
                    .map(|(expr, name)| {
                        try_parse_physical_expr(expr, &input_schema).and_then(|expr| {
                            Ok(bind(expr, &input_schema).map(|expr| GroupingExpr {
                                expr,
                                field_name: name.to_owned(),
                            })?)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let physical_aggs: Vec<AggExpr> = agg
                    .agg_expr
                    .iter()
                    .zip(&agg.agg_expr_name)
                    .zip(&agg_modes)
                    .map(|((expr, name), &mode)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty aggregate physical expression")
                        })?;

                        let agg_node = match expr_type {
                            ExprType::AggExpr(agg_node) => agg_node,
                            _ => {
                                return Err(PlanSerDeError::General(
                                    "Invalid aggregate expression for AggExec"
                                        .to_string(),
                                ));
                            }
                        };

                        let agg_function =
                            protobuf::AggFunction::from_i32(agg_node.agg_function)
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "Received an unknown aggregate function: {}",
                                        agg_node.agg_function
                                    ))
                                })?;
                        let agg_children_exprs = agg_node
                            .children
                            .iter()
                            .map(|expr| {
                                try_parse_physical_expr(expr, &input_schema)
                                    .and_then(|expr| Ok(bind(expr, &input_schema)?))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        Ok(AggExpr {
                            agg: create_agg(
                                AggFunction::from(agg_function),
                                &agg_children_exprs,
                                &input_schema,
                            )?,
                            mode,
                            field_name: name.to_owned(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(AggExec::try_new(
                    exec_mode,
                    physical_groupings,
                    physical_aggs,
                    agg.initial_input_buffer_offset as usize,
                    input,
                )?))
            }
            PhysicalPlanType::Limit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LimitExec::new(input, limit.limit)))
            }
            PhysicalPlanType::FfiReader(ffi_reader) => {
                let schema = Arc::new(convert_required!(ffi_reader.schema)?);
                Ok(Arc::new(FFIReaderExec::new(
                    ffi_reader.num_partitions as usize,
                    ffi_reader.export_iter_provider_resource_id.clone(),
                    schema,
                )))
            }
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(coalesce_batches.input)?;
                Ok(Arc::new(LimitExec::new(input, coalesce_batches.batch_size)))
            }
            PhysicalPlanType::Expand(expand) => {
                let schema = Arc::new(convert_required!(expand.schema)?);
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(expand.input)?;
                let projections = expand
                    .projections
                    .iter()
                    .map(|projection| {
                        projection
                            .expr
                            .iter()
                            .map(|expr| {
                                Ok(bind(
                                    try_parse_physical_expr(expr, &input.schema())?,
                                    &input.schema(),
                                )?)
                            })
                            .collect::<Result<Vec<_>, Self::Error>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(ExpandExec::try_new(schema, projections, input)?))
            }
        }
    }
}

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

impl From<&protobuf::ScalarFunction> for BuiltinScalarFunction {
    fn from(f: &protobuf::ScalarFunction) -> BuiltinScalarFunction {
        use protobuf::ScalarFunction;
        match f {
            ScalarFunction::Sqrt => Self::Sqrt,
            ScalarFunction::Sin => Self::Sin,
            ScalarFunction::Cos => Self::Cos,
            ScalarFunction::Tan => Self::Tan,
            ScalarFunction::Asin => Self::Asin,
            ScalarFunction::Acos => Self::Acos,
            ScalarFunction::Atan => Self::Atan,
            ScalarFunction::Exp => Self::Exp,
            ScalarFunction::Log => Self::Log,
            ScalarFunction::Ln => Self::Ln,
            ScalarFunction::Log10 => Self::Log10,
            ScalarFunction::Floor => Self::Floor,
            ScalarFunction::Ceil => Self::Ceil,
            ScalarFunction::Round => Self::Round,
            ScalarFunction::Trunc => Self::Trunc,
            ScalarFunction::Abs => Self::Abs,
            ScalarFunction::OctetLength => Self::OctetLength,
            ScalarFunction::Concat => Self::Concat,
            ScalarFunction::Lower => Self::Lower,
            ScalarFunction::Upper => Self::Upper,
            ScalarFunction::Trim => Self::Trim,
            ScalarFunction::Ltrim => Self::Ltrim,
            ScalarFunction::Rtrim => Self::Rtrim,
            ScalarFunction::ToTimestamp => Self::ToTimestamp,
            ScalarFunction::Array => Self::MakeArray,
            ScalarFunction::NullIf => Self::NullIf,
            ScalarFunction::DatePart => Self::DatePart,
            ScalarFunction::DateTrunc => Self::DateTrunc,
            ScalarFunction::Md5 => Self::MD5,
            ScalarFunction::Sha224 => Self::SHA224,
            ScalarFunction::Sha256 => Self::SHA256,
            ScalarFunction::Sha384 => Self::SHA384,
            ScalarFunction::Sha512 => Self::SHA512,
            ScalarFunction::Digest => Self::Digest,
            ScalarFunction::ToTimestampMillis => Self::ToTimestampMillis,
            ScalarFunction::Log2 => Self::Log2,
            ScalarFunction::Signum => Self::Signum,
            ScalarFunction::Ascii => Self::Ascii,
            ScalarFunction::BitLength => Self::BitLength,
            ScalarFunction::Btrim => Self::Btrim,
            ScalarFunction::CharacterLength => Self::CharacterLength,
            ScalarFunction::Chr => Self::Chr,
            ScalarFunction::ConcatWithSeparator => Self::ConcatWithSeparator,
            ScalarFunction::InitCap => Self::InitCap,
            ScalarFunction::Left => Self::Left,
            ScalarFunction::Lpad => Self::Lpad,
            ScalarFunction::Random => Self::Random,
            ScalarFunction::RegexpReplace => Self::RegexpReplace,
            ScalarFunction::Repeat => Self::Repeat,
            ScalarFunction::Replace => Self::Replace,
            ScalarFunction::Reverse => Self::Reverse,
            ScalarFunction::Right => Self::Right,
            ScalarFunction::Rpad => Self::Rpad,
            ScalarFunction::SplitPart => Self::SplitPart,
            ScalarFunction::StartsWith => Self::StartsWith,
            ScalarFunction::Strpos => Self::Strpos,
            ScalarFunction::Substr => Self::Substr,
            ScalarFunction::ToHex => Self::ToHex,
            ScalarFunction::ToTimestampMicros => Self::ToTimestampMicros,
            ScalarFunction::ToTimestampSeconds => Self::ToTimestampSeconds,
            ScalarFunction::Now => Self::Now,
            ScalarFunction::Translate => Self::Translate,
            ScalarFunction::RegexpMatch => Self::RegexpMatch,
            ScalarFunction::Coalesce => Self::Coalesce,
            ScalarFunction::SparkExtFunctions => {
                unreachable!()
            }
        }
    }
}

fn try_parse_physical_expr(
    expr: &protobuf::PhysicalExprNode,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, PlanSerDeError> {
    let expr_type = expr
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        ExprType::Column(c) => {
            let pcol: Column = c.into();
            Arc::new(pcol)
        }
        ExprType::Literal(scalar) => {
            Arc::new(Literal::new(convert_required!(scalar.value)?))
        }
        ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
            try_parse_physical_expr_box_required(&binary_expr.l.clone(), input_schema)?,
            from_proto_binary_op(&binary_expr.op)?,
            try_parse_physical_expr_box_required(&binary_expr.r.clone(), input_schema)?,
        )),
        ExprType::AggExpr(_) => {
            return Err(PlanSerDeError::General(
                "Cannot convert aggregate expr node to physical expression".to_owned(),
            ));
        }
        ExprType::Sort(_) => {
            return Err(PlanSerDeError::General(
                "Cannot convert sort expr node to physical expression".to_owned(),
            ));
        }
        ExprType::IsNullExpr(e) => Arc::new(IsNullExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
        )),
        ExprType::IsNotNullExpr(e) => Arc::new(IsNotNullExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
        )),
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
        )),
        ExprType::Negative(e) => Arc::new(NegativeExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
        )),
        ExprType::InList(e) => Arc::new(InListExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
            e.list
                .iter()
                .map(|x| try_parse_physical_expr(x, input_schema))
                .collect::<Result<Vec<_>, _>>()?,
            e.negated,
            input_schema,
        )),
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| try_parse_physical_expr(e.as_ref(), input_schema))
                .transpose()?,
            e.when_then_expr
                .iter()
                .map(|e| {
                    Ok((
                        try_parse_physical_expr_required(&e.when_expr, input_schema)?,
                        try_parse_physical_expr_required(&e.then_expr, input_schema)?,
                    ))
                })
                .collect::<Result<Vec<_>, PlanSerDeError>>()?,
            e.else_expr
                .as_ref()
                .map(|e| try_parse_physical_expr(e.as_ref(), input_schema))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            try_parse_physical_expr_box_required(&e.expr, input_schema)?,
            convert_required!(e.arrow_type)?,
            DEFAULT_DATAFUSION_CAST_OPTIONS,
        )),
        ExprType::TryCast(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            let cast_type = convert_required!(e.arrow_type)?;
            Arc::new(TryCastExpr::new(expr, cast_type))
        }
        ExprType::ScalarFunction(e) => {
            let scalar_function =
                protobuf::ScalarFunction::from_i32(e.fun).ok_or_else(|| {
                    proto_error(
                        format!("Received an unknown scalar function: {}", e.fun,),
                    )
                })?;

            let args = e
                .args
                .iter()
                .map(|x| try_parse_physical_expr(x, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            let execution_props = ExecutionProps::new();
            let fun_expr =
                if scalar_function == protobuf::ScalarFunction::SparkExtFunctions {
                    datafusion_ext_functions::create_spark_ext_function(&e.name)?
                } else {
                    functions::create_physical_fun(
                        &(&scalar_function).into(),
                        &execution_props,
                    )?
                };

            Arc::new(ScalarFunctionExpr::new(
                &e.name,
                fun_expr,
                args,
                &convert_required!(e.return_type)?,
            ))
        }
        ExprType::SparkExpressionWrapperExpr(e) => {
            Arc::new(SparkExpressionWrapperExpr::try_new(
                e.serialized.clone(),
                convert_required!(e.return_type)?,
                e.return_nullable,
                e.params
                    .iter()
                    .map(|x| try_parse_physical_expr(x, input_schema))
                    .collect::<Result<Vec<_>, _>>()?,
                input_schema.clone(),
            )?)
        }
        ExprType::GetIndexedFieldExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            let key = convert_required!(e.key)?;
            match expr.data_type(input_schema)? {
                DataType::FixedSizeList(_, _) => {
                    Arc::new(FixedSizeListGetIndexedFieldExpr::new(expr, key))
                }
                _ => Arc::new(GetIndexedFieldExpr::new(expr, key)),
            }
        }
        ExprType::StringStartsWithExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            Arc::new(StringStartsWithExpr::new(expr, e.prefix.clone()))
        }
        ExprType::StringEndsWithExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            Arc::new(StringEndsWithExpr::new(expr, e.suffix.clone()))
        }
        ExprType::StringContainsExpr(e) => {
            let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
            Arc::new(StringContainsExpr::new(expr, e.infix.clone()))
        }
        ExprType::SparkLogicalExpr(e) => {
            let arg1 = try_parse_physical_expr_box_required(&e.arg1, input_schema)?;
            let arg2 = try_parse_physical_expr_box_required(&e.arg2, input_schema)?;
            let op = match e.op.as_str() {
                "And" => SparkLogicalOp::And,
                "Or" => SparkLogicalOp::Or,
                _ => panic!("SparkLogicalExpr.op must be one of [And, Or]"),
            };
            Arc::new(SparkLogicalExpr::new(arg1, arg2, op))
        }
        ExprType::IifExpr(e) => {
            let condition =
                try_parse_physical_expr_box_required(&e.condition, input_schema)?;
            let truthy = try_parse_physical_expr_box_required(&e.truthy, input_schema)?;
            let falsy = try_parse_physical_expr_box_required(&e.falsy, input_schema)?;
            let data_type = convert_required!(e.data_type)?;
            Arc::new(IIfExpr::new(condition, truthy, falsy, data_type))
        }
    };

    Ok(pexpr)
}

fn try_parse_physical_expr_required(
    proto: &Option<protobuf::PhysicalExprNode>,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, PlanSerDeError> {
    if let Some(field) = proto.as_ref() {
        try_parse_physical_expr(field, input_schema)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

fn try_parse_physical_expr_box_required(
    proto: &Option<Box<protobuf::PhysicalExprNode>>,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, PlanSerDeError> {
    if let Some(field) = proto.as_ref() {
        try_parse_physical_expr(field, input_schema)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

pub fn parse_protobuf_hash_partitioning(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
) -> Result<Option<Partitioning>, PlanSerDeError> {
    match partitioning {
        Some(hash_part) => {
            let expr = hash_part
                .hash_expr
                .iter()
                .map(|e| {
                    try_parse_physical_expr(e, &input.schema())
                        .and_then(|e| Ok(bind(e, &input.schema())?))
                })
                .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = PlanSerDeError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            object_meta: ObjectMeta {
                location: val.path.clone(),
                size: val.size as usize,
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
            extensions: None,
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = PlanSerDeError;

    fn try_from(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for Vec<PartitionedFile> {
    type Error = PlanSerDeError;

    fn try_from(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        val.files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, _>>()
    }
}

impl From<&protobuf::ColumnStats> for ColumnStatistics {
    fn from(cs: &protobuf::ColumnStats) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Some(cs.null_count as usize),
            max_value: cs.max_value.as_ref().map(|m| m.try_into().unwrap()),
            min_value: cs.min_value.as_ref().map(|m| m.try_into().unwrap()),
            distinct_count: Some(cs.distinct_count as usize),
        }
    }
}

impl TryInto<Statistics> for &protobuf::Statistics {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<Statistics, Self::Error> {
        let column_statistics = self
            .column_stats
            .iter()
            .map(|s| s.into())
            .collect::<Vec<_>>();
        Ok(Statistics {
            num_rows: Some(self.num_rows as usize),
            total_byte_size: Some(self.total_byte_size as usize),
            // No column statistic (None) is encoded with empty array
            column_statistics: if column_statistics.is_empty() {
                None
            } else {
                Some(column_statistics)
            },
            is_exact: self.is_exact,
        })
    }
}

impl TryInto<FileScanConfig> for &protobuf::FileScanExecConf {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<FileScanConfig, Self::Error> {
        let schema = Arc::new(convert_required!(self.schema)?);
        let projection = self
            .projection
            .iter()
            .map(|i| *i as usize)
            .collect::<Vec<_>>();
        let projection = if projection.is_empty() {
            None
        } else {
            Some(projection)
        };
        let statistics = convert_required!(self.statistics)?;
        let partition_schema = Arc::new(convert_required!(self.partition_schema)?);
        Ok(FileScanConfig {
            file_schema: schema,
            file_groups: self
                .file_groups
                .iter()
                .map(|f| f.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            statistics,
            projection,
            limit: self.limit.as_ref().map(|sl| sl.limit as usize),
            table_partition_cols: self.table_partition_cols.clone(),
            partition_schema,
        })
    }
}
