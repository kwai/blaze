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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::error::{FromOptionalField, PlanSerDeError};
use crate::protobuf::physical_expr_node::ExprType;
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::protobuf::repartition_exec_node::PartitionMethod;
use crate::{convert_box_required, convert_required, into_required, protobuf, Schema};
use crate::{from_proto_binary_op, proto_error, str_to_byte};
use chrono::{TimeZone, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datafusion_data_access::{FileMeta, SizedFile};
use datafusion::datasource::{FileRange, PartitionedFile};
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_plan;
use datafusion::logical_plan::window_frames::WindowFrame;
use datafusion::logical_plan::*;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::file_format::{
    AvroExec, CsvExec, FileScanConfig, ParquetExec,
};
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::PartitionMode;
use datafusion::physical_plan::sorts::sort::{SortExec, SortOptions};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::window_functions::WindowFunction;
use datafusion::physical_plan::windows::{create_window_expr, WindowAggExec};
use datafusion::physical_plan::{
    coalesce_batches::CoalesceBatchesExec,
    cross_join::CrossJoinExec,
    empty::EmptyExec,
    expressions::{
        BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
        Literal, NegativeExpr, NotExpr, PhysicalSortExpr, TryCastExpr,
        DEFAULT_DATAFUSION_CAST_OPTIONS,
    },
    filter::FilterExec,
    functions::{self, BuiltinScalarFunction, ScalarFunctionExpr},
    hash_join::HashJoinExec,
    limit::{GlobalLimitExec, LocalLimitExec},
    projection::ProjectionExec,
    repartition::RepartitionExec,
    Partitioning,
};
use datafusion::physical_plan::{
    AggregateExpr, ColumnStatistics, ExecutionPlan, PhysicalExpr, Statistics, WindowExpr,
};
use datafusion::scalar::ScalarValue;
use datafusion_ext::global_object_store_registry;
use datafusion_ext::shuffle_reader_exec::ShuffleReaderExec;
use datafusion_ext::shuffle_writer_exec::ShuffleWriterExec;

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
    } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        let binary_expr = Arc::new(BinaryExpr::new(
            bind(expr.left().clone(), input_schema)?,
            *expr.op(),
            bind(expr.right().clone(), input_schema)?,
        ));
        Ok(binary_expr)
    } else if let Some(expr) = expr.downcast_ref::<CaseExpr>() {
        let case_expr = Arc::new(CaseExpr::try_new(
            expr.expr()
                .as_ref()
                .map(|exp| bind(exp.clone(), input_schema))
                .transpose()?,
            expr.when_then_expr()
                .iter()
                .map(|(when_expr, then_expr)| {
                    (
                        bind(when_expr.clone(), input_schema).unwrap(),
                        bind(then_expr.clone(), input_schema).unwrap(),
                    )
                })
                .collect::<Vec<_>>()
                .as_slice(),
            expr.else_expr()
                .as_ref()
                .map(|exp| bind((**exp).clone(), input_schema))
                .transpose()?,
        )?);
        Ok(case_expr)
    } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
        let not_expr = Arc::new(NotExpr::new(bind(expr.arg().clone(), input_schema)?));
        Ok(not_expr)
    } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
        let is_null = Arc::new(IsNullExpr::new(bind(expr.arg().clone(), input_schema)?));
        Ok(is_null)
    } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
        let is_not_null =
            Arc::new(IsNotNullExpr::new(bind(expr.arg().clone(), input_schema)?));
        Ok(is_not_null)
    } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
        let in_list = Arc::new(InListExpr::new(
            bind(expr.expr().clone(), input_schema)?,
            expr.list()
                .iter()
                .map(|a| bind(a.clone(), input_schema))
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            expr.negated(),
        ));
        Ok(in_list)
    } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
        let neg = Arc::new(NegativeExpr::new(bind(expr.arg().clone(), input_schema)?));
        Ok(neg)
    } else if expr.downcast_ref::<Literal>().is_some() {
        Ok(expr_in)
    } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        let cast = Arc::new(CastExpr::new(
            bind(cast.expr().clone(), input_schema)?,
            cast.cast_type().clone(),
            DEFAULT_DATAFUSION_CAST_OPTIONS,
        ));
        Ok(cast)
    } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        let try_cast = Arc::new(TryCastExpr::new(
            bind(cast.expr().clone(), input_schema)?,
            cast.cast_type().clone(),
        ));
        Ok(try_cast)
    } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let sfe = Arc::new(ScalarFunctionExpr::new(
            expr.name(),
            expr.fun().clone(),
            expr.args()
                .iter()
                .map(|e| bind(e.clone(), input_schema))
                .collect::<Result<Vec<_>, _>>()?,
            expr.return_type(),
        ));
        Ok(sfe)
    } else {
        unimplemented!("Expression binding not implemented yet")
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
                        Ok((bind(expr.try_into()?, &input.schema())?, name.to_string()))
                    })
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, Self::Error>>(
                    )?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicate = filter
                    .expr
                    .as_ref()
                    .ok_or_else(|| {
                        PlanSerDeError::General(
                            "filter (FilterExecNode) in PhysicalPlanNode is missing."
                                .to_owned(),
                        )
                    })?
                    .try_into()?;
                Ok(Arc::new(FilterExec::try_new(
                    bind(predicate, &input.schema())?,
                    input,
                )?))
            }
            PhysicalPlanType::CsvScan(scan) => Ok(Arc::new(CsvExec::new(
                scan.base_conf.as_ref().unwrap().try_into()?,
                scan.has_header,
                str_to_byte(&scan.delimiter)?,
            ))),
            PhysicalPlanType::ParquetScan(scan) => {
                let predicate = scan
                    .pruning_predicate
                    .as_ref()
                    .map(|expr| expr.try_into())
                    .transpose()?;
                Ok(Arc::new(ParquetExec::new(
                    scan.base_conf.as_ref().unwrap().try_into()?,
                    predicate,
                )))
            }
            PhysicalPlanType::AvroScan(scan) => Ok(Arc::new(AvroExec::new(
                scan.base_conf.as_ref().unwrap().try_into()?,
            ))),
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(coalesce_batches.input)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(merge.input)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(repart.input)?;
                let schema = input.schema();
                match repart.partition_method {
                    Some(PartitionMethod::Hash(ref hash_part)) => {
                        let expr = hash_part
                            .hash_expr
                            .iter()
                            .map(|e| bind(e.try_into().unwrap(), &schema))
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(
                                expr,
                                hash_part.partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::RoundRobin(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::RoundRobinBatch(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::Unknown(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::UnknownPartitioning(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    _ => Err(PlanSerDeError::General(
                        "Invalid partitioning scheme".to_owned(),
                    )),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(GlobalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(limit.input)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(window_agg.input)?;
                let input_schema = window_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        PlanSerDeError::General(
                            "input_schema in WindowAggrNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let physical_schema: SchemaRef =
                    SchemaRef::new((&input_schema).try_into()?);

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = window_agg
                    .window_expr
                    .iter()
                    .zip(window_agg.window_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty window physical expression")
                        })?;

                        match expr_type {
                            ExprType::WindowExpr(window_node) => {
                                let window_node_expr = bind(
                                    convert_box_required!(window_node.expr)?,
                                    &input.schema(),
                                )?;
                                Ok(create_window_expr(
                                    &convert_required!(window_node.window_function)?,
                                    name.to_owned(),
                                    &[window_node_expr],
                                    &[],
                                    &[],
                                    Some(WindowFrame::default()),
                                    &physical_schema,
                                )?)
                            }
                            _ => Err(PlanSerDeError::General(
                                "Invalid expression for WindowAggrExec".to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(WindowAggExec::try_new(
                    physical_window_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
            }
            PhysicalPlanType::HashAggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    convert_box_required!(hash_agg.input)?;
                let mode = protobuf::AggregateMode::from_i32(hash_agg.mode).ok_or_else(|| {
                    proto_error(format!(
                        "Received a HashAggregateNode message with unknown AggregateMode {}",
                        hash_agg.mode
                    ))
                })?;
                let agg_mode: AggregateMode = match mode {
                    protobuf::AggregateMode::Partial => AggregateMode::Partial,
                    protobuf::AggregateMode::Final => AggregateMode::Final,
                    protobuf::AggregateMode::FinalPartitioned => {
                        AggregateMode::FinalPartitioned
                    }
                };
                let group = hash_agg
                    .group_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        expr.try_into().and_then(|expr: Arc<dyn PhysicalExpr>| {
                            bind(expr, &input.schema())
                                .map(|expr| (expr, name.to_string()))
                                .map_err(PlanSerDeError::DataFusionError)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let input_schema = hash_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        PlanSerDeError::General(
                            "input_schema in HashAggregateNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let physical_schema: SchemaRef =
                    SchemaRef::new((&input_schema).try_into()?);

                let physical_aggr_expr: Vec<Arc<dyn AggregateExpr>> = hash_agg
                    .aggr_expr
                    .iter()
                    .zip(hash_agg.aggr_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty aggregate physical expression")
                        })?;

                        match expr_type {
                            ExprType::AggregateExpr(agg_node) => {
                                let aggr_function =
                                    protobuf::AggregateFunction::from_i32(
                                        agg_node.aggr_function,
                                    )
                                    .ok_or_else(
                                        || {
                                            proto_error(format!(
                                            "Received an unknown aggregate function: {}",
                                            agg_node.aggr_function
                                        ))
                                        },
                                    )?;
                                let agg_expr = bind(
                                    convert_box_required!(agg_node.expr)?,
                                    &input.schema(),
                                )?;
                                Ok(create_aggregate_expr(
                                    &aggr_function.into(),
                                    false,
                                    &[agg_expr],
                                    &physical_schema,
                                    name.to_string(),
                                )?)
                            }
                            _ => Err(PlanSerDeError::General(
                                "Invalid aggregate  expression for HashAggregateExec"
                                    .to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(HashAggregateExec::try_new(
                    agg_mode,
                    group,
                    physical_aggr_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
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
                Ok(Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    on,
                    &join_type.into(),
                    partition_mode,
                    &hashjoin.null_equals_null,
                )?))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(crossjoin.left)?;
                let right: Arc<dyn ExecutionPlan> =
                    convert_box_required!(crossjoin.right)?;
                Ok(Arc::new(CrossJoinExec::try_new(left, right)?))
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
                    shuffle_writer.shuffle_id as usize,
                    shuffle_writer.map_id as usize,
                )?))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let schema = Arc::new(convert_required!(shuffle_reader.schema)?);
                let shuffle_reader = ShuffleReaderExec::new(
                    shuffle_reader.native_shuffle_id.clone(),
                    schema,
                );
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
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
                                expr: bind(expr.try_into()?, &input.schema()).unwrap(),
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
                Ok(Arc::new(SortExec::new_with_partitioning(
                    exprs, input, true,
                )))
            }
            PhysicalPlanType::Union(union) => {
                let inputs: Vec<Arc<dyn ExecutionPlan>> = union
                    .children
                    .iter()
                    .map(|i| i.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(UnionExec::new(inputs)))
            }
            PhysicalPlanType::Unresolved(_unresolved_shuffle) => {
                unreachable!()
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
            ScalarFunction::Array => Self::Array,
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
        }
    }
}

impl TryFrom<&protobuf::PhysicalExprNode> for Arc<dyn PhysicalExpr> {
    type Error = PlanSerDeError;

    fn try_from(expr: &protobuf::PhysicalExprNode) -> Result<Self, Self::Error> {
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
                convert_box_required!(&binary_expr.l)?,
                from_proto_binary_op(&binary_expr.op)?,
                convert_box_required!(&binary_expr.r)?,
            )),
            ExprType::AggregateExpr(_) => {
                return Err(PlanSerDeError::General(
                    "Cannot convert aggregate expr node to physical expression"
                        .to_owned(),
                ));
            }
            ExprType::WindowExpr(_) => {
                return Err(PlanSerDeError::General(
                    "Cannot convert window expr node to physical expression".to_owned(),
                ));
            }
            ExprType::Sort(_) => {
                return Err(PlanSerDeError::General(
                    "Cannot convert sort expr node to physical expression".to_owned(),
                ));
            }
            ExprType::IsNullExpr(e) => {
                Arc::new(IsNullExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::IsNotNullExpr(e) => {
                Arc::new(IsNotNullExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::NotExpr(e) => {
                Arc::new(NotExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::Negative(e) => {
                Arc::new(NegativeExpr::new(convert_box_required!(e.expr)?))
            }
            ExprType::InList(e) => Arc::new(InListExpr::new(
                convert_box_required!(e.expr)?,
                e.list
                    .iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                e.negated,
            )),
            ExprType::Case(e) => Arc::new(CaseExpr::try_new(
                e.expr.as_ref().map(|e| e.as_ref().try_into()).transpose()?,
                e.when_then_expr
                    .iter()
                    .map(|e| {
                        Ok((
                            convert_required!(e.when_expr)?,
                            convert_required!(e.then_expr)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, PlanSerDeError>>()?
                    .as_slice(),
                e.else_expr
                    .as_ref()
                    .map(|e| e.as_ref().try_into())
                    .transpose()?,
            )?),
            ExprType::Cast(e) => Arc::new(CastExpr::new(
                convert_box_required!(e.expr)?,
                convert_required!(e.arrow_type)?,
                DEFAULT_DATAFUSION_CAST_OPTIONS,
            )),
            ExprType::TryCast(e) => Arc::new(TryCastExpr::new(
                convert_box_required!(e.expr)?,
                convert_required!(e.arrow_type)?,
            )),
            ExprType::ScalarFunction(e) => {
                let scalar_function = protobuf::ScalarFunction::from_i32(e.fun)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown scalar function: {}",
                            e.fun,
                        ))
                    })?;

                let args = e
                    .args
                    .iter()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                let execution_props = ExecutionProps::new();
                let fun_expr = functions::create_physical_fun(
                    &(&scalar_function).into(),
                    &execution_props,
                )?;

                Arc::new(ScalarFunctionExpr::new(
                    &e.name,
                    fun_expr,
                    args,
                    &convert_required!(e.return_type)?,
                ))
            }
        };

        Ok(pexpr)
    }
}

impl TryFrom<&protobuf::physical_window_expr_node::WindowFunction> for WindowFunction {
    type Error = PlanSerDeError;

    fn try_from(
        expr: &protobuf::physical_window_expr_node::WindowFunction,
    ) -> Result<Self, Self::Error> {
        match expr {
            protobuf::physical_window_expr_node::WindowFunction::AggrFunction(n) => {
                let f = protobuf::AggregateFunction::from_i32(*n).ok_or_else(|| {
                    proto_error(format!(
                        "Received an unknown window aggregate function: {}",
                        n
                    ))
                })?;

                Ok(WindowFunction::AggregateFunction(f.into()))
            }
            protobuf::physical_window_expr_node::WindowFunction::BuiltInFunction(n) => {
                let f =
                    protobuf::BuiltInWindowFunction::from_i32(*n).ok_or_else(|| {
                        proto_error(format!(
                            "Received an unknown window builtin function: {}",
                            n
                        ))
                    })?;

                Ok(WindowFunction::BuiltInWindowFunction(f.into()))
            }
        }
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
                    e.try_into().and_then(|e| {
                        bind(e, &input.schema()).map_err(PlanSerDeError::DataFusionError)
                    })
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
            file_meta: FileMeta {
                sized_file: SizedFile {
                    path: val.path.clone(),
                    size: val.size,
                },
                last_modified: if val.last_modified_ns == 0 {
                    None
                } else {
                    Some(Utc.timestamp_nanos(val.last_modified_ns as i64))
                },
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
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

        Ok(FileScanConfig {
            // use datafusion_ext::global_object_store_registry to get object score
            // decide object store scheme using first input file
            object_store: global_object_store_registry()
                .get_by_uri(
                    self.file_groups
                        .get(0)
                        .and_then(|file_group| file_group.files.get(0))
                        .map(|file| file.path.as_ref())
                        .unwrap_or("default"),
                )?
                .0,
            file_schema: schema,
            file_groups: self
                .file_groups
                .iter()
                .map(|f| f.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            statistics,
            projection,
            limit: self.limit.as_ref().map(|sl| sl.limit as usize),
            table_partition_cols: vec![],
        })
    }
}

impl TryFrom<&protobuf::LogicalExprNode> for Expr {
    type Error = PlanSerDeError;

    fn try_from(expr: &protobuf::LogicalExprNode) -> Result<Self, Self::Error> {
        use crate::protobuf::logical_expr_node::ExprType;
        use crate::protobuf::ScalarFunction;

        let expr_type = expr
            .expr_type
            .as_ref()
            .ok_or_else(|| PlanSerDeError::required("expr_type"))?;

        match expr_type {
            ExprType::BinaryExpr(binary_expr) => Ok(Self::BinaryExpr {
                left: Box::new(binary_expr.l.as_deref().required("l")?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(binary_expr.r.as_deref().required("r")?),
            }),
            ExprType::Column(column) => Ok(Self::Column(column.into())),
            ExprType::Literal(literal) => {
                let scalar_value: ScalarValue = literal.try_into()?;
                Ok(Self::Literal(scalar_value))
            }
            ExprType::Alias(alias) => Ok(Self::Alias(
                Box::new(alias.expr.as_deref().required("expr")?),
                alias.alias.clone(),
            )),
            ExprType::IsNullExpr(is_null) => Ok(Self::IsNull(Box::new(
                is_null.expr.as_deref().required("expr")?,
            ))),
            ExprType::IsNotNullExpr(is_not_null) => Ok(Self::IsNotNull(Box::new(
                is_not_null.expr.as_deref().required("expr")?,
            ))),
            ExprType::NotExpr(not) => {
                Ok(Self::Not(Box::new(not.expr.as_deref().required("expr")?)))
            }
            ExprType::Between(between) => Ok(Self::Between {
                expr: Box::new(between.expr.as_deref().required("expr")?),
                negated: between.negated,
                low: Box::new(between.low.as_deref().required("low")?),
                high: Box::new(between.high.as_deref().required("high")?),
            }),
            ExprType::Case(case) => {
                let when_then_expr = case
                    .when_then_expr
                    .iter()
                    .map(|e| {
                        let when_expr = e.when_expr.as_ref().required("when_expr")?;
                        let then_expr = e.then_expr.as_ref().required("then_expr")?;
                        Ok((Box::new(when_expr), Box::new(then_expr)))
                    })
                    .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, PlanSerDeError>>()?;
                Ok(Self::Case {
                    expr: parse_optional_expr(&case.expr)?.map(Box::new),
                    when_then_expr,
                    else_expr: parse_optional_expr(&case.else_expr)?.map(Box::new),
                })
            }
            ExprType::Cast(cast) => {
                let expr = Box::new(cast.expr.as_deref().required("expr")?);
                let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
                Ok(Self::Cast { expr, data_type })
            }
            ExprType::TryCast(cast) => {
                let expr = Box::new(cast.expr.as_deref().required("expr")?);
                let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
                Ok(Self::TryCast { expr, data_type })
            }
            ExprType::Negative(negative) => Ok(Self::Negative(Box::new(
                negative.expr.as_deref().required("expr")?,
            ))),
            ExprType::InList(in_list) => Ok(Self::InList {
                expr: Box::new(in_list.expr.as_deref().required("expr")?),
                list: in_list
                    .list
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                negated: in_list.negated,
            }),
            ExprType::Wildcard(_) => Ok(Self::Wildcard),
            ExprType::ScalarFunction(expr) => {
                let scalar_function = protobuf::ScalarFunction::from_i32(expr.fun)
                    .ok_or_else(|| PlanSerDeError::unknown("ScalarFunction", expr.fun))?;
                let args = &expr.args;

                match scalar_function {
                    ScalarFunction::Asin => Ok(asin((&args[0]).try_into()?)),
                    ScalarFunction::Acos => Ok(acos((&args[0]).try_into()?)),
                    ScalarFunction::Array => Ok(array(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Sqrt => Ok(sqrt((&args[0]).try_into()?)),
                    ScalarFunction::Sin => Ok(sin((&args[0]).try_into()?)),
                    ScalarFunction::Cos => Ok(cos((&args[0]).try_into()?)),
                    ScalarFunction::Tan => Ok(tan((&args[0]).try_into()?)),
                    ScalarFunction::Atan => Ok(atan((&args[0]).try_into()?)),
                    ScalarFunction::Exp => Ok(exp((&args[0]).try_into()?)),
                    ScalarFunction::Log2 => Ok(log2((&args[0]).try_into()?)),
                    ScalarFunction::Ln => Ok(ln((&args[0]).try_into()?)),
                    ScalarFunction::Log10 => Ok(log10((&args[0]).try_into()?)),
                    ScalarFunction::Floor => Ok(floor((&args[0]).try_into()?)),
                    ScalarFunction::Ceil => Ok(ceil((&args[0]).try_into()?)),
                    ScalarFunction::Round => Ok(round((&args[0]).try_into()?)),
                    ScalarFunction::Trunc => Ok(trunc((&args[0]).try_into()?)),
                    ScalarFunction::Abs => Ok(abs((&args[0]).try_into()?)),
                    ScalarFunction::Signum => Ok(signum((&args[0]).try_into()?)),
                    ScalarFunction::OctetLength => {
                        Ok(octet_length((&args[0]).try_into()?))
                    }
                    ScalarFunction::Lower => Ok(lower((&args[0]).try_into()?)),
                    ScalarFunction::Upper => Ok(upper((&args[0]).try_into()?)),
                    ScalarFunction::Trim => Ok(trim((&args[0]).try_into()?)),
                    ScalarFunction::Ltrim => Ok(ltrim((&args[0]).try_into()?)),
                    ScalarFunction::Rtrim => Ok(rtrim((&args[0]).try_into()?)),
                    ScalarFunction::DatePart => {
                        Ok(date_part((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::DateTrunc => {
                        Ok(date_trunc((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Sha224 => Ok(sha224((&args[0]).try_into()?)),
                    ScalarFunction::Sha256 => Ok(sha256((&args[0]).try_into()?)),
                    ScalarFunction::Sha384 => Ok(sha384((&args[0]).try_into()?)),
                    ScalarFunction::Sha512 => Ok(sha512((&args[0]).try_into()?)),
                    ScalarFunction::Md5 => Ok(md5((&args[0]).try_into()?)),
                    ScalarFunction::NullIf => Ok(nullif((&args[0]).try_into()?)),
                    ScalarFunction::Digest => {
                        Ok(digest((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Ascii => Ok(ascii((&args[0]).try_into()?)),
                    ScalarFunction::BitLength => Ok((&args[0]).try_into()?),
                    ScalarFunction::CharacterLength => {
                        Ok(character_length((&args[0]).try_into()?))
                    }
                    ScalarFunction::Chr => Ok(chr((&args[0]).try_into()?)),
                    ScalarFunction::InitCap => Ok(ascii((&args[0]).try_into()?)),
                    ScalarFunction::Left => {
                        Ok(left((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Random => Ok(random()),
                    ScalarFunction::Repeat => {
                        Ok(repeat((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Replace => Ok(replace(
                        (&args[0]).try_into()?,
                        (&args[1]).try_into()?,
                        (&args[2]).try_into()?,
                    )),
                    ScalarFunction::Reverse => Ok(reverse((&args[0]).try_into()?)),
                    ScalarFunction::Right => {
                        Ok(right((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Concat => Ok(concat_expr(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::ConcatWithSeparator => Ok(concat_ws_expr(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Lpad => Ok(lpad(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Rpad => Ok(rpad(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::RegexpReplace => Ok(regexp_replace(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::RegexpMatch => Ok(regexp_match(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Btrim => Ok(btrim(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::SplitPart => Ok(split_part(
                        (&args[0]).try_into()?,
                        (&args[1]).try_into()?,
                        (&args[2]).try_into()?,
                    )),
                    ScalarFunction::StartsWith => {
                        Ok(starts_with((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Strpos => {
                        Ok(strpos((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Substr => {
                        Ok(substr((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::ToHex => Ok(to_hex((&args[0]).try_into()?)),
                    ScalarFunction::ToTimestampMillis => {
                        Ok(to_timestamp_millis((&args[0]).try_into()?))
                    }
                    ScalarFunction::ToTimestampMicros => {
                        Ok(to_timestamp_micros((&args[0]).try_into()?))
                    }
                    ScalarFunction::ToTimestampSeconds => {
                        Ok(to_timestamp_seconds((&args[0]).try_into()?))
                    }
                    ScalarFunction::Now => Ok(now_expr(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Translate => Ok(translate(
                        (&args[0]).try_into()?,
                        (&args[1]).try_into()?,
                        (&args[2]).try_into()?,
                    )),
                    _ => Err(proto_error(
                        "Protobuf deserialization error: Unsupported scalar function",
                    )),
                }
            }
        }
    }
}

fn parse_optional_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Option<Expr>, PlanSerDeError> {
    match p {
        Some(expr) => expr.as_ref().try_into().map(Some),
        None => Ok(None),
    }
}

impl From<protobuf::Column> for logical_plan::Column {
    fn from(c: protobuf::Column) -> Self {
        let protobuf::Column { relation, name } = c;

        Self {
            relation: relation.map(|r| r.relation),
            name,
        }
    }
}

impl From<&protobuf::Column> for logical_plan::Column {
    fn from(c: &protobuf::Column) -> Self {
        c.clone().into()
    }
}
