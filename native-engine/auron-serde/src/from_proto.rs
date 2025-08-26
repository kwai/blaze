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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::{
    any::Any,
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use arrow::{
    array::ArrayRef,
    compute::SortOptions,
    datatypes::{DataType, Field, FieldRef, SchemaRef},
    row::{RowConverter, SortField},
};
use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use datafusion::{
    common::{Result, ScalarValue, stats::Precision},
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::{FileRange, PartitionedFile},
        object_store::ObjectStoreUrl,
        physical_plan::{FileGroup, FileOpener, FileScanConfig, FileSource},
    },
    logical_expr::{Operator, ScalarUDF, Volatility},
    physical_expr::{
        PhysicalExprRef, ScalarFunctionExpr,
        expressions::{LikeExpr, SCAndExpr, SCOrExpr, in_list},
    },
    physical_plan::{
        ColumnStatistics, ExecutionPlan, PhysicalExpr, Statistics, expressions as phys_expr,
        expressions::{
            BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal,
            NegativeExpr, NotExpr, PhysicalSortExpr,
        },
        metrics::ExecutionPlanMetricsSet,
    },
    prelude::create_udf,
};
use datafusion_ext_exprs::{
    bloom_filter_might_contain::BloomFilterMightContainExpr, cast::TryCastExpr,
    get_indexed_field::GetIndexedFieldExpr, get_map_value::GetMapValueExpr,
    named_struct::NamedStructExpr, row_num::RowNumExpr,
    spark_scalar_subquery_wrapper::SparkScalarSubqueryWrapperExpr,
    spark_udf_wrapper::SparkUDFWrapperExpr, string_contains::StringContainsExpr,
    string_ends_with::StringEndsWithExpr, string_starts_with::StringStartsWithExpr,
};
use datafusion_ext_plans::{
    agg::{
        AggExecMode, AggExpr, AggFunction, AggMode, GroupingExpr,
        agg::{create_agg, create_udaf_agg},
    },
    agg_exec::AggExec,
    broadcast_join_build_hash_map_exec::BroadcastJoinBuildHashMapExec,
    broadcast_join_exec::BroadcastJoinExec,
    debug_exec::DebugExec,
    empty_partitions_exec::EmptyPartitionsExec,
    expand_exec::ExpandExec,
    ffi_reader_exec::FFIReaderExec,
    filter_exec::FilterExec,
    generate::{create_generator, create_udtf_generator},
    generate_exec::GenerateExec,
    ipc_reader_exec::IpcReaderExec,
    ipc_writer_exec::IpcWriterExec,
    limit_exec::LimitExec,
    orc_exec::OrcExec,
    parquet_exec::ParquetExec,
    parquet_sink_exec::ParquetSinkExec,
    project_exec::ProjectExec,
    rename_columns_exec::RenameColumnsExec,
    rss_shuffle_writer_exec::RssShuffleWriterExec,
    shuffle::Partitioning,
    shuffle_writer_exec::ShuffleWriterExec,
    sort_exec::SortExec,
    sort_merge_join_exec::SortMergeJoinExec,
    union_exec::{UnionExec, UnionInput},
    window::{WindowExpr, WindowFunction, WindowRankType},
    window_exec::WindowExec,
};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use parking_lot::Mutex as SyncMutex;

use crate::{
    Schema, convert_box_required, convert_required,
    error::PlanSerDeError,
    from_proto_binary_op, proto_error, protobuf,
    protobuf::{
        GenerateFunction, PhysicalRepartition, SortExecNode, physical_expr_node::ExprType,
        physical_plan_node::PhysicalPlanType, physical_repartition::RepartitionType,
    },
};

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
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(projection.input)?;
                let input_schema = input.schema();
                let data_types: Vec<DataType> = projection
                    .data_type
                    .iter()
                    .map(|data_type| data_type.try_into())
                    .collect::<Result<Vec<_>, Self::Error>>()?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .zip(data_types)
                    .map(|((expr, name), data_type)| {
                        let physical_expr = try_parse_physical_expr(expr, &input_schema)?;
                        let casted_expr = if physical_expr.data_type(&input_schema)? == data_type {
                            physical_expr
                        } else {
                            Arc::new(TryCastExpr::new(physical_expr, data_type))
                        };
                        Ok((casted_expr, name.to_string()))
                    })
                    .collect::<Result<Vec<(PhysicalExprRef, String)>, Self::Error>>()?;

                Ok(Arc::new(ProjectExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(filter.input)?;
                let predicates = filter
                    .expr
                    .iter()
                    .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                    .collect::<Result<_, Self::Error>>()?;
                Ok(Arc::new(FilterExec::try_new(predicates, input)?))
            }
            PhysicalPlanType::ParquetScan(scan) => {
                let conf: FileScanConfig = scan.base_conf.as_ref().unwrap().try_into()?;
                let predicate = scan
                    .pruning_predicates
                    .iter()
                    .filter_map(|predicate| {
                        try_parse_physical_expr(predicate, &conf.file_schema).ok()
                    })
                    .fold(phys_expr::lit(true), |a, b| {
                        Arc::new(BinaryExpr::new(a, Operator::And, b))
                    });
                Ok(Arc::new(ParquetExec::new(
                    conf,
                    scan.fs_resource_id.clone(),
                    Some(predicate),
                )))
            }
            PhysicalPlanType::OrcScan(scan) => {
                let conf: FileScanConfig = scan.base_conf.as_ref().unwrap().try_into()?;
                let predicate = scan
                    .pruning_predicates
                    .iter()
                    .filter_map(|predicate| {
                        try_parse_physical_expr(predicate, &conf.file_schema).ok()
                    })
                    .fold(phys_expr::lit(true), |a, b| {
                        Arc::new(BinaryExpr::new(a, Operator::And, b))
                    });
                Ok(Arc::new(OrcExec::new(
                    conf,
                    scan.fs_resource_id.clone(),
                    Some(predicate),
                )))
            }
            PhysicalPlanType::HashJoin(hash_join) => {
                let schema = Arc::new(convert_required!(hash_join.schema)?);
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(hash_join.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(hash_join.right)?;
                let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = hash_join
                    .on
                    .iter()
                    .map(|col| {
                        let left_key =
                            try_parse_physical_expr(&col.left.as_ref().unwrap(), &left.schema())?;
                        let right_key =
                            try_parse_physical_expr(&col.right.as_ref().unwrap(), &right.schema())?;
                        Ok((left_key, right_key))
                    })
                    .collect::<Result<_, Self::Error>>()?;

                let join_type =
                    protobuf::JoinType::try_from(hash_join.join_type).expect("invalid JoinType");

                let build_side =
                    protobuf::JoinSide::try_from(hash_join.build_side).expect("invalid BuildSide");

                Ok(Arc::new(BroadcastJoinExec::try_new(
                    schema,
                    left,
                    right,
                    on,
                    join_type
                        .try_into()
                        .map_err(|_| proto_error("invalid JoinType"))?,
                    build_side
                        .try_into()
                        .map_err(|_| proto_error("invalid BuildSide"))?,
                    false,
                    None,
                )?))
            }
            PhysicalPlanType::SortMergeJoin(sort_merge_join) => {
                let schema = Arc::new(convert_required!(sort_merge_join.schema)?);
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(sort_merge_join.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(sort_merge_join.right)?;
                let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = sort_merge_join
                    .on
                    .iter()
                    .map(|col| {
                        let left_key =
                            try_parse_physical_expr(&col.left.as_ref().unwrap(), &left.schema())?;
                        let right_key =
                            try_parse_physical_expr(&col.right.as_ref().unwrap(), &right.schema())?;
                        Ok((left_key, right_key))
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

                let join_type = protobuf::JoinType::try_from(sort_merge_join.join_type)
                    .expect("invalid JoinType");

                Ok(Arc::new(SortMergeJoinExec::try_new(
                    schema,
                    left,
                    right,
                    on,
                    join_type
                        .try_into()
                        .map_err(|_| proto_error("invalid JoinType"))?,
                    sort_options,
                )?))
            }
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(shuffle_writer.input)?;

                let output_partitioning = parse_protobuf_partitioning(
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

                let output_partitioning = parse_protobuf_partitioning(
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
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(ipc_writer.input)?;

                Ok(Arc::new(IpcWriterExec::new(
                    input,
                    ipc_writer.ipc_consumer_resource_id.clone(),
                )))
            }
            PhysicalPlanType::IpcReader(ipc_reader) => {
                let schema = Arc::new(convert_required!(ipc_reader.schema)?);
                Ok(Arc::new(IpcReaderExec::new(
                    ipc_reader.num_partitions as usize,
                    ipc_reader.ipc_provider_resource_id.clone(),
                    schema,
                )))
            }
            PhysicalPlanType::Debug(debug) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(debug.input)?;
                Ok(Arc::new(DebugExec::new(input, debug.debug_id.clone())))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(sort.input)?;
                let exprs = try_parse_physical_sort_expr(&input, sort).unwrap_or_else(|e| {
                    panic!("Failed to parse physical sort expressions: {}", e);
                });

                // always preserve partitioning
                Ok(Arc::new(SortExec::new(
                    input,
                    exprs,
                    sort.fetch_limit.as_ref().map(|limit| limit.limit as usize),
                )))
            }
            PhysicalPlanType::BroadcastJoinBuildHashMap(bhm) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(bhm.input)?;
                let keys = bhm
                    .keys
                    .iter()
                    .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                    .collect::<Result<Vec<PhysicalExprRef>, Self::Error>>()?;
                Ok(Arc::new(BroadcastJoinBuildHashMapExec::new(input, keys)))
            }
            PhysicalPlanType::BroadcastJoin(broadcast_join) => {
                let schema = Arc::new(convert_required!(broadcast_join.schema)?);
                let left: Arc<dyn ExecutionPlan> = convert_box_required!(broadcast_join.left)?;
                let right: Arc<dyn ExecutionPlan> = convert_box_required!(broadcast_join.right)?;
                let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = broadcast_join
                    .on
                    .iter()
                    .map(|col| {
                        let left_key =
                            try_parse_physical_expr(&col.left.as_ref().unwrap(), &left.schema())?;
                        let right_key =
                            try_parse_physical_expr(&col.right.as_ref().unwrap(), &right.schema())?;
                        Ok((left_key, right_key))
                    })
                    .collect::<Result<_, Self::Error>>()?;

                let join_type = protobuf::JoinType::try_from(broadcast_join.join_type)
                    .expect("invalid JoinType");

                let broadcast_side = protobuf::JoinSide::try_from(broadcast_join.broadcast_side)
                    .expect("invalid BroadcastSide");

                let cached_build_hash_map_id = broadcast_join.cached_build_hash_map_id.clone();

                Ok(Arc::new(BroadcastJoinExec::try_new(
                    schema,
                    left,
                    right,
                    on,
                    join_type
                        .try_into()
                        .map_err(|_| proto_error("invalid JoinType"))?,
                    broadcast_side
                        .try_into()
                        .map_err(|_| proto_error("invalid BroadcastSide"))?,
                    true,
                    Some(cached_build_hash_map_id),
                )?))
            }
            PhysicalPlanType::Union(union) => {
                let schema: SchemaRef = Arc::new(convert_required!(union.schema)?);
                let num_partitions = union.num_partitions as usize;
                let cur_partition = union.cur_partition as usize;
                let inputs = union
                    .input
                    .iter()
                    .map(|input| {
                        let input_exec = convert_required!(input.input)?;
                        Ok(UnionInput(input_exec, input.partition as usize))
                    })
                    .collect::<Result<Vec<_>, Self::Error>>()?;

                Ok(Arc::new(UnionExec::new(
                    inputs,
                    schema,
                    num_partitions,
                    cur_partition,
                )))
            }
            PhysicalPlanType::EmptyPartitions(empty_partitions) => {
                let schema = Arc::new(convert_required!(empty_partitions.schema)?);
                Ok(Arc::new(EmptyPartitionsExec::new(
                    schema,
                    empty_partitions.num_partitions as usize,
                )))
            }
            PhysicalPlanType::RenameColumns(rename_columns) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(rename_columns.input)?;
                Ok(Arc::new(RenameColumnsExec::try_new(
                    input,
                    rename_columns.renamed_column_names.clone(),
                )?))
            }
            PhysicalPlanType::Agg(agg) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(agg.input)?;
                let input_schema = input.schema();

                let exec_mode = match protobuf::AggExecMode::try_from(agg.exec_mode)
                    .expect("invalid AggExecMode")
                {
                    protobuf::AggExecMode::HashAgg => AggExecMode::HashAgg,
                    protobuf::AggExecMode::SortAgg => AggExecMode::SortAgg,
                };

                let agg_modes = agg
                    .mode
                    .iter()
                    .map(|&mode| {
                        match protobuf::AggMode::try_from(mode).expect("invalid AggMode") {
                            protobuf::AggMode::Partial => AggMode::Partial,
                            protobuf::AggMode::PartialMerge => AggMode::PartialMerge,
                            protobuf::AggMode::Final => AggMode::Final,
                        }
                    })
                    .collect::<Vec<_>>();

                let physical_groupings: Vec<GroupingExpr> = agg
                    .grouping_expr
                    .iter()
                    .zip(agg.grouping_expr_name.iter())
                    .map(|(expr, name)| {
                        try_parse_physical_expr(expr, &input_schema).and_then(|expr| {
                            Ok(GroupingExpr {
                                expr,
                                field_name: name.to_owned(),
                            })
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
                                    "Invalid aggregate expression for AggExec".to_string(),
                                ));
                            }
                        };

                        let agg_function = protobuf::AggFunction::try_from(agg_node.agg_function)
                            .expect("invalid AggFunction");
                        let agg_children_exprs = agg_node
                            .children
                            .iter()
                            .map(|expr| try_parse_physical_expr(expr, &input_schema))
                            .collect::<Result<Vec<_>, _>>()?;
                        let return_type = convert_required!(agg_node.return_type)?;

                        let agg = match AggFunction::from(agg_function) {
                            AggFunction::Udaf => {
                                let udaf = agg_node.udaf.as_ref().unwrap();
                                let serialized = udaf.serialized.clone();
                                create_udaf_agg(serialized, return_type, agg_children_exprs)?
                            }
                            _ => create_agg(
                                AggFunction::from(agg_function),
                                &agg_children_exprs,
                                &input_schema,
                                return_type,
                            )?,
                        };

                        Ok(AggExpr {
                            agg,
                            mode,
                            field_name: name.to_owned(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(AggExec::try_new(
                    exec_mode,
                    physical_groupings,
                    physical_aggs,
                    agg.supports_partial_skipping,
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
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(coalesce_batches.input)?;
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
                            .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                            .collect::<Result<Vec<_>, Self::Error>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(ExpandExec::try_new(schema, projections, input)?))
            }
            PhysicalPlanType::Window(window) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(window.input)?;
                let window_exprs = window
                    .window_expr
                    .iter()
                    .map(|w| {
                        let field: FieldRef = Arc::new(
                            w.field
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .try_into()?,
                        );

                        let children = w
                            .children
                            .iter()
                            .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                            .collect::<Result<Vec<_>, Self::Error>>()?;
                        let return_type = convert_required!(w.return_type)?;

                        let window_func = match w.func_type() {
                            protobuf::WindowFunctionType::Window => match w.window_func() {
                                protobuf::WindowFunction::RowNumber => {
                                    WindowFunction::RankLike(WindowRankType::RowNumber)
                                }
                                protobuf::WindowFunction::Rank => {
                                    WindowFunction::RankLike(WindowRankType::Rank)
                                }
                                protobuf::WindowFunction::DenseRank => {
                                    WindowFunction::RankLike(WindowRankType::DenseRank)
                                }
                            },
                            protobuf::WindowFunctionType::Agg => match w.agg_func() {
                                protobuf::AggFunction::Min => WindowFunction::Agg(AggFunction::Min),
                                protobuf::AggFunction::Max => WindowFunction::Agg(AggFunction::Max),
                                protobuf::AggFunction::Sum => WindowFunction::Agg(AggFunction::Sum),
                                protobuf::AggFunction::Avg => WindowFunction::Agg(AggFunction::Avg),
                                protobuf::AggFunction::Count => {
                                    WindowFunction::Agg(AggFunction::Count)
                                }
                                protobuf::AggFunction::CollectList => {
                                    WindowFunction::Agg(AggFunction::CollectList)
                                }
                                protobuf::AggFunction::CollectSet => {
                                    WindowFunction::Agg(AggFunction::CollectSet)
                                }
                                protobuf::AggFunction::First => {
                                    WindowFunction::Agg(AggFunction::First)
                                }
                                protobuf::AggFunction::FirstIgnoresNull => {
                                    WindowFunction::Agg(AggFunction::FirstIgnoresNull)
                                }
                                protobuf::AggFunction::BloomFilter => {
                                    WindowFunction::Agg(AggFunction::BloomFilter)
                                }
                                protobuf::AggFunction::BrickhouseCollect => {
                                    WindowFunction::Agg(AggFunction::BrickhouseCollect)
                                }
                                protobuf::AggFunction::BrickhouseCombineUnique => {
                                    WindowFunction::Agg(AggFunction::BrickhouseCombineUnique)
                                }
                                protobuf::AggFunction::Udaf => {
                                    WindowFunction::Agg(AggFunction::Udaf)
                                }
                            },
                        };
                        Ok::<_, Self::Error>(WindowExpr::new(
                            window_func,
                            children,
                            field,
                            return_type,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let partition_specs = window
                    .partition_spec
                    .iter()
                    .map(|expr| try_parse_physical_expr(expr, &input.schema()))
                    .collect::<Result<Vec<_>, Self::Error>>()?;

                let order_specs = window
                    .order_spec
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}",
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
                                expr: try_parse_physical_expr(expr, &input.schema())?,
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

                let group_limit = window.group_limit.as_ref().map(|gl| gl.k as usize);
                let output_window_cols = window.output_window_cols;

                Ok(Arc::new(WindowExec::try_new(
                    input,
                    window_exprs,
                    partition_specs,
                    order_specs,
                    group_limit,
                    output_window_cols,
                )?))
            }
            PhysicalPlanType::Generate(generate) => {
                let input: Arc<dyn ExecutionPlan> = convert_box_required!(generate.input)?;
                let input_schema = input.schema();
                let pb_generator = generate.generator.as_ref().expect("missing generator");
                let pb_generator_children = &pb_generator.child;
                let pb_generate_func = GenerateFunction::try_from(pb_generator.func)
                    .expect("unsupported generate function");

                let children = pb_generator_children
                    .iter()
                    .map(|expr| try_parse_physical_expr(expr, &input_schema))
                    .collect::<Result<Vec<_>, _>>()?;

                let generator = match pb_generate_func {
                    GenerateFunction::Explode => create_generator(
                        &input_schema,
                        datafusion_ext_plans::generate::GenerateFunc::Explode,
                        children,
                    )?,
                    GenerateFunction::PosExplode => create_generator(
                        &input_schema,
                        datafusion_ext_plans::generate::GenerateFunc::PosExplode,
                        children,
                    )?,
                    GenerateFunction::JsonTuple => create_generator(
                        &input_schema,
                        datafusion_ext_plans::generate::GenerateFunc::JsonTuple,
                        children,
                    )?,
                    GenerateFunction::Udtf => {
                        let udtf = pb_generator.udtf.as_ref().unwrap();
                        let serialized = udtf.serialized.clone();
                        let return_schema = Arc::new(convert_required!(udtf.return_schema)?);
                        create_udtf_generator(serialized, return_schema, children)?
                    }
                };
                let generator_output_schema = Arc::new(Schema::new(
                    generate
                        .generator_output
                        .iter()
                        .map(|field| Ok(Arc::new(field.try_into()?)))
                        .collect::<Result<Vec<FieldRef>, PlanSerDeError>>()?,
                ));

                let required_child_output_cols = generate
                    .required_child_output
                    .iter()
                    .map(|name| Ok(Column::new_with_schema(name, &input_schema)?))
                    .collect::<Result<_, PlanSerDeError>>()?;

                Ok(Arc::new(GenerateExec::try_new(
                    input,
                    generator,
                    required_child_output_cols,
                    generator_output_schema,
                    generate.outer,
                )?))
            }
            PhysicalPlanType::ParquetSink(parquet_sink) => {
                let mut props: Vec<(String, String)> = vec![];
                for prop in &parquet_sink.prop {
                    props.push((prop.key.clone(), prop.value.clone()));
                }
                Ok(Arc::new(ParquetSinkExec::new(
                    convert_box_required!(parquet_sink.input)?,
                    parquet_sink.fs_resource_id.clone(),
                    parquet_sink.num_dyn_parts as usize,
                    props,
                )))
            }
        }
    }
}

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

impl From<&protobuf::BoundReference> for Column {
    fn from(c: &protobuf::BoundReference) -> Column {
        Column::new("__bound_reference__", c.index as usize)
    }
}

impl From<protobuf::ScalarFunction> for Arc<ScalarUDF> {
    fn from(f: protobuf::ScalarFunction) -> Self {
        use datafusion::functions as f;
        use datafusion_spark::function as spark_fun;
        use protobuf::ScalarFunction;

        match f {
            ScalarFunction::Sqrt => f::math::sqrt(),
            ScalarFunction::Sin => f::math::sin(),
            ScalarFunction::Cos => f::math::cos(),
            ScalarFunction::Tan => f::math::tan(),
            ScalarFunction::Asin => f::math::asin(),
            ScalarFunction::Acos => f::math::acos(),
            ScalarFunction::Atan => f::math::atan(),
            ScalarFunction::Exp => f::math::exp(),
            ScalarFunction::Log => f::math::log(),
            ScalarFunction::Ln => f::math::ln(),
            ScalarFunction::Log10 => f::math::log10(),
            ScalarFunction::Floor => f::math::floor(),
            ScalarFunction::Ceil => f::math::ceil(),
            ScalarFunction::Round => f::math::round(),
            ScalarFunction::Trunc => f::math::trunc(),
            ScalarFunction::Abs => f::math::abs(),
            ScalarFunction::OctetLength => f::string::octet_length(),
            ScalarFunction::Concat => f::string::concat(),
            ScalarFunction::Lower => f::string::lower(),
            ScalarFunction::Upper => f::string::upper(),
            ScalarFunction::Trim => f::string::btrim(),
            ScalarFunction::Ltrim => f::string::ltrim(),
            ScalarFunction::Rtrim => f::string::rtrim(),
            ScalarFunction::ToTimestamp => f::datetime::to_timestamp(),
            ScalarFunction::NullIf => f::core::nullif(),
            ScalarFunction::DatePart => f::datetime::date_part(),
            ScalarFunction::DateTrunc => f::datetime::date_trunc(),
            ScalarFunction::Md5 => f::crypto::md5(),
            // ScalarFunction::Sha224 => f::crypto::sha224(),
            // ScalarFunction::Sha256 => f::crypto::sha256(),
            // ScalarFunction::Sha384 => f::crypto::sha384(),
            // ScalarFunction::Sha512 => f::crypto::sha512(),
            ScalarFunction::Digest => f::crypto::digest(),
            ScalarFunction::ToTimestampMillis => f::datetime::to_timestamp_millis(),
            ScalarFunction::Log2 => f::math::log2(),
            ScalarFunction::Signum => f::math::signum(),
            ScalarFunction::Ascii => f::string::ascii(),
            ScalarFunction::BitLength => f::string::bit_length(),
            ScalarFunction::Btrim => f::string::btrim(),
            ScalarFunction::CharacterLength => f::unicode::character_length(),
            ScalarFunction::Chr => f::string::chr(),
            ScalarFunction::ConcatWithSeparator => f::string::concat_ws(),
            ScalarFunction::InitCap => f::unicode::initcap(),
            ScalarFunction::Left => f::unicode::left(),
            ScalarFunction::Lpad => f::unicode::lpad(),
            ScalarFunction::Random => f::math::random(),
            ScalarFunction::RegexpReplace => f::regex::regexp_replace(),
            ScalarFunction::Repeat => f::string::repeat(),
            ScalarFunction::Replace => f::string::replace(),
            ScalarFunction::Reverse => f::unicode::reverse(),
            ScalarFunction::Right => f::unicode::right(),
            ScalarFunction::Rpad => f::unicode::rpad(),
            ScalarFunction::SplitPart => f::string::split_part(),
            ScalarFunction::StartsWith => f::string::starts_with(),
            ScalarFunction::Strpos => f::unicode::strpos(),
            ScalarFunction::Substr => f::unicode::substr(),
            // ScalarFunction::ToHex => f::string::to_hex(),
            ScalarFunction::ToTimestampMicros => f::datetime::to_timestamp_micros(),
            ScalarFunction::ToTimestampSeconds => f::datetime::to_timestamp_seconds(),
            ScalarFunction::Now => f::datetime::now(),
            ScalarFunction::Translate => f::unicode::translate(),
            ScalarFunction::RegexpMatch => f::regex::regexp_match(),
            ScalarFunction::Coalesce => f::core::coalesce(),

            // -- datafusion-spark functions
            // math functions
            ScalarFunction::Expm1 => spark_fun::math::expm1(),
            ScalarFunction::Factorial => spark_fun::math::factorial(),
            ScalarFunction::Hex => spark_fun::math::hex(),

            ScalarFunction::SparkExtFunctions => {
                unreachable!()
            }
        }
    }
}

fn try_parse_physical_expr(
    expr: &protobuf::PhysicalExprNode,
    input_schema: &SchemaRef,
) -> Result<PhysicalExprRef, PlanSerDeError> {
    let expr_type = expr
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: PhysicalExprRef =
        match expr_type {
            ExprType::Column(c) => Arc::new(Column::new(&c.name, input_schema.index_of(&c.name)?)),
            ExprType::Literal(scalar) => Arc::new(Literal::new(scalar.try_into()?)),
            ExprType::BoundReference(bound_reference) => {
                let pcol: Column = bound_reference.into();
                Arc::new(pcol)
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
            ExprType::NotExpr(e) => Arc::new(NotExpr::new(try_parse_physical_expr_box_required(
                &e.expr,
                input_schema,
            )?)),
            ExprType::Negative(e) => Arc::new(NegativeExpr::new(
                try_parse_physical_expr_box_required(&e.expr, input_schema)?,
            )),
            ExprType::InList(e) => {
                let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
                let dt = expr.data_type(input_schema)?;
                let list_exprs = e
                    .list
                    .iter()
                    .map(|x| -> Result<PhysicalExprRef, PlanSerDeError> {
                        let e = try_parse_physical_expr(x, input_schema)?;
                        if e.data_type(input_schema)? != dt {
                            return Ok(Arc::new(TryCastExpr::new(e, dt.clone())));
                        }
                        Ok(e)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                in_list(expr, list_exprs, &e.negated, &input_schema)?
            }
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
                None,
            )),
            ExprType::TryCast(e) => {
                let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
                let cast_type = convert_required!(e.arrow_type)?;
                Arc::new(TryCastExpr::new(expr, cast_type))
            }
            ExprType::ScalarFunction(e) => {
                let scalar_function =
                    protobuf::ScalarFunction::try_from(e.fun).expect("invalid ScalarFunction");
                let args = e
                    .args
                    .iter()
                    .map(|x| try_parse_physical_expr(x, input_schema))
                    .collect::<Result<Vec<_>, _>>()?;

                let scalar_udf = if scalar_function == protobuf::ScalarFunction::SparkExtFunctions {
                    let fun_name = &e.name;
                    let fun = datafusion_ext_functions::create_spark_ext_function(fun_name)?;
                    Arc::new(create_udf(
                        &format!("spark_ext_function_{}", fun_name),
                        args.iter()
                            .map(|e| e.data_type(input_schema))
                            .collect::<Result<Vec<_>, _>>()?,
                        convert_required!(e.return_type)?,
                        Volatility::Volatile,
                        fun,
                    ))
                } else {
                    let scalar_udf: Arc<ScalarUDF> = scalar_function.into();
                    scalar_udf
                };
                Arc::new(ScalarFunctionExpr::new(
                    scalar_udf.name(),
                    scalar_udf.clone(),
                    args,
                    Arc::new(Field::new(
                        "result",
                        convert_required!(e.return_type)?,
                        true,
                    )),
                ))
            }
            ExprType::SparkUdfWrapperExpr(e) => Arc::new(SparkUDFWrapperExpr::try_new(
                e.serialized.clone(),
                convert_required!(e.return_type)?,
                e.return_nullable,
                e.params
                    .iter()
                    .map(|x| try_parse_physical_expr(x, input_schema))
                    .collect::<Result<Vec<_>, _>>()?,
                e.expr_string.clone(),
            )?),
            ExprType::SparkScalarSubqueryWrapperExpr(e) => {
                Arc::new(SparkScalarSubqueryWrapperExpr::try_new(
                    e.serialized.clone(),
                    convert_required!(e.return_type)?,
                    e.return_nullable,
                )?)
            }
            ExprType::GetIndexedFieldExpr(e) => {
                let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
                let key = convert_required!(e.key)?;
                Arc::new(GetIndexedFieldExpr::new(expr, key))
            }
            ExprType::GetMapValueExpr(e) => {
                let expr = try_parse_physical_expr_box_required(&e.expr, input_schema)?;
                let key = convert_required!(e.key)?;
                Arc::new(GetMapValueExpr::new(expr, key))
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
            ExprType::RowNumExpr(_) => Arc::new(RowNumExpr::default()),
            ExprType::BloomFilterMightContainExpr(e) => Arc::new(BloomFilterMightContainExpr::new(
                e.uuid.clone(),
                try_parse_physical_expr_box_required(&e.bloom_filter_expr, input_schema)?,
                try_parse_physical_expr_box_required(&e.value_expr, input_schema)?,
            )),
            ExprType::ScAndExpr(e) => {
                let l = try_parse_physical_expr_box_required(&e.left, input_schema)?;
                let r = try_parse_physical_expr_box_required(&e.right, input_schema)?;
                Arc::new(SCAndExpr::new(l, r))
            }
            ExprType::ScOrExpr(e) => {
                let l = try_parse_physical_expr_box_required(&e.left, input_schema)?;
                let r = try_parse_physical_expr_box_required(&e.right, input_schema)?;
                Arc::new(SCOrExpr::new(l, r))
            }
            ExprType::LikeExpr(e) => Arc::new(LikeExpr::new(
                e.negated,
                e.case_insensitive,
                try_parse_physical_expr_box_required(&e.expr, input_schema)?,
                try_parse_physical_expr_box_required(&e.pattern, input_schema)?,
            )),

            ExprType::NamedStruct(e) => {
                let data_type = convert_required!(e.return_type)?;
                Arc::new(NamedStructExpr::try_new(
                    e.values
                        .iter()
                        .map(|x| try_parse_physical_expr(x, input_schema))
                        .collect::<Result<Vec<_>, _>>()?,
                    data_type,
                )?)
            }
        };

    Ok(pexpr)
}

fn try_parse_physical_expr_required(
    proto: &Option<protobuf::PhysicalExprNode>,
    input_schema: &SchemaRef,
) -> Result<PhysicalExprRef, PlanSerDeError> {
    if let Some(field) = proto.as_ref() {
        try_parse_physical_expr(field, input_schema)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

fn try_parse_physical_expr_box_required(
    proto: &Option<Box<protobuf::PhysicalExprNode>>,
    input_schema: &SchemaRef,
) -> Result<PhysicalExprRef, PlanSerDeError> {
    if let Some(field) = proto.as_ref() {
        try_parse_physical_expr(field, input_schema)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}

fn try_parse_physical_sort_expr(
    input: &Arc<dyn ExecutionPlan>,
    sort: &Box<SortExecNode>,
) -> Result<Vec<PhysicalSortExpr>, PlanSerDeError> {
    let pyhsical_sort_expr = sort
        .expr
        .iter()
        .map(|expr| {
            let expr = expr.expr_type.as_ref().ok_or_else(|| {
                proto_error(format!(
                    "physical_plan::from_proto() Unexpected expr {:?}",
                    input
                ))
            })?;
            if let ExprType::Sort(sort_expr) = expr {
                let expr = sort_expr
                    .expr
                    .as_ref()
                    .ok_or_else(|| {
                        proto_error(format!(
                            "physical_plan::from_proto() Unexpected sort expr {:?}",
                            input
                        ))
                    })?
                    .as_ref();
                Ok(PhysicalSortExpr {
                    expr: try_parse_physical_expr(expr, &input.schema())?,
                    options: SortOptions {
                        descending: !sort_expr.asc,
                        nulls_first: sort_expr.nulls_first,
                    },
                })
            } else {
                Err(PlanSerDeError::General(format!(
                    "physical_plan::from_proto() {:?}",
                    input
                )))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(pyhsical_sort_expr)
}

pub fn parse_protobuf_partitioning(
    input: Arc<dyn ExecutionPlan>,
    partitioning: Option<&Box<PhysicalRepartition>>,
) -> Result<Option<Partitioning>, PlanSerDeError> {
    partitioning.map_or(Ok(None), |p| {
        let plan = p.repartition_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "partition::from_proto() Unsupported partition '{:?}'",
                p
            ))
        })?;
        match plan {
            RepartitionType::SingleRepartition(..) => Ok(Some(Partitioning::SinglePartitioning())),
            RepartitionType::HashRepartition(hash_part) => {
                // let hash_part = p.hash_repartition;
                let expr = hash_part
                    .hash_expr
                    .iter()
                    .map(|e| try_parse_physical_expr(e, &input.schema()))
                    .collect::<Result<Vec<PhysicalExprRef>, _>>()?;
                Ok(Some(Partitioning::HashPartitioning(
                    expr,
                    hash_part.partition_count.try_into().unwrap(),
                )))
            }

            RepartitionType::RoundRobinRepartition(round_robin_part) => {
                Ok(Some(Partitioning::RoundRobinPartitioning(
                    round_robin_part.partition_count.try_into().unwrap(),
                )))
            }

            RepartitionType::RangeRepartition(range_part) => {
                if range_part.partition_count == 1 {
                    Ok(Some(Partitioning::SinglePartitioning()))
                } else {
                    let sort = range_part.sort_expr.clone().unwrap();
                    let exprs = try_parse_physical_sort_expr(&input, &sort).unwrap_or_else(|e| {
                        panic!("Failed to parse physical sort expressions: {}", e);
                    });

                    let value_list: Vec<ScalarValue> = range_part
                        .list_value
                        .iter()
                        .map(|v| v.try_into())
                        .collect::<Result<Vec<_>, _>>()?;

                    let sort_row_converter = Arc::new(SyncMutex::new(RowConverter::new(
                        exprs
                            .iter()
                            .map(|expr: &PhysicalSortExpr| {
                                Ok(SortField::new_with_options(
                                    expr.expr.data_type(&input.schema())?,
                                    expr.options,
                                ))
                            })
                            .collect::<Result<Vec<SortField>>>()?,
                    )?));

                    let bound_cols: Vec<ArrayRef> = value_list
                        .iter()
                        .map(|x| {
                            if let ScalarValue::List(single) = x {
                                return single.value(0);
                            } else {
                                unreachable!("expect list scalar value");
                            }
                        })
                        .collect::<Vec<ArrayRef>>();

                    let bound_rows = sort_row_converter.lock().convert_columns(&bound_cols)?;
                    Ok(Some(Partitioning::RangePartitioning(
                        exprs,
                        range_part.partition_count.try_into().unwrap(),
                        Arc::new(bound_rows),
                    )))
                }
            }
        }
    })
}

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = PlanSerDeError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(format!("/{}", BASE64_URL_SAFE_NO_PAD.encode(&val.path))),
                size: val.size,
                last_modified: Default::default(),
                e_tag: None,
                version: None,
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
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
            null_count: Precision::Exact(cs.null_count as usize),
            max_value: cs
                .max_value
                .as_ref()
                .map(|m| Precision::Exact(m.try_into().unwrap()))
                .unwrap_or(Precision::Absent),
            min_value: cs
                .min_value
                .as_ref()
                .map(|m| Precision::Exact(m.try_into().unwrap()))
                .unwrap_or(Precision::Absent),
            sum_value: Precision::Absent,
            distinct_count: Precision::Exact(cs.distinct_count as usize),
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
            num_rows: Precision::Exact(self.num_rows as usize),
            total_byte_size: Precision::Exact(self.total_byte_size as usize),
            // No column statistic (None) is encoded with empty array
            column_statistics,
        })
    }
}

impl TryInto<FileScanConfig> for &protobuf::FileScanExecConf {
    type Error = PlanSerDeError;

    fn try_into(self) -> Result<FileScanConfig, Self::Error> {
        let schema: SchemaRef = Arc::new(convert_required!(self.schema)?);
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
        let partition_schema: SchemaRef = Arc::new(convert_required!(self.partition_schema)?);
        let mut statistics: Statistics = convert_required!(self.statistics)?;
        if statistics.column_statistics.is_empty() {
            statistics.column_statistics = schema
                .fields()
                .iter()
                .map(|_| ColumnStatistics::new_unknown())
                .collect();
        }
        let statistics = Arc::new(statistics);

        let file_groups = (0..self.num_partitions)
            .map(|i| {
                if i == self.partition_index {
                    let file_group = FileGroup::new(
                        self.file_group
                            .as_ref()
                            .expect("missing FileScanConfig.file_group")
                            .try_into()?,
                    );
                    Ok(file_group.with_statistics(statistics.clone()))
                } else {
                    Ok(FileGroup::new(vec![]))
                }
            })
            .collect::<Result<Vec<_>, PlanSerDeError>>()?;

        struct UnusedFileSource(Statistics);
        impl FileSource for UnusedFileSource {
            fn create_file_opener(
                &self,
                _object_store: Arc<dyn ObjectStore>,
                _base_config: &FileScanConfig,
                _partition: usize,
            ) -> Arc<dyn FileOpener> {
                unimplemented!()
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
                unimplemented!()
            }

            fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
                unimplemented!()
            }

            fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
                unimplemented!()
            }

            fn with_statistics(&self, _statistics: Statistics) -> Arc<dyn FileSource> {
                unimplemented!()
            }

            fn metrics(&self) -> &ExecutionPlanMetricsSet {
                unimplemented!()
            }

            fn statistics(&self) -> Result<Statistics> {
                Ok(self.0.clone())
            }

            fn file_type(&self) -> &str {
                "unused"
            }
        }

        Ok(FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(), // not used
            file_schema: schema,
            file_groups,
            constraints: Default::default(),
            projection,
            limit: self.limit.as_ref().map(|sl| sl.limit as usize),
            table_partition_cols: partition_schema
                .fields()
                .iter()
                .map(|field| Field::new(field.name().clone(), field.data_type().clone(), true))
                .map(Arc::new)
                .collect(),
            output_ordering: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED, // unused
            new_lines_in_values: false,
            file_source: Arc::new(UnusedFileSource(statistics.as_ref().clone())),
            batch_size: None,
            expr_adapter_factory: None,
        })
    }
}
