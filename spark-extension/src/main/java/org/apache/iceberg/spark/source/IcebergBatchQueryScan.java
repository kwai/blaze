/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.execution.datasources.SparkExpressionConverter;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.types.StructType;

public class IcebergBatchQueryScan implements Serializable {

    String tableIdent;
    StructType projection;
    StructType partType;
    Schema icebergPartitionSchema;
    org.apache.iceberg.expressions.Expression dataFilter;

    Table table;

    public IcebergBatchQueryScan(Scan scan) {
        if (scan instanceof SparkBatchQueryScan) {
            SparkBatchQueryScan sparkBatchQueryScan = (SparkBatchQueryScan) scan;
            table = sparkBatchQueryScan.table();
            tableIdent = table.name();
            Schema expected = sparkBatchQueryScan.scan().schema();
            projection = SparkSchemaUtil.convert(expected);
            icebergPartitionSchema = icebergPartitionSchema();
            partType = SparkSchemaUtil.convert(icebergPartitionSchema);
            dataFilter = sparkBatchQueryScan.scan().filter();
            table = sparkBatchQueryScan.table();
        } else {
            throw new IllegalStateException("error converting scan " + scan.toString());
        }
    }

    public String tableIdent() {
        return tableIdent;
    }

    public StructType projection() {
        return projection;
    }

    public StructType partitionSchema() {
        return partType;
    }

    public StructType tableSchema() {
        return SparkSchemaUtil.convert(table.schema());
    }

    public Schema icebergPartitionSchema() {
        PartitionSpec partitionSpec = table.spec();
        Schema fullSchema = table.schema();
        return new Schema(partitionSpec.fields().stream()
                .map(field -> fullSchema.findField(field.fieldId()))
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList()));
    }

    public Expression dataFilter(SparkSession spark) {
        try {
            Filter v1Filter = SparkFilters.convertToSparkFilter(dataFilter);
            String where = FilterToSql.filterToSql(v1Filter);
            return SparkExpressionConverter.collectResolvedSparkExpression(spark, tableIdent(), where);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Configuration getConf() {
        if (table.io() instanceof HadoopFileIO) {
            HadoopFileIO hadoopFileIO = (HadoopFileIO) table.io();
            return hadoopFileIO.getConf();
        } else {
            throw new UnsupportedOperationException();
        }
    }
}

class FilterToSql {

    public static String filterToSql(Filter filter) {
        if (filter instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) filter;
            return equalTo.attribute() + " = " + quoteValue(equalTo.value());
        } else if (filter instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) filter;
            return greaterThan.attribute() + " > " + quoteValue(greaterThan.value());
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
            return greaterThanOrEqual.attribute() + " >= " + quoteValue(greaterThanOrEqual.value());
        } else if (filter instanceof LessThan) {
            LessThan lessThan = (LessThan) filter;
            return lessThan.attribute() + " < " + quoteValue(lessThan.value());
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
            return lessThanOrEqual.attribute() + " <= " + quoteValue(lessThanOrEqual.value());
        } else if (filter instanceof In) {
            In inFilter = (In) filter;
            List<String> vals = Arrays.stream(inFilter.values())
                    .map(FilterToSql::quoteValue)
                    .collect(Collectors.toList());
            String values = String.join(", ", vals);
            return inFilter.attribute() + " IN (" + values + ")";
        } else if (filter instanceof IsNull) {
            IsNull isNull = (IsNull) filter;
            return isNull.attribute() + " IS NULL";
        } else if (filter instanceof IsNotNull) {
            IsNotNull isNotNull = (IsNotNull) filter;
            return isNotNull.attribute() + " IS NOT NULL";
        } else if (filter instanceof And) {
            And andFilter = (And) filter;
            return "(" + filterToSql(andFilter.left()) + " AND " + filterToSql(andFilter.right()) + ")";
        } else if (filter instanceof Or) {
            Or orFilter = (Or) filter;
            return "(" + filterToSql(orFilter.left()) + " OR " + filterToSql(orFilter.right()) + ")";
        } else if (filter instanceof Not) {
            Not notFilter = (Not) filter;
            return "NOT (" + filterToSql(notFilter.child()) + ")";
        } else {
            throw new UnsupportedOperationException("Unsupported filter: " + filter);
        }
    }

    private static String quoteValue(Object value) {
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        } else {
            return String.valueOf(value);
        }
    }

    public static void main(String[] args) {
        Filter filter = new And(new EqualTo("column1", "value1"), new GreaterThan("column2", 42));
        String sql = filterToSql(filter);
        System.out.println(sql); // Output: (column1 = 'value1' AND column2 > 42)
    }
}
