/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import scala.sys.process._

import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * This class was copied from `spark-sql-perf` and modified slightly.
 */
class Tables(sqlContext: SQLContext, scaleFactor: Int) extends Serializable {
  import sqlContext.implicits._

  private def sparkContext = sqlContext.sparkContext

  private object Table {

    def apply(name: String, partitionColumns: Seq[String], schemaString: String): Table = {
      new Table(name, partitionColumns, StructType.fromDDL(schemaString))
    }
  }

  private case class Table(name: String, partitionColumns: Seq[String], schema: StructType) {
    def nonPartitioned: Table = {
      Table(name, Nil, schema)
    }

    /**
     *  If convertToSchema is true, the data from generator will be parsed into columns and
     *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean, numPartition: Int): DataFrame = {
      val numDefaultPartitions = sparkContext
        .conf
        .getInt("tpcds.gen.defaultParallel", defaultValue = 1000)

      val partitions = if (partitionColumns.isEmpty) numDefaultPartitions else numPartition
      val generatedData = {
        sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
          val datagen = DsdgenNative()
          // Note: RNGSEED is the RNG seed used by the data generator. Right now, it is fixed to 100
          val parallel = if (partitions > 1) s"-parallel $partitions -child $i" else ""
          val commands = Seq(
            "bash", "-c",
            s"cd ${datagen.dir} && ./${datagen.cmd} -table $name -filter Y " +
              s"-scale $scaleFactor $parallel"
          )
          commands.lines
        }
      }
      generatedData.setName(s"$name, sf=$scaleFactor, strings")

      var rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))

        val convertedData = {
          val columns = schema.fields.map { f =>
            val expr = f.dataType match {
              // TODO: In branch-3.1, we need right-side padding for char types
              case CharType(n) => rpad(Column(f.name), n, " ")
              case _ => Column(f.name).cast(f.dataType)
            }
            expr.as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def useDoubleForDecimal(): Table = {
      val newFields = schema.fields.map { field =>
        val newDataType = field.dataType match {
          case _: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, StructType(newFields))
    }

    def useStringForChar(): Table = {
      val newFields = schema.fields.map { field =>
        val newDataType = field.dataType match {
          case _: CharType | _: VarcharType => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, StructType(newFields))
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        clusterByPartitionColumns: Boolean,
        filterOutNullPartitionValues: Boolean,
        numPartitions: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore

      val data = df(format != "text", numPartitions)
      val tempTableName = s"${name}_text"
      data.createOrReplaceTempView(tempTableName)

      val writer = if (partitionColumns.nonEmpty) {
        if (clusterByPartitionColumns) {
          val columnString = data.schema.fields.map { field =>
            field.name
          }.mkString(",")
          val partitionColumnString = partitionColumns.mkString(",")
          val predicates = if (filterOutNullPartitionValues) {
            partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
          } else {
            ""
          }

          val query =
            s"""
              |SELECT
              |  $columnString
              |FROM
              |  $tempTableName
              |$predicates
              |DISTRIBUTE BY
              |  $partitionColumnString
            """.stripMargin
          val grouped = sqlContext.sql(query)
          grouped.write
        } else {
          data.write
        }
      } else {
        // If the table is not partitioned, repartition the data into file with estimated target size.
        val cached = data.cache()
        val plainDataSize = cached
          .mapPartitions { iter =>
            Iterator.single(iter.map(_.mkString("\t").length).sum.toLong)
          }
          .collect()
          .sum

        // 4 GB plain data per file
        val numTargetFiles = math.max(1, math.ceil(plainDataSize / 4 / 1024.0 / 1024.0 / 1024.0).toInt)
        cached.repartition(numTargetFiles).write
      }
      writer.format(format).mode(mode)
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns : _*)
      }
      writer.save(location)
      sqlContext.dropTempTable(tempTableName)
    }

    def createExternalTable(
        location: String, format: String, databaseName: String, overwrite: Boolean): Unit = {
      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        // In `3.0.0-preview2`, this method has been removed though,
        // the community is planning to revert it back in the 3.0 official release.
        // sqlContext.createExternalTable(qualifiedTableName, location, format)
        sqlContext.sparkSession.catalog.createTable(qualifiedTableName, location, format)
      }
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      // In `3.0.0-preview2`, this method has been removed though,
      // the community is planning to revert it back in the 3.0 official release.
      // sqlContext.read.format(format).load(location).registerTempTable(name)
      sqlContext.read.format(format).load(location).createOrReplaceTempView(name)
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      useDoubleForDecimal: Boolean,
      useStringForChar: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: Set[String] = Set.empty,
      numPartitions: Int = 100): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (tableFilter.nonEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter { case t => tableFilter.contains(t.name) }
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    val withSpecifiedDataType = {
      var tables = tablesToBeGenerated
      if (useDoubleForDecimal) tables = tables.map(_.useDoubleForDecimal())
      if (useStringForChar) tables = tables.map(_.useStringForChar())
      tables
    }

    withSpecifiedDataType.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions)
    }
  }

  /**
   * Table data for TPC-DS related tests.
   *
   * Datatype mapping for TPC-DS and Spark SQL, fully matching schemas defined in `tpcds.sql` of the
   * official tpcds toolkit
   * see more at:
   *   http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v2.9.0.pdf
   *
   *    |---------------|---------------|
   *    |    TPC-DS     |  Spark  SQL   |
   *    |---------------|---------------|
   *    |  Identifier   |      INT      |
   *    |---------------|---------------|
   *    |    Integer    |      INT      |
   *    |---------------|---------------|
   *    | Decimal(d, f) | Decimal(d, f) |
   *    |---------------|---------------|
   *    |    Char(N)    |    Char(N)    |
   *    |---------------|---------------|
   *    |  Varchar(N)   |  Varchar(N)   |
   *    |---------------|---------------|
   *    |     Date      |     Date      |
   *    |---------------|---------------|
   */
  private val tables = Seq(
    Table("catalog_sales",
      partitionColumns = "cs_sold_date_sk" :: Nil,
      """
        |`cs_sold_date_sk` INT,
        |`cs_sold_time_sk` INT,
        |`cs_ship_date_sk` INT,
        |`cs_bill_customer_sk` INT,
        |`cs_bill_cdemo_sk` INT,
        |`cs_bill_hdemo_sk` INT,
        |`cs_bill_addr_sk` INT,
        |`cs_ship_customer_sk` INT,
        |`cs_ship_cdemo_sk` INT,
        |`cs_ship_hdemo_sk` INT,
        |`cs_ship_addr_sk` INT,
        |`cs_call_center_sk` INT,
        |`cs_catalog_page_sk` INT,
        |`cs_ship_mode_sk` INT,
        |`cs_warehouse_sk` INT,
        |`cs_item_sk` INT,
        |`cs_promo_sk` INT,
        |`cs_order_number` INT,
        |`cs_quantity` INT,
        |`cs_wholesale_cost` DECIMAL(7,2),
        |`cs_list_price` DECIMAL(7,2),
        |`cs_sales_price` DECIMAL(7,2),
        |`cs_ext_discount_amt` DECIMAL(7,2),
        |`cs_ext_sales_price` DECIMAL(7,2),
        |`cs_ext_wholesale_cost` DECIMAL(7,2),
        |`cs_ext_list_price` DECIMAL(7,2),
        |`cs_ext_tax` DECIMAL(7,2),
        |`cs_coupon_amt` DECIMAL(7,2),
        |`cs_ext_ship_cost` DECIMAL(7,2),
        |`cs_net_paid` DECIMAL(7,2),
        |`cs_net_paid_inc_tax` DECIMAL(7,2),
        |`cs_net_paid_inc_ship` DECIMAL(7,2),
        |`cs_net_paid_inc_ship_tax` DECIMAL(7,2),
        |`cs_net_profit` DECIMAL(7,2)
      """.stripMargin),
    Table("catalog_returns",
      "cr_returned_date_sk" :: Nil,
      """
        |`cr_returned_date_sk` INT,
        |`cr_returned_time_sk` INT,
        |`cr_item_sk` INT,
        |`cr_refunded_customer_sk` INT,
        |`cr_refunded_cdemo_sk` INT,
        |`cr_refunded_hdemo_sk` INT,
        |`cr_refunded_addr_sk` INT,
        |`cr_returning_customer_sk` INT,
        |`cr_returning_cdemo_sk` INT,
        |`cr_returning_hdemo_sk` INT,
        |`cr_returning_addr_sk` INT,
        |`cr_call_center_sk` INT,
        |`cr_catalog_page_sk` INT,
        |`cr_ship_mode_sk` INT,
        |`cr_warehouse_sk` INT,
        |`cr_reason_sk` INT,`cr_order_number` INT,
        |`cr_return_quantity` INT,
        |`cr_return_amount` DECIMAL(7,2),
        |`cr_return_tax` DECIMAL(7,2),
        |`cr_return_amt_inc_tax` DECIMAL(7,2),
        |`cr_fee` DECIMAL(7,2),
        |`cr_return_ship_cost` DECIMAL(7,2),
        |`cr_refunded_cash` DECIMAL(7,2),
        |`cr_reversed_charge` DECIMAL(7,2),
        |`cr_store_credit` DECIMAL(7,2),
        |`cr_net_loss` DECIMAL(7,2)
      """.stripMargin),
    Table("inventory",
      partitionColumns = "inv_date_sk" :: Nil,
      """
        |`inv_date_sk` INT,
        |`inv_item_sk` INT,
        |`inv_warehouse_sk` INT,
        |`inv_quantity_on_hand` INT
      """.stripMargin),
    Table("store_sales",
      partitionColumns = "ss_sold_date_sk" :: Nil,
      """
        |`ss_sold_date_sk` INT,
        |`ss_sold_time_sk` INT,
        |`ss_item_sk` INT,
        |`ss_customer_sk` INT,
        |`ss_cdemo_sk` INT,
        |`ss_hdemo_sk` INT,
        |`ss_addr_sk` INT,
        |`ss_store_sk` INT,
        |`ss_promo_sk` INT,
        |`ss_ticket_number` INT,
        |`ss_quantity` INT,
        |`ss_wholesale_cost` DECIMAL(7,2),
        |`ss_list_price` DECIMAL(7,2),
        |`ss_sales_price` DECIMAL(7,2),
        |`ss_ext_discount_amt` DECIMAL(7,2),
        |`ss_ext_sales_price` DECIMAL(7,2),
        |`ss_ext_wholesale_cost` DECIMAL(7,2),
        |`ss_ext_list_price` DECIMAL(7,2),
        |`ss_ext_tax` DECIMAL(7,2),
        |`ss_coupon_amt` DECIMAL(7,2),
        |`ss_net_paid` DECIMAL(7,2),
        |`ss_net_paid_inc_tax` DECIMAL(7,2),
        |`ss_net_profit` DECIMAL(7,2)
      """.stripMargin),
    Table("store_returns",
      partitionColumns = "sr_returned_date_sk" ::Nil,
      """
        |`sr_returned_date_sk` INT,
        |`sr_return_time_sk` INT,
        |`sr_item_sk` INT,
        |`sr_customer_sk` INT,
        |`sr_cdemo_sk` INT,
        |`sr_hdemo_sk` INT,
        |`sr_addr_sk` INT,
        |`sr_store_sk` INT,
        |`sr_reason_sk` INT,
        |`sr_ticket_number` INT,
        |`sr_return_quantity` INT,
        |`sr_return_amt` DECIMAL(7,2),
        |`sr_return_tax` DECIMAL(7,2),
        |`sr_return_amt_inc_tax` DECIMAL(7,2),
        |`sr_fee` DECIMAL(7,2),
        |`sr_return_ship_cost` DECIMAL(7,2),
        |`sr_refunded_cash` DECIMAL(7,2),
        |`sr_reversed_charge` DECIMAL(7,2),
        |`sr_store_credit` DECIMAL(7,2),
        |`sr_net_loss` DECIMAL(7,2)
      """.stripMargin),
    Table("web_sales",
      partitionColumns = "ws_sold_date_sk" :: Nil,
      """
        |`ws_sold_date_sk` INT,
        |`ws_sold_time_sk` INT,
        |`ws_ship_date_sk` INT,
        |`ws_item_sk` INT,
        |`ws_bill_customer_sk` INT,
        |`ws_bill_cdemo_sk` INT,
        |`ws_bill_hdemo_sk` INT,
        |`ws_bill_addr_sk` INT,
        |`ws_ship_customer_sk` INT,
        |`ws_ship_cdemo_sk` INT,
        |`ws_ship_hdemo_sk` INT,
        |`ws_ship_addr_sk` INT,
        |`ws_web_page_sk` INT,
        |`ws_web_site_sk` INT,
        |`ws_ship_mode_sk` INT,
        |`ws_warehouse_sk` INT,
        |`ws_promo_sk` INT,
        |`ws_order_number` INT,
        |`ws_quantity` INT,
        |`ws_wholesale_cost` DECIMAL(7,2),
        |`ws_list_price` DECIMAL(7,2),
        |`ws_sales_price` DECIMAL(7,2),
        |`ws_ext_discount_amt` DECIMAL(7,2),
        |`ws_ext_sales_price` DECIMAL(7,2),
        |`ws_ext_wholesale_cost` DECIMAL(7,2),
        |`ws_ext_list_price` DECIMAL(7,2),
        |`ws_ext_tax` DECIMAL(7,2),
        |`ws_coupon_amt` DECIMAL(7,2),
        |`ws_ext_ship_cost` DECIMAL(7,2),
        |`ws_net_paid` DECIMAL(7,2),
        |`ws_net_paid_inc_tax` DECIMAL(7,2),
        |`ws_net_paid_inc_ship` DECIMAL(7,2),
        |`ws_net_paid_inc_ship_tax` DECIMAL(7,2),
        |`ws_net_profit` DECIMAL(7,2)
      """.stripMargin),
    Table("web_returns",
      partitionColumns = "wr_returned_date_sk" ::Nil,
      """
        |`wr_returned_date_sk` INT,
        |`wr_returned_time_sk` INT,
        |`wr_item_sk` INT,
        |`wr_refunded_customer_sk` INT,
        |`wr_refunded_cdemo_sk` INT,
        |`wr_refunded_hdemo_sk` INT,
        |`wr_refunded_addr_sk` INT,
        |`wr_returning_customer_sk` INT,
        |`wr_returning_cdemo_sk` INT,
        |`wr_returning_hdemo_sk` INT,
        |`wr_returning_addr_sk` INT,
        |`wr_web_page_sk` INT,
        |`wr_reason_sk` INT,
        |`wr_order_number` INT,
        |`wr_return_quantity` INT,
        |`wr_return_amt` DECIMAL(7,2),
        |`wr_return_tax` DECIMAL(7,2),
        |`wr_return_amt_inc_tax` DECIMAL(7,2),
        |`wr_fee` DECIMAL(7,2),
        |`wr_return_ship_cost` DECIMAL(7,2),
        |`wr_refunded_cash` DECIMAL(7,2),
        |`wr_reversed_charge` DECIMAL(7,2),
        |`wr_account_credit` DECIMAL(7,2),
        |`wr_net_loss` DECIMAL(7,2)
      """.stripMargin),
    Table("call_center",
      partitionColumns = Nil,
      """
        |`cc_call_center_sk` INT,
        |`cc_call_center_id` CHAR(16),
        |`cc_rec_start_date` DATE,
        |`cc_rec_end_date` DATE,
        |`cc_closed_date_sk` INT,
        |`cc_open_date_sk` INT,
        |`cc_name` VARCHAR(50),
        |`cc_class` VARCHAR(50),
        |`cc_employees` INT,
        |`cc_sq_ft` INT,
        |`cc_hours` CHAR(20),
        |`cc_manager` VARCHAR(40),
        |`cc_mkt_id` INT,
        |`cc_mkt_class` CHAR(50),
        |`cc_mkt_desc` VARCHAR(100),
        |`cc_market_manager` VARCHAR(40),
        |`cc_division` INT,
        |`cc_division_name` VARCHAR(50),
        |`cc_company` INT,
        |`cc_company_name` CHAR(50),
        |`cc_street_number` CHAR(10),
        |`cc_street_name` VARCHAR(60),
        |`cc_street_type` CHAR(15),
        |`cc_suite_number` CHAR(10),
        |`cc_city` VARCHAR(60),
        |`cc_county` VARCHAR(30),
        |`cc_state` CHAR(2),
        |`cc_zip` CHAR(10),
        |`cc_country` VARCHAR(20),
        |`cc_gmt_offset` DECIMAL(5,2),
        |`cc_tax_percentage` DECIMAL(5,2)
      """.stripMargin),
    Table("catalog_page",
      partitionColumns = Nil,
      """
        |`cp_catalog_page_sk` INT,
        |`cp_catalog_page_id` CHAR(16),
        |`cp_start_date_sk` INT,
        |`cp_end_date_sk` INT,
        |`cp_department` VARCHAR(50),
        |`cp_catalog_number` INT,
        |`cp_catalog_page_number` INT,
        |`cp_description` VARCHAR(100),
        |`cp_type` VARCHAR(100)
      """.stripMargin),
    Table("customer",
      partitionColumns = Nil,
      """
        |`c_customer_sk` INT,
        |`c_customer_id` CHAR(16),
        |`c_current_cdemo_sk` INT,
        |`c_current_hdemo_sk` INT,
        |`c_current_addr_sk` INT,
        |`c_first_shipto_date_sk` INT,
        |`c_first_sales_date_sk` INT,
        |`c_salutation` CHAR(10),
        |`c_first_name` CHAR(20),
        |`c_last_name` CHAR(30),
        |`c_preferred_cust_flag` CHAR(1),
        |`c_birth_day` INT,
        |`c_birth_month` INT,
        |`c_birth_year` INT,
        |`c_birth_country` VARCHAR(20),
        |`c_login` CHAR(13),
        |`c_email_address` CHAR(50),
        |`c_last_review_date` INT
      """.stripMargin),
    Table("customer_address",
      partitionColumns = Nil,
      """
        |`ca_address_sk` INT,
        |`ca_address_id` CHAR(16),
        |`ca_street_number` CHAR(10),
        |`ca_street_name` VARCHAR(60),
        |`ca_street_type` CHAR(15),
        |`ca_suite_number` CHAR(10),
        |`ca_city` VARCHAR(60),
        |`ca_county` VARCHAR(30),
        |`ca_state` CHAR(2),
        |`ca_zip` CHAR(10),
        |`ca_country` VARCHAR(20),
        |`ca_gmt_offset` DECIMAL(5,2),
        |`ca_location_type` CHAR(20)
      """.stripMargin),
    Table("customer_demographics",
      partitionColumns = Nil,
      """
        |`cd_demo_sk` INT,
        |`cd_gender` CHAR(1),
        |`cd_marital_status` CHAR(1),
        |`cd_education_status` CHAR(20),
        |`cd_purchase_estimate` INT,
        |`cd_credit_rating` CHAR(10),
        |`cd_dep_count` INT,
        |`cd_dep_employed_count` INT,
        |`cd_dep_college_count` INT
      """.stripMargin),
    Table("date_dim",
      partitionColumns = Nil,
      """
        |`d_date_sk` INT,
        |`d_date_id` CHAR(16),
        |`d_date` DATE,
        |`d_month_seq` INT,
        |`d_week_seq` INT,
        |`d_quarter_seq` INT,
        |`d_year` INT,
        |`d_dow` INT,
        |`d_moy` INT,
        |`d_dom` INT,
        |`d_qoy` INT,
        |`d_fy_year` INT,
        |`d_fy_quarter_seq` INT,
        |`d_fy_week_seq` INT,
        |`d_day_name` CHAR(9),
        |`d_quarter_name` CHAR(6),
        |`d_holiday` CHAR(1),
        |`d_weekend` CHAR(1),
        |`d_following_holiday` CHAR(1),
        |`d_first_dom` INT,
        |`d_last_dom` INT,
        |`d_same_day_ly` INT,
        |`d_same_day_lq` INT,
        |`d_current_day` CHAR(1),
        |`d_current_week` CHAR(1),
        |`d_current_month` CHAR(1),
        |`d_current_quarter` CHAR(1),
        |`d_current_year` CHAR(1)
      """.stripMargin),
    Table("household_demographics",
      partitionColumns = Nil,
      """
        |`hd_demo_sk` INT,
        |`hd_income_band_sk` INT,
        |`hd_buy_potential` CHAR(15),
        |`hd_dep_count` INT,
        |`hd_vehicle_count` INT
      """.stripMargin),
    Table("income_band",
      partitionColumns = Nil,
      """
        |`ib_income_band_sk` INT,
        |`ib_lower_bound` INT,
        |`ib_upper_bound` INT
      """.stripMargin),
    Table("item",
      partitionColumns = Nil,
      """
        |`i_item_sk` INT,
        |`i_item_id` CHAR(16),
        |`i_rec_start_date` DATE,
        |`i_rec_end_date` DATE,
        |`i_item_desc` VARCHAR(200),
        |`i_current_price` DECIMAL(7,2),
        |`i_wholesale_cost` DECIMAL(7,2),
        |`i_brand_id` INT,
        |`i_brand` CHAR(50),
        |`i_class_id` INT,
        |`i_class` CHAR(50),
        |`i_category_id` INT,
        |`i_category` CHAR(50),
        |`i_manufact_id` INT,
        |`i_manufact` CHAR(50),
        |`i_size` CHAR(20),
        |`i_formulation` CHAR(20),
        |`i_color` CHAR(20),
        |`i_units` CHAR(10),
        |`i_container` CHAR(10),
        |`i_manager_id` INT,
        |`i_product_name` CHAR(50)
      """.stripMargin),
    Table("promotion",
      partitionColumns = Nil,
      """
        |`p_promo_sk` INT,
        |`p_promo_id` CHAR(16),
        |`p_start_date_sk` INT,
        |`p_end_date_sk` INT,
        |`p_item_sk` INT,
        |`p_cost` DECIMAL(15,2),
        |`p_response_target` INT,
        |`p_promo_name` CHAR(50),
        |`p_channel_dmail` CHAR(1),
        |`p_channel_email` CHAR(1),
        |`p_channel_catalog` CHAR(1),
        |`p_channel_tv` CHAR(1),
        |`p_channel_radio` CHAR(1),
        |`p_channel_press` CHAR(1),
        |`p_channel_event` CHAR(1),
        |`p_channel_demo` CHAR(1),
        |`p_channel_details` VARCHAR(100),
        |`p_purpose` CHAR(15),
        |`p_discount_active` CHAR(1)
      """.stripMargin),
    Table("reason",
      partitionColumns = Nil,
      """
        |`r_reason_sk` INT,
        |`r_reason_id` CHAR(16),
        |`r_reason_desc` CHAR(100)
      """.stripMargin),
    Table("ship_mode",
      partitionColumns = Nil,
      """
        |`sm_ship_mode_sk` INT,
        |`sm_ship_mode_id` CHAR(16),
        |`sm_type` CHAR(30),
        |`sm_code` CHAR(10),
        |`sm_carrier` CHAR(20),
        |`sm_contract` CHAR(20)
      """.stripMargin),
    Table("store",
      partitionColumns = Nil,
      """
        |`s_store_sk` INT,
        |`s_store_id` CHAR(16),
        |`s_rec_start_date` DATE,
        |`s_rec_end_date` DATE,
        |`s_closed_date_sk` INT,
        |`s_store_name` VARCHAR(50),
        |`s_number_employees` INT,
        |`s_floor_space` INT,
        |`s_hours` CHAR(20),
        |`s_manager` VARCHAR(40),
        |`s_market_id` INT,
        |`s_geography_class` VARCHAR(100),
        |`s_market_desc` VARCHAR(100),
        |`s_market_manager` VARCHAR(40),
        |`s_division_id` INT,
        |`s_division_name` VARCHAR(50),
        |`s_company_id` INT,
        |`s_company_name` VARCHAR(50),
        |`s_street_number` VARCHAR(10),
        |`s_street_name` VARCHAR(60),
        |`s_street_type` CHAR(15),
        |`s_suite_number` CHAR(10),
        |`s_city` VARCHAR(60),
        |`s_county` VARCHAR(30),
        |`s_state` CHAR(2),
        |`s_zip` CHAR(10),
        |`s_country` VARCHAR(20),
        |`s_gmt_offset` DECIMAL(5,2),
        |`s_tax_percentage` DECIMAL(5,2)
      """.stripMargin),
    Table("time_dim",
      partitionColumns = Nil,
      """
        |`t_time_sk` INT,
        |`t_time_id` CHAR(16),
        |`t_time` INT,
        |`t_hour` INT,
        |`t_minute` INT,
        |`t_second` INT,
        |`t_am_pm` CHAR(2),
        |`t_shift` CHAR(20),
        |`t_sub_shift` CHAR(20),
        |`t_meal_time` CHAR(20)
      """.stripMargin),
    Table("warehouse",
      partitionColumns = Nil,
      """
        |`w_warehouse_sk` INT,
        |`w_warehouse_id` CHAR(16),
        |`w_warehouse_name` VARCHAR(20),
        |`w_warehouse_sq_ft` INT,
        |`w_street_number` CHAR(10),
        |`w_street_name` VARCHAR(20),
        |`w_street_type` CHAR(15),
        |`w_suite_number` CHAR(10),
        |`w_city` VARCHAR(60),
        |`w_county` VARCHAR(30),
        |`w_state` CHAR(2),
        |`w_zip` CHAR(10),
        |`w_country` VARCHAR(20),
        |`w_gmt_offset` DECIMAL(5,2)
      """.stripMargin),
    Table("web_page",
      partitionColumns = Nil,
      """
        |`wp_web_page_sk` INT,
        |`wp_web_page_id` CHAR(16),
        |`wp_rec_start_date` DATE,
        |`wp_rec_end_date` DATE,
        |`wp_creation_date_sk` INT,
        |`wp_access_date_sk` INT,
        |`wp_autogen_flag` CHAR(1),
        |`wp_customer_sk` INT,
        |`wp_url` VARCHAR(100),
        |`wp_type` CHAR(50),
        |`wp_char_count` INT,
        |`wp_link_count` INT,
        |`wp_image_count` INT,
        |`wp_max_ad_count` INT
      """.stripMargin),
    Table("web_site",
      partitionColumns = Nil,
      """
        |`web_site_sk` INT,
        |`web_site_id` CHAR(16),
        |`web_rec_start_date` DATE,
        |`web_rec_end_date` DATE,
        |`web_name` VARCHAR(50),
        |`web_open_date_sk` INT,
        |`web_close_date_sk` INT,
        |`web_class` VARCHAR(50),
        |`web_manager` VARCHAR(40),
        |`web_mkt_id` INT,
        |`web_mkt_class` VARCHAR(50),
        |`web_mkt_desc` VARCHAR(100),
        |`web_market_manager` VARCHAR(40),
        |`web_company_id` INT,
        |`web_company_name` CHAR(50),
        |`web_street_number` CHAR(10),
        |`web_street_name` VARCHAR(60),
        |`web_street_type` CHAR(15),
        |`web_suite_number` CHAR(10),
        |`web_city` VARCHAR(60),
        |`web_county` VARCHAR(30),
        |`web_state` CHAR(2),
        |`web_zip` CHAR(10),
        |`web_country` VARCHAR(20),
        |`web_gmt_offset` DECIMAL(5,2),
        |`web_tax_percentage` DECIMAL(5,2)
      """.stripMargin)
  )
}

object TPCDSDatagen {

  def main(args: Array[String]): Unit = {
    val datagenArgs = new TPCDSDatagenArguments(args)
    val spark = SparkSession.builder.getOrCreate()
    val tpcdsTables = new Tables(spark.sqlContext, datagenArgs.scaleFactor.toInt)
    tpcdsTables.genData(
      datagenArgs.outputLocation,
      datagenArgs.format,
      datagenArgs.overwrite,
      datagenArgs.partitionTables,
      datagenArgs.useDoubleForDecimal,
      datagenArgs.useStringForChar,
      datagenArgs.clusterByPartitionColumns,
      datagenArgs.filterOutNullPartitionValues,
      datagenArgs.tableFilter,
      datagenArgs.numPartitions.toInt)
    spark.stop()
  }
}
