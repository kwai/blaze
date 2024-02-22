package org.apache.spark.sql.hive.blaze

import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hive.client.HiveClientImpl

object HiveClientHelper {
  def toHiveTable(table: CatalogTable, userName: Option[String] = None): Table = {
    HiveClientImpl.toHiveTable(table, userName)
  }
}
