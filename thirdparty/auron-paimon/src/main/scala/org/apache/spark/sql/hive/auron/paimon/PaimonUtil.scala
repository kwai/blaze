/*
 * Copyright 2022 The Auron Authors
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
package org.apache.spark.sql.hive.auron

import java.util.{HashMap => JHashMap}

import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.options.Options
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.FileStoreTableFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object PaimonUtil extends Logging {
  private val paimonCowOptionKey = "full-compaction.delta-commits"
  private val paimonFileFormatOptionKey = "file.format"
  val parquetFormat = "parquet"
  val orcFormat = "orc"

  def loadTable(path: String): FileStoreTable = {
    val parameters = new JHashMap[String, String]()
    parameters.put(CoreOptions.PATH.key, path)
    val catalogContext =
      CatalogContext.create(
        Options.fromMap(parameters),
        SparkSession.active.sessionState.newHadoopConf())
    FileStoreTableFactory.create(catalogContext)
  }

  def isPaimonCowTable(table: FileStoreTable): Boolean = {
    // https://paimon.apache.org/docs/master/primary-key-table/table-mode/
    // Paimon COW mode: 'full-compaction.delta-commits' = '1'
    table
      .options()
      .get(paimonCowOptionKey) != null && table.options().get(paimonCowOptionKey).equals("1")
  }

  def paimonFileFormat(table: FileStoreTable): String = {
    if (table.options().get(paimonFileFormatOptionKey) != null) {
      table.options().get(paimonFileFormatOptionKey)
    } else {
      parquetFormat
    }
  }
}
