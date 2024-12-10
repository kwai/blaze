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
package org.apache.spark.sql.blaze.kwai

import java.security.PrivilegedExceptionAction

import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.HDFSBlockManager
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.ExternalBlockStore

class KwaiPrivilegedHDFSBlockManager extends HDFSBlockManager {
  override def init(blockManager: BlockManager): Unit = {
    val initHDFSBlockManager = () => {
      super.init(blockManager)
    }

    NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = initHDFSBlockManager()
    })
  }
}

object KwaiPrivilegedHDFSBlockManager extends Logging {
  def setup(conf: SparkConf): Unit = {
    val externalBlockManagerClassName = conf
      .getOption(ExternalBlockStore.BLOCK_MANAGER_NAME)
      .getOrElse(ExternalBlockStore.DEFAULT_BLOCK_MANAGER_NAME)
    val thisClassName = classOf[KwaiPrivilegedHDFSBlockManager].getName

    if (externalBlockManagerClassName == classOf[HDFSBlockManager].getName) {
      logInfo(s"replacing external block manager to $thisClassName")
      conf.set(ExternalBlockStore.BLOCK_MANAGER_NAME, thisClassName)
    }
  }
}
