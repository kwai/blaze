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

package org.apache.spark.sql.blaze.execution

import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.io.CompressionCodec
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

object Util extends Logging {

  // only zstd compression is supported at the moment
  def getZCodecForShuffle: CompressionCodec = {
    val sparkConf = SparkEnv.get.conf
    val zcodecConfName = "spark.blaze.shuffle.compression.codec"
    val zcodecName =
      sparkConf.get(zcodecConfName, defaultValue = sparkConf.get(IO_COMPRESSION_CODEC))

    if (zcodecName != "zstd") {
      logWarning(
        s"Overriding config ${IO_COMPRESSION_CODEC}=${zcodecName} in shuffling, force using zstd")
    }
    CompressionCodec.createCodec(sparkConf, "zstd")
  }
}
