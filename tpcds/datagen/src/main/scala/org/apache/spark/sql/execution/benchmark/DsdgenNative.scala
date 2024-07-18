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

import java.io.{File, FileOutputStream, InputStream, OutputStream}

import org.xerial.snappy.OSInfo

case class DsdgenNative() {

  val (dir, cmd) = {
    val classLoader = Thread.currentThread().getContextClassLoader
    val tempDir = Utils.createTempDir()
    val srcDatagenDir = s"binaries/${OSInfo.getOSName}/${OSInfo.getArchName}"

    def copyNativeBinaryFromResource(resourceName: String, to: File) = {
      var in: InputStream = null
      var out: OutputStream = null
      try {
        in = classLoader.getResourceAsStream(resourceName)
        if (in == null) {
          throw new RuntimeException(
            s"Unsupported platform: OS=${OSInfo.getOSName} Arch=${OSInfo.getArchName}")
        }

        out = new FileOutputStream(to)
        val buffer = new Array[Byte](8192)
        var bytesRead = 0
        val canRead = () => {
          bytesRead = in.read(buffer)
          bytesRead != -1
        }
        while (canRead()) {
          out.write(buffer, 0, bytesRead)
        }
      } finally {
        if (in != null) {
          in.close()
        }
        if (out != null) {
          out.close()
        }
      }
    }

    Seq("dsdgen", "tpcds.idx").foreach { resource =>
      copyNativeBinaryFromResource(s"$srcDatagenDir/$resource", new File(tempDir, resource))
    }

    // Set executable (x) flag to run the copyed binary
    val dstDatagenPath = new File(tempDir, "dsdgen")
    if (!dstDatagenPath.setExecutable(true)) {
      throw new RuntimeException(
        s"Can't set a executable flag on: ${dstDatagenPath.getAbsolutePath}")
    }

    (tempDir.getAbsolutePath, "dsdgen")
  }
}
