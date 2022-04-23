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

package org.apache.spark.sql.blaze;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.shuffle.ShuffleManager;

public class JniBridge {
  public static final ConcurrentHashMap<String, Object> resourcesMap = new ConcurrentHashMap<>();
  static final FileSystem defaultFS;

  static {
    System.loadLibrary("blaze");
  }

  static {
    // init default filesystem
    try {
      Configuration conf = SparkHadoopUtil.get().newConfiguration(SparkEnv.get().conf());
      URI defaultUri = FileSystem.getDefaultUri(conf);
      conf.setBoolean(String.format("fs.%s.impl.disable.cache", defaultUri.getScheme()), true);
      defaultFS = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void raiseThrowable(Throwable t) throws Throwable {
    throw t;
  }

  // JVM -> Native
  public static ClassLoader getContextClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }

  // JVM -> Native
  public static void setContextClassLoader(ClassLoader cl) {
    Thread.currentThread().setContextClassLoader(cl);
  }

  // JVM -> Native
  public static FileSystem getHDFSFileSystem(String uriString) {
    return defaultFS;
  }

  // JVM -> Native
  public static ShuffleManager getShuffleManager() {
    return SparkEnv.get().shuffleManager();
  }

  // JVM -> Native
  public static Object getResource(String key) {
    return resourcesMap.get(key);
  }

  // Native -> JVM
  public static native long callNative(
      byte[] taskDefinition,
      long tokioPoolSize,
      long batchSize,
      long nativeMemory,
      double memoryFraction,
      String tmpDirs,
      MetricNode metrics);

  public static native int loadNext(long iter_ptr, long schema_ptr, long array_ptr);

  // JVM -> Native
  // shim method to FSDataInputStream.read()
  public static int readFSDataInputStream(FSDataInputStream in, ByteBuffer bb, long pos)
      throws IOException {
    int bytesRead;

    synchronized (in) {
      in.seek(pos);
      try {
        bytesRead = in.read(bb);
      } catch (UnsupportedOperationException e) {
        ReadableByteChannel channel = Channels.newChannel(in);
        bytesRead = channel.read(bb);
      }
      return bytesRead;
    }
  }
}