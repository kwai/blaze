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

package org.apache.spark.sql.blaze;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import scala.Function1;
import scala.Unit;
import scala.collection.Iterator;

public class JniBridge {
  public static final ConcurrentHashMap<String, Object> resourcesMap = new ConcurrentHashMap<>();

  public static native void initNative(
      long batchSize, long nativeMemory, double memoryFraction, String tmpDirs);

  public static native void callNative(BlazeCallNativeWrapper wrapper);

  public static ClassLoader getContextClassLoader() {
    return Thread.currentThread().getContextClassLoader();
  }

  public static void setContextClassLoader(ClassLoader cl) {
    Thread.currentThread().setContextClassLoader(cl);
  }

  public static Object getResource(String key) {
    return resourcesMap.remove(key);
  }

  public static TaskContext getTaskContext() {
    return TaskContext$.MODULE$.get();
  }

  public static void setTaskContext(TaskContext tc) {
    TaskContext$.MODULE$.setTaskContext(tc);
  }

  public static boolean isTaskRunning() {
    TaskContext tc = getTaskContext();
    return !tc.isCompleted() && !tc.isInterrupted();
  }

  public static native void mergeIpcs(
      Iterator<ReadableByteChannel> ipcBytesIter,
      Function1<ByteBuffer, Unit> mergedIpcBytesHandler);
}
