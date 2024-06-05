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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.blaze.memory.OnHeapSpillManager;
import org.apache.spark.sql.blaze.memory.OnHeapSpillManager$;

@SuppressWarnings("unused")
public class JniBridge {
    public static final ConcurrentHashMap<String, Object> resourcesMap = new ConcurrentHashMap<>();

    public static native long callNative(long initNativeMemory, BlazeCallNativeWrapper wrapper);

    public static native boolean nextBatch(long ptr);

    public static native void finalizeNative(long ptr);

    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static void setContextClassLoader(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }

    public static String getSparkEnvConfAsString(String key) {
        return SparkEnv.get().conf().get(key);
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

    public static OnHeapSpillManager getTaskOnHeapSpillManager() {
        return OnHeapSpillManager$.MODULE$.current();
    }

    public static boolean isTaskRunning() {
        TaskContext tc = getTaskContext();
        if (tc == null) { // driver is always running
            return true;
        }
        return !tc.isCompleted() && !tc.isInterrupted();
    }

    public static boolean isDriverSide() {
        TaskContext tc = getTaskContext();
        return tc == null;
    }

    private static final List<BufferPoolMXBean> directMXBeans =
            ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

    public static long getDirectMemoryUsed() {
        return directMXBeans.stream()
                .mapToLong(BufferPoolMXBean::getTotalCapacity)
                .sum();
    }

    public static String getDirectWriteSpillToDiskFile() {
        return SparkEnv.get()
                .blockManager()
                .diskBlockManager()
                .createTempLocalBlock()
                ._2
                .getPath();
    }
}
