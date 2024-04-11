package org.apache.spark.memory.blaze;

import org.apache.spark.SparkEnv$;
import org.apache.spark.memory.MemoryPool;

public class OnHeapSpillManagerHelper {
    public static MemoryPool getOnHeapExecutionMemoryPool() {
        return SparkEnv$.MODULE$.get().memoryManager().onHeapExecutionMemoryPool();
    }
}
