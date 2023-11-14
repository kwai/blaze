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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.ScanTaskGroup;
import org.apache.spark.sql.connector.read.InputPartition;

public class IcebergInputPartition {
    SparkInputPartition sparkInputPartition;

    public IcebergInputPartition(InputPartition inputPartition) {
        if (inputPartition instanceof SparkInputPartition) {
            this.sparkInputPartition = (SparkInputPartition) inputPartition;
        } else {
            throw new UnsupportedOperationException("Invalid InputPartition");
        }
    }

    public ScanTaskGroup<?> scanTaskGroup() {
        return sparkInputPartition.taskGroup();
    }

    public static ScanTaskGroup<?> tasks(InputPartition inputPartition) {
        IcebergInputPartition icebergInputPartition = new IcebergInputPartition(inputPartition);
        return icebergInputPartition.scanTaskGroup();
    }
}
