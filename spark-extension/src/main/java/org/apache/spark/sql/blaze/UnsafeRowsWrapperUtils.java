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

import org.apache.spark.sql.catalyst.InternalRow;

// for jni_bridge usage

public class UnsafeRowsWrapperUtils {

    public static void serialize(
            InternalRow[] unsafeRows, int numFields, Long importFFIArrayPtr, Long exportFFIArrayPtr) {
        UnsafeRowsWrapper$.MODULE$.serialize(unsafeRows, numFields, importFFIArrayPtr, exportFFIArrayPtr);
    }

    public static InternalRow[] deserialize(int numFields, Long importFFIArrayPtr, Long exportFFIArrayPtr) {
        return UnsafeRowsWrapper$.MODULE$.deserialize(numFields, importFFIArrayPtr, exportFFIArrayPtr);
    }

    public static int getRowNum(InternalRow[] unsafeRows) {
        return UnsafeRowsWrapper$.MODULE$.getRowNum(unsafeRows);
    }

    public static InternalRow[] getEmptyObject(int rowNum) {
        return UnsafeRowsWrapper$.MODULE$.getNullObject(rowNum);
    }
}
