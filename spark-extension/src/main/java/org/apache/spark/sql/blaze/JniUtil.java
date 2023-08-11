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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class JniUtil {
    public static void readFullyFromFSDataInputStream(FSDataInputStream in, long pos, ByteBuffer buf)
            throws IOException {

        synchronized (in) {
            if (pos != in.getPos()) {
                in.seek(pos);
            }
            ReadableByteChannel channel = Channels.newChannel(in);

            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) {
                    throw new EOFException("readFullyFromFSDataInputStream() got unexpected EOF");
                }
            }
        }
    }

    public static void writeFullyToFSDataOutputStream(FSDataOutputStream out, ByteBuffer buf) throws IOException {

        synchronized (out) {
            WritableByteChannel channel = Channels.newChannel(out);

            while (buf.hasRemaining()) {
                if (channel.write(buf) == -1) {
                    throw new EOFException("writeFullyToFSDataOutputStream() got unexpected EOF");
                }
            }
        }
    }
}
