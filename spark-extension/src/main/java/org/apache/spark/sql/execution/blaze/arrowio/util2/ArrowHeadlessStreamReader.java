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

package org.apache.spark.sql.execution.blaze.arrowio.util2;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;

/** This class reads from an input stream and produces ArrowRecordBatches. */
public class ArrowHeadlessStreamReader extends ArrowReader {

  private MessageChannelReader messageReader;
  private Schema schema;

  public ArrowHeadlessStreamReader(
      ReadableByteChannel in, BufferAllocator allocator, StructType schema, String timeZoneId) {
    super(allocator, NoCompressionCodec.Factory.INSTANCE);
    this.messageReader = new MessageChannelReader(new ReadChannel(in), allocator);
    this.schema = ArrowUtils2.toArrowSchema(schema, timeZoneId);
  }

  /**
   * Get the number of bytes read from the stream since constructing the reader.
   *
   * @return number of bytes
   */
  @Override
  public long bytesRead() {
    return messageReader.bytesRead();
  }

  /**
   * Closes the underlying read source.
   *
   * @throws IOException on error
   */
  @Override
  protected void closeReadSource() throws IOException {
    messageReader.close();
  }

  /**
   * Load the next ArrowRecordBatch to the vector schema root if available.
   *
   * @return true if a batch was read, false on EOS
   * @throws IOException on error
   */
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    MessageResult result = messageReader.readNext();

    // Reached EOS
    if (result == null) {
      return false;
    }

    if (result.getMessage().headerType() == MessageHeader.RecordBatch) {
      ArrowBuf bodyBuffer = result.getBodyBuffer();

      // For zero-length batches, need an empty buffer to deserialize the batch
      if (bodyBuffer == null) {
        bodyBuffer = allocator.getEmpty();
      }

      ArrowRecordBatch batch =
          MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
      loadRecordBatch(batch);
      return true;
    } else {
      throw new IOException(
          "Expected RecordBatch or DictionaryBatch but header was "
              + result.getMessage().headerType());
    }
  }

  @Override
  protected Schema readSchema() {
    return this.schema;
  }
}
