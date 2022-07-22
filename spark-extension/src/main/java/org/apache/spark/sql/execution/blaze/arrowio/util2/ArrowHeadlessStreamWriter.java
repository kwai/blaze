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
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base class for implementing Arrow writers for IPC over a WriteChannel. */
public class ArrowHeadlessStreamWriter implements AutoCloseable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ArrowHeadlessStreamWriter.class);

  // schema with fields in message format, not memory format
  protected final Schema schema;

  protected final WriteChannel out;
  private final VectorUnloader unloader;
  protected IpcOption option;

  public ArrowHeadlessStreamWriter(VectorSchemaRoot root, WritableByteChannel out) {
    this.unloader = new VectorUnloader(root);
    this.out = new WriteChannel(out);
    this.option = IpcOption.DEFAULT;

    List<Field> fields = new ArrayList<>(root.getSchema().getFields().size());
    this.schema = new Schema(fields, root.getSchema().getCustomMetadata());
  }

  public void start() throws IOException {
    ensureStarted();
  }

  /** Writes the record batch currently loaded in this instance's VectorSchemaRoot. */
  public void writeBatch() throws IOException {
    ensureStarted();
    try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
      writeRecordBatch(batch);
    }
  }

  protected ArrowBlock writeRecordBatch(ArrowRecordBatch batch) throws IOException {
    ArrowBlock block = MessageSerializer.serialize(out, batch, option);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "RecordBatch at {}, metadata: {}, body: {}",
          block.getOffset(),
          block.getMetadataLength(),
          block.getBodyLength());
    }
    return block;
  }

  public void end() throws IOException {
    ensureStarted();
    ensureEnded();
  }

  public long bytesWritten() {
    return out.getCurrentPosition();
  }

  private void ensureStarted() {}

  private void ensureEnded() {}

  @Override
  public void close() {
    try {
      end();
      out.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
