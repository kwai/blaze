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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.deploy.SparkHadoopUtil;

public class HDFSObjectStoreBridge {

  public static long size(String location) throws IOException, URISyntaxException {
    FileSystem fs = getFileSystem(location);
    Path path = new Path(location);
    FileStatus fileStatus = fs.getFileStatus(path);
    return fileStatus.getLen();
  }

  public static void read(String location, long offset, ByteBuffer buf)
      throws IOException, URISyntaxException {
    FileSystem fs = getFileSystem(location);
    Path path = new Path(location);

    try (FSDataInputStream in = fs.open(path)) {
      in.seek(offset);
      ReadableByteChannel channel = Channels.newChannel(in);

      while (true) {
        if (!buf.hasRemaining() || channel.read(buf) < 0) {
          break;
        }
      }
      if (buf.hasRemaining()) {
        throw new EOFException();
      }
    }
  }

  private static FileSystem getFileSystem(String location) throws IOException, URISyntaxException {
    URI uri = new URI(location);
    FileSystem fs = FileSystem.get(uri, SparkHadoopUtil.get().conf());
    return fs;
  }
}
