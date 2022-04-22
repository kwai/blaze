package org.blaze;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import org.apache.spark.network.util.JavaUtils;

public class FileSegmentSeekableByteChannel implements SeekableByteChannel {
  private final long offset;
  private final long length;
  private File file;
  private int position = 0;

  public FileSegmentSeekableByteChannel(File file, long offset, long length) {
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    FileChannel channel = null;
    try {
      channel = (new RandomAccessFile(this.file, "r")).getChannel();
      return channel.read(dst, this.offset + position);
    } catch (IOException e) {
      String errorMessage = "Error in reading " + this;
      try {
        if (channel != null) {
          long size = channel.size();
          errorMessage = "Error in reading " + this + " (actual file length " + size + ")";
        }
      } catch (IOException ignored) {
        // ignore
      }
      throw new IOException(errorMessage, e);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  @Override
  public long position() throws IOException {
    return this.position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    this.position = (int) newPosition;
    return this;
  }

  @Override
  public long size() throws IOException {
    return this.length;
  }

  @Override
  public boolean isOpen() {
    return this.file != null;
  }

  @Override
  public void close() throws IOException {
    this.file = null;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Read only");
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Read only");
  }
}
