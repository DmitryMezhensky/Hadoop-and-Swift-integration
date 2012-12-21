package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper for input stream
 */
class SwiftNativeInputStream extends FSInputStream {

  /**
   * Default buffer size 64mb
   */
  private static final long BUFFER_SIZE = 64 * 1024 * 1024;

  /**
   * File nativeStore instance
   */
  private SwiftNativeFileSystemStore nativeStore;

  /**
   * Hadoop statistics. Used to get info about number of reads, writes, etc.
   */
  private FileSystem.Statistics statistics;

  /**
   * Data input stream
   */
  private InputStream in;

  /**
   * File path
   */
  private final Path path;

  /**
   * Current position
   */
  private long pos = 0;

  public SwiftNativeInputStream(SwiftNativeFileSystemStore storeNative, FileSystem.Statistics statistics, Path path)
          throws IOException {
    this.nativeStore = storeNative;
    this.statistics = statistics;
    this.in = storeNative.getObject(path);
    this.path = path;
  }

  @Override
  public synchronized int read() throws IOException {
    int result;
    try {
      result = in.read();
    } catch (IOException e) {
      seek(pos);
      result = in.read();
    }
    if (result != -1) {
      pos++;
    }
    if (statistics != null && result != -1) {
      statistics.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    int result;
    result = in.read(b, off, len);
    if (result > 0) {
      pos += result;
    }
    if (statistics != null && result > 0) {
      statistics.incrementBytesRead(result);
    }

    return result;
  }

  @Override
  public void close() throws IOException {
    if (in != null)
      in.close();
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    in.close();
    in = nativeStore.getObject(path, pos, pos + BUFFER_SIZE);
    this.pos = pos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}