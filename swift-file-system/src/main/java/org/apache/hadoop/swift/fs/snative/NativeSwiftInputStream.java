package org.apache.hadoop.swift.fs.snative;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrapper for input stream
 */
class NativeSwiftInputStream extends FSInputStream {

  /**
   * Default buffer size
   */
  private static final long BUFFER_SIZE = 1024 * 1024;

  /**
   * File store instance
   */
  private SwiftFileSystemStore store;

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

  public NativeSwiftInputStream(SwiftFileSystemStore store, FileSystem.Statistics statistics, Path path) {
    this.store = store;
    this.statistics = statistics;
    this.in = store.getObject(path, 0l, BUFFER_SIZE);
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
    try {
      result = in.read(b, off, len);
    } catch (IOException e) {
      seek(pos);
      result = in.read(b, off, len);
    }
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
    in = store.getObject(path, pos, BUFFER_SIZE);
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