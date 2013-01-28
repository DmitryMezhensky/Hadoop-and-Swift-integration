/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  public SwiftNativeInputStream(SwiftNativeFileSystemStore storeNative,
                                FileSystem.Statistics statistics,
                                Path path)
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

  /**
   * close the stream. After this the stream is not usable.
   * This method is thread-safe and idempotent.
   * @throws IOException on IO problems.
   */
  @Override
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      in = null;
    }
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    close();
    in = nativeStore.getObject(path, targetPos, targetPos + BUFFER_SIZE);
    this.pos = targetPos;
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