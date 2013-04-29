/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.util.SwiftUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * The input stream from remote Swift blobs.
 * The class attempts to be buffer aware, and react to a forward seek operation
 * by trying to scan ahead through the current block of data to find it.
 * This accelerates some operations that do a lot of seek()/read() actions,
 * including work (such as in the MR engine) that do a seek() immediately after
 * an open().
 */
class SwiftNativeInputStream extends FSInputStream {

  private static final Log LOG = LogFactory.getLog(SwiftNativeInputStream.class);

  /**
   * range requested off the server: {@value}
   */
  private static final long RANGE_SIZE = 64 * 1024 * 1024;

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

  /**
   * Offset in the range requested last
   */
  private long rangeOffset = 0;

  public SwiftNativeInputStream(SwiftNativeFileSystemStore storeNative,
                                FileSystem.Statistics statistics,
                                Path path)
          throws IOException {
    this.nativeStore = storeNative;
    this.statistics = statistics;
    this.in = storeNative.getObject(path);
    this.path = path;
  }

  /**
   * Move to a new position within the file relative to where the pointer is now.
   * Always call from a synchronized clause
   * @param offset offset
   */
  private synchronized void incPos(int offset) {
    pos += offset;
    rangeOffset += offset;
    SwiftUtils.trace(LOG, "Inc: pos=%d bufferOffset=%d", pos, rangeOffset);
  }

  /**
   * Update the start of the buffer; always call from a sync'd clause
   * @param seekPos position sought.
   */
  private synchronized void updateStartOfBufferPosition(long seekPos) {
    //reset the seek pointer
    pos = seekPos;
    //and put the buffer offset to 0
    rangeOffset = 0;
    SwiftUtils.trace(LOG, "Move: pos=%d bufferOffset=%d", pos, rangeOffset);
  }

  @Override
  public synchronized int read() throws IOException {
    int result;
    try {
      result = in.read();
    } catch (IOException e) {
      String msg = "IOException while reading " + path
              + ": ' +e, attempting to reopen.";
      LOG.info(msg);
      LOG.debug(msg, e);

      seek(pos);
      result = in.read();
    }
    if (result != -1) {
      incPos(1);
    }
    if (statistics != null && result != -1) {
      statistics.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    int result = -1;
    try {
      result = in.read(b, off, len);
    } catch (IOException e) {
      LOG.info("Received IOException while reading '" + path +
              "', attempting to reopen.");
      seek(pos);
      result = in.read(b, off, len);
    }
    if (result > 0) {
      incPos(result);
      if (statistics != null) {
        statistics.incrementBytesRead(result);
      }
    }

    return result;
  }

  /**
   * close the stream. After this the stream is not usable.
   * This method is thread-safe and idempotent.
   *
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

  /**
   * Treats any finalize() call without the input stream being closed
   * as a serious problem, logging at error level
   * @throws Throwable n/a
   */
  @Override
  protected void finalize() throws Throwable {
    if (in != null) {
      LOG.error(
              "Input stream is leaking handles by not being closed() properly!");
    }
  }

  /**
   * Read through the specified number of bytes.
   * The implementation iterates a byte a time, which may seem inefficient
   * compared to the read(bytes[]) method offered by input streams.
   * However, if you look at the code that implements that method, it comes
   * down to read() one char at a time -only here the return value is discarded.
   * @param bytes number of bytes to read.
   * @throws IOException IO problems
   * @throws SwiftException if a read returned -1.
   */
  private void chompBytes(long bytes) throws IOException {
    int result;
    for (long i = 0; i < bytes; i++) {
      result = in.read();
      if (result <= 0) {
        throw new SwiftException("Received error code while chomping input");
      }
      incPos(1);
    }
  }

  /**
   * Seek to an offset. If the data is already in the buffer, move to it
   * @param targetPos target position
   * @throws IOException on any problem
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Seek to " + targetPos);
    }
    //there's some special handling of near-local data
    //as the seek can be omitted if it is in/adjacent
    long offset = targetPos - pos;
    if (offset == 0) {
      LOG.debug("seek is no-op");
      return;
    }

    if (rangeOffset + offset < RANGE_SIZE) {
      //if the seek is in range of that requested, scan forwards
      //instead of closing and re-opening a new HTTP connection
      SwiftUtils.debug(LOG,
              "seek is within current stream"
                      + "; pos= %d ; targetPos=%d; "
                      + "offset= %d ; bufferOffset=%d",
              pos, targetPos, offset, rangeOffset);
      try {
        LOG.debug("chomping ");
        chompBytes(offset);
      } catch (IOException e) {
        //this is assumed to be recoverable with a seek -or more likely to fail
        LOG.debug("while chomping ",e);
      }
      if (targetPos - pos == 0) {
        LOG.trace("chomping successful");
        return;
      }
      LOG.trace("chomping failed");
    }

    close();
    in = nativeStore.getObject(path, targetPos, targetPos + RANGE_SIZE);
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