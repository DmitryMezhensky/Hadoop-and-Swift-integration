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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;

import java.io.*;

/**
 * Output stream, buffers data on local disk.
 * Writes to Swift on close() method
 */
class SwiftNativeOutputStream extends OutputStream {
  private long FILE_PART_SIZE = 4768709000l; // files greater than 4.5Gb are divided into parts
  private static final Log LOG =
          LogFactory.getLog(SwiftNativeOutputStream.class);
  private Configuration conf;
  private String key;
  private File backupFile;
  private OutputStream backupStream;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private int partNumber;
  private long blockSize;
  private boolean partUpload = false;
  private boolean abortWrite = false;

  public SwiftNativeOutputStream(Configuration conf,
                                 SwiftNativeFileSystemStore nativeStore,
                                 String key) throws IOException {
    this.conf = conf;
    this.key = key;
    this.backupFile = newBackupFile();
    this.nativeStore = nativeStore;
    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    this.partNumber = 1;
    this.blockSize = 0;
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("hadoop.tmp.dir"));
    if (!dir.mkdirs() && !dir.exists()) {
      throw new SwiftException("Cannot create Swift buffer directory: " + dir);
    }
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  /**
   * check that the output stream is open
   *
   * @throws SwiftException if it is not
   */
  private synchronized void verifyOpen() throws SwiftException {
    if (closed) {
      throw new SwiftException("Output stream is closed");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    //formally declare as closed.
    closed = true;
    backupStream.close();

    try {
      if (!abortWrite) {
        if (partUpload) {
          partUpload();
          nativeStore.createManifestForPartUpload(new Path(key));
        } else {
          nativeStore.uploadFile(new Path(key),
                  new FileInputStream(backupFile),
                  backupFile.length());
        }
      }
    } finally {
      if (!backupFile.delete()) {
        LOG.warn("Could not delete " + backupFile);
      }
      backupStream = null;
    }
  }

  @Override
  public void write(int b) throws IOException {
    verifyOpen();
    backupStream.write(b);
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    //validate args
    if (off < 0 || len < 0 || (off + len) > b.length) {
      throw new IndexOutOfBoundsException("Invalid offset/length for write");
    }
    verifyOpen();

    //if size of file is greater than 5Gb Swift limit - than divide file into parts and upload parts
    if (blockSize + len >= FILE_PART_SIZE) {
      partUpload();
    }

    blockSize += len;
    backupStream.write(b, off, len);
  }

  private void partUpload() throws IOException {
    partUpload = true;
    backupStream.close();
    nativeStore.uploadFilePart(new Path(key),
            partNumber,
            new FileInputStream(backupFile),
            backupFile.length());
    backupFile.delete();
    backupFile = newBackupFile();
    backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    blockSize = 0;
    partNumber++;
  }

  /**
   * Cancel the write-on-close operation. This permits a faster bailout
   * during some failures.
   */
  public void abortWrite() {
    abortWrite = true;
  }
}