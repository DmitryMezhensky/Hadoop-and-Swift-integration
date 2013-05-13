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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;

import java.io.*;

/**
 * Output stream, buffers data on local disk.
 * Writes to Swift on close() method
 */
class SwiftNativeOutputStream extends OutputStream {
  private long filePartSize = 4768709000L; // files greater than 4.5Gb are divided into parts
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

    try {
      closed = true;
      //formally declare as closed.
      backupStream.close();
      Path keypath = new Path(key);
      if (partUpload) {
        partUpload();
        nativeStore.createManifestForPartUpload(keypath);
      } else {
        nativeStore.uploadFile(keypath,
                new FileInputStream(backupFile),
                backupFile.length());
      }
    } finally {
      delete(backupFile);
      backupStream = null;
      backupFile = null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    if (!closed) {
      LOG.warn("stream not closed");
    }
    if (backupFile != null) {
      LOG.warn("Leaking backing file " + backupFile);
    }
  }

  private void delete(File file) {
    if (!file.delete()) {
      LOG.warn("Could not delete " + file);
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
    if (blockSize + len >= filePartSize) {
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
    delete(backupFile);
    backupFile = newBackupFile();
    backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    blockSize = 0;
    partNumber++;
  }

  /**
   * Partition size can be set for testing purposes.
   * This is intended for testing
   *
   * @param filePartSize new partition size
   */
  synchronized void setFilePartSize(long filePartSize) {
    this.filePartSize = filePartSize;
  }

  /**
   * Query the number of partitions written
   * This is intended for testing
   *
   * @return the of partitions already written to the remote FS
   */
  synchronized int getPartitionsWritten() {
    return partNumber - 1;
  }
}