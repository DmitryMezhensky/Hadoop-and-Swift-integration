package org.apache.hadoop.swift.fs.snative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Output stream, buffers data on local disk.
 * Writes to Swift on close() method
 */
class NativeSwiftOutputStream extends OutputStream {

  private Configuration conf;
  private String key;
  private File backupFile;
  private OutputStream backupStream;
  private SwiftFileSystemStore store;
  private boolean closed;

  public NativeSwiftOutputStream(Configuration conf, SwiftFileSystemStore store, String key) throws IOException {
    this.conf = conf;
    this.key = key;
    this.backupFile = newBackupFile();
    this.store = store;
    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("hadoop.tmp.dir"));
    if (!dir.mkdirs() && !dir.exists()) {
      throw new IOException("Cannot create Swift buffer directory: " + dir);
    }
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    backupStream.close();

    try {
      store.uploadFile(new Path(key), new FileInputStream(backupFile), backupFile.length());
    } finally {
      if (!backupFile.delete()) {
      }
      super.close();
      closed = true;
    }
  }

  @Override
  public void write(int b) throws IOException {
    backupStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    backupStream.write(b, off, len);
  }
}