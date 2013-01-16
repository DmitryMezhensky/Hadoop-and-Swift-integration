package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Output stream, buffers data on local disk.
 * Writes to Swift on close() method
 */
class SwiftNativeOutputStream extends OutputStream {
  private static final Log LOG = LogFactory.getLog(SwiftNativeOutputStream.class);
  private static final long FILE_PART_SIZE = 4768709000l; // files greater than 4.5Gb are divided into parts

  private Configuration conf;
  private String key;
  private File backupFile;
  private OutputStream backupStream;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private int partNumber;
  private long blockSize;
  private boolean partUpload = false;

  public SwiftNativeOutputStream(Configuration conf, SwiftNativeFileSystemStore nativeStore, String key) throws IOException {
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
      if (partUpload) {
        partUpload();
        nativeStore.createManifestForPartUpload(new Path(key));
      } else {
        nativeStore.uploadFile(new Path(key), new FileInputStream(backupFile), backupFile.length());
      }
    } finally {
      if (!backupFile.delete()) {
        LOG.warn("couldn't delete " + backupFile + " file");
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
    //if size of file is greater than 5Gb Swift limit - than divide file into parts and upload parts
    if (blockSize + len >= FILE_PART_SIZE) {
      partUpload();
    }

    blockSize += len;
    backupStream.write(b, off, len);
  }

  private void partUpload() throws IOException {
    partUpload = true;
    nativeStore.uploadFilePart(new Path(key), partNumber, new FileInputStream(backupFile), backupFile.length());
    backupStream.close();
    if (!backupFile.delete()) {
      LOG.warn("couldn't delete " + backupFile + " file");
    }
    backupFile = newBackupFile();
    backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    blockSize = 0;
    partNumber++;
  }
}