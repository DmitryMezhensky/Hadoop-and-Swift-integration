package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystemStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * In-memory Swift emulator for tests
 */
public class InMemorySwiftNativeStore extends SwiftNativeFileSystemStore {
  private SortedMap<String, FileStatus> metadataMap =
          new TreeMap<String, FileStatus>();
  private SortedMap<String, byte[]> dataMap = new TreeMap<String, byte[]>();

  public void initialize(URI uri, Configuration conf) {
    //nothing to do
  }

  @Override
  public void uploadFile(Path path, InputStream inputStream, long length) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int numRead;
    long size = 0;
    try {
      while ((numRead = inputStream.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
        size += numRead;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    metadataMap.put(path.toUri().toString(), new FileStatus(size, false, 0, 0, System.currentTimeMillis(), path));
    dataMap.put(path.toUri().toString(), out.toByteArray());
  }

  @Override
  public FileStatus getObjectMetadata(Path path) {
    return metadataMap.get(path.toUri().toString());
  }

  @Override
  public InputStream getObject(Path path) {
    return getObject(path, 0, 0);
  }

  @Override
  public InputStream getObject(Path path, long byteRangeStart, long length) {
    byte[] data = dataMap.get(path.toUri().toString());

    return new ByteArrayInputStream(data);
  }

  @Override
  public FileStatus[] listSubPaths(Path path) throws IOException {
    throw new UnsupportedOperationException("not implemented for testing purposes");
  }

  @Override
  public void createDirectory(Path path) {
    metadataMap.put(path.toUri().toString(), new FileStatus(0, false, 0, 0, System.currentTimeMillis(), path));
  }

  @Override
  public List<URI> getObjectLocation(Path path) throws IOException {
    //TODO
    return super.getObjectLocation(path);
  }

  @Override
  public void deleteObject(Path path) throws IOException {
    metadataMap.remove(path.toUri().toString());
    dataMap.remove(path.toUri().toString());
  }

  @Override
  public boolean objectExists(Path path) {
    return metadataMap.containsKey(path.toUri().toString());
  }

  @Override
  public boolean renameDirectory(Path src, Path dst) throws IOException {
    if (src.equals(dst))
      return false;

    final FileStatus fileStatus = metadataMap.get(src.toUri().toString());
    if (fileStatus == null)
      return false;

    metadataMap.remove(src.toUri().toString());
    metadataMap.put(dst.toUri().toString(), fileStatus);
    if (!fileStatus.isDir()) {
      final byte[] bytes = dataMap.get(src.toUri().toString());
      dataMap.remove(src.toUri().toString());
      dataMap.put(dst.toUri().toString(), bytes);
    }

    return true;
  }
}
