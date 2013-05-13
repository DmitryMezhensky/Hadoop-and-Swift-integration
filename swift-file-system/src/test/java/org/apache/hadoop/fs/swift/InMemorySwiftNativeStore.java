/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.snative.SwiftFileStatus;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystemStore;

import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * In-memory Swift emulator for tests.
 * <p/>
 * This is very unrealistic, and so the functional tests should be preferred.
 */
public class InMemorySwiftNativeStore extends SwiftNativeFileSystemStore {
  private static final Log LOG = LogFactory.getLog(InMemorySwiftNativeStore.class);
  private SortedMap<String, FileStatus> metadataMap =
          new TreeMap<String, FileStatus>();
  private SortedMap<String, byte[]> dataMap = new TreeMap<String, byte[]>();

  @Override
  public void initialize(URI fsURI, Configuration conf) {
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
      IOUtils.closeQuietly(inputStream);
    }
    metadataMap.put(path.toUri().toString(),
            new FileStatus(size, false, 0, 0, System.currentTimeMillis(),
                    path));
    dataMap.put(path.toUri().toString(), out.toByteArray());
  }

  @Override
  public SwiftFileStatus getObjectMetadata(Path path) throws IOException {
    FileStatus status = metadataMap.get(path.toUri().toString());
    if (status == null) {
      throw new FileNotFoundException("Not found " + path.toUri());
    }
    return (SwiftFileStatus) status;
  }

  @Override
  public InputStream getObject(Path path) throws IOException {
    return getObject(path, 0, 0);
  }

  @Override
  public InputStream getObject(Path path, long byteRangeStart, long length) throws
          IOException {
    byte[] data = dataMap.get(path.toUri().toString());
    if (data == null) {
      throw new FileNotFoundException("Not found" + path.toUri());
    }
    return new ByteArrayInputStream(data);
  }

  @Override
  public void createDirectory(Path path) {
    metadataMap.put(path.toUri().toString(),
            new FileStatus(0, false, 0, 0, System.currentTimeMillis(),
                    path));
  }

  @Override
  public List<URI> getObjectLocation(Path path) throws IOException {
    throw new UnsupportedOperationException("not implemented for testing purposes");
  }

  @Override
  public boolean deleteObject(Path path) throws IOException {
    boolean found = null != metadataMap.remove(path.toUri().toString());
    dataMap.remove(path.toUri().toString());
    return found;
  }

  @Override
  public boolean objectExists(Path path) {
    return metadataMap.containsKey(path.toUri().toString());
  }

  @Override
  public void rename(Path src, Path dst) throws IOException {
    if (src.equals(dst)) {
      throw new SwiftOperationFailedException("source equals dest");
    }

    final FileStatus fileStatus = metadataMap.get(src.toUri().toString());
    if (fileStatus == null) {
      throw new SwiftOperationFailedException("source does not exist");
    }

    metadataMap.remove(src.toUri().toString());
    metadataMap.put(dst.toUri().toString(), fileStatus);
    if (!fileStatus.isDir()) {
      final byte[] bytes = dataMap.get(src.toUri().toString());
      dataMap.remove(src.toUri().toString());
      dataMap.put(dst.toUri().toString(), bytes);
    }
  }
}
