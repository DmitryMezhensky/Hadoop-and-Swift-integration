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

package org.apache.hadoop.fs.swift.block;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.Block;
import org.apache.hadoop.fs.s3.FileSystemStore;
import org.apache.hadoop.fs.s3.INode;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystemStore;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Block store for Swift. Implements Hadoop S3 FileSystemStore interface.
 */
public class SwiftBlockFileSystemStore implements FileSystemStore {
  private static final String FILE_SYSTEM_VERSION_VALUE = "1";
  private static final int DEFAULT_BUFFER_SIZE = 67108864;    //64 mb
  private static final String BLOCK_PREFIX = "block_";
  public static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";

  private Configuration conf;
  private SwiftRestClient swiftRestClient;
  private URI uri;

  private int bufferSize;

  public void initialize(URI uri, Configuration conf) throws IOException {
    this.conf = conf;
    this.uri = uri;
    this.swiftRestClient = SwiftRestClient.getInstance(uri, conf);
    this.bufferSize = conf.getInt(IO_FILE_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
  }

  public String getVersion() throws IOException {
    return FILE_SYSTEM_VERSION_VALUE;
  }

  private void delete(String key) throws IOException {

    swiftRestClient.delete(SwiftObjectPath.fromPath(uri, keyToPath(key)));
  }

  public void deleteINode(Path path) throws IOException {
    delete(pathToKey(path));
  }

  public void deleteBlock(Block block) throws IOException {
    delete(blockToKey(block));
  }

  public boolean inodeExists(Path path) throws IOException {
    InputStream in = get(pathToKey(path));
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }

  public boolean blockExists(long blockId) throws IOException {
    InputStream in = get(blockToKey(blockId));
    if (in == null) {
      return false;
    }
    in.close();
    return true;
  }

  /**
   * Get the data at the end of the key -on any failure the input stream
   * is closed.
   * @param key object in the store
   * @return a stream to get at the object -or null if not
   * @throws IOException IO problem
   */
  private InputStream get(String key) throws IOException {
    InputStream inputStream = null;
    try {
      inputStream =
        swiftRestClient.getDataAsInputStream(
          SwiftObjectPath.fromPath(uri, keyToPath(key))
          , SwiftRestClient.NEWEST);
      inputStream.available();
      return inputStream;
    } catch (NullPointerException e) {
      IOUtils.closeQuietly(inputStream);
      return null;
    } catch (IOException e) {
      //cleanup
      IOUtils.closeQuietly(inputStream);
      //rethrow
      throw e;
    }
  }

  /**
   * Get the input stream starting from a specific point.
   * @param key object key
   * @param byteRangeStart starting point
   * @param length no. of bytes
   * @return an input stream
   * @throws IOException IO problems
   */
  private InputStream get(String key, long byteRangeStart, long length) throws IOException {

    return swiftRestClient.getDataAsInputStream(SwiftObjectPath.fromPath(uri, keyToPath(key)), byteRangeStart, length);
  }

  public INode retrieveINode(Path path) throws IOException {
    return INode.deserialize(get(pathToKey(path)));
  }

  public File retrieveBlock(Block block, long byteRangeStart)
          throws IOException {
    File fileBlock = null;
    InputStream in = null;
    OutputStream out = null;
    try {
      fileBlock = newBackupFile();
      in = get(blockToKey(block), byteRangeStart, block.getLength() - byteRangeStart);
      out = new BufferedOutputStream(new FileOutputStream(fileBlock));
      byte[] buf = new byte[bufferSize];
      int numRead;
      while ((numRead = in.read(buf)) >= 0) {
        out.write(buf, 0, numRead);
      }
      return fileBlock;
    } catch (IOException e) {
      closeQuietly(out);
      out = null;
      if (fileBlock != null) {
        fileBlock.delete();
      }
      throw e;
    } finally {
      closeQuietly(out);
      closeQuietly(in);
    }
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("hadoop.tmp.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create Swift buffer directory: " + dir);
    }
    File result = File.createTempFile("input-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  public Set<Path> listSubPaths(Path path) throws IOException {
    String uriString = path.toString();
    if (!uriString.endsWith(Path.SEPARATOR)) {
      uriString += Path.SEPARATOR;
    }

    InputStream inputStream = null;
    try {
      inputStream =
        swiftRestClient.getDataAsInputStream(
          SwiftObjectPath.fromPath(uri, path),
          SwiftRestClient.NEWEST);
      final ByteArrayOutputStream data = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024 * 1024]; // 1 mb

      while (inputStream.read(buffer) > 0) {
        data.write(buffer);
      }

      final StringTokenizer tokenizer = new StringTokenizer(new String(data.toByteArray()), "\n");

      final Set<Path> paths = new HashSet<Path>();
      while (tokenizer.hasMoreTokens()) {
        paths.add(new Path(tokenizer.nextToken()));
      }

      return paths;
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
  }

  public Set<Path> listDeepSubPaths(Path path) throws IOException {
    String uriString = path.toString();
    if (!uriString.endsWith(Path.SEPARATOR)) {
      uriString += Path.SEPARATOR;
    }

    final byte[] buffer;
    try {
      buffer =
        swiftRestClient.findObjectsByPrefix(SwiftObjectPath.fromPath(uri, path));
    } catch (FileNotFoundException e) {
      return Collections.emptySet();
    }
    final StringTokenizer tokenizer = new StringTokenizer(new String(buffer), "\n");

    final Set<Path> paths = new HashSet<Path>();
    while (tokenizer.hasMoreTokens()) {
      paths.add(new Path(tokenizer.nextToken()));
    }

    return paths;
  }

  private void put(String key, InputStream in, long length)
          throws IOException {

    swiftRestClient.upload(SwiftObjectPath.fromPath(uri, keyToPath(key)), in, length);
  }

  public void storeINode(Path path, INode inode) throws IOException {
    put(pathToKey(path), inode.serialize(), inode.getSerializedLength());
  }

  public void storeBlock(Block block, File file) throws IOException {
    BufferedInputStream in = null;
    try {
      in = new BufferedInputStream(new FileInputStream(file));
      put(blockToKey(block), in, block.getLength());
    } finally {
      closeQuietly(in);
    }
  }

  public List<URI> getObjectLocation(Path path) throws IOException {
    final byte[] objectLocation;
    try {
      objectLocation = swiftRestClient.getObjectLocation(SwiftObjectPath.fromPath(uri, path));
    } catch (SwiftException e) {
      throw new IOException(e);
    }
    return SwiftNativeFileSystemStore.extractUris(new String(objectLocation));
  }

  private void closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }
    return path.toUri().getPath();
  }

  private Path keyToPath(String key) {
    return new Path(key);
  }

  private String blockToKey(long blockId) {
    return BLOCK_PREFIX + blockId;
  }

  private String blockToKey(Block block) {
    return blockToKey(block.getId());
  }

  /**
   * Deletes ALL objects from container
   * Used in testing
   *
   * @throws IOException
   */
  public void purge() throws IOException {
    final Set<Path> paths = listSubPaths(new Path("/"));
    for (Path path : paths) {
      swiftRestClient.delete(SwiftObjectPath.fromPath(uri, path));
    }

  }

  /**
   * Dumps content of file system
   * Used for testing
   *
   * @throws IOException
   */
  public void dump() throws IOException {

    //this method is used for testing
  }
}
