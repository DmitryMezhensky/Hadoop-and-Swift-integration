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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.Block;
import org.apache.hadoop.fs.s3.INode;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation storing data in Swift as array ob blocks.
 * another applications can't read data in such representation
 */
public class SwiftBlockFileSystem extends FileSystem {
  private static final Log LOG = LogFactory.getLog(SwiftBlockFileSystem.class);
  /**
   * fs URI
   */
  private URI uri;

  /**
   * File system store instance
   */
  private SwiftBlockFileSystemStore store;

  /**
   * temporary working dir
   */
  private Path workingDir;

  /**
   * class initialization
   *
   * @param uri  fs URI
   * @param conf fs configuration
   * @throws IOException
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    if (store == null) {
      store = new SwiftBlockFileSystemStore();
      store.initialize(uri, conf);
    }
    store.initialize(uri, conf);
    this.uri = URI.create(String.format("bswift://%s:%d", uri.getHost(), uri.getPort()));
    this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this);
  }

  /**
   * @return fs URI
   */
  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * @return path to working dir
   */
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * @param dir fs working directory
   */
  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(path);
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);

    boolean result = true;
    for (Path p : paths) {
      if (p.getParent() == null)
        continue;
      result &= mkdir(p);
    }
    return result;
  }

  @Override
  public boolean isFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      return new FileStatus[]{};
    }
    if (inode.isFile()) {
      return new FileStatus[]{getFileStatus(f.makeQualified(this), inode)
      };
    }
    ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
    for (Path p : store.listSubPaths(absolutePath)) {
      ret.add(getFileStatus(p.makeQualified(this)));
    }
    return ret.toArray(new FileStatus[ret.size()]);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  private boolean mkdir(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      store.storeINode(absolutePath, INode.DIRECTORY_INODE);
    } else if (inode.isFile()) {
      throw new IOException(String.format(
              "Can't make directory for path %s since it is a file.",
              absolutePath));
    }
    return true;
  }

  private INode checkFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      throw new IOException("No such file.");
    }
    if (inode.isDirectory()) {
      throw new IOException("Path " + path + " is a directory.");
    }
    return inode;
  }

  /**
   * This operation is not supported yet.
   */
  public FSDataOutputStream append(Path f, int bufferSize,
                                   Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
          throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new IOException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new IOException("Mkdirs failed to create " + parent.toString());
        }
      }
    }
    return new FSDataOutputStream
            (new SwiftBlockOutputStream(getConf(), store, makeAbsolute(file),
                    blockSize, bufferSize),
                    statistics);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    INode inode = checkFile(path);
    return new FSDataInputStream(new SwiftBlockInputStream(store, inode, statistics));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteSrc = makeAbsolute(src);
    INode srcINode = store.retrieveINode(absoluteSrc);
    if (srcINode == null) {
      // src path doesn't exist
      return false;
    }
    Path absoluteDst = makeAbsolute(dst);
    INode dstINode = store.retrieveINode(absoluteDst);
    if (dstINode != null && dstINode.isDirectory()) {
      absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
      dstINode = store.retrieveINode(absoluteDst);
    }
    if (dstINode != null) {
      // dst path already exists - can't overwrite
      return false;
    }
    Path dstParent = absoluteDst.getParent();
    if (dstParent != null) {
      INode dstParentINode = store.retrieveINode(dstParent);
      if (dstParentINode == null || dstParentINode.isFile()) {
        // dst parent doesn't exist or is a file
        return false;
      }
    }
    return renameRecursive(absoluteSrc, absoluteDst);
  }

  private boolean renameRecursive(Path src, Path dst) throws IOException {
    INode srcINode = store.retrieveINode(src);
    store.storeINode(dst, srcINode);
    store.deleteINode(src);
    if (srcINode.isDirectory()) {
      for (Path oldSrc : store.listDeepSubPaths(src)) {
        INode inode = store.retrieveINode(oldSrc);
        if (inode == null) {
          return false;
        }
        String oldSrcPath = oldSrc.toUri().getPath();
        String srcPath = src.toUri().getPath();
        String dstPath = dst.toUri().getPath();
        Path newDst = new Path(oldSrcPath.replaceFirst(srcPath, dstPath));
        store.storeINode(newDst, inode);
        store.deleteINode(oldSrc);
      }
    }
    return true;
  }

  public boolean delete(Path path, boolean recursive) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      return false;
    }
    if (inode.isFile()) {
      store.deleteINode(absolutePath);
      for (Block block : inode.getBlocks()) {
        store.deleteBlock(block);
      }
    } else {
      FileStatus[] contents = listStatus(absolutePath);
      if (contents == null) {
        return false;
      }
      if ((contents.length != 0) && (!recursive)) {
        throw new IOException("Directory " + path.toString()
                + " is not empty.");
      }
      for (FileStatus p : contents) {
        if (!delete(p.getPath(), recursive)) {
          return false;
        }
      }
      store.deleteINode(absolutePath);
    }
    return true;
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  /**
   * FileStatus for Swift file systems.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new FileNotFoundException(f + ": No such file or directory.");
    }
    try {
      return getFileStatus(getCorrectSwiftPath(f), inode);
    } catch (URISyntaxException e) {
      throw new IOException("path " + f + " is incorrect", e);
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
    final INode iNode = store.retrieveINode(file.getPath());
    final BlockLocation[] blockLocations = new BlockLocation[iNode.getBlocks().length];

    int idx = 0;
    long offset = 0l;
    for (Block block : iNode.getBlocks()) {
      final List<URI> locations =
              store.getObjectLocation(new Path(new SwiftObjectPath(uri.getHost(), String.valueOf(block.getId())).toString()));
      final String[] names = new String[locations.size()];
      final String[] hosts = new String[locations.size()];
      int i = 0;
      for (URI uri : locations) {
        hosts[i] = uri.getHost();
        names[i] = uri.getAuthority();
        i++;
      }
      blockLocations[idx++] = new BlockLocation(names, hosts, offset, block.getLength());
      offset += block.getLength();
      LOG.debug("block location: " + Arrays.toString(names) +
              " hosts  " + Arrays.toString(hosts) + " : length: " + block.getLength());
    }

    return blockLocations;
  }

  @Override
  public long getDefaultBlockSize() {
    //64 mb
    return 64 * 1024 * 1024;
  }

  private FileStatus getFileStatus(Path f, INode inode) throws IOException {

    return new FileStatus(getFileLength(inode), inode.isDirectory(), 1,
            getBlockSize(inode), 0, f);
  }

  private long getFileLength(INode inode) {
    if (!inode.isDirectory()) {
      long length = 0L;
      for (Block block : inode.getBlocks()) {
        length += block.getLength();
      }
      return length;
    }
    return 0;
  }

  private long getBlockSize(INode inode) {
    final Block[] ret = inode.getBlocks();
    return ret == null ? 0L : ret[0].getLength();
  }

  private Path getCorrectSwiftPath(Path path) throws URISyntaxException {
    final URI fullUri = new URI(uri.getScheme(), uri.getAuthority(), path.toUri().getPath(), null, null);

    return new Path(fullUri);
  }
}
