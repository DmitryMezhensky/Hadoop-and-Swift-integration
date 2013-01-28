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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftNotDirectoryException;
import org.apache.hadoop.fs.swift.exceptions.SwiftUnsupportedFeatureException;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Swift file system implementation. Extends Hadoop FileSystem
 */
public class SwiftNativeFileSystem extends FileSystem {


  private static final Log LOG =
    LogFactory.getLog(SwiftNativeFileSystem.class);

  /**
   * URI constant for this filesystem: {@value}
   */
  public static final String SWIFT_FS = "swift";
  /**
   * path to user work directory for storing temporary files
   */
  private Path workingDir;

  /**
   * Swift URI
   */
  private URI uri;

  /**
   * reference to swiftFileSystemStore
   */
  private SwiftNativeFileSystemStore store;

  /**
   * Default constructor for Hadoop
   */
  public SwiftNativeFileSystem() {
    // set client in initialize()
  }

  /**
   * This constructor used for testing purposes
   */
  public SwiftNativeFileSystem(SwiftNativeFileSystemStore store)
    throws IOException {
    this.store = store;
  }

  /**
   * default class initialization
   *
   * @param fsuri  path to Swift
   * @param conf Hadoop configuration
   * @throws IOException
   */
  @Override
  public void initialize(URI fsuri, Configuration conf) throws IOException {
    super.initialize(fsuri, conf);
    setConf(conf);
    if (store == null) {
      store = new SwiftNativeFileSystemStore();
    }
    this.uri = fsuri;
    this.workingDir = new Path("/user",
                               System.getProperty("user.name")).makeQualified(this);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing SwiftNativeFileSystem against URI "+ uri
               + " and working dir " + workingDir);
    }
    store.initialize(uri, conf);
    LOG.debug("SwiftFileSystem initialized");
  }

  /**
   * @return path to Swift
   */
  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Path to user working directory
   *
   * @return Hadoop path
   */
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * @param dir user working directory
   */
  @Override
  public void setWorkingDirectory(Path dir) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.setWorkingDirectory to " + dir);
    }

    workingDir = dir;
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    final FileStatus objectMetadata = store.getObjectMetadata(f);
    return objectMetadata;
  }


  @Override
  public boolean isFile(Path f) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(f);
      return !SwiftUtils.isDirectory(fileStatus);
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {
    try {
      FileStatus fileStatus = getFileStatus(f);
      return SwiftUtils.isDirectory(fileStatus);
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file.  For a nonexistent
   * file or regions, null will be returned.
   * <p/>
   * This call is most helpful with DFS, where it returns
   * hostnames of machines that contain the given file.
   * <p/>
   * The FileSystem will simply return an elt containing 'localhost'.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
                                               long start,
                                               long len) throws IOException {
    // Check if requested file in Swift is more than 5Gb. In this case
    // each block has its own location -which may be determinable
    // from the Swift client API, depending on the remote server

    final FileStatus[] listOfFileBlocks = store.listSubPaths(file.getPath());
    List<URI> locations = new ArrayList<URI>();
    if (listOfFileBlocks.length > 1) {
      for (FileStatus fileStatus : listOfFileBlocks) {
        if (SwiftObjectPath.fromPath(uri, fileStatus.getPath())
          .equals(SwiftObjectPath.fromPath(uri, file.getPath()))) {
          continue;
        }
        locations.addAll(store.getObjectLocation(fileStatus.getPath()));
      }
    } else {
      locations = store.getObjectLocation(file.getPath());
    }

    final String[] names = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    int i = 0;
    for (URI location : locations) {
      hosts[i] = location.getHost();
      names[i] = location.getAuthority();
      i++;
    }
    return new BlockLocation[]{
      new BlockLocation(names, hosts, 0, file.getLen())
    };
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.mkdirs: " + path);
    }
    Path absolutePath = makeAbsolute(path);
    //build a list of paths to create, with shortest one at the front
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);

    boolean result = true;
    for (Path p : paths) {
      if (p.getParent() != null) {
        result &= mkdir(p);
      }
    }
    return result;
  }

  /**
   * internal implementation of directory creation
   *
   * @param path path to file
   * @return boolean file is created
   * @throws IOException if specified path is file instead of directory
   */
  private boolean mkdir(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
//    if (!store.objectExists(absolutePath)) {
//      store.createDirectory(absolutePath);
//    }

    //TODO: define a consistent semantic for a directory/subdirectory
    //in the hadoop:swift bridge. Hadoop FS assumes that there
    //are files and directories, whereas SwiftFS assumes
    //that there are just "objects"

    FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(absolutePath);
      if (!SwiftUtils.isDirectory(fileStatus)) {
        throw new SwiftNotDirectoryException(path,
          String.format(": can't mkdir since it is not a directory: %s",
          fileStatus));
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping mkdir(" + path + ") as it exists already");
        }
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Making dir '" + path + "' in Swift");
      }
      //file is not found: it must be created
      store.createDirectory(absolutePath);
    }
    return true;
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given path
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.listStatus for: " + f);
    }
    return store.listSubPaths(f);
  }

  /**
   * This optional operation is not supported yet
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws
                                                                                  IOException {
    LOG.debug("SwiftFileSystem.append");
    throw new SwiftUnsupportedFeatureException("Not supported: append()");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
    throws IOException {

    LOG.debug("SwiftFileSystem.create");
    FileStatus fileStatus = null;
    try {
      fileStatus = getFileStatus(makeAbsolute(file));
    } catch (FileNotFoundException e) {
      //nothing to do
    }
    if (fileStatus != null && !SwiftUtils.isDirectory(fileStatus)) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new SwiftException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new SwiftException("Mkdirs failed to create " + parent.toString());
        }
      }
    }

    SwiftNativeOutputStream out = new SwiftNativeOutputStream(getConf(),
                                                              store,
                                                              file.toUri()
                                                                  .toString());
    return new FSDataOutputStream(out, statistics);
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param path       the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return new FSDataInputStream(
      new BufferedFSInputStream(
        new SwiftNativeInputStream(store, statistics, path), bufferSize));
  }

  /**
   * Renames Path src to Path dst. On swift this uses copy-and-delete
   * and <i>is not atomic</i>.
   *
   * @param src path
   * @param dst path
   * @return true if directory renamed, false otherwise
   * @throws IOException on problems
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    return store.renameDirectory(src, dst);
  }


  /**
   * Delete a file or directory
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException IO problems
   */
  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      return innerDelete(path, recursive);
    } catch (FileNotFoundException e) {
      //base path was not found.
      return false;
    }
  }

  /**
   * Delete the entire tree. This is an internal one with slightly different
   * behavior: if an entry is missing, a {@link FileNotFoundException} is
   * raised. This lets the caller distinguish a file not found with
   * other reasons for failure, so handles race conditions in recursive
   * directory deletes better.
   *
   * The problem being addressed is: caller A requests a recursive directory
   * of directory /dir ; caller B requests a delete of a file /dir/file,
   * between caller A enumerating the files contents, and requesting a delete
   * of /dir/file. We want to recognise the special case
   * "directed file is no longer there" and not convert that into a failure
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException IO problems
   * @throws FileNotFoundException if a file/dir being deleted is not there -
   * this includes entries below the specified path, (if the path is a dir
   * and recursive is true)
   */
  private boolean innerDelete(Path path, boolean recursive) throws IOException {
    Path absolutePath = makeAbsolute(path);
    final FileStatus fileStatus;
    fileStatus = getFileStatus(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting path '" + path + "'");
    }
    if (!SwiftUtils.isDirectory(fileStatus)) {
      //simple file: delete it
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting simple file '" + path + "'");
      }
      store.deleteObject(absolutePath);
    }
    else {
      //it's a directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting directory '" + path + "'");
      }
      FileStatus[] contents = listStatus(absolutePath);
      if (contents == null) {
        //the directory went away during the non-atomic stages of the operation.
        // Return false as it was not this thread doing the deletion.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Path '" + path + "' has no status -it has 'gone away'");
        }
        return false;
      }
      //now build a list without ourselves in it
      Path dirPath = fileStatus.getPath();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found " + contents.length + " child entries under " + dirPath);
      }
      ArrayList<FileStatus> children =
        new ArrayList<FileStatus>(contents.length);
      for (FileStatus child : contents) {
        if (!(child.getPath().equals(dirPath))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(child.toString());
          }
          children.add(child);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("skipping own entry");
          }
        }
      }

      //look to see if there are now any children
      if (!children.isEmpty() && !recursive) {
        //if there are unless this is a recursive operation, fail immediately
        throw new SwiftException("Directory " + path + " is not empty.");
      }
      //delete the children
      for (FileStatus child : children) {
        Path childPath = child.getPath();
        try {
          if (!innerDelete(childPath, true)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to  recursively delete '" + childPath + "'");
            }
            return false;
          }
        } catch (FileNotFoundException e) {
          //the path went away -race conditions.
          //do not fail, as the outcome is still OK.
          LOG.info("Path " + childPath + " is no longer present");
        }
      }
      //here any children that existed have been deleted
      //so rm the directory
      store.rmdir(absolutePath);
    }

    return true;
  }

  /**
   * Makes path absolute
   *
   * @param path path to file
   * @return absolute path
   */
  protected Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }
}
