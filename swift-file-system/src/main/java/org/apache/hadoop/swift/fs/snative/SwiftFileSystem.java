package org.apache.hadoop.swift.fs.snative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Swift file system implementation. Extends Hadoop FileSystem
 */
public class SwiftFileSystem extends FileSystem {
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
  private SwiftFileSystemStore store;

  /**
   * Hadoop configuration
   */
  private Configuration conf;

  /**
   * default class initialization
   *
   * @param uri  path to Swift
   * @param conf Hadoop configuration
   * @throws IOException
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    this.uri = URI.create(String.format("swift://%s:%d", uri.getHost(), uri.getPort()));
    this.workingDir = new Path("/user", System.getProperty("user.name"));
    this.conf = conf;
    this.store = new SwiftFileSystemStore(this.uri, conf);
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
    try {
      return store.getObjectMetadata(f);
    } catch (URISyntaxException e) {
      throw new IOException("path " + f + " is incorrect", e);
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
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) {
    final List<URI> locations = store.getObjectLocation(file.getPath());

    final String[] names = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    int i = 0;
    for (URI uri : locations) {
      hosts[i] = uri.getHost();
      names[i] = uri.getAuthority();
      i++;
    }
    return new BlockLocation[]{new BlockLocation(names, hosts, 0, file.getLen())};
  }

  /**
   * Make the given file and all non-existent parents into
   * directories.Existence of the directory hierarchy is not an error.
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

  /**
   * internal implementation of directory creation
   *
   * @param path path to file
   * @return boolean file is created
   * @throws IOException if specified path is file instead of directory
   */
  private boolean mkdir(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    try {
      if (!store.objectExists(absolutePath)) {
        store.createDirectory(absolutePath);
      }
    } catch (URISyntaxException e) {
      throw new IOException("path " + path + " is incorrect", e);
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

    return store.listSubPaths(f);
  }

  /**
   * This optional operation is not supported yet
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Not supported for Swift file system");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
          throws IOException {

    if (!getFileStatus(makeAbsolute(file)).isDir()) {
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

    return new FSDataOutputStream(new NativeSwiftOutputStream(conf, store, pathToKey(file)), statistics);
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
            new BufferedFSInputStream(new NativeSwiftInputStream(store, statistics, path), bufferSize));
  }

  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * @param src path
   * @param dst path
   * @return true if directory renamed, false otherwise
   * @throws IOException
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    return store.renameDirectory(src, dst);
  }


  /**
   * Delete a file.
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception. In
   *                  case of a file the recursive can be set to either true or false.
   * @return true if delete is successful else false.
   * @throws IOException
   */
  public boolean delete(Path path, boolean recursive) throws IOException {
    Path absolutePath = makeAbsolute(path);
    final FileStatus fileStatus;
    try {
      fileStatus = getFileStatus(path);
    } catch (FileNotFoundException e) {
      return false;
    }
    if (!fileStatus.isDir()) {
      store.deleteObject(absolutePath);
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
    }

    return true;
  }

  /**
   * Delete a file.
   */
  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  /**
   * Makes path absolute
   *
   * @param path path to file
   * @return absolute path
   */
  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  private static String pathToKey(Path path) {
    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    String key = path.toUri().getPath().substring(1);
    if (key.endsWith("/") && (key.indexOf("/") != key.length() - 1)) {
      key = key.substring(0, key.length() - 1);
    }
    return key;
  }
}
