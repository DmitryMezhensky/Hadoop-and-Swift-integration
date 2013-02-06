package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftInvalidResponseException;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 *
 *
 * File system store implementation.
 * Makes REST requests, parses data from responses
 */

public class SwiftNativeFileSystemStore {
  private static final Pattern URI_PATTERN = Pattern.compile("\"\\S+?\"");
  private static final String PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  private static final Log LOG =
          LogFactory.getLog(SwiftNativeFileSystemStore.class);
  private URI uri;
  private SwiftRestClient swiftRestClient;

  /**
   * Initalize the filesystem store -this creates the REST client binding.
   *
   * @param fsURI URI of the filesystem, which is used to map to the filesystem-specific
   * options in the configuration file
   * @param configuration configuration
   * @throws IOException on any failure.
   */
  public void initialize(URI fsURI, Configuration configuration) throws IOException {
    this.uri = fsURI;
    this.swiftRestClient = SwiftRestClient.getInstance(fsURI, configuration);
  }

  @Override
  public String toString() {
    return "SwiftNativeFileSystemStore with "
            + swiftRestClient;
  }

  /**
   * Upload a file
   *
   * @param path destination path in the swift filesystem
   * @param inputStream input data
   * @param length length of the data
   * @throws IOException on a problem
   */
  public void uploadFile(Path path, InputStream inputStream, long length) throws IOException {
    swiftRestClient.upload(toObjectPath(path), inputStream, length);
  }

  /**
   * Upload part of a larger file.
   *
   * @param path destination path
   * @param partNumber item number in the path
   * @param inputStream input data
   * @param length length of the data
   * @throws IOException on a problem
   */
  public void uploadFilePart(Path path, int partNumber, InputStream inputStream, long length) throws IOException {
    String stringPath = path.toUri().toString();
    if (stringPath.endsWith("/")) {
      stringPath = stringPath.concat(String.valueOf(partNumber));
    } else {
      stringPath = stringPath.concat("/").concat(String.valueOf(partNumber));
    }

    swiftRestClient.upload(new SwiftObjectPath(toDirPath(path).getContainer(), stringPath), inputStream, length);
  }

  /**
   * Tell the Swift server to expect a multi-part upload by submitting
   * a 0-byte file with the X-Object-Manifest header
   *
   * @param path path of final final
   * @throws IOException
   */
  public void createManifestForPartUpload(Path path) throws IOException {
    String pathString = toObjectPath(path).toString();
    if (!pathString.endsWith("/")) {
      pathString = pathString.concat("/");
    }
    if (pathString.startsWith("/")) {
      pathString = pathString.substring(1);
    }

    swiftRestClient.upload(toObjectPath(path),
            new ByteArrayInputStream(new byte[0]),
            0,
            new Header(SwiftProtocolConstants.X_OBJECT_MANIFEST, pathString));
  }

  /**
   * Get the metadata of an object
   *
   * @param path path
   * @return file metadata. -or null if no headers were received back from the server.
   * @throws IOException on a problem
   * @throws FileNotFoundException if there is nothing at the end
   */
  public FileStatus getObjectMetadata(Path path) throws IOException {
    SwiftObjectPath objectPath = toObjectPath(path);
    final Header[] headers;
    headers = swiftRestClient.headRequest(objectPath,
            SwiftRestClient.NEWEST);
    //no headers is treated as a missing file
    if (headers.length == 0) {
      throw new FileNotFoundException("Not Found " + path.toUri());
    }

    boolean isDir = false;
    long length = 0;
    long lastModified = System.currentTimeMillis();
    for (Header header : headers) {
      String headerName = header.getName();
      if (headerName.equals(SwiftProtocolConstants.X_CONTAINER_OBJECT_COUNT) ||
              headerName.equals(SwiftProtocolConstants.X_CONTAINER_BYTES_USED)) {
        length = 0;
        isDir = true;
      }
      if (SwiftProtocolConstants.HEADER_CONTENT_LENGTH.equals(headerName)) {
        length = Long.parseLong(header.getValue());
      }
      if (SwiftProtocolConstants.HEADER_LAST_MODIFIED.equals(headerName)) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
        try {
          lastModified = simpleDateFormat.parse(header.getValue()).getTime();
        } catch (ParseException e) {
          throw new SwiftException("Failed to parse " + header.toString(), e);
        }
      }
    }

    Path correctSwiftPath = getCorrectSwiftPath(path);
    return new FileStatus(length, isDir, 0, 0L, lastModified, correctSwiftPath);
  }


  /**
   * Get the object as an input stream
   *
   * @param path object path
   * @return the input stream -this must be closed to terminate the connection
   * @throws IOException IO problems
   * @throws FileNotFoundException path doesn't resolve to an object
   */
  public InputStream getObject(Path path) throws IOException {
    return swiftRestClient.getDataAsInputStream(toObjectPath(path),
            SwiftRestClient.NEWEST);
  }

  /**
   * Get the input stream starting from a specific point.
   *
   * @param path path to object
   * @param byteRangeStart starting point
   * @param length no. of bytes
   * @return an input stream that must be closed
   * @throws IOException IO problems
   */
  public InputStream getObject(Path path, long byteRangeStart, long length)
          throws IOException {
    return swiftRestClient.getDataAsInputStream(
            toObjectPath(path), byteRangeStart, length);
  }

  /**
   * List all elements in this directory
   *
   * @param path path to work with
   * @param nameOnly should the status be minimal and not make any calls
   * to the system to determine attributes beyond the name?
   * @return the file statuses, or an empty array if there are no children
   * @throws IOException on IO problems
   * @throws FileNotFoundException if the path is nonexistent
   */
  public FileStatus[] listSubPaths(Path path,
                                   boolean recursive,
                                   boolean nameOnly) throws IOException {
    final Collection<FileStatus> fileStatuses;
    fileStatuses = listDirectory(toDirPath(path), recursive, nameOnly);
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }
  /**
   * List all elements in this directory
   *
   *
   * @param path path to work with
   * @param nameOnly should the status be minimal and not make any calls
   * to the system to determine attributes beyond the name?
   * @return the file statuses, or an empty list if there are no children
   * @throws IOException on IO problems
   * @throws FileNotFoundException if the path is nonexistent
   */
  public List<FileStatus> listDirectory(Path path,
                                        boolean recursive,
                                        boolean nameOnly) throws IOException {
    return listDirectory(toDirPath(path), recursive, nameOnly);
  }


  /**
   * Create a directory
   *
   * @param path path
   * @throws IOException
   */
  public void createDirectory(Path path) throws IOException {
    swiftRestClient.putRequest(toDirPath(path));
  }

  private SwiftObjectPath toDirPath(Path path) throws
          SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path, false);
  }

  private SwiftObjectPath toObjectPath(Path path) throws
          SwiftConfigurationException {
    return SwiftObjectPath.fromPath(uri, path);
  }

  public List<URI> getObjectLocation(Path path) throws IOException {
    final byte[] objectLocation;
    objectLocation = swiftRestClient.getObjectLocation(toObjectPath(path));
    return extractUris(new String(objectLocation));
  }

  /**
   * deletes object from Swift
   *
   * @param path path to delete
   * @return true if the path was deleted by this specific operation.
   * @throws IOException on a failure
   */
  public boolean deleteObject(Path path) throws IOException {
    return swiftRestClient.delete(toObjectPath(path));
  }

  /**
   * deletes a directory from Swift
   *
   * @param path path to delete
   * @return true if the path was deleted by this specific operation.
   * @throws IOException on a failure
   */
  public boolean rmdir(Path path) throws IOException {
    return swiftRestClient.delete(toDirPath(path));
  }

  /**
   * Does the object exist
   *
   * @param path object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which
   * is downgraded to an object does not exist return code
   */
  public boolean objectExists(Path path) throws IOException {
    return objectExists(toObjectPath(path));
  }
  /**
   * Does the object exist
   *
   * @param path swift object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which
   * is downgraded to an object does not exist return code
   */
  public boolean objectExists(SwiftObjectPath path) throws IOException {
    try {
      Header[] headers = swiftRestClient.headRequest(path,
              SwiftRestClient.NEWEST);
      //no headers is treated as a missing file
      return headers.length != 0;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Rename through copy-and-delete. this is clearly very inefficient, and
   * is a consequence of the classic Swift filesystem using the path as the hash
   * into the Distributed Hash Table, "the ring" of filenames.
   * <p/>
   * Because of the nature of the operation, it is not atomic.
   *
   * @param src source file/dir
   * @param dst destination
   * @return true if the entire rename was successful.
   * @throws IOException
   */
  public boolean renameDirectory(Path src, Path dst) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("mv " + src + " " + dst);
    }
    if (src.equals(dst)) {
      LOG.debug("Destination==source -failing");
      return false;
    }

    SwiftObjectPath srcObject = toObjectPath(src);
    SwiftObjectPath destObject = toObjectPath(dst);

    if (SwiftUtils.isRootDir(srcObject)) {
      LOG.debug("cannot rename root dir");
      return false;
    }

    if (SwiftUtils.isChildOf(srcObject, destObject)) {
      LOG.debug("cannot move a directory under itself");
      return false;
    }

    final FileStatus srcMetadata;
    try {
      srcMetadata = getObjectMetadata(src);
    } catch (FileNotFoundException e) {
      LOG.debug("source path not found -failing");
      return false;
    }
    FileStatus dstMetadata;
    try {
      dstMetadata = getObjectMetadata(dst);
    } catch (FileNotFoundException e) {
      //destination does not exist.
      LOG.debug("Destination does not exist");
      dstMetadata = null;
    }

    //check to see if the destination parent directory exists
    Path srcParent = src.getParent();
    Path dstParent = dst.getParent();
    //skip the overhead of a HEAD call if the src and dest share the same
    //parent dir (in which case the dest dir exists), or the destination
    //directory is root, in which case it must also exist
    if (dstParent !=null && !dstParent.equals(srcParent)) {
      try {
        getObjectMetadata(dstParent);
      } catch (FileNotFoundException e) {
        //destination parent doesn't exist; bail out
        LOG.debug("destination parent directory "+ dstParent + " doesn't exist");
        return false;
      }
    }

    boolean destExists = dstMetadata != null;
    boolean destIsDir = destExists && SwiftUtils.isDirectory(dstMetadata);
    //calculate the destination
    SwiftObjectPath destPath;


    boolean srcIsFile = !SwiftUtils.isDirectory(srcMetadata);
    if (srcIsFile) {

      //source is a simple file
      // outcomes:
      // #1 dest exists and is file: fail
      // #2 dest exists and is dir: destination path becomes under dest dir
      // #3 dest does not exist: use dest as name
      if (destExists) {

        if (destIsDir) {
          //outcome #2 -move to subdir of dest
          destPath = toObjectPath(new Path(dst, src.getName()));
        } else {
          //outcome #1 dest it's a file: fail
          LOG.debug("cannot rename a file over one that already exists");
          return false;
        }
      } else {
        //outcome #3 -new entry
        destPath = toObjectPath(dst);
      }

      boolean success = swiftRestClient.
              copyObject(srcObject, destPath);
      if (success) {
        swiftRestClient.delete(srcObject);
      } else {
        throw new SwiftException("Copy of " + srcObject + " to "
                + destPath + "failed");
      }
      return true;
    } else {

      //here the source exists and is a directory
      // outcomes (given we know the parent dir exists if we get this far)
      // #1 destination is a file: fail
      // #2 destination is a directory: create a new dir under that one
      // #3 destination doesn't exist: create a new dir with that name

      if (destExists && !destIsDir) {
        // #1 destination is a file: fail
        LOG.debug("the source is a directory, but not the destination");
        return false;
      }

      Path targetPath;
      if (destExists) {
        // #2 destination is a directory: create a new dir under that one
        targetPath = new Path(dst, src.getName());
      } else {
        // #3 destination doesn't exist: create a new dir with that name
        targetPath = dst;
      }
      SwiftObjectPath targetObjectPath = toObjectPath(targetPath);

      //enum the child entries amd everything underneath
      List<FileStatus> fileStatuses = listDirectory(toObjectPath(src), true, false);

      boolean result = true;

      //iterative copy of everything under the directory
      for (FileStatus fileStatus : fileStatuses) {
        Path copySourcePath = fileStatus.getPath();
        SwiftObjectPath copyDestination = toObjectPath(
                new Path(targetPath, copySourcePath.getName()));
        try {
          boolean success = swiftRestClient.
                  copyObject(toObjectPath(copySourcePath), copyDestination);
          if (success) {
            swiftRestClient.delete(toObjectPath(copySourcePath));
          } else {
            throw new SwiftException("Copy of " + toObjectPath(fileStatus.getPath()) + " to "
                    + targetObjectPath + "failed");
          }
        } catch (FileNotFoundException e) {
          LOG.info("Skipping rename of " + copySourcePath + " as it not found");
        }
      }
      try {
      swiftRestClient.delete(toObjectPath(src));
      }catch (FileNotFoundException e) {
        LOG.debug("Source directory " + src.toString() + " doesn't exist");
      }
      return result;
    }
  }

  public void copy(Path srcKey, Path dstKey) throws IOException {
    SwiftObjectPath srcObject = toObjectPath(srcKey);
    SwiftObjectPath destObject = toObjectPath(dstKey);
    swiftRestClient.copyObject(srcObject, destObject);
  }


  /**
   * List a directory.
   * This is O(n) for the number of objects in this path.
   *
   * @param path path to list
   * @param nameOnly should the status be minimal (name) or should
   * the (expensive) operation be made to ask for it.
   * @return the filestats of all the entities in the directory -or
   * an empty list if no objects were found listed under that prefix
   * @throws IOException IO problems
   */
  private List<FileStatus> listDirectory(SwiftObjectPath path,
                                         boolean recursive,
                                         boolean nameOnly) throws IOException {
    final byte[] bytes;
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();
    try {
      if (recursive) {
        bytes = swiftRestClient.findObjectsByPrefix(path);
      } else {
        bytes = swiftRestClient.listObjectsInDirectory(path);
      }
    } catch (FileNotFoundException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("" +
                "File/Directory not found " + path);
      }
      if (SwiftUtils.isRootDir(path)) {
        return Collections.emptyList();
      } else {
        throw e;
      }
    } catch (SwiftInvalidResponseException e) {
      //bad HTTP error code
      if (e.getStatusCode() == HttpStatus.SC_NO_CONTENT) {
        //this can come back on a root list if the container is empty
        if (LOG.isDebugEnabled()) {
          LOG.debug("lsdir " + path +
                  " status code says NO_CONTENT; "
                  + e.toString(),
                  e);
        }
        if (SwiftUtils.isRootDir(path)) {
          return Collections.emptyList();
        } else {
          //NO_CONTENT returned on something other than the root directory;
          //see if it is there, and conver to empty list or not found
          //depending on whether the entry exists.
          FileStatus stat = getObjectMetadata(getCorrectSwiftPath(path));

          if (SwiftUtils.isDirectory(stat)) {
            //it's an empty directory. state that
            return Collections.emptyList();
          } else {
            //it's a file -return that as the status
            files.add(stat);
            return files;
          }
        }
      } else {
        //a different status code: rethrow immediately
        throw e;
      }
    }

    //the byte array contains the files separated by newlines
    final StringTokenizer tokenizer =
            new StringTokenizer(new String(bytes), "\n");

    Map<String, Boolean> names = new HashMap<String, Boolean>();
    //insert own name as one to skip
    names.put(path.getObject(), true);

    while (tokenizer.hasMoreTokens()) {
      String pathInSwift = tokenizer.nextToken();
      if (LOG.isDebugEnabled()) {
        LOG.debug("entry: " + pathInSwift);
      }
      if (!pathInSwift.startsWith("/")) {
        pathInSwift = "/".concat(pathInSwift);
      }
      Path childPath = new Path(pathInSwift);
      if (!names.containsKey(pathInSwift)) {
        names.put(pathInSwift, true);
        names.put(pathInSwift + "/", true);
        //For each entry, get the metadata.
        try {
          FileStatus metadata;
          if (nameOnly) {
            metadata = new FileStatus(0, false, 0, 0, 0, childPath);
          } else {
            metadata = getObjectMetadata(childPath);
          }
          files.add(metadata);
        } catch (FileNotFoundException e) {
          //get Object metadata failed
          LOG.info(
                  "Object " + childPath + " was deleting during directory listing");
        }
      } else {
        //hash map
        LOG.debug("skipping adding self to path");
      }
    }
    return files;
  }

  private Path getCorrectSwiftPath(Path path) throws
          SwiftException {
    try {
      final URI fullUri = new URI(uri.getScheme(),
              uri.getAuthority(),
              path.toUri().getPath(),
              null,
              null);

      return new Path(fullUri);
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
  }

  private Path getCorrectSwiftPath(SwiftObjectPath path) throws
          SwiftException {
    try {
      final URI fullUri = new URI(uri.getScheme(),
              uri.getAuthority(),
              path.getObject(),
              null,
              null);

      return new Path(fullUri);
    } catch (URISyntaxException e) {
      throw new SwiftException("Specified path " + path + " is incorrect", e);
    }
  }



  /**
   * extracts URIs from json
   *
   * @return URIs
   */
  public static List<URI> extractUris(String json) {
    final Matcher matcher = URI_PATTERN.matcher(json);
    final List<URI> result = new ArrayList<URI>();
    while (matcher.find()) {
      final String s = matcher.group();
      final String uri = s.substring(1, s.length() - 1);
      result.add(URI.create(uri));
    }
    return result;
  }
}