/*
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
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.http.SwiftProtocolConstants;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.JSONUtil;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.codehaus.jackson.map.type.CollectionType;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
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
   * @param fsURI         URI of the filesystem, which is used to map to the filesystem-specific
   *                      options in the configuration file
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
   * @param path        destination path in the swift filesystem
   * @param inputStream input data
   * @param length      length of the data
   * @throws IOException on a problem
   */
  public void uploadFile(Path path, InputStream inputStream, long length)
          throws IOException {
    swiftRestClient.upload(toObjectPath(path), inputStream, length);
  }

  /**
   * Upload part of a larger file.
   *
   * @param path        destination path
   * @param partNumber  item number in the path
   * @param inputStream input data
   * @param length      length of the data
   * @throws IOException on a problem
   */
  public void uploadFilePart(Path path, int partNumber,
                             InputStream inputStream, long length)
          throws IOException {

    String stringPath = path.toUri().toString();
    if (stringPath.endsWith("/")) {
      stringPath = stringPath.concat(String.valueOf(partNumber));
    } else {
      stringPath = stringPath.concat("/").concat(String.valueOf(partNumber));
    }

    swiftRestClient.upload(new SwiftObjectPath(toDirPath(path).getContainer(), stringPath),
            inputStream, length);
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
   * @throws IOException           on a problem
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
    return new SwiftFileStatus(length, isDir, 0, 0L, lastModified, correctSwiftPath);
  }


  /**
   * Get the object as an input stream
   *
   * @param path object path
   * @return the input stream -this must be closed to terminate the connection
   * @throws IOException           IO problems
   * @throws FileNotFoundException path doesn't resolve to an object
   */
  public InputStream getObject(Path path) throws IOException {
    return swiftRestClient.getDataAsInputStream(toObjectPath(path),
            SwiftRestClient.NEWEST);
  }

  /**
   * Get the input stream starting from a specific point.
   *
   * @param path           path to object
   * @param byteRangeStart starting point
   * @param length         no. of bytes
   * @return an input stream that must be closed
   * @throws IOException IO problems
   */
  public InputStream getObject(Path path, long byteRangeStart, long length)
          throws IOException {
    return swiftRestClient.getDataAsInputStream(
            toObjectPath(path), byteRangeStart, length);
  }

  /**
   * List a directory.
   * This is O(n) for the number of objects in this path.
   *
   * @param path working path
   * @return Collection of file statuses
   * @throws IOException IO problems
   */
  private List<FileStatus> listDirectory(SwiftObjectPath path, boolean listDeep) throws IOException {
    final byte[] bytes;
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();
    try {
      bytes = swiftRestClient.listDeepObjectsInDirectory(path, listDeep);
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
        if (SwiftUtils.isRootDir(path)) {
          return Collections.emptyList();
        } else {
          //NO_CONTENT returned on something other than the root directory;
          //see if it is there, and convert to empty list or not found
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

    final CollectionType collectionType = JSONUtil.getJsonMapper().getTypeFactory().
            constructCollectionType(List.class, SwiftObjectFileStatus.class);

    final List<SwiftObjectFileStatus> fileStatusList =
            JSONUtil.toObject(new String(bytes), collectionType);

    //this can happen if user lists file /data/files/file
    //in this case swift will return empty array
    if (fileStatusList.isEmpty()) {
      final SwiftFileStatus objectMetadata =
              (SwiftFileStatus) getObjectMetadata(getCorrectSwiftPath(path));
      if (objectMetadata.isFile()) {
        files.add(objectMetadata);
      }

      return files;
    }

    for (SwiftObjectFileStatus status : fileStatusList) {
      if (status.getName() != null) {
          files.add(new SwiftFileStatus(status.getBytes(), status.getBytes() == 0,
                  1, 0, status.getLast_modified().getTime(),
                  getCorrectSwiftPath(new Path(status.getName()))));
      }
    }

    return files;
  }

  /**
   * List all elements in this directory
   *
   * @param path     path to work with
   * @return the file statuses, or an empty array if there are no children
   * @throws IOException           on IO problems
   * @throws FileNotFoundException if the path is nonexistent
   */
  public FileStatus[] listSubPaths(Path path) throws IOException {
    final Collection<FileStatus> fileStatuses;
    fileStatuses = listDirectory(toDirPath(path), false);
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  /**
   * Create a directory
   *
   * @param path path
   * @throws IOException
   */
  public void createDirectory(Path path) throws IOException {
    innerCreateDirectory(toDirPath(path));
  }

  private void innerCreateDirectory(SwiftObjectPath swiftObjectPath)
          throws IOException {

    swiftRestClient.putRequest(swiftObjectPath);
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
    SwiftObjectPath swiftObjectPath = toObjectPath(path);
    if (!SwiftUtils.isRootDir(swiftObjectPath)) {
      return swiftRestClient.delete(swiftObjectPath);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not deleting root directory entry");
      }
      return true;
    }
  }

  /**
   * deletes a directory from Swift. This is not recursive
   *
   * @param path path to delete
   * @return true if the path was deleted by this specific operation -or
   *         the path was root and not acted on.
   * @throws IOException on a failure
   */
  public boolean rmdir(Path path) throws IOException {
    return deleteObject(path);
  }

  /**
   * Does the object exist
   *
   * @param path object path
   * @return true if the metadata of an object could be retrieved
   * @throws IOException IO problems other than FileNotFound, which
   *                     is downgraded to an object does not exist return code
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
   *                     is downgraded to an object does not exist return code
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
   * Rename through copy-and-delete. this is a consequence of the
   * Swift filesystem using the path as the hash
   * into the Distributed Hash Table, "the ring" of filenames.
   * <p/>
   * Because of the nature of the operation, it is not atomic.
   *
   * @param src source file/dir
   * @param dst destination
   * @throws IOException                   IO failure
   * @throws SwiftOperationFailedException if the rename failed
   * @throws FileNotFoundException         if the source directory is missing, or
   *                                       the parent directory of the destination
   */
  public void rename(Path src, Path dst) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("mv " + src + " " + dst);
    }
    boolean renamingOnToSelf = src.equals(dst);

    SwiftObjectPath srcObject = toObjectPath(src);
    SwiftObjectPath destObject = toObjectPath(dst);

    if (SwiftUtils.isRootDir(srcObject)) {
      throw new SwiftOperationFailedException("cannot rename root dir");
    }

    if (SwiftUtils.isChildOf(srcObject, destObject)) {
      throw new SwiftOperationFailedException("cannot move a directory under itself");
    }

    final FileStatus srcMetadata;
    srcMetadata = getObjectMetadata(src);
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
    if (dstParent != null && !dstParent.equals(srcParent)) {
      try {
        getObjectMetadata(dstParent);
      } catch (FileNotFoundException e) {
        //destination parent doesn't exist; bail out
        LOG.debug("destination parent directory " + dstParent + " doesn't exist");
        throw e;
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
          //outcome #1 dest it's a file: fail if differeent
          if (!renamingOnToSelf) {
            throw new SwiftOperationFailedException(
                    "cannot rename a file over one that already exists");
          } else {
            //is mv self self where self is a file. this becomes a no-op
            return;
          }
        }
      } else {
        //outcome #3 -new entry
        destPath = toObjectPath(dst);
      }

      copyThenDeleteObject(srcObject, destPath);
    } else {

      //here the source exists and is a directory
      // outcomes (given we know the parent dir exists if we get this far)
      // #1 destination is a file: fail
      // #2 destination is a directory: create a new dir under that one
      // #3 destination doesn't exist: create a new dir with that name

      if (destExists && !destIsDir) {
        // #1 destination is a file: fail
        throw new SwiftOperationFailedException(
                "the source is a directory, but not the destination");
      }

      if (renamingOnToSelf) {
        //you can't rename a directory onto itself
        throw new SwiftOperationFailedException("Destination==source -failing");
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

      //enum the child entries and everything underneath
      List<FileStatus> fileStatuses = listDirectory(srcObject, true);

      LOG.info("mv  " + srcObject + " " + targetPath);

      logDirectory("Directory to copy ", srcObject, fileStatuses);

      //iterative copy of everything under the directory
      String srcURI = src.toUri().toString();
      int prefixStripCount = srcURI.length() + 1;
      for (FileStatus fileStatus : fileStatuses) {
        Path copySourcePath = fileStatus.getPath();
        String copySourceURI = copySourcePath.toUri().toString();

        String copyDestSubPath = copySourceURI.substring(prefixStripCount);

        Path copyDestPath = new Path(targetPath, copyDestSubPath);
        if (LOG.isTraceEnabled()) {
          //trace to debug some low-level rename path problems; retained
          //in case they ever come back.
          LOG.trace("srcURI=" + srcURI
                  + "; copySourceURI=" + copySourceURI
                  + "; copyDestSubPath=" + copyDestSubPath
                  + "; copyDestPath=" + copyDestPath);
        }
        SwiftObjectPath copyDestination = toObjectPath(copyDestPath);

        try {
          copyThenDeleteObject(toObjectPath(copySourcePath),
                  copyDestination);
        } catch (FileNotFoundException e) {
          LOG.info("Skipping rename of " + copySourcePath);
        }
      }
      //now rename self. If missing, create the dest directory and warn
      if (!SwiftUtils.isRootDir(srcObject)) {
        try {
          copyThenDeleteObject(srcObject,
                  targetObjectPath);
        } catch (FileNotFoundException e) {
          //create the destination directory
          LOG.warn("Source directory deleted during rename", e);
          innerCreateDirectory(destObject);
        }
      }
    }
  }

  /**
   * Debug action to dump directory statuses to the debug log
   *
   * @param message    explanation
   * @param objectPath object path (can be null)
   * @param statuses   listing output
   */
  private void logDirectory(String message, SwiftObjectPath objectPath,
                            Iterable<FileStatus> statuses) {

    if (LOG.isDebugEnabled()) {
      LOG.debug(message + ": listing of " + objectPath);
      for (FileStatus fileStatus : statuses) {
        LOG.debug(fileStatus.getPath());
      }
    }
  }

  public void copy(Path srcKey, Path dstKey) throws IOException {
    SwiftObjectPath srcObject = toObjectPath(srcKey);
    SwiftObjectPath destObject = toObjectPath(dstKey);
    swiftRestClient.copyObject(srcObject, destObject);
  }


  /**
   * Copy and object then, if the copy worked, delete it.
   * If the copy failed, the source object is not deleted.
   * No checks are made on the validity of the arguments,
   * the assumption is that the caller has already done this.
   *
   * @param srcObject  source object path
   * @param destObject destination object path
   * @throws IOException
   */
  private void copyThenDeleteObject(SwiftObjectPath srcObject,
                                    SwiftObjectPath destObject) throws
          IOException {
    LOG.debug("Copying " + srcObject + " to " + destObject);
    boolean copySucceeded = swiftRestClient.copyObject(srcObject, destObject);
    if (copySucceeded) {
      //if the copy worked delete the original
      if (getObjectMetadata(getCorrectSwiftPath(destObject)) == null) {
        innerCreateDirectory(destObject);
      }
      swiftRestClient.delete(srcObject);
    } else {
      throw new SwiftException("Copy of " + srcObject + " to "
              + destObject + "failed");
    }
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
