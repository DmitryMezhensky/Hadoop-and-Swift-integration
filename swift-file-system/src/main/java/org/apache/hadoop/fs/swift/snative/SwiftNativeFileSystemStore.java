package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;

import java.io.ByteArrayInputStream;
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
 * File system store implementation.
 * Makes REST requests, parses data from responses
 */
public class SwiftNativeFileSystemStore {
  private static final Pattern URI_PATTERN = Pattern.compile("\"\\S+?\"");
  private static final String PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  private URI uri;
  private SwiftRestClient swiftRestClient;

  public void initialize(URI uri, Configuration configuration) throws IOException {
    this.uri = uri;
    try {
      this.swiftRestClient = SwiftRestClient.getInstance(configuration);
    } catch (SwiftException e) {
      throw new IOException("Initialization of SwiftRestClient failed", e);
    }
  }

  public void uploadFile(Path path, InputStream inputStream, long length) throws IOException {
    try {
      swiftRestClient.upload(SwiftObjectPath.fromPath(uri, path), inputStream, length);
    } catch (SwiftException e) {
      throw new IOException(e);
    }
  }

  public void uploadFilePart(Path path, int partNumber, InputStream inputStream, long length) throws IOException {
    String stringPath = path.toUri().toString();
    if (stringPath.endsWith("/")) {
      stringPath = stringPath.concat(String.valueOf(partNumber));
    } else {
      stringPath = stringPath.concat("/").concat(String.valueOf(partNumber));
    }

    try {
      swiftRestClient.upload(new SwiftObjectPath(uri.getHost(), stringPath), inputStream, length);
    } catch (SwiftException e) {
      throw new IOException(e);
    }
  }

  public void createManifestForPartUpload(Path path) throws IOException {
    String pathString = SwiftObjectPath.fromPath(uri, path).toString();
    if (!pathString.endsWith("/"))
      pathString = pathString.concat("/");
    if (pathString.startsWith("/"))
      pathString = pathString.substring(1);

    try {
      swiftRestClient.upload(SwiftObjectPath.fromPath(uri, path), new ByteArrayInputStream(new byte[0]),
              0, new Header("X-Object-Manifest", pathString));
    } catch (SwiftException e) {
      throw new IOException(e);
    }
  }

  public FileStatus getObjectMetadata(Path path) throws IOException {
    final Header[] headers;
    try {
      headers = swiftRestClient.headRequest(SwiftObjectPath.fromPath(uri, path));
    } catch (SwiftException e) {
      throw new IOException(e);
    }
    if (headers == null || headers.length == 0)
      return null;

    boolean isDir = false;
    long length = 0;
    long lastModified = System.currentTimeMillis();
    for (Header header : headers) {
      if (header.getName().equals("X-Container-Object-Count") ||
              header.getName().equals("X-Container-Bytes-Used")) {
        length = 0;
        isDir = true;
      }
      if (header.getName().equals("Content-Length")) {
        length = Long.parseLong(header.getValue());
      }
      if (header.getName().equals("Last-Modified")) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
        try {
          lastModified = simpleDateFormat.parse(header.getValue()).getTime();
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
    }

    final Path correctSwiftPath;
    try {
      correctSwiftPath = getCorrectSwiftPath(path);
    } catch (URISyntaxException e) {
      throw new IOException("Specified path " + path + " is incorrect", e);
    }
    return new FileStatus(length, isDir, 0, 0l, lastModified, correctSwiftPath);
  }

  public InputStream getObject(Path path) throws IOException {
    try {
      return swiftRestClient.getDataAsInputStream(SwiftObjectPath.fromPath(uri, path));
    } catch (SwiftException e) {
      throw new IOException("Internal I/O error", e);
    }
  }

  public InputStream getObject(Path path, long byteRangeStart, long length) throws IOException {
    try {
      return swiftRestClient.getDataAsInputStream(SwiftObjectPath.fromPath(uri, path), byteRangeStart, length);
    } catch (SwiftException e) {
      throw new IOException("Internal I/O error", e);
    }
  }

  public FileStatus[] listSubPaths(Path path) throws IOException {
    final Collection<FileStatus> fileStatuses;
    fileStatuses = listDirectory(SwiftObjectPath.fromPath(uri, path));

    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  public void createDirectory(Path path) throws IOException {

    try {
      swiftRestClient.putRequest(SwiftObjectPath.fromPath(uri, path));
    } catch (SwiftException e) {
      throw new IOException(e);
    }
  }

  public List<URI> getObjectLocation(Path path) throws IOException {
    final byte[] objectLocation;
    try {
      objectLocation = swiftRestClient.getObjectLocation(SwiftObjectPath.fromPath(uri, path));
    } catch (SwiftException e) {
      throw new IOException(e);
    }
    return extractUris(new String(objectLocation));
  }

  /**
   * deletes object from Swift
   */
  public void deleteObject(Path path) throws IOException {

    swiftRestClient.delete(SwiftObjectPath.fromPath(uri, path));
  }

  /**
   * Checks if specified path exists
   *
   * @param path to check
   * @return true - path exists, false otherwise
   */
  public boolean objectExists(Path path) throws IOException {
    try {
      return listDirectory(SwiftObjectPath.fromPath(uri, path)).size() != 0;
    } catch (IOException e) {
      throw new IOException(e);
    }
  }

  public boolean renameDirectory(Path src, Path dst) throws IOException {
    final FileStatus srcMetadata = getObjectMetadata(src);
    final FileStatus dstMetadata = getObjectMetadata(dst);
    if (srcMetadata != null && !srcMetadata.isDir()) {
      if (dstMetadata != null && !dstMetadata.isDir()) {
        throw new IOException("file already exists: " + dst);
      }

      if (dstMetadata != null && dstMetadata.isDir()) {
        return swiftRestClient.copyObject(SwiftObjectPath.fromPath(uri, src),
                SwiftObjectPath.fromPath(uri, new Path(dst.getParent(), src.getName())));
      } else {
        return swiftRestClient.copyObject(SwiftObjectPath.fromPath(uri, src), SwiftObjectPath.fromPath(uri, dst));
      }
    }
    final List<FileStatus> fileStatuses = listDirectory(SwiftObjectPath.fromPath(uri, src.getParent()));
    final List<FileStatus> dstPath = listDirectory(SwiftObjectPath.fromPath(uri, dst.getParent()));

    if (dstPath.size() == 1 && !dstPath.get(0).isDir())
      throw new IOException("Destination path is file: " + dst.toString());

    boolean result = true;
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isDir()) {
        result &= swiftRestClient.copyObject(SwiftObjectPath.fromPath(uri, fileStatus.getPath()),
                SwiftObjectPath.fromPath(uri, dst));

        swiftRestClient.delete(SwiftObjectPath.fromPath(uri, fileStatus.getPath()));
      }
    }

    return result;
  }

  private List<FileStatus> listDirectory(SwiftObjectPath path) throws IOException {
    String uri = path.toUriPath();
    if (!uri.endsWith(Path.SEPARATOR))
      uri += Path.SEPARATOR;

    final byte[] bytes;
    try {
      bytes = swiftRestClient.findObjectsByPrefix(path);
    } catch (SwiftException e) {
      throw new IOException(e);
    }
    if (bytes == null)
      return Collections.emptyList();

    final StringTokenizer tokenizer = new StringTokenizer(new String(bytes), "\n");
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();

    while (tokenizer.hasMoreTokens()) {
      String pathInSwift = tokenizer.nextToken();
      if (!pathInSwift.startsWith("/")) {
        pathInSwift = "/".concat(pathInSwift);
      }
      final FileStatus metadata = getObjectMetadata(new Path(pathInSwift));
      if (metadata != null)
        files.add(metadata);
    }

    return files;
  }

  private Path getCorrectSwiftPath(Path path) throws URISyntaxException {
    final URI fullUri = new URI(uri.getScheme(), uri.getAuthority(), path.toUri().getPath(), null, null);

    return new Path(fullUri);
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
