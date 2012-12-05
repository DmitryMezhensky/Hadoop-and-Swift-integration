package org.apache.hadoop.swift.fs.snative;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.swift.fs.http.RestClient;
import org.apache.hadoop.swift.fs.util.SwiftObjectPath;

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
public class SwiftFileSystemStore {
  private static final Pattern URI_PATTERN = Pattern.compile("\"\\S+?\"");
  private static final String PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  private URI uri;
  private final RestClient restClient;

  public SwiftFileSystemStore(URI uri, Configuration configuration) {
    this.uri = uri;
    this.restClient = RestClient.getInstance(configuration);
  }

  public void uploadFile(Path path, InputStream inputStream, long length) {
    restClient.upload(SwiftObjectPath.fromPath(path), inputStream, length);
  }

  public FileStatus getObjectMetadata(Path path) throws URISyntaxException {
    final Header[] headers = restClient.headRequest(SwiftObjectPath.fromPath(path));
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

    return new FileStatus(length, isDir, 0, 0l, lastModified, getCorrectSwiftPath(path));
  }

  public InputStream getObject(Path path) {
    return restClient.getDataAsInputStream(SwiftObjectPath.fromPath(path));
  }

  public InputStream getObject(Path path, long byteRangeStart, long length) {
    return restClient.getDataAsInputStream(SwiftObjectPath.fromPath(path), byteRangeStart, length);
  }

  public FileStatus[] listSubPaths(Path path) throws IOException {
    final Collection<FileStatus> fileStatuses;
    try {
      fileStatuses = listDirectory(SwiftObjectPath.fromPath(path));
    } catch (URISyntaxException e) {
      throw new IOException("path " + path + " is incorrect", e);
    }

    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  public void createDirectory(Path path) {

    restClient.putRequest(SwiftObjectPath.fromPath(path));
  }

  public List<URI> getObjectLocation(Path path) {
    final byte[] objectLocation = restClient.getObjectLocation(SwiftObjectPath.fromPath(path));
    return extractUris(new String(objectLocation));
  }

  /**
   * deletes object from Swift
   */
  public void deleteObject(Path path) throws IOException {

    restClient.delete(SwiftObjectPath.fromPath(path));
  }

  /**
   * Checks if specified path exists
   *
   * @param path to check
   * @return true - path exists, false otherwise
   */
  public boolean objectExists(Path path) throws URISyntaxException {

    return listDirectory(SwiftObjectPath.fromPath(path)).size() != 0;
  }

  public boolean renameDirectory(Path src, Path dst) throws IOException {
    final List<FileStatus> fileStatuses;
    final List<FileStatus> dstPath;
    try {
      fileStatuses = listDirectory(SwiftObjectPath.fromPath(src));
      dstPath = listDirectory(SwiftObjectPath.fromPath(dst));
    } catch (URISyntaxException e) {
      throw new IOException("path " + src + " or " + dst + " is incorrect", e);
    }

    if (dstPath.size() == 1 && !dstPath.get(0).isDir())
      throw new IOException("Destination path is file: " + dst.toString());

    boolean result = true;
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isDir()) {
        result &= restClient.copyObject(SwiftObjectPath.fromPath(fileStatus.getPath()),
                SwiftObjectPath.fromPath(dst));

        restClient.delete(SwiftObjectPath.fromPath(fileStatus.getPath()));
      }
    }

    return result;
  }

  private List<FileStatus> listDirectory(SwiftObjectPath path) throws URISyntaxException {
    String uri = path.toUriPath();
    if (!uri.endsWith(Path.SEPARATOR))
      uri += Path.SEPARATOR;

    final byte[] bytes = restClient.findObjectsByPrefix(path);
    if (bytes == null)
      return Collections.emptyList();

    final StringTokenizer tokenizer = new StringTokenizer(new String(bytes), "\n");
    final ArrayList<FileStatus> files = new ArrayList<FileStatus>();

    while (tokenizer.hasMoreTokens()) {
      final String pathInSwift = tokenizer.nextToken();
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
