package org.apache.hadoop.fs.swift;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.junit.Test;

import java.net.URI;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit tests for SwiftObjectPath class.
 */
public class TestSwiftObjectPath {

  @Test
  public void testParsePath() throws Exception {
    final String pathString = "/home/user/files/file1";
    final Path path = new Path(pathString);
    final URI uri = new URI("http://localhost:35357");
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(uri.getHost(), pathString);

    assertEquals(expected, actual);
  }

  @Test
  public void testParseUrlPath() throws Exception {
    final String pathString = "swift://host1.vm.net:8090/home/user/files/file1";
    final URI uri = new URI(pathString);
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(uri.getHost(), "/home/user/files/file1");

    assertEquals(expected, actual);
  }

  @Test
  public void testParseAuthenticatedUrl() throws Exception {
    final String pathString = "swift://host1.vm.net:8090/v2/AUTH_00345h34l93459y4/home/tom/documents/finance.docx";
    final URI uri = new URI(pathString);
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(uri, path);
    final SwiftObjectPath actual = new SwiftObjectPath(uri.getHost(), "/home/tom/documents/finance.docx");

    assertEquals(expected, actual);
  }
}
