package org.apache.hadoop.swift.fs.container;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.swift.fs.block.SwiftBlockFileSystem;
import org.apache.hadoop.swift.fs.snative.SwiftFileSystem;
import org.apache.hadoop.swift.fs.util.SwiftObjectPath;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.URI;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit tests for SwiftObjectPath class.
 */
public class SwiftObjectPathTest {

  @Test
  public void testParsePath() throws Exception {
    final String pathString = "/home/user/files/file1";
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(path);
    final SwiftObjectPath actual = new SwiftObjectPath(pathString);

    assertEquals(expected, actual);
  }

  @Test
  public void testParseUrlPath() throws Exception {
    final String pathString = "swift://host1.vm.net:8090/home/user/files/file1";
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(path);
    final SwiftObjectPath actual = new SwiftObjectPath("/home/user/files/file1");

    assertEquals(expected, actual);
  }

  @Test
  public void testParseAuthenticatedUrl() throws Exception {
    final String pathString = "swift://host1.vm.net:8090/v2/AUTH_00345h34l93459y4/home/tom/documents/finance.docx";
    final Path path = new Path(pathString);
    final SwiftObjectPath expected = SwiftObjectPath.fromPath(path);
    final SwiftObjectPath actual = new SwiftObjectPath("/home/tom/documents/finance.docx");

    assertEquals(expected, actual);
  }
}
