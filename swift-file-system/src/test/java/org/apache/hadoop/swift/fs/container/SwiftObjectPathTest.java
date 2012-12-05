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

  public void testFoo1() throws Exception {
    final SwiftBlockFileSystem swiftBlockFileSystem = new SwiftBlockFileSystem();
    final Configuration conf = new Configuration();
    swiftBlockFileSystem.initialize(new URI("swift://bigdata-centos1:8080"), conf);

    final FileSystem swiftFileSystem = new SwiftFileSystem();
    swiftFileSystem.initialize(new URI("swift://bigdata-centos1:8080"), conf);

    final FSDataOutputStream nativeOS = swiftFileSystem.create(new Path("/home/test-native/file2"));
    final FSDataOutputStream blockOS = swiftBlockFileSystem.create(new Path("/home/test-block/file2"));

    final BufferedReader reader = new BufferedReader(new FileReader("/home/dmitry/hadoop-test-data/test2"));

    final char[] buffer = new char[1024 * 4096];
    while (reader.read(buffer) > 0) {
      final byte[] bytes = new String(buffer).getBytes();
      blockOS.write(bytes);
      nativeOS.write(bytes);
    }

    blockOS.close();
    nativeOS.close();
  }

  public void testFoo() throws Exception {
    final SwiftBlockFileSystem swiftFileSystem = new SwiftBlockFileSystem();
    swiftFileSystem.initialize(new URI("swift://bigdata-centos1:8080"), new Configuration());

    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(new Path("/home/dmitry/demo-block/130mb-file"));
    final BufferedReader reader = new BufferedReader(new FileReader("/home/dmitry/projects/bigdata/swift-file-system-locality-test/object0"));
    String line;
    while ((line = reader.readLine()) != null) {
      fsDataOutputStream.write(line.concat("\n").getBytes());
    }

    fsDataOutputStream.close();

    final Path f = new Path("bswift://bigdata-centos1:8080/home/dmitry/demo-block/130mb-file");
    final PrintWriter printWriter = new PrintWriter("/home/dmitry/projects/bigdata/swift-file-system-locality-test/130mb-file");

    final FSDataInputStream open = swiftFileSystem.open(f);

    final byte[] bytes = new byte[1024];
    int read;
    while ((read = open.read(bytes)) > 0) {
      if (read < 1024) {
        final byte[] buffer = new byte[read];
        System.arraycopy(bytes, 0, buffer, 0, read);
        printWriter.write(new String(buffer));
      } else {
        printWriter.write(new String(bytes));
      }
    }
    printWriter.close();
  }

  public void testNativeFS() throws Exception {
    final Configuration conf = new Configuration();

    final FileSystem swiftFileSystem = new SwiftFileSystem();
    swiftFileSystem.initialize(new URI("swift://bigdata-centos1:8080"), conf);

    final Path f = new Path("swift://172.18.66.200:8080/home/dmitry/test1");
    final FileStatus fileStatus = swiftFileSystem.getFileStatus(f);

    final BlockLocation[] fileBlockLocations = swiftFileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    System.out.println();

    final FSDataInputStream open = swiftFileSystem.open(f);
    System.out.println(fileStatus);
    System.out.println();
    final byte[] bytes = new byte[1024];
    while (open.read(bytes) > 0) {
      System.out.println(new String(bytes));
    }
  }
}
