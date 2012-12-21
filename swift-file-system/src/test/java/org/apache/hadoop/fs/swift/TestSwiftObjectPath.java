package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
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

  @Test
  public void testFoo() throws Exception {
    final SwiftNativeFileSystem swiftFileSystem = new SwiftNativeFileSystem();
    final Configuration conf = new Configuration();
    conf.set("swift.auth.url", "http://172.18.66.110:5000/v2.0/tokens");
    conf.set("swift.tenant", "superuser");
    conf.set("swift.username", "admin1");
    conf.set("swift.password", "password");
    conf.setInt("swift.http.port", 8080);
    conf.setInt("swift.https.port", 443);

    final URI uri = new URI("swift://bigdata-centos1:8080");
    swiftFileSystem.initialize(uri, conf);

    final Path f = new Path("/home/user/complex/file7_5Gb");
    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(f);
    fsDataOutputStream.write("!!!!!!!!".getBytes());
    fsDataOutputStream.close();
    final BlockLocation[] fileBlockLocations =
            swiftFileSystem.getFileBlockLocations(new FileStatus(64 * 1024 * 1024, false, 0, 0, 0, f), 0, 0);
    System.out.println();


/*
    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(f);
    final BufferedReader reader = new BufferedReader(new FileReader("/home/dmitry/projects/bigdata/swift-file-system-locality-test/object0"));
    String line;
    while ((line = reader.readLine()) != null) {
      fsDataOutputStream.write(line.concat("\n").getBytes());
    }

    fsDataOutputStream.close();
    System.out.println("object was written");
*/

    /*final PrintWriter printWriter = new PrintWriter("/home/dmitry/projects/bigdata/swift-file-system-locality-test/130mb-file");
    final FSDataInputStream open = swiftFileSystem.open(f);

    final byte[] bytes = new byte[1024];
    int read;
    while ((read = open.read(bytes)) >= 0) {
      if (read < 1024) {
        final byte[] buffer = new byte[read];
        System.arraycopy(bytes, 0, buffer, 0, read);
        printWriter.write(new String(buffer));
      } else {
        printWriter.write(new String(bytes));
      }
    }

    open.close();
    printWriter.close();*/

    ///swiftFileSystem.delete(new Path("/home/user/file5"), true);
    //swiftFileSystem.delete(new Path("/home/user/7_5Gb_file"), true);

    // swiftFileSystem.rename(new Path("/home/user/file3"), new Path("/home/user/file5"));
  }

}
