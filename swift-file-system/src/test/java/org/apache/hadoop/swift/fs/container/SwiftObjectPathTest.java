package org.apache.hadoop.swift.fs.container;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.swift.fs.SwiftObjectPath;
import org.apache.hadoop.swift.fs.block.SwiftBlockFileSystem;
import org.apache.hadoop.swift.fs.snative.SwiftFileSystem;

import java.net.URI;

/**
 * @author dmezhensky
 */
public class SwiftObjectPathTest extends TestCase {

    public void testParsePath() throws Exception {
        final String pathString = "/home/user/files/file1";
        final Path path = new Path(pathString);
        final SwiftObjectPath expected = SwiftObjectPath.fromPath(path);
        final SwiftObjectPath actual = new SwiftObjectPath(pathString);

        assertEquals(expected, actual);
    }

    public void testFoo() throws Exception {
        final Configuration configuration = new Configuration();
        final SwiftBlockFileSystem swiftBlockFileSystem = new SwiftBlockFileSystem(configuration);
        swiftBlockFileSystem.initialize(new URI("http://bigdata-centos1:8080"), configuration);

        final Path path = new Path("/home/dmitry/try-to-read-locally");
        final FSDataOutputStream fsDataOutputStream = swiftBlockFileSystem.create(path);
        fsDataOutputStream.write("file to be read locally".getBytes());
        fsDataOutputStream.close();

        final FSDataInputStream inputStream = swiftBlockFileSystem.open(path);
        final byte[] buffer = new byte[1024];
        while (inputStream.read(buffer) > 0) {
            System.out.println(new String(buffer));
        }

    }
}
