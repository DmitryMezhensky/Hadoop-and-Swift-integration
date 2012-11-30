package org.apache.hadoop.swift.fs.container;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.swift.fs.block.SwiftBlockFileSystem;
import org.apache.hadoop.swift.fs.snative.SwiftFileSystem;
import org.apache.hadoop.swift.fs.util.SwiftObjectPath;

import java.io.BufferedReader;
import java.io.FileReader;
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
}
