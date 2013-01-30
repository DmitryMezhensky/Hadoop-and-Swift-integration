package org.apache.hadoop.fs.swift.functional.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftFileSystemForFunctionalTests;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * these tests currently are unit tests, but will be
 * moved to functional/integration tests
 */
public class SwiftFileSystemTest {
  URI uri;
  Configuration conf;

  @Before
  public void initialization() throws URISyntaxException {
    this.conf = new Configuration();
    this.conf.set("fs.swift.service.rackspace.auth.url", "");
    this.conf.set("fs.swift.service.rackspace.tenant", "");
    this.conf.set("fs.swift.service.rackspace.username", "");
    this.conf.set("fs.swift.service.rackspace.password", "");
    this.conf.set("fs.swift.service.rackspace.public", "true");
    this.conf.setInt("fs.swift.service.rackspace.http.port", 8080);
    this.conf.setInt("fs.swift.service.rackspace.https.port", 443);

    this.uri = new URI("swift://data.rackspace");
  }

  /**
   * tests functionality for big files ( > 5Gb) upload
   */
  @Test
  public void testFilePartUpload() throws IOException, URISyntaxException {
    final SwiftFileSystemForFunctionalTests fs =
            new SwiftFileSystemForFunctionalTests();
    fs.initialize(uri, conf);

    final Path f = new Path("/home/huge/file/test/file");
    final FSDataOutputStream fsDataOutputStream =
            fs.create(f);

    final String originalString = createDataSize(2000);
    final String secondString = "bbb";
    fsDataOutputStream.write(originalString.getBytes());
    fsDataOutputStream.write(secondString.getBytes());
    fsDataOutputStream.close();

    final FSDataInputStream open = fs.open(f);

    final StringBuilder readData = new StringBuilder();
    final byte[] buffer = new byte[1024];
    int read = 0;
    while (read >= 0) {
      read = open.read(buffer);
      if (read > 0) {
        readData.append(new String(buffer, 0, read));
      }
    }

    assertEquals(originalString.concat(secondString), readData.toString());
  }

  /**
   * test on concurrent file system changes
   */
  @Test(expected = FileNotFoundException.class)
  public void raceConditionOnDirDeleteTest() throws IOException, URISyntaxException, InterruptedException {
    final SwiftNativeFileSystem fileSystem = new SwiftNativeFileSystem();
    fileSystem.initialize(uri, conf);

    final String message = "message";
    final Path fileToRead = new Path("/home/huge/files/many-files/file");
    final ExecutorService executorService = Executors.newFixedThreadPool(2);
    fileSystem.create(new Path("/home/huge/file/test/file1"));
    fileSystem.create(new Path("/home/huge/documents/doc1"));
    fileSystem.create(new Path("/home/huge/pictures/picture"));

    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          fileSystem.delete(new Path("/home/huge"), true);
        } catch (IOException e) {
          throw new RuntimeException("test failed", e);
        }
      }
    });
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          final FSDataOutputStream outputStream = fileSystem.create(fileToRead);
          outputStream.write(message.getBytes());
          outputStream.close();
        } catch (IOException e) {
          throw new RuntimeException("test failed", e);
        }
      }
    });

    executorService.awaitTermination(2, TimeUnit.MINUTES);

    fileSystem.open(fileToRead);

  }

  @Test
  public void testRenameDirWithSubDis() throws IOException {
    final SwiftNativeFileSystem fileSystem = new SwiftNativeFileSystem();
    fileSystem.initialize(uri, conf);

    final String message = "message";
    final Path filePath = new Path("/home/user/documents/file.txt");
    final Path newFilePath = new Path("/home/user/files/file.txt");

    final FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    fileSystem.rename(filePath, newFilePath);

    final FSDataInputStream inputStream = fileSystem.open(newFilePath);
    final byte[] data = new byte[20];
    final int read = inputStream.read(data);

    assertEquals(message.length(), read);
    assertEquals(message, new String(data, 0, read));
  }

  private static String createDataSize(int msgSize) {
    StringBuilder sb = new StringBuilder(msgSize);
    for (int i = 0; i < msgSize; i++) {
      sb.append('a');
    }
    return sb.toString();
  }
}
