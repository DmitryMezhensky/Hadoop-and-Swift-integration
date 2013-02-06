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
 * Integration tests, to run it read documentation
 * https://github.com/DmitryMezhensky/Hadoop-and-Swift-integration/wiki/HowTo
 *
 */
public class SwiftFileSystemTest {
  URI uri;
  Configuration conf;

  @Before
  public void initialization() throws URISyntaxException {
    this.conf = new Configuration();

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
  public void testRenameFile() throws IOException {
    final SwiftNativeFileSystem fileSystem = new SwiftNativeFileSystem();
    fileSystem.initialize(uri, conf);

    final String message = "message";
    final Path filePath = new Path("/home/tom/documents/file");
    final Path newFilePath = new Path("/home/tom/documents/file1");

    final FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    fileSystem.rename(filePath, newFilePath);

    final FSDataInputStream inputStream = fileSystem.open(newFilePath);
    final byte[] data = new byte[20];
    final int read = inputStream.read(data);

    assertEquals(message.length(), read);
    assertEquals(message, new String(data, 0, read));

    fileSystem.delete(filePath, true);
    fileSystem.delete(newFilePath, true);
  }

  @Test
  public void testRenameDirectoryWithFile() throws Exception {
    final SwiftNativeFileSystem fileSystem = new SwiftNativeFileSystem();
    fileSystem.initialize(uri, conf);

    final Path filePath = new Path("/home/user/files/secret file.docx");
    final Path newFilePath = new Path("/home/user/docs");

    final FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
    fsDataOutputStream.write("this is a file".getBytes());
    fsDataOutputStream.close();

    fileSystem.create(newFilePath).close();
    assertTrue(fileSystem.rename(filePath, newFilePath));

    assertNotNull(fileSystem.getFileStatus(new Path("/home/user/docs/secret file.docx")));
    fileSystem.delete(newFilePath, true);
  }

  @Test
  public void testRenameDirectoryWithFiles() throws Exception {
    final SwiftNativeFileSystem fileSystem = new SwiftNativeFileSystem();
    fileSystem.initialize(uri, conf);

    final Path logPath1 = new Path("/var/log/hadoop/logs/log1");
    final Path logPath2 = new Path("/var/log/hadoop/logs/log2");
    final Path logPath3 = new Path("/var/log/hadoop/logs/log3");
    final Path logPath4 = new Path("/var/log/hadoop/logs/log4");
    final Path logPath5 = new Path("/var/log/hadoop/logs/log5");
    final Path newFilePath = new Path("/var/log/user");

    fileSystem.create(logPath1).close();
    fileSystem.create(logPath2).close();
    fileSystem.create(logPath3).close();
    fileSystem.create(logPath4).close();
    fileSystem.create(logPath5).close();

    fileSystem.create(newFilePath);
    assertTrue(fileSystem.rename(new Path("/var/log/hadoop"), newFilePath));

    try {
      fileSystem.getFileStatus(logPath1);
      throw new AssertionError("Directory exists after renaming");
    } catch (FileNotFoundException e) {
    }
    try {
      fileSystem.getFileStatus(logPath2);
      throw new AssertionError("Directory exists after renaming");
    } catch (FileNotFoundException e) {
    }
    try {
      fileSystem.getFileStatus(logPath3);
      throw new AssertionError("Directory exists after renaming");
    } catch (FileNotFoundException e) {
    }
    try {
      fileSystem.getFileStatus(logPath4);
      throw new AssertionError("Directory exists after renaming");
    } catch (FileNotFoundException e) {
    }
    try {
      fileSystem.getFileStatus(logPath5);
      throw new AssertionError("Directory exists after renaming");
    } catch (FileNotFoundException e) {
    }


    assertNotNull(fileSystem.getFileStatus(new Path("/var/log/user/log1")));
    assertNotNull(fileSystem.getFileStatus(new Path("/var/log/user/log2")));
    assertNotNull(fileSystem.getFileStatus(new Path("/var/log/user/log3")));
    assertNotNull(fileSystem.getFileStatus(new Path("/var/log/user/log4")));
    assertNotNull(fileSystem.getFileStatus(new Path("/var/log/user/log5")));
  }

  private static String createDataSize(int msgSize) {
    StringBuilder sb = new StringBuilder(msgSize);
    for (int i = 0; i < msgSize; i++) {
      sb.append('a');
    }
    return sb.toString();
  }
}
