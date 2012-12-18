package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Tests for NativeSwiftFS using in-memory Swift emulator
 */
public class NativeSwiftFileSystemContractBaseTest extends FileSystemContractBaseTest {

  private SwiftNativeFileSystem swiftFileSystem;

  @Override
  protected void setUp() throws Exception {
    swiftFileSystem = new SwiftNativeFileSystem();
    final URI uri = new URI("swift://localhost:8080");
    final Configuration conf = new Configuration();

    swiftFileSystem = new SwiftNativeFileSystem(uri, conf, new InMemorySwiftNativeStore());
  }

  @Override
  public void tearDown() throws Exception {
  }

  public void testCreateFile() throws Exception {
    final Path f = new Path("/home/user");
    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(f);
    fsDataOutputStream.close();

    assertTrue(swiftFileSystem.exists(f));
  }

  public void testDeleteFile() throws IOException {
    final Path f = new Path("/home/user");
    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(f);
    fsDataOutputStream.close();

    assertTrue(swiftFileSystem.exists(f));

    swiftFileSystem.delete(f, true);
    final boolean exists = swiftFileSystem.exists(f);
    assertFalse(exists);
  }

  public void testWriteReadFile() throws Exception {
    final Path f = new Path("/home/user");
    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(f);
    final String message = "Test string";
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    assertTrue(swiftFileSystem.exists(f));
    final FSDataInputStream open = swiftFileSystem.open(f);
    final byte[] bytes = new byte[512];
    final int read = open.read(bytes);
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(message, new String(buffer));
  }

  public void testRenameFile() throws Exception {
    final Path old = new Path("/home/user/file");
    final Path newPath = new Path("/home/bob/file");
    final FSDataOutputStream fsDataOutputStream = swiftFileSystem.create(old);
    final byte[] message = "Some data".getBytes();
    fsDataOutputStream.write(message);
    fsDataOutputStream.close();

    assertTrue(swiftFileSystem.exists(old));
    assertTrue(swiftFileSystem.rename(old, newPath));
    assertTrue(swiftFileSystem.exists(newPath));

    final FSDataInputStream open = swiftFileSystem.open(newPath);
    final byte[] bytes = new byte[512];
    final int read = open.read(bytes);
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(new String(message), new String(buffer));
  }

  public void testRenameDirectory() throws Exception {
    final Path old = new Path("/data/logs");
    final Path newPath = new Path("/var/logs");
    swiftFileSystem.mkdirs(old);

    assertTrue(swiftFileSystem.exists(old));
    assertTrue(swiftFileSystem.rename(old, newPath));
    assertTrue(swiftFileSystem.exists(newPath));
  }

  public void testRenameTheSameDirectory() throws Exception {
    final Path old = new Path("/usr/data");
    swiftFileSystem.mkdirs(old);

    assertTrue(swiftFileSystem.exists(old));
    assertFalse(swiftFileSystem.rename(old, old));
    assertTrue(swiftFileSystem.exists(old));
  }
}
