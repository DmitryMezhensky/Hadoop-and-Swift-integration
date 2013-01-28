/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift;

import junit.framework.AssertionFailedError;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.http.RestClientBindings;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Properties;

public class TestSwiftFileSystemExtendedContract {

  private static final Log LOG =
    LogFactory.getLog(TestSwiftFileSystemExtendedContract.class);

  protected final static String TEST_UMASK = "062";
  protected FileSystem fs;
  protected byte[] data = dataset(getBlockSize() * 2, 0, 255);

  @Before
  public void setUp() throws Exception {
    final URI uri = getFilesystemURI();
    final Configuration conf = new Configuration();

    SwiftNativeFileSystem swiftFS = createSwiftFS();
    fs = swiftFS;
    fs.initialize(uri, conf);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (fs != null) {
        fs.delete(path("/test"), true);
      }
    } catch (IOException e) {
      LOG.error("Error deleting /test: " + e, e);
    }
  }

  public void downgrade(String message, AssertionFailedError failure) {
    LOG.warn("Skipping test " + message, failure);
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return SwiftTestUtils.getServiceURI(new Configuration());
  }

  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    SwiftNativeFileSystem swiftNativeFileSystem =
      new SwiftNativeFileSystem();
    return swiftNativeFileSystem;
  }

  protected int getBlockSize() {
    return 1024;
  }

  protected String getDefaultWorkingDirectory() {
    return "/user/" + System.getProperty("user.name");
  }


  protected boolean renameSupported() {
    return true;
  }

  protected Path path(String pathString) {
    return new Path(pathString).makeQualified(fs);
  }

  protected void createFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(data, 0, data.length);
    out.close();
  }

  private void rename(Path src, Path dst, boolean renameSucceeded,
                      boolean srcExists, boolean dstExists) throws IOException {
    boolean renamed = fs.rename(src, dst);
    String lsDst = ls(dst);
    Path parent = dst.getParent();
    String lsParent = parent != null ? ls(parent) : "";
    String outcome = "  result of " + src + " => " + dst
                     + " - " + lsDst
                     + " \n" + lsParent;
    LOG.info(outcome);
    assertEquals("Rename " + outcome,
                 renameSucceeded, renamed);
    assertEquals("Source " + src + "exists: "+ outcome,
                 srcExists, fs.exists(src));
    assertEquals("Destination " + dstExists + " exists" + outcome,
                 dstExists, fs.exists(dst));
  }


  /**
   * Create a dataset for use in the tests; all data is in the range
   * base to (base+modulo-1) inclusive
   * @param len length of data
   * @param base base of the data
   * @param modulo the modulo
   * @return the newly generated dataset
   */
  protected byte[] dataset(int len, int base, int modulo) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base + (i % modulo));
    }
    return dataset;
  }

  protected String ls(Path path) throws IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of the path
      return "/";
    }
    FileStatus[] stats = new FileStatus[0];
    try {
      stats = fs.listStatus(path);
    } catch (FileNotFoundException e) {
      return "ls " + path + " -file not found";
    }
    String pathname = path.toString();
    return dumpStats(pathname, stats);
  }

  private String dumpStats(String pathname, FileStatus[] stats) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    buf.append("ls ").append(pathname).append(": ").append(stats.length)
       .append("\n");
    for (FileStatus stat : stats) {
      buf.append(stat.toString()).append("\n");
    }
    return buf.toString();
  }

  @Test
  public void testRenameFileAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;

    Path src = path("/test/hadoop/file");
    createFile(src);
    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    Path newFile = path("/test/new/newdir/file");
    if (!fs.exists(newFile)) {
      String ls = ls(dst);
      LOG.info(ls(path("/test/new")));
      LOG.info(ls(path("/test/hadoop")));
      fail("did not find " + newFile + " - directory: " + ls);
    }
    assertTrue("Destination changed",
               fs.exists(path("/test/new/newdir/file")));
  }


  @Test
  public void testRenameFile() throws Exception {
    if (!renameSupported()) return;

    final Path old = new Path("/test/alice/file");
    final Path newPath = new Path("/test/bob/file");
    fs.mkdirs(newPath.getParent());
    final FSDataOutputStream fsDataOutputStream = fs.create(old);
    final byte[] message = "Some data".getBytes();
    fsDataOutputStream.write(message);
    fsDataOutputStream.close();

    assertTrue(fs.exists(old));
    rename(old, newPath, true, false, true);

    final FSDataInputStream open = fs.open(newPath);
    final byte[] bytes = new byte[512];
    final int read = open.read(bytes);
    final byte[] buffer = new byte[read];
    System.arraycopy(bytes, 0, buffer, 0, read);
    assertEquals(new String(message), new String(buffer));
  }

  @Test

  public void testRenameDirectory() throws Exception {
    if (!renameSupported()) return;

    final Path old = new Path("/test/data/logs");
    final Path newPath = new Path("/test/var/logs");
    fs.mkdirs(old);
    fs.mkdirs(newPath.getParent());
    assertTrue(fs.exists(old));
    rename(old, newPath, true, false, true);
  }

  @Test
  public void testRenameTheSameDirectory() throws Exception {
    if (!renameSupported()) return;

    final Path old = new Path("/test/usr/data");
    fs.mkdirs(old);
    rename(old, old, false, true, true);
  }

  @Test
  public void testListStatus() throws Exception {
    Path[] testDirs = {
      path("/test/hadoop/a"),
      path("/test/hadoop/b"),
      path("/test/hadoop/c/1"),
    };
    assertFalse(fs.exists(testDirs[0]));

    for (Path path : testDirs) {
      assertTrue(fs.mkdirs(path));
    }

    FileStatus[] paths = fs.listStatus(path("/test"));
    assertEquals(dumpStats("/test", paths), 1, paths.length);
    assertEquals(path("/test/hadoop"), paths[0].getPath());

    paths = fs.listStatus(path("/test/hadoop"));
    assertEquals(dumpStats("/test/hadoop", paths),3, paths.length);
    assertEquals(path("/test/hadoop/a"), paths[0].getPath());
    assertEquals(path("/test/hadoop/b"), paths[1].getPath());
    assertEquals(path("/test/hadoop/c"), paths[2].getPath());

    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(dumpStats("/test/hadoop/a", paths),0, paths.length);
  }


  @Test
  public void testOpenNonExistFile() throws IOException {
    final Path p = new Path("/test/testOpenNonExistFile");
    //open it as a file, should get FileNotFoundException
    try {
      final FSDataInputStream in = fs.open(p);
      in.read();
      in.close();
      fail("didn't expect to get here");
    } catch (FileNotFoundException fnfe) {
      LOG.info("This is expected.", fnfe);
    }
  }


  @Test
  public void testHasURI() throws Throwable {
    assertNotNull(fs.getUri());
  }

  @Test
  public void testCreateFile() throws Exception {
    final Path f = new Path("/test/testCreateFile");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    fsDataOutputStream.close();
    assertTrue(fs.exists(f));
  }

  @Test
  public void testDeleteFile() throws IOException {
    final Path f = new Path("/test/testDeleteFile");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    fsDataOutputStream.close();

    assertTrue(fs.exists(f));

    fs.delete(f, true);
    final boolean exists = fs.exists(f);
    assertFalse(exists);
  }

  @Test
  public void testWriteReadFile() throws Exception {
    final Path f = new Path("/test/test");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    final String message = "Test string";
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();

    assertTrue(fs.exists(f));
    try {
      final FSDataInputStream open = fs.open(f);
      final byte[] bytes = new byte[512];
      final int read = open.read(bytes);
      final byte[] buffer = new byte[read];
      System.arraycopy(bytes, 0, buffer, 0, read);
      assertEquals(message, new String(buffer));
    } finally {
      fs.delete(f, false);
    }
  }

  @Test
  public void testConfDefinesFilesystem() throws Throwable {
    Configuration conf = new Configuration();
    URI fsURI = SwiftTestUtils.getServiceURI(conf);
  }

  @Test
  public void testConfIsValid() throws Throwable {
    Configuration conf = new Configuration();
    URI fsURI = SwiftTestUtils.getServiceURI(conf);
    Properties properties = RestClientBindings.bind(fsURI, conf);
  }

  @Test
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) return;

    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));

    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertExists("new dir", path("/test/new/newdir/dir"));
    assertExists("Renamed nested file1", path("/test/new/newdir/dir/file1"));
    assertExists("Renamed nested subdir",
                 path("/test/new/newdir/dir/subdir/file2"));
    assertFalse("Nested file1 should have been deleted",
                fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested /test/hadoop/dir/subdir/file2 still exists",
                fs.exists(path("/test/hadoop/dir/subdir/file2")));
  }

  public void assertExists(String message, Path path) throws IOException {
    if(!fs.exists(path)) {
      //failure, report it
      fail(message + ": not found " + path + " in " + ls(path.getParent()));
    }
  }

  /**
   * Assert that a filesystem is case sensitive.
   * This is done by creating a mixed-case filename and asserting that
   * its lower case version is not there.
   * @throws Exception failures
   */
  @Test
  public void testFilesystemIsCaseSensitive() throws Exception {
    String mixedCaseFilename = "/test/UPPER.TXT";
    Path upper = path(mixedCaseFilename);
    Path lower = path(mixedCaseFilename.toLowerCase(Locale.ENGLISH));
    assertFalse("File exists" + upper, fs.exists(upper));
    assertFalse("File exists" + lower, fs.exists(lower));
    FSDataOutputStream out = fs.create(upper);
    out.writeUTF("UPPER");
    out.close();
    FileStatus upperStatus = fs.getFileStatus(upper);
    assertTrue("File does not exists" + upper, fs.exists(upper));
    //verify the lower-case version of the filename doesn't exist
    assertFalse("File exists" + lower, fs.exists(lower));
    //now overwrite the lower case version of the filename with a
    //new version.
    out = fs.create(lower);
    out.writeUTF("l");
    out.close();
    assertTrue("File exists" + lower, fs.exists(lower));
    //verify the length of the upper file hasn't changed
    FileStatus newStatus = fs.getFileStatus(upper);
    assertEquals("Expected status:" + upperStatus
                 + " actual status " + newStatus,
                 upperStatus.getLen(),
                 newStatus.getLen());
  }

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   * @throws Exception on failures
   */
  public void testZeroByteFilesAreFiles() throws Exception {
    Path src = path("/test/testZeroByteFilesAreFiles");
    //create a zero byte file
    FSDataOutputStream out = fs.create(src);
    out.close();
    assertIsFile(src);
  }

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   * @throws Exception on failures
   */
  public void testMultiByteFilesAreFiles() throws Exception {
    Path src = path("/test/testMultiByteFilesAreFiles");
    FSDataOutputStream out = fs.create(src);
    out.writeUTF("testMultiByteFilesAreFiles");
    out.close();
    assertIsFile(src);
  }

  /**
   * Assert that root directory renames are not allowed
   * @throws Exception on failures
   */
  public void testRenameRootDirForbidden() throws Exception {
    if (!renameSupported()) return;

    rename(path("/"),
           path("/test/newRootDir"),
           false, true, false);
  }

  /**
   * Assert that renaming a parent directory to be a child
   * of itself is forbidden
   * @throws Exception on failures
   */
  public void testRenameChildDirForbidden() throws Exception {
    if (!renameSupported()) return;

    Path parentdir = path("/test/parentdir");
    fs.mkdirs(parentdir);
    Path childFile = new Path(parentdir, "childfile");
    createFile(childFile);
    //verify one level down
    Path childdir = new Path(parentdir, "childdir");
    rename(parentdir, childdir, false, true, false);
    //now another level
    fs.mkdirs(childdir);
    Path childchilddir = new Path(childdir, "childdir");
    rename(parentdir, childchilddir, false, true, false);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  private void assertIsFile(Path filename) throws IOException {
    assertTrue("Does not exit: " + filename, fs.exists(filename));
    FileStatus status = fs.getFileStatus(filename);
    String fileInfo = filename + "  " + status;
    assertTrue("Not a file " + fileInfo, status.isFile());
    assertFalse("File claims to be a symlink " + fileInfo,
                status.isSymlink());
    assertFalse("File claims to be a directory " + fileInfo,
                status.isDirectory());
  }

}
