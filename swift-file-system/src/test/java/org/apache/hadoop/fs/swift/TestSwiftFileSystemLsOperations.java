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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.apache.hadoop.fs.swift.SwiftTestUtils.cleanupInTeardown;
import static org.apache.hadoop.fs.swift.SwiftTestUtils.touch;
import static org.junit.Assert.assertEquals;

public class TestSwiftFileSystemLsOperations extends SwiftFileSystemBaseTest {

  private Path[] testDirs;

  /**
   * Setup creates dirs under test/hadoop
   *
   * @throws Exception
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    //delete the test directory
    Path test = path("/test");
    fs.delete(test, true);
    testDirs = new Path[]{
            path("/test/hadoop/a"),
            path("/test/hadoop/b"),
            path("/test/hadoop/c/1"),
    };

    try {
      SwiftTestUtils.assertPathDoesNotExist(fs, "test directory setup", testDirs[0]);
    } catch (AssertionError e) {
      cleanupInTeardown(fs, "/test");
    }

    for (Path path : testDirs) {
      mkdirs(path);
    }
  }

  @Test
  public void testListLevelTest() throws Exception {
    FileStatus[] paths = fs.listStatus(path("/test"));
    assertEquals(SwiftTestUtils.dumpStats("/test", paths), 1, paths.length);
    assertEquals(path("/test/hadoop"), paths[0].getPath());
  }

  @Test
  public void testListLevelTestHadoop() throws Exception {
    FileStatus[] paths;
    paths = fs.listStatus(path("/test/hadoop"));
    String stats = SwiftTestUtils.dumpStats("/test/hadoop", paths);
    assertEquals(stats, 3, paths.length);
    assertEquals(stats, path("/test/hadoop/a"), paths[0].getPath());
    assertEquals(stats, path("/test/hadoop/b"), paths[1].getPath());
    assertEquals(stats, path("/test/hadoop/c"), paths[2].getPath());
  }

  @Test
  public void testListStatusEmptyDirectory() throws Exception {
    FileStatus[] paths;
    paths = fs.listStatus(path("/test/hadoop/a"));
    assertEquals(SwiftTestUtils.dumpStats("/test/hadoop/a", paths), 0,
            paths.length);
  }

  @Test
  public void testListStatusFile() throws Exception {
    Path file = path("/test/filename");
    createFile(file);
    FileStatus[] paths = fs.listStatus(file);
    assertEquals(SwiftTestUtils.dumpStats("/test/", paths), 1,
            paths.length);
  }

  @Test
  public void testListEmptyRoot() throws Throwable {
    FileStatus[] fileStatuses = fs.listStatus(path("/"));
    assertEquals(1, fileStatuses.length);
  }

  @Test
  public void testListNonEmptyRoot() throws Throwable {
    Path file = path("/test");
    touch(fs, file, "some data");
    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
    assertEquals(1, fileStatuses.length);
    FileStatus status = fileStatuses[0];
    assertEquals(file, status.getPath());
  }


  @Test
  public void testLSRootDir() throws Throwable {
    Path dir = path("/");
    Path child = new Path(dir, "test");
    SwiftTestUtils.assertListFilesFinds(fs, dir, child);
  }

  @Test
  public void testListStatusRootDir() throws Throwable {
    Path dir = path("/");
    Path child = path("/test");
    SwiftTestUtils.assertListStatusFinds(fs, dir, child);
  }
}
