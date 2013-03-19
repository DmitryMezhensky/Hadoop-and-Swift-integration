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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSwiftFileSystemDirectories extends SwiftFileSystemBaseTest {

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   *
   * @throws Exception on failures
   */
  @Test
  public void testZeroByteFilesAreDirectories() throws Exception {
    Path src = path("/test/testZeroByteFilesAreFiles");
    //create a zero byte file
    SwiftTestUtils.touch(fs, src, null);
    SwiftTestUtils.assertIsDirectory(fs, src);
  }

  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   *
   * @throws Exception on failures
   */
  @Test
  public void testDirectoriesHaveMatchingFileStatus() throws Exception {
    Path test = path("/test");
    mkdirs(test);
    Path src = path("/test/file");

    //create a zero byte file
    SwiftTestUtils.touch(fs, src, null);
    //stat it
    FileStatus[] statuses = fs.listStatus(test);
    assertNotNull(statuses);
    assertEquals("Wrong number of elements in file status", 1, statuses.length);
    FileStatus stat = statuses[0];
    assertTrue("isDir(): Not a directory: " + stat, stat.isDir());
    assertTrue("isDirectory(): Not a directory: " + stat, stat.isDirectory());
    assertFalse("isFile(): declares itself a file: " + stat, stat.isFile());
  }


  /**
   * Asserts that a zero byte file has a status of file and not
   * directory or symlink
   *
   * @throws Exception on failures
   */
  @Test
  public void testMultiByteFilesAreFiles() throws Exception {
    Path src = path("/test/testMultiByteFilesAreFiles");
    SwiftTestUtils.writeTextFile(fs, src, "testMultiByteFilesAreFiles", false);
    assertIsFile(src);
    FileStatus status = fs.getFileStatus(src);
    assertFalse(status.isDirectory());
  }

}
