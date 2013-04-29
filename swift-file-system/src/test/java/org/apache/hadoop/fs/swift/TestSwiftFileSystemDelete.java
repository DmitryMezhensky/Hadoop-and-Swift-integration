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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
/**
 * Test deletion operations
 */
public class TestSwiftFileSystemDelete extends SwiftFileSystemBaseTest {

  @Test
  public void testDeleteEmptyFile() throws IOException {
    final Path file = new Path("/test/testDeleteFile");
    createEmptyFile(file);
    SwiftTestUtils.noteAction("about to delete");
    assertDeleted(file, true);
  }

  @Test
  public void testDeleteNonEmptyFile() throws IOException {
    final Path file = new Path("/test/testDeleteFile");
    createFile(file);
    assertDeleted(file, true);
  }


  /**
   * Test recursive root directory deletion fails if there is an entry underneath
   * @throws Throwable
   */
  @Test
  public void testRmRootDirRecursiveIsForbidden() throws Throwable {
    Path root = path("/");
    Path testFile = path("/test");
    createFile(testFile);
    assertTrue("rm(/) returned false", fs.delete(root, true));
    assertExists("Root dir is missing", root);
    assertPathDoesNotExist("test file not deleted", testFile);
  }


}
