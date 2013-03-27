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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Test block location logic.
 * The endpoint may or may not be location-aware
 */
public class SwiftFileSystemTestBlockLocation extends SwiftFileSystemBaseTest {

  @Test
  public void testLocateSingleFileBlocks() throws Throwable {
    Path path = path("/test/locatedFile");
    createFile(path);
    FileStatus fileStatus = fs.getFileStatus(path);
    BlockLocation[] locations =
            getFs().getFileBlockLocations(fileStatus, 0, 1);

    assertNotEqual("No block locations supplied for " + fileStatus,
            0, locations.length);

    for (BlockLocation location : locations) {
      LOG.info(location);
      String[] hosts = location.getHosts();
      assertNotEqual("No hosts supplied for " + location, 0, hosts.length);
    }
  }

  protected void assertNotEqual(String message, int expected, int actual) {
    assertTrue(message,
            actual != expected);
  }
}
