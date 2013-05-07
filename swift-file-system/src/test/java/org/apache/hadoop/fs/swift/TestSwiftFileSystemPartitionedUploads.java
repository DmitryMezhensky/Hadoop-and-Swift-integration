/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.snative.SwiftFileSystemForFunctionalTests;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;

import static org.apache.hadoop.fs.swift.util.SwiftTestUtils.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Test partitioned uploads
 */
public class TestSwiftFileSystemPartitionedUploads extends SwiftFileSystemBaseTest {

  public static final String WRONG_PARTITION_COUNT =
    "wrong number of partitions written";
  private URI uri;
  private Configuration conf;
  private SwiftFileSystemForFunctionalTests swiftFS;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    uri = getFilesystemURI();
    conf = new Configuration();
    //patch the configuration with the factory of the new driver


    swiftFS = new SwiftFileSystemForFunctionalTests();
    swiftFS.setPartitionSize(1024L);
    fs = swiftFS;
    fs.initialize(uri, conf);
  }

  @Override
  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    swiftFS = new SwiftFileSystemForFunctionalTests();
    swiftFS.setPartitionSize(1024L);
    return swiftFS;
  }

  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return SwiftTestUtils.getServiceURI(new Configuration());
  }

  /**
   * tests functionality for big files ( > 5Gb) upload
   */
  @Test
  public void testFilePartUpload() throws IOException, URISyntaxException {

    final Path path = new Path("/test/huge-file");

    int len = 4096;
    final byte[] src = SwiftTestUtils.dataset(len,32,144);
    FSDataOutputStream out = fs.create(path, false,
                                       fs.getConf()
                                         .getInt("io.file.buffer.size",
                                                 4096),
                                       (short) 1,
                                       1024);
    assertEquals(WRONG_PARTITION_COUNT,
                 0, swiftFS.getPartitionsWritten(out));
    //write first half
    out.write(src, 0, len / 2);
    assertEquals(WRONG_PARTITION_COUNT,
                 1, swiftFS.getPartitionsWritten(out));
    //write second half
    out.write(src, len / 2, len / 2);
    assertEquals(WRONG_PARTITION_COUNT,
                 2, swiftFS.getPartitionsWritten(out));
    out.close();
    assertEquals(WRONG_PARTITION_COUNT,
                 3, swiftFS.getPartitionsWritten(out));

    assertTrue("Exists", fs.exists(path));
    FileStatus status = fs.getFileStatus(path);
    assertEquals("Length", len, status.getLen());


    byte[] dest = readDataset(fs, path, len);
    SwiftTestUtils.compareByteArrays(src, dest, len);

    //now see what block location info comes back. 
    //This will vary depending on the Swift version, so the results
    //aren't checked -merely that the test actually worked
    BlockLocation[] locations = fs.getFileBlockLocations(status, 0, len);
    assertNotNull("Null getFileBlockLocations()", locations);
    assertTrue("empty array returned for getFileBlockLocations()",
               locations.length > 0);
  }


}
