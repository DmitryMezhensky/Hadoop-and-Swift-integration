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
 *
 * Integration tests, to run it read documentation
 * https://github.com/DmitryMezhensky/Hadoop-and-Swift-integration/wiki/HowTo
 */

package org.apache.hadoop.fs.swift;

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


public class TestSwiftFileSystemPartFileUpload {
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

  private static String createDataSize(int msgSize) {
    StringBuilder sb = new StringBuilder(msgSize);
    for (int i = 0; i < msgSize; i++) {
      sb.append('a');
    }
    return sb.toString();
  }
}
