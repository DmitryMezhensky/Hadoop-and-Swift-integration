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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Tests for NativeSwiftFS using either the in-memory Swift emulator
 * or the real client
 */
public abstract class NativeSwiftFileSystemContractBaseTest
        extends FileSystemContractBaseTest {

  private static final Log LOG = LogFactory
          .getLog(NativeSwiftFileSystemContractBaseTest.class);

  @Override
  protected void setUp() throws Exception {
    final URI uri = getFilesystemURI();
    final Configuration conf = new Configuration();
    fs = createSwiftFS();
    fs.initialize(uri, conf);
    super.setUp();
  }

  /**
   * Get the URI of this filesystem
   *
   * @return a filesystem URI
   * @throws URISyntaxException Any URI parse failure
   * @throws IOException        other problems
   */
  protected abstract URI getFilesystemURI()
          throws URISyntaxException, IOException;

  /**
   * Create a basic SwiftFS. This can be done differently for
   * the different implementations (memory vs. live)
   *
   * @throws IOException
   */
  protected abstract SwiftNativeFileSystem createSwiftFS() throws IOException;

}
