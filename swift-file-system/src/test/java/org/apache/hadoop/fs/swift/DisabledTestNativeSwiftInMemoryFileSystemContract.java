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

import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Use the {@link InMemorySwiftNativeStore} to simulate swift.
 * This is an unrealistic test -the real test is preferable.
 */
public class DisabledTestNativeSwiftInMemoryFileSystemContract
  extends NativeSwiftFileSystemContractBaseTest {

  protected SwiftNativeFileSystem createSwiftFS() throws IOException {
    SwiftNativeFileSystem
      swiftNativeFileSystem =
      new SwiftNativeFileSystem(new InMemorySwiftNativeStore());
    return swiftNativeFileSystem;
  }

  @Override
  protected URI getFilesystemURI() throws URISyntaxException, IOException {
    return new URI("swift://localhost:8080");
  }
}
