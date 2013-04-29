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
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;

/**
 * Test the swift service-specific configuration binding features
 */
public class TestSwiftConfig {


  public static final String SERVICE = "openstack";

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testEmptyUrl() throws Exception {
    final Configuration configuration = new Configuration();

    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_USERNAME, "username");
    set(configuration, DOT_PASSWORD, "password");
    mkInstance(configuration);
  }


  @Test
  public void testEmptyTenant() throws Exception {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_USERNAME, "username");
    set(configuration, DOT_PASSWORD, "password");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testEmptyUsername() throws Exception {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_PASSWORD, "password");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testEmptyPassword() throws Exception {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_USERNAME, "username");
    mkInstance(configuration);
  }

  public void testGoodRetryCount() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_RETRY_COUNT, "3");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testBadRetryCount() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_RETRY_COUNT, "three");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testBadConnectTimeout() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_CONNECTION_TIMEOUT, "three");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testZeroBlocksize() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_BLOCKSIZE, "0");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testNegativeBlocksize() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_BLOCKSIZE, "-1");
    mkInstance(configuration);
  }

  @Test
  public void testPositiveBlocksize() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_BLOCKSIZE, "1");
    mkInstance(configuration);
  }

  private Configuration createCoreConfig() {
    final Configuration configuration = new Configuration();
    set(configuration, DOT_AUTH_URL, "http://localhost:8080");
    set(configuration, DOT_TENANT, "tenant");
    set(configuration, DOT_USERNAME, "username");
    set(configuration, DOT_PASSWORD, "password");
    return configuration;
  }

  private void set(Configuration configuration, String field, String value) {
    configuration.set(SWIFT_SERVICE_PREFIX + SERVICE + field, value);
  }

  private void mkInstance(Configuration configuration) throws
          IOException,
          URISyntaxException {
    URI uri = new URI("swift://container.openstack/");
    SwiftRestClient.getInstance(uri, configuration);
  }
}
