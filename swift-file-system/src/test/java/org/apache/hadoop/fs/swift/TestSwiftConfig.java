package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import static org.apache.hadoop.fs.swift.http.SwiftProtocolConstants.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
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
    configuration.set(SWIFT_RETRY_COUNT,"3");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testBadRetryCount() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_RETRY_COUNT,"three");
    mkInstance(configuration);
  }

  @Test(expected = org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException.class)
  public void testBadConnectTimeout() throws Exception {
    final Configuration configuration = createCoreConfig();
    configuration.set(SWIFT_CONNECTION_TIMEOUT,"three");
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
