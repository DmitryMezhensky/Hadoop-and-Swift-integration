package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.http.SwiftRestClient;
import org.junit.Test;

/**
 *
 */
public class TestSwiftConfig {

  @Test(expected = IllegalArgumentException.class)
  public void emptyUrl() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set("swift.tenant", "tenant");
    configuration.set("swift.username", "username");
    configuration.set("swift.password", "password");

    SwiftRestClient.getInstance(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyTenant() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set("swift.auth.url", "http://localhost:8080");
    configuration.set("swift.username", "username");
    configuration.set("swift.password", "password");

    SwiftRestClient.getInstance(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyUsername() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set("swift.tenant", "tenant");
    configuration.set("swift.auth.url", "http://localhost:8080");
    configuration.set("swift.password", "password");

    SwiftRestClient.getInstance(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyPassword() throws Exception {
    final Configuration configuration = new Configuration();
    configuration.set("swift.tenant", "tenant");
    configuration.set("swift.username", "username");
    configuration.set("swift.auth.url", "http://localhost:8080");

    SwiftRestClient.getInstance(configuration);
  }
}
