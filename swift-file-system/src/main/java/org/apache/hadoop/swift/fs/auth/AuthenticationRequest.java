package org.apache.hadoop.swift.fs.auth;

/**
 * Class that represents authentication request to Openstack Keystone.
 * Contains basic authentication information.
 */
public class AuthenticationRequest {
  /**
   * tenant name
   */
  private String tenantName;

  /**
   * Credentials for login
   */
  private PasswordCredentials passwordCredentials;

  /**
   * @param tenantName
   * @param passwordCredentials
   */
  public AuthenticationRequest(String tenantName, PasswordCredentials passwordCredentials) {
    this.tenantName = tenantName;
    this.passwordCredentials = passwordCredentials;
  }

  /**
   * @return tenant name for Keystone authorization
   */
  public String getTenantName() {
    return tenantName;
  }

  /**
   * @param tenantName tenant name for authorization
   */
  public void setTenantName(String tenantName) {
    this.tenantName = tenantName;
  }

  /**
   * @return credentials for login into Keystone
   */
  public PasswordCredentials getPasswordCredentials() {
    return passwordCredentials;
  }

  /**
   * @param passwordCredentials credentials for login into Keystone
   */
  public void setPasswordCredentials(PasswordCredentials passwordCredentials) {
    this.passwordCredentials = passwordCredentials;
  }
}
