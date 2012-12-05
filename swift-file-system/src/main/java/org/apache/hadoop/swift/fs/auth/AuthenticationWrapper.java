package org.apache.hadoop.swift.fs.auth;

/**
 * This class is used for correct hierarchy mapping of
 * Keystone authentication model and java code
 */
public class AuthenticationWrapper {

  /**
   * authentication response field
   */
  private AuthenticationResponse access;

  /**
   * @return authentication response
   */
  public AuthenticationResponse getAccess() {
    return access;
  }

  /**
   * @param access sets authentication response
   */
  public void setAccess(AuthenticationResponse access) {
    this.access = access;
  }
}
