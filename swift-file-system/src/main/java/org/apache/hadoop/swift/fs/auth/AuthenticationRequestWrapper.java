package org.apache.hadoop.swift.fs.auth;

/**
 * This class is used for correct hierarchy mapping of
 * Keystone authentication model and java code
 */
public class AuthenticationRequestWrapper {
  /**
   * authentication request
   */
  private AuthenticationRequest auth;

  /**
   * defalt constructor used for json parsing
   */
  public AuthenticationRequestWrapper() {
  }

  /**
   * @param auth authentication requests
   */
  public AuthenticationRequestWrapper(AuthenticationRequest auth) {
    this.auth = auth;
  }

  /**
   * @return authentication request
   */
  public AuthenticationRequest getAuth() {
    return auth;
  }

  /**
   * @param auth authentication request
   */
  public void setAuth(AuthenticationRequest auth) {
    this.auth = auth;
  }
}
