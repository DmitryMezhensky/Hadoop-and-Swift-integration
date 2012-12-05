package org.apache.hadoop.swift.fs.auth;


/**
 * Describes credentials to log in Swift using Keystone authentication
 */
public class PasswordCredentials {
  /**
   * user login
   */
  private String username;

  /**
   * user password
   */
  private String password;

  /**
   * default constructor
   */
  public PasswordCredentials() {
  }

  /**
   * @param username user login
   * @param password user password
   */
  public PasswordCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  /**
   * @return user password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password user password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return login
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username login
   */
  public void setUsername(String username) {
    this.username = username;
  }
}

