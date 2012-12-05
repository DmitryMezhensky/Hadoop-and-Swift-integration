package org.apache.hadoop.swift.fs.entities;

/**
 * Tenant is abstraction in Openstack which describes all account information
 * and user privileges in system
 */
public class Tenant {

  /**
   * tenant id
   */
  private String id;

  /**
   * tenant short description which Keystone returns
   */
  private String description;

  /**
   * boolean enabled user account or no
   */
  private boolean enabled;

  /**
   * tenant human readable name
   */
  private String name;

  /**
   * @return tenant name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name tenant name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return true if account enabled and false otherwise
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled enable or disable
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * @return account short description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description set account description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @return set tenant id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id tenant id
   */
  public void setId(String id) {
    this.id = id;
  }
}
