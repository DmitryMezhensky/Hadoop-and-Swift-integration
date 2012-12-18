package org.apache.hadoop.fs.swift.auth;

/**
 * Describes user roles in Openstack system
 */
public class Roles {
  /**
   * role name
   */
  private String name;

  /**
   * This field user in RackSpace auth model
   */
  private String id;

  /**
   * This field user in RackSpace auth model
   */
  private String description;

  /**
   * @return role name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name role name
   */
  public void setName(String name) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
