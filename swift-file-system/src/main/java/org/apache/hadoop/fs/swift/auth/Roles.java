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
   * Service id used in HP public Cloud
   */
  private String serviceId;

  /**
   * Service id used in HP public Cloud
   */
  private String tenantId;

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

  public String getServiceId() {
    return serviceId;
  }

  public void setServiceId(String serviceId) {
    this.serviceId = serviceId;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }
}
