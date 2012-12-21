package org.apache.hadoop.fs.swift.auth.entities;

import java.util.List;

/**
 * Describes Openstack Swift REST endpoints
 */
public class Catalog {
  /**
   * List of valid swift endpoints
   */
  private List<Endpoint> endpoints;
  /**
   * endpoint links are additional information description
   * which aren't used in Hadoop and Swift integration scope
   */
  private List<Object> endpoints_links;
  /**
   * Openstack REST service name. In our case name = "keystone"
   */
  private String name;

  /**
   * Type of REST service. In our case type = "identity"
   */
  private String type;

  /**
   * @return List of endpoints
   */
  public List<Endpoint> getEndpoints() {
    return endpoints;
  }

  /**
   * @param endpoints list of endpoints
   */
  public void setEndpoints(List<Endpoint> endpoints) {
    this.endpoints = endpoints;
  }

  /**
   * @return list of endpoint links
   */
  public List<Object> getEndpoints_links() {
    return endpoints_links;
  }

  /**
   * @param endpoints_links list of endpoint links
   */
  public void setEndpoints_links(List<Object> endpoints_links) {
    this.endpoints_links = endpoints_links;
  }

  /**
   * @return name of Openstack REST service
   */
  public String getName() {
    return name;
  }

  /**
   * @param name of Openstack REST service
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return type of Openstack REST service
   */
  public String getType() {
    return type;
  }

  /**
   * @param type of REST service
   */
  public void setType(String type) {
    this.type = type;
  }
}
