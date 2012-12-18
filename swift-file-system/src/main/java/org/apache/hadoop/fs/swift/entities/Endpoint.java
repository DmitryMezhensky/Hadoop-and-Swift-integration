package org.apache.hadoop.fs.swift.entities;

import java.net.URI;

/**
 * Openstack Swift endpoint description
 */
public class Endpoint {

  /**
   * endpoint id
   */
  private String id;

  /**
   * Keystone admin URL
   */
  private URI adminURL;

  /**
   * Keystone internal URL
   */
  private URI internalURL;

  /**
   * public accessible URL
   */
  private URI publicURL;

  /**
   * Openstack region name
   */
  private String region;

  /**
   * This field is used in RackSpace authentication model
   */
  private String tenantId;

  /**
   * This field user in RackSpace auth model
   */
  private String versionId;

  /**
   * This field user in RackSpace auth model
   */
  private String versionInfo;

  /**
   * This field user in RackSpace auth model
   */
  private String versionList;


  /**
   * @return endpoint id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id endpoint id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return Keystone admin URL
   */
  public URI getAdminURL() {
    return adminURL;
  }

  /**
   * @param adminURL Keystone admin URL
   */
  public void setAdminURL(URI adminURL) {
    this.adminURL = adminURL;
  }

  /**
   * @return internal Keystone
   */
  public URI getInternalURL() {
    return internalURL;
  }

  /**
   * @param internalURL Keystone internal URL
   */
  public void setInternalURL(URI internalURL) {
    this.internalURL = internalURL;
  }

  /**
   * @return public accessible URL
   */
  public URI getPublicURL() {
    return publicURL;
  }

  /**
   * @param publicURL public URL
   */
  public void setPublicURL(URI publicURL) {
    this.publicURL = publicURL;
  }


  /**
   * @return Openstack region name
   */
  public String getRegion() {
    return region;
  }

  /**
   * @param region Openstack region name
   */
  public void setRegion(String region) {
    this.region = region;
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getVersionId() {
    return versionId;
  }

  public void setVersionId(String versionId) {
    this.versionId = versionId;
  }

  public String getVersionInfo() {
    return versionInfo;
  }

  public void setVersionInfo(String versionInfo) {
    this.versionInfo = versionInfo;
  }

  public String getVersionList() {
    return versionList;
  }

  public void setVersionList(String versionList) {
    this.versionList = versionList;
  }
}
