package org.apache.hadoop.fs.swift.auth.entities;

import org.apache.hadoop.fs.swift.auth.entities.Tenant;

/**
 * Access token representation of Openstack Keystone authentication.
 * Class holds token id, tenant and expiration time.
 */
public class AccessToken {
  /**
   * token expiration time
   */
  private String expires;
  /**
   * token id
   */
  private String id;
  /**
   * tenant name for whom id is attached
   */
  private Tenant tenant;

  /**
   * @return token expiration time
   */
  public String getExpires() {
    return expires;
  }

  /**
   * @param expires the token expiration time
   */
  public void setExpires(String expires) {
    this.expires = expires;
  }

  /**
   * @return token value
   */
  public String getId() {
    return id;
  }

  /**
   * @param id token value
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return tenant authenticated in Openstack Keystone
   */
  public Tenant getTenant() {
    return tenant;
  }

  /**
   * @param tenant tenant authenticated in Openstack Keystone
   */
  public void setTenant(Tenant tenant) {
    this.tenant = tenant;
  }
}
