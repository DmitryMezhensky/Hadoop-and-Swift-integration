package org.apache.hadoop.fs.swift.auth;

import org.apache.hadoop.fs.swift.entities.Catalog;
import org.apache.hadoop.fs.swift.entities.User;

import java.util.List;


public class AuthenticationResponse {
  private Object metadata;
  private List<Catalog> serviceCatalog;
  private User user;
  private AccessToken token;

  public Object getMetadata() {
    return metadata;
  }

  public void setMetadata(Object metadata) {
    this.metadata = metadata;
  }

  public List<Catalog> getServiceCatalog() {
    return serviceCatalog;
  }

  public void setServiceCatalog(List<Catalog> serviceCatalog) {
    this.serviceCatalog = serviceCatalog;
  }

  public User getUser() {
    return user;
  }

  public void setUser(User user) {
    this.user = user;
  }

  public AccessToken getToken() {
    return token;
  }

  public void setToken(AccessToken token) {
    this.token = token;
  }
}
