package org.apache.hadoop.fs.swift.exceptions;

/**
 * Thrown to indicate that data locality can't be calculated or requested path is incorrect.
 * Data locality can't be calculated if Openstack Swift version is old.
 */
public class SwiftIllegalDataLocalityRequest extends SwiftException {
  public SwiftIllegalDataLocalityRequest() {
  }

  public SwiftIllegalDataLocalityRequest(String message) {
    super(message);
  }

  public SwiftIllegalDataLocalityRequest(String message, Throwable cause) {
    super(message, cause);
  }
}
