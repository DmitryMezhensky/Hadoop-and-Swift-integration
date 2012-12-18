package org.apache.hadoop.fs.swift.exceptions;

/**
 * Thrown to indicate that connection is lost of request is incorrect
 */
public class SwiftConnectionException extends SwiftException {
  public SwiftConnectionException() {
  }

  public SwiftConnectionException(String message) {
    super(message);
  }

  public SwiftConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
