package org.apache.hadoop.fs.swift.exceptions;

import java.io.IOException;

/**
 * Thrown when I/O exception, interruption happens
 */
public class SwiftException extends IOException {
  public SwiftException() {
    super();
  }

  public SwiftException(String message) {
    super(message);
  }

  public SwiftException(String message, Throwable cause) {
    super(message, cause);
  }

  public SwiftException(Throwable cause) {
    super(cause);
  }
}
