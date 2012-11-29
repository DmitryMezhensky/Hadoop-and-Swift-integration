package org.apache.hadoop.swift.fs.exceptions;

/**
 * Thrown to indicate that connection is lost of request is incorrect
 */
public class SwiftConnectionException extends RuntimeException {
    public SwiftConnectionException() {
    }

    public SwiftConnectionException(String message) {
        super(message);
    }

    public SwiftConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
