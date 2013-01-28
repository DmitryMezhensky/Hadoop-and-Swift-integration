/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.exceptions;

import java.net.URI;

/**
 * Exception raised when the HTTP code is invalid. The status code,
 * method name and operation URI are all in the response.
 */
public class SwiftInvalidResponseException extends SwiftConnectionException {

  public final int statusCode;
  public final String operation;
  public final URI uri;

  public SwiftInvalidResponseException(String message,
                                       int statusCode,
                                       String operation, URI uri) {
    super(message);
    this.statusCode = statusCode;
    this.operation = operation;
    this.uri = uri;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getOperation() {
    return operation;
  }

  public URI getUri() {
    return uri;
  }
}
