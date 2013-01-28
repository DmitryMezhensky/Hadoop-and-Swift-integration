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

package org.apache.hadoop.fs.swift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Utilities used across test cases
 */
public class SwiftTestUtils {
  protected static final String TEST_FS_SWIFT = "test.fs.swift.name";

  /**
   * Get the test URI
   * @param conf configuration
   * @throws SwiftConfigurationException missing parameter or bad URI
   */
  public static URI getServiceURI(Configuration conf) throws
                                                      SwiftConfigurationException {
    String instance = conf.getTrimmed(TEST_FS_SWIFT);
    if (instance == null) {
      throw new SwiftConfigurationException(
        "Missing configuration entry " + TEST_FS_SWIFT);
    }
    try {
      return new URI(instance);
    } catch (URISyntaxException e) {
      throw new SwiftConfigurationException("Bad URI: " + instance);
    }
  }

  public static boolean hasServiceURI(Configuration conf) {
    String instance = conf.getTrimmed(TEST_FS_SWIFT);
    return instance != null;
  }

  /**
   * Assert that a property in the property set matches the expected value
   * @param props property set
   * @param key property name
   * @param expected expected value. If null, the property must not be in the set
   */
  public static void assertPropertyEquals(Properties props,
                                          String key,
                                          String expected) {
    String val = props.getProperty(key);
    if (expected == null) {
      assertNull("Non null property " + key + " = " + val, val);
    }
    else {
      assertEquals("property " + key + " = " + val,
                   expected,
                   val);
    }
  }

  /**
   * Convert a byte to a character for printing. If the
   * byte value is < 32 -and hence unprintable- the byte is
   * returned as a two digit hex value
   * @param b byte
   * @return the printable character string
   */
  public static String toChar(byte b) {
    if (b >= 0x20) {
      return Character.toString((char) b);
    }
    else {
      return String.format("%02x", b);
    }
  }

  public static String toChar(byte[] buffer) {
    StringBuilder builder = new StringBuilder(buffer.length);
    for (byte b:buffer) {
      builder.append(toChar(b));
    }
    return builder.toString();
  }

  public static byte[] toAsciiByteArray(String s) {
    char[] chars = s.toCharArray();
    int len = chars.length;
    byte[] buffer = new byte[len];
    for (int i=0; i<len; i++) {
      buffer[i]=(byte)(chars[i]&0xff);
    }
    return buffer;
  }
}
