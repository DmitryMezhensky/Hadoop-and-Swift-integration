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

package org.apache.hadoop.fs.swift.util;

import org.apache.hadoop.fs.FileStatus;

/**
 * Various utility classes for SwiftFS support
 */
public final class SwiftUtils {

  /**
   *
   * Join two (non null) paths, inserting a forward slash between them
   * if needed
   * @param path1 first path
   * @param path2 second path
   * @return the combined path
   */
  public static String joinPaths(String path1, String path2) {
    StringBuilder result =
      new StringBuilder(path1.length() + path2.length() + 1);
    result.append(path1);
    boolean insertSlash = true;
    if (path1.endsWith("/")) {
      insertSlash = false;
    }
    else if (path2.startsWith("/")) {
      insertSlash = false;
    }
    if (insertSlash) {
      result.append("/");
    }
    result.append(path2);
    return result.toString();
  }

  /**
   * This test contains the is-directory logic for Swift, so if
   * changed there is only one place for it.
   * @param fileStatus status to examine
   * @return true if we consider this status to be representative of a
   * directory.
   */
  public static boolean isDirectory(FileStatus fileStatus) {
    return fileStatus.isDir() || isFilePretendingToBeDirectory(fileStatus);
  }

  /**
   * Test for the entry being a file that is treated as if it is a
   * directory
   * @param fileStatus status
   * @return true if it meets the rules for being a directory
   */
  public static boolean isFilePretendingToBeDirectory(FileStatus fileStatus) {
    return fileStatus.getLen() == 0;
  }

  public static boolean isRootDir(SwiftObjectPath swiftObject) {
    return swiftObject.objectMatches("") || swiftObject.objectMatches("/");
  }

  /**
   * Query to see if the possibleChild object is a child path of the parent.
   * The test is done by probing for the path of the first object being
   * at the start of the second -with a trailing slash.
   * @param parent Parent dir
   * @param possibleChild possible child dir
   * @return true iff the possibleChild is under the parent directory
   */
  public static boolean isChildOf(SwiftObjectPath parent,
                                  SwiftObjectPath possibleChild) {
    return possibleChild.getObject().startsWith(parent.getObject() + "/");
  }
}
