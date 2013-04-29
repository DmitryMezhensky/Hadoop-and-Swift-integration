/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Class for functional testing huge file upload to Swift FS.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Class for functional huge file upload to Swift FS.
 */
public class SwiftFileSystemForFunctionalTests extends SwiftNativeFileSystem {

  private long partitionSize;

  @Override
  public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite,
                                   int bufferSize, short replication, long blockSize,
                                   Progressable progress) throws IOException {

    FSDataOutputStream fsDataOutputStream =
      super.create(file, permission, overwrite, bufferSize, replication,
                   blockSize, progress);
    SwiftNativeOutputStream out =
      (SwiftNativeOutputStream) fsDataOutputStream.getWrappedStream();
    partitionSize = 1024L;
    out.setFilePartSize(partitionSize);
    return fsDataOutputStream;
  }


  public long getPartitionSize() {
    return partitionSize;
  }

  public void setPartitionSize(long partitionSize) {
    this.partitionSize = partitionSize;
  }

  /**
   * Get the number of partitions written
   * @param outputStream output stream
   * @return the #of partitions written by that stream
   */
  public int getPartitionsWritten(FSDataOutputStream outputStream) {
    OutputStream wrappedStream = outputStream.getWrappedStream();
    SwiftNativeOutputStream snos = (SwiftNativeOutputStream)wrappedStream;
    return snos.getPartitionsWritten();
  }

}
