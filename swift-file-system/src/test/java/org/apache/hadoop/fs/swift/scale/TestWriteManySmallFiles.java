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

package org.apache.hadoop.fs.swift.scale;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

public class TestWriteManySmallFiles extends SwiftScaleTestBase {

  public static final Log LOG = LogFactory.getLog(TestWriteManySmallFiles.class);

  @Test
  public void testScaledWriteThenRead() throws Throwable {
    Path dir = new Path("/test/manysmallfiles");
    Duration rm1 = new Duration();
    fs.delete(dir, true);
    rm1.finished();
    fs.mkdirs(dir);
    Duration ls1 = new Duration();
    FileStatus[] status1 = (FileStatus[]) fs.listStatus(dir);
    ls1.finished();
    long count = getOperationCount();
    DurationStats writeStats = new DurationStats("write");
    DurationStats readStats = new DurationStats("read");
    String format = "%08d";
    for (long l = 0; l < count; l++) {
      String name = String.format(format, l);
      Path p = new Path(dir, "part-" + name);
      Duration d = new Duration();
      SwiftTestUtils.writeTextFile(fs, p, name, false);
      d.finished();
      writeStats.add(d);
    }
    //at this point, the directory is full.
    Duration ls2 = new Duration();
    FileStatus[] status2 = (FileStatus[]) fs.listStatus(dir);
    ls2.finished();

    for (long l = 0; l < count; l++) {
      String name = String.format(format, l);
      Path p = new Path(dir, "part-" + name);
      Duration d = new Duration();
      String result = SwiftTestUtils.readBytesToString(fs, p, name.length());
      assertEquals(name, result);
      d.finished();
      readStats.add(d);
    }
    //do a recursive delete again
    Duration rm2 = new Duration();
    fs.delete(dir, true);
    rm2.finished();
    //print the stats
    LOG.info(String.format("'filesystem','%s'", fs.getUri()));
    LOG.info(writeStats.toString());
    LOG.info(readStats.toString());
    LOG.info(String.format(
            "'rm1',%d,'ls1',%d",
            rm1.value(),
            ls1.value()));
    LOG.info(String.format(
            "'rm2',%d,'ls2',%d",
            rm2.value(),
            ls2.value()));
  }

}
