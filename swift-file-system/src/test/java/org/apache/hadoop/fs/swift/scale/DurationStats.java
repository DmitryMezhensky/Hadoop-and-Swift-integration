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

/**
 * Build ongoing statistics from duration data
 */
public class DurationStats {

  final String operation;
  int n;
  long sum;
  long min=10000000;
  long max=0;
  double mean, m2;

  public DurationStats(String operation) {
    this.operation = operation;
  }

  /**
   * Add a duration
   * @param duration the new duration
   */
  public void add(Duration duration) {
    add(duration.value());
  }

  /**
   * Add a number
   * @param x the number
   */
  public void add(long x) {
    n++;
    sum += x;
    double delta = x - mean;
    mean += delta / n;
    m2 += delta * (x - mean);
    if (x<min) min=x;
    if (x>max) max=x;
  }

  public int getCount() {
    return n;
  }

  public long getSum() {
    return sum;
  }

  public double getArithmeticMean() {
    return mean;
  }

  public double getVariance() {
    return m2 / (n - 1);
  }

  public double getDeviation() {
    return Math.sqrt(getVariance());
  }

  @Override
  public String toString() {
    return String.format(
      "'operation','%s','count',%d,'total',%d,'mean',%f,'dev',%f,'min',%d,'max',%d",
      operation,
      n,
      sum,
      mean,
      getDeviation(),
      min,max
      );
  }
}
