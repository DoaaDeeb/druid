/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.variablewidthhistogram.txtartest;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.variablewidthhistogram.VariableWidthHistogram;

/**
 * Data Transfer Object for deserializing histogram JSON data from txtar test files.
 * This matches the JSON structure in the Go test data.
 */
public class HistogramData
{
  @JsonProperty("numBuckets")
  private int numBuckets;

  @JsonProperty("maxNumBuckets")
  private int maxNumBuckets;

  @JsonProperty("boundaries")
  private double[] boundaries;

  @JsonProperty("counts")
  private double[] counts;

  @JsonProperty("missingValueCount")
  private long missingValueCount;

  @JsonProperty("count")
  private double count;

  @JsonProperty("max")
  private double max;

  @JsonProperty("min")
  private double min;

  public HistogramData()
  {
  }

  public int getNumBuckets()
  {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
  }

  public int getMaxNumBuckets()
  {
    return maxNumBuckets;
  }

  public void setMaxNumBuckets(int maxNumBuckets)
  {
    this.maxNumBuckets = maxNumBuckets;
  }

  public double[] getBoundaries()
  {
    return boundaries;
  }

  public void setBoundaries(double[] boundaries)
  {
    this.boundaries = boundaries;
  }

  public double[] getCounts()
  {
    return counts;
  }

  public void setCounts(double[] counts)
  {
    this.counts = counts;
  }

  public long getMissingValueCount()
  {
    return missingValueCount;
  }

  public void setMissingValueCount(long missingValueCount)
  {
    this.missingValueCount = missingValueCount;
  }

  public double getCount()
  {
    return count;
  }

  public void setCount(double count)
  {
    this.count = count;
  }

  public double getMax()
  {
    return max;
  }

  public void setMax(double max)
  {
    this.max = max;
  }

  public double getMin()
  {
    return min;
  }

  public void setMin(double min)
  {
    this.min = min;
  }

  /**
   * Convert this DTO to a VariableWidthHistogram object.
   * Since the full constructor is protected, we use the serialization round-trip approach.
   */
  public VariableWidthHistogram toVariableWidthHistogram() throws Exception
  {
    // Always build the binary representation to match the expected format
    // This ensures correct handling of all fields including edge cases
    
    // Build the binary representation manually to match the expected format
    // This mirrors the toBytes() method in VariableWidthHistogram
    java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(
        8 + 4 + 4 + 8 + 8 + 8 +  // fixed fields
        (boundaries != null ? boundaries.length * 8 : 0) +
        (counts != null ? counts.length * 8 : 0)
    );
    
    buf.putDouble(count);
    buf.putInt(maxNumBuckets);
    buf.putInt(numBuckets);
    buf.putLong(missingValueCount);
    buf.putDouble(max);
    buf.putDouble(min);
    
    if (boundaries != null && boundaries.length > 0) {
      buf.asDoubleBuffer().put(boundaries);
      buf.position(buf.position() + 8 * boundaries.length);
    }
    if (counts != null && counts.length > 0) {
      buf.asDoubleBuffer().put(counts);
      buf.position(buf.position() + 8 * counts.length);
    }
    
    // Convert to base64 and deserialize
    String base64 = org.apache.druid.java.util.common.StringUtils.fromUtf8(
        org.apache.druid.java.util.common.StringUtils.encodeBase64(buf.array())
    );
    
    return VariableWidthHistogram.fromBase64(base64);
  }
}

