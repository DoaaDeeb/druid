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

package org.apache.druid.query.aggregation.variablewidthhistogram;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.variablewidthhistogram.proto.VariableWidthHistogramProto;

import java.nio.ByteBuffer;

public class VariableWidthHistogramProtoUtils
{
  private VariableWidthHistogramProtoUtils()
  {
  }

  /**
   * Convert a VariableWidthHistogram to protobuf message
   */
  public static VariableWidthHistogramProto.VariableWidthHistogram toProto(VariableWidthHistogram histogram)
  {
    if (histogram == null) {
      return null;
    }


    VariableWidthHistogramProto.VariableWidthHistogram.Builder builder = VariableWidthHistogramProto.VariableWidthHistogram.newBuilder();
    builder.setNumBuckets(histogram.getNumBuckets())
          .setMaxNumBuckets(histogram.getMaxNumBuckets())
          .setMissingValueCount(histogram.getMissingValueCount())
          .setCount(histogram.getCount())
          .setMax(histogram.getMax())
          .setMin(histogram.getMin());

    // Add boundaries
    double[] boundaries = histogram.getBoundaries();
    if (boundaries != null) {
      for (double boundary : boundaries) {
        builder.addBoundaries(boundary);
      }
    }

    // Add counts
    double[] counts = histogram.getCounts();
    if (counts != null) {
      for (double count : counts) {
        builder.addCounts(count);
      }
    }

    return builder.build();
  }

  /**
   * Convert a protobuf message to VariableWidthHistogram
   */
  public static VariableWidthHistogram fromProto(VariableWidthHistogramProto.VariableWidthHistogram proto)
  {
    if (proto == null) {
      return null;
    }

    int numBuckets = proto.getNumBuckets();
    int maxNumBuckets = proto.getMaxNumBuckets();

    // Convert repeated fields to arrays
    double[] boundaries = new double[proto.getBoundariesCount()];
    for (int i = 0; i < proto.getBoundariesCount(); i++) {
      boundaries[i] = proto.getBoundaries(i);
    }

    double[] counts = new double[proto.getCountsCount()];
    for (int i = 0; i < proto.getCountsCount(); i++) {
      counts[i] = proto.getCounts(i);
    }

    double max = proto.hasMax() ? proto.getMax() : Double.POSITIVE_INFINITY;
    double min = proto.hasMin() ? proto.getMin() : Double.NEGATIVE_INFINITY;

    return new VariableWidthHistogram(
        maxNumBuckets,  // maxNumBuckets comes first in constructor
        numBuckets,     // numBuckets comes second
        boundaries,
        counts,
        proto.getMissingValueCount(),
        proto.getCount(),
        max,
        min
    );
  }

  /**
   * Serialize histogram to bytes using protobuf
   */
  public static byte[] toBytes(VariableWidthHistogram histogram)
  {
    if (histogram == null) {
      return new byte[0];
    }
    return toProto(histogram).toByteArray();
  }

  /**
   * Deserialize histogram from bytes using protobuf
   */
  public static VariableWidthHistogram fromBytes(byte[] bytes)
  {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    try {
      VariableWidthHistogramProto.VariableWidthHistogram proto =
          VariableWidthHistogramProto.VariableWidthHistogram.parseFrom(bytes);
      return fromProto(proto);
    }
    catch (InvalidProtocolBufferException e) {
      throw new IAE(e, "Unable to deserialize VariableWidthHistogram from bytes");
    }
  }

  /**
   * Deserialize histogram from ByteBuffer using protobuf
   */
  public static VariableWidthHistogram fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (buffer == null || numBytes <= 0) {
      return null;
    }
    try {
      buffer.limit(buffer.position() + numBytes);
      return fromProto(VariableWidthHistogramProto.VariableWidthHistogram.parseFrom(buffer));
    }
    catch (InvalidProtocolBufferException e) {
      throw new IAE(e, "Unable to deserialize VariableWidthHistogram from ByteBuffer");
    }
  }
}

