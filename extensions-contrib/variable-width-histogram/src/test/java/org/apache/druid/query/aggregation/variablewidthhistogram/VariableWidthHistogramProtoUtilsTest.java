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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.variablewidthhistogram.proto.VariableWidthHistogramProto;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class VariableWidthHistogramProtoUtilsTest
{
  /**
   * Helper to build a histogram for testing
   */
  private VariableWidthHistogram buildHistogram()
  {
    return new VariableWidthHistogram(
        10,    // maxNumBuckets (capacity)
        3,     // numBuckets (current usage)
        new double[]{5.0, 10.0},  // boundaries
        new double[]{2.0, 3.0, 1.0},  // counts
        5,     // missingValueCount
        6.0,   // count
        12.0,  // max
        1.0    // min
    );
  }

  /**
   * Helper to build an empty histogram for testing
   */
  private VariableWidthHistogram buildEmptyHistogram()
  {
    return new VariableWidthHistogram(0);
  }

  @Test
  public void testToProto()
  {
    VariableWidthHistogram histogram = buildHistogram();
    VariableWidthHistogramProto.VariableWidthHistogram proto = VariableWidthHistogramProtoUtils.toProto(histogram);

    Assert.assertNotNull(proto);
    Assert.assertEquals(histogram.getNumBuckets(), proto.getNumBuckets());
    Assert.assertEquals(histogram.getMaxNumBuckets(), proto.getMaxNumBuckets());
    Assert.assertEquals(histogram.getMissingValueCount(), proto.getMissingValueCount());
    Assert.assertEquals(histogram.getCount(), proto.getCount(), 0.0001);
    Assert.assertEquals(histogram.getMax(), proto.getMax(), 0.0001);
    Assert.assertEquals(histogram.getMin(), proto.getMin(), 0.0001);

    // Convert protobuf lists to double arrays for comparison
    double[] protoBoundaries = proto.getBoundariesList().stream().mapToDouble(Double::doubleValue).toArray();
    double[] protoCounts = proto.getCountsList().stream().mapToDouble(Double::doubleValue).toArray();
    Assert.assertArrayEquals(histogram.getBoundaries(), protoBoundaries, 0.0001);
    Assert.assertArrayEquals(histogram.getCounts(), protoCounts, 0.0001);
  }

  @Test
  public void testToProtoNull()
  {
    VariableWidthHistogramProto.VariableWidthHistogram proto = VariableWidthHistogramProtoUtils.toProto(null);
    Assert.assertNull(proto); 
  }

  @Test
  public void testFromProto()
  {
    VariableWidthHistogram histogram = buildHistogram();
    VariableWidthHistogramProto.VariableWidthHistogram proto = VariableWidthHistogramProtoUtils.toProto(histogram);
    VariableWidthHistogram deserialized = VariableWidthHistogramProtoUtils.fromProto(proto);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(histogram.getNumBuckets(), deserialized.getNumBuckets());
    Assert.assertEquals(histogram.getMaxNumBuckets(), deserialized.getMaxNumBuckets());
    Assert.assertEquals(histogram.getMissingValueCount(), deserialized.getMissingValueCount());
    Assert.assertEquals(histogram.getCount(), deserialized.getCount(), 0.0001);
    Assert.assertEquals(histogram.getMax(), deserialized.getMax(), 0.0001);
    Assert.assertEquals(histogram.getMin(), deserialized.getMin(), 0.0001);

    Assert.assertArrayEquals(histogram.getBoundaries(), deserialized.getBoundaries(), 0.0001);
    Assert.assertArrayEquals(histogram.getCounts(), deserialized.getCounts(), 0.0001);
  }

  @Test
  public void testFromProtoNull()
  {
    VariableWidthHistogram histogram = VariableWidthHistogramProtoUtils.fromProto(null);
    Assert.assertNull(histogram);
  }

  @Test
  public void testToBytesAndFromBytes()
  {
    VariableWidthHistogram histogram = buildHistogram();
    byte[] bytes = VariableWidthHistogramProtoUtils.toBytes(histogram);

    Assert.assertNotNull(bytes);
    Assert.assertTrue(bytes.length > 0);

    VariableWidthHistogram deserialized = VariableWidthHistogramProtoUtils.fromBytes(bytes);
    Assert.assertNotNull(deserialized);
    Assert.assertEquals(histogram.getNumBuckets(), deserialized.getNumBuckets());
    Assert.assertEquals(histogram.getMaxNumBuckets(), deserialized.getMaxNumBuckets());
    Assert.assertEquals(histogram.getCount(), deserialized.getCount(), 0.0001);
    Assert.assertEquals(histogram.getMax(), deserialized.getMax(), 0.0001);
    Assert.assertEquals(histogram.getMin(), deserialized.getMin(), 0.0001);

    Assert.assertArrayEquals(histogram.getBoundaries(), deserialized.getBoundaries(), 0.0001);
    Assert.assertArrayEquals(histogram.getCounts(), deserialized.getCounts(), 0.0001);
  }

  @Test
  public void testToBytesNull()
  {
    byte[] bytes = VariableWidthHistogramProtoUtils.toBytes(null);
    Assert.assertNotNull(bytes);
    Assert.assertEquals(0, bytes.length);
  }

  @Test
  public void testFromBytesNull()
  {
    Assert.assertNull(VariableWidthHistogramProtoUtils.fromBytes(null));
    Assert.assertNull(VariableWidthHistogramProtoUtils.fromBytes(new byte[0]));
  }

  @Test
  public void testFromByteBuffer()
  {
    VariableWidthHistogram histogram = buildHistogram();
    byte[] bytes = VariableWidthHistogramProtoUtils.toBytes(histogram);

    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    VariableWidthHistogram deserialized = VariableWidthHistogramProtoUtils.fromByteBuffer(buffer, bytes.length);

    Assert.assertNotNull(deserialized);
    Assert.assertEquals(histogram.getNumBuckets(), deserialized.getNumBuckets());
    Assert.assertEquals(histogram.getMaxNumBuckets(), deserialized.getMaxNumBuckets());
    Assert.assertEquals(histogram.getCount(), deserialized.getCount(), 0.0001);
    Assert.assertEquals(histogram.getMax(), deserialized.getMax(), 0.0001);
    Assert.assertEquals(histogram.getMin(), deserialized.getMin(), 0.0001);
    Assert.assertArrayEquals(histogram.getBoundaries(), deserialized.getBoundaries(), 0.0001);
    Assert.assertArrayEquals(histogram.getCounts(), deserialized.getCounts(), 0.0001);
  }

  @Test
  public void testFromByteBufferEmpty()
  {
    ByteBuffer buffer = ByteBuffer.allocate(10);
    VariableWidthHistogram deserialized = VariableWidthHistogramProtoUtils.fromByteBuffer(buffer, 0);
    Assert.assertNull(deserialized);
  }

  @Test(expected = IAE.class)
  public void testFromByteBufferInvalidData()
  {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
    VariableWidthHistogramProtoUtils.fromByteBuffer(buffer, 5);
  }

  @Test
  public void testByteLengthComparisonOldVsProtobuf()
  {
    System.out.println("\n=== Byte Length Comparison: Old Format vs Protobuf ===");
    System.out.println(StringUtils.format("%-12s | %-12s | %-12s | %-12s", 
        "Buckets", "Old (bytes)", "Proto (bytes)", "Difference"));
    System.out.println("-------------------------------------------------------");

    // Test with different bucket counts
    int[] bucketCounts = {0, 3, 5, 10, 20, 50, 100};
    
    for (int numBuckets : bucketCounts) {
      VariableWidthHistogram histogram;
      
      if (numBuckets == 0) {
        histogram = buildEmptyHistogram();
      } else {
        // Build histogram with specific number of buckets
        double[] boundaries = new double[numBuckets - 1];
        double[] counts = new double[numBuckets];
        
        for (int i = 0; i < numBuckets - 1; i++) {
          boundaries[i] = (i + 1) * 10.0;
        }
        for (int i = 0; i < numBuckets; i++) {
          counts[i] = i + 1.0;
        }
        
        histogram = new VariableWidthHistogram(
            numBuckets,  // maxNumBuckets
            numBuckets,  // numBuckets
            boundaries,
            counts,
            0,           // missingValueCount
            numBuckets * (numBuckets + 1) / 2.0,  // count (sum of counts)
            (numBuckets - 1) * 10.0 + 5.0,  // max
            0.0          // min
        );
      }
      
      // Get byte lengths
      byte[] oldBytes = histogram.toBytes();
      byte[] protoBytes = histogram.toBytesProto();
      
      int oldLength = oldBytes.length;
      int protoLength = protoBytes.length;
      int difference = protoLength - oldLength;
      
      System.out.println(StringUtils.format("%-12d | %-12d | %-12d | %+12d", 
          numBuckets, oldLength, protoLength, difference));
      
      // Basic sanity checks
      Assert.assertTrue("Old format should have positive length", oldLength > 0);
      Assert.assertTrue("Protobuf format should have positive length", protoLength > 0);
    }
    
    System.out.println("=======================================================\n");
  }

}

