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

 import com.fasterxml.jackson.annotation.JsonIgnore;
 import com.fasterxml.jackson.annotation.JsonProperty;
 import com.fasterxml.jackson.annotation.JsonValue;
 import com.google.common.annotations.VisibleForTesting;
 import org.apache.druid.java.util.common.ISE;
 import org.apache.druid.java.util.common.StringUtils;
 
 import javax.annotation.Nullable;
 import java.nio.ByteBuffer;
 import java.nio.charset.StandardCharsets;
 import java.util.Arrays;
 import java.util.Objects;
 import java.util.concurrent.locks.ReadWriteLock;
 import java.util.concurrent.locks.ReentrantReadWriteLock;
 
 public class VariableWidthHistogram
 {
   public static final byte SERIALIZATION_VERSION = 0x01;
 
   /**
    * Serialization header format:
    *
    * byte: serialization version, must be 0x01
    */

  /**
   * fixed size serialization fields format:
   *
    * double: count
    * int: num buckets
    * double: max
    * double: min
   */
  public static final int FIXED_SIZE_FIELDS_SIZE = Double.BYTES +
                                                Integer.BYTES +
                                                Double.BYTES +
                                                Double.BYTES;
 
   /**
    * Serialization format:
    *
    * double: count
    * int: num buckets
    * double: max
    * double: min
    * array of doubles: bucket counts (length: num buckets - 1)
    * array of doubles: bucket positions (length: num buckets)
    */
 
   /**
    * Locking is needed when the non-buffer aggregator is used within a realtime ingestion task.
    *
    * The following areas are locked:
    * - Merge histograms
    * - Compute percentiles
    * - Serialization
    */
   @JsonIgnore
   private final ReadWriteLock readWriteLock;
 
   private int numBuckets;
   private double[] boundaries;
   private double[] counts;
   private long missingValueCount = 0;
   private double count = 0;
   private double max = Double.NEGATIVE_INFINITY;
   private double min = Double.POSITIVE_INFINITY;
 
   public VariableWidthHistogram(int numBuckets)
   {
     this.readWriteLock = new ReentrantReadWriteLock(true);
     this.numBuckets = numBuckets;
     this.boundaries = new double[numBuckets - 1];
     this.counts = new double[numBuckets];
   }
 
   @VisibleForTesting
   protected VariableWidthHistogram(
    int numBuckets,
    double[] boundaries,
    double[] counts,
    long missingValueCount,
    double count,
    double max,
    double min
   )
   {
     this.numBuckets = numBuckets;
     this.missingValueCount = missingValueCount;
     this.boundaries = boundaries;
     this.counts = counts;
     this.count = count;
     this.max = max;
     this.min = min;
     this.readWriteLock = new ReentrantReadWriteLock(true);
   }
 
   @VisibleForTesting
   public VariableWidthHistogram getCopy()
   {
     // only used for tests/benchmarks
     return new VariableWidthHistogram(
        numBuckets,
        Arrays.copyOf(boundaries, boundaries.length),
        Arrays.copyOf(counts, counts.length),
        missingValueCount,
        count,
        max,
        min
     );
   }
 
  @JsonProperty
  public double[] getBoundaries()
  {
    return boundaries;
  }

  @JsonProperty
  public double[] getCounts()
  {
    return counts;
  }
 
   @JsonProperty
   public int getNumBuckets()
   {
     return numBuckets;
   }
   
   @JsonProperty
   public double getCount()
   {
     return count;
   }
   
   @JsonProperty
   public long getMissingValueCount()
   {
     return missingValueCount;
   }
   
   @JsonProperty
   public double getMax()
   {
     return max;
   }
 
   @JsonProperty
   public double getMin()
   {
     return min;
   }
 
   @Override
   public String toString()
   {
     return "{" +
            "numBuckets=" + numBuckets +
            ", boundaries=" + Arrays.toString(boundaries) +
            ", counts=" + Arrays.toString(counts) +
            ", missingValueCount=" + missingValueCount +
            ", count=" + count +
            ", max=" + max +
            ", min=" + min +
            '}';
   }
 
   @Override
   public boolean equals(Object o)
   {
     if (this == o) {
       return true;
     }
     if (o == null || getClass() != o.getClass()) {
       return false;
     }
     VariableWidthHistogram that = (VariableWidthHistogram) o;
     return getNumBuckets() == that.getNumBuckets() &&
            Double.compare(that.getCount(), getCount()) == 0 &&
            Double.compare(that.getMax(), getMax()) == 0 &&
            Double.compare(that.getMin(), getMin()) == 0 &&
            Arrays.equals(getBoundaries(), that.getBoundaries()) &&
            Arrays.equals(getCounts(), that.getCounts());
   }
 
   @Override
   public int hashCode()
   {
     return Objects.hash(
         getNumBuckets(),
         Arrays.hashCode(getBoundaries()),
         Arrays.hashCode(getCounts()),
         getCount(),
         getMax(),
         getMin()
     );
   }
 
   public ReadWriteLock getReadWriteLock()
   {
     return readWriteLock;
   }
 
   /**
    * Called when the histogram encounters a null value.
    */
   public void incrementMissing()
   {
     readWriteLock.writeLock().lock();
 
     try {
       missingValueCount++;
     }
     finally {
       readWriteLock.writeLock().unlock();
     }
   }
 
   /**
    * Resets all the counts to 0 and min/max values +/- infinity.
    */
   public void reset()
   {
     readWriteLock.writeLock().lock();
     this.missingValueCount = 0;
     this.count = 0;
     this.max = Double.NEGATIVE_INFINITY;
     this.min = Double.POSITIVE_INFINITY;
     Arrays.fill(counts, 0);
     Arrays.fill(boundaries, 0);
     readWriteLock.writeLock().unlock();
   }
 
   /**
    * Merge another datapoint into this one. The other datapoint could be
    *  - base64 encoded string of {@code VariableWidthHistogram}
    *  - {@code VariableWidthHistogram} object
    *  - Numeric value
    *
    * @param val
    */
   @VisibleForTesting
   public void combine(@Nullable Object val)
   {
     if (val == null) {
       incrementMissing();
     } else if (val instanceof String) {
       combineHistogram(fromBase64((String) val));
     } else if (val instanceof VariableWidthHistogram) {
       combineHistogram((VariableWidthHistogram) val);
     } else {
       throw new ISE("Unknown class for object: " + val.getClass());
     }
   }
 
   /**
    * Merge another histogram into this one. Only the state of this histogram is updated.
    *
    * If the two histograms have identical buckets, a simpler algorithm is used.
    *
    * @param otherHistogram
    */
   public void combineHistogram(VariableWidthHistogram otherHistogram)
   {
     if (otherHistogram == null) {
       return;
     }
 
     readWriteLock.writeLock().lock();
     otherHistogram.getReadWriteLock().readLock().lock();
 
     try {
       missingValueCount += otherHistogram.getMissingValueCount();
 
       if (numBuckets == otherHistogram.getNumBuckets() &&
           Arrays.equals(boundaries, otherHistogram.getBoundaries())) {
         combineHistogramSameBuckets(otherHistogram);
       } else {
         combineHistogramDifferentBuckets(otherHistogram);
       }
     }
     finally {
       readWriteLock.writeLock().unlock();
       otherHistogram.getReadWriteLock().readLock().unlock();
     }
   }
 
   /**
    * Merge another histogram that has the same range and same buckets.
    *
    * @param otherHistogram
    */
   private void combineHistogramSameBuckets(VariableWidthHistogram otherHistogram)
   {
     double[] otherCountsArray = otherHistogram.getCounts();
     for (int i = 0; i < numBuckets; i++) {
       counts[i] += otherCountsArray[i];
     }
 
     count += otherHistogram.getCount();
     max = Math.max(max, otherHistogram.getMax());
     min = Math.min(min, otherHistogram.getMin());
   }
 
   /**
    * Merge another histogram that has different buckets from mine.
    *
    * First, check if the other histogram falls entirely outside of my defined range. If so, call the appropriate
    * function for optimized handling.
    *
    * Otherwise, this merges the histograms with a more general function.
    *
    * @param otherHistogram
    */
   private void combineHistogramDifferentBuckets(VariableWidthHistogram otherHistogram)
   {
      simpleInterpolateMerge(otherHistogram);
   }
 
   /**
    * Combines this histogram with another histogram by "rebucketing" the other histogram to match our bucketing scheme,
    * assuming that the original input values are uniformly distributed within each bucket in the other histogram.
    *
    * Suppose we have the following histograms:
    *
    *    0   |   0   |   0   |   0   |   0   |   0
    *        1       3       4       6       7
    *
    *    6   |   7   |   0   |   0
    *        2       6       8
    *
    * We will preserve the bucketing scheme of our histogram, and determine what cut-off points within the other
    * histogram align with our bucket boundaries.
    *
    * Using this example, we would effectively rebucket the second histogram to the following:
    *
    *    3   |   4.75   |   1.75   |   3.5   |   0   |   0
    *        1          3          4         6       7
    *
    *
    * @param otherHistogram other histogram to be merged
    */
  private void simpleInterpolateMerge(VariableWidthHistogram otherHistogram)
  {
    if (otherHistogram.areBoundariesAllZero()) {
      return;
     }

    if (areBoundariesAllZero()) {
     mergeBoundariesAllZero(otherHistogram);
     return;
    }

    double[] otherBoundaries = otherHistogram.getBoundaries();
    double[] otherCounts = otherHistogram.getCounts();
    int otherNumBuckets = otherHistogram.getNumBuckets();

    // TODO: review this code
    
    // For each bucket in this histogram, interpolate counts from other histogram
    for (int i = 0; i < numBuckets; i++) {
      // Determine the range of this bucket [leftBound, rightBound]
      double leftBound = (i == 0) ? Double.NEGATIVE_INFINITY : boundaries[i - 1];
      double rightBound = (i == numBuckets - 1) ? Double.POSITIVE_INFINITY : boundaries[i];
      
      // Iterate through all buckets in the other histogram to find overlaps
      for (int j = 0; j < otherNumBuckets; j++) {
        double otherLeftBound = (j == 0) ? Double.NEGATIVE_INFINITY : otherBoundaries[j - 1];
        double otherRightBound = (j == otherNumBuckets - 1) ? Double.POSITIVE_INFINITY : otherBoundaries[j];
        
        // Calculate the overlap between this bucket and other bucket
        double overlapLeft = Math.max(leftBound, otherLeftBound);
        double overlapRight = Math.min(rightBound, otherRightBound);
        
        // If there's an overlap, interpolate the count
        if (overlapLeft < overlapRight) {
          double otherBucketWidth = getBucketWidth(otherLeftBound, otherRightBound, 
                                                     otherHistogram.getMin(), otherHistogram.getMax());
          double overlapWidth = getBucketWidth(overlapLeft, overlapRight, 
                                               otherHistogram.getMin(), otherHistogram.getMax());
          
          if (otherBucketWidth > 0) {
            // Proportion of the other bucket that overlaps with this bucket
            double proportion = overlapWidth / otherBucketWidth;
            counts[i] += otherCounts[j] * proportion;
          } else if (otherCounts[j] > 0) {
            // If the bucket width is 0 but has count, it's a point mass
            // Add the entire count if the point falls within this bucket
            counts[i] += otherCounts[j];
          }
        }
      }
    }
    
    // Update aggregate statistics
    count += otherHistogram.getCount();
    max = Math.max(max, otherHistogram.getMax());
    min = Math.min(min, otherHistogram.getMin());
  }
  
  /**
   * Calculate the effective width of a bucket, handling infinite bounds.
   * For buckets with infinite bounds, we use the histogram's min/max values.
   * 
   * @param left the left bound of the bucket
   * @param right the right bound of the bucket
   * @param histMin the minimum value in the histogram
   * @param histMax the maximum value in the histogram
   * @return the effective width of the bucket
   */
  private double getBucketWidth(double left, double right, double histMin, double histMax)
  {
    // Handle infinite bounds by using histogram min/max
    if (Double.isInfinite(left)) {
      left = histMin;
    }
    if (Double.isInfinite(right)) {
      right = histMax;
    }
    
    // If both are still infinite or invalid, return 0
    if (Double.isInfinite(left) || Double.isInfinite(right)) {
      return 0;
    }
    
    return Math.max(0, right - left);
  }

   /**
    * Checks if all boundaries of the histogram are zeros.
    *
    * @return true if all entries in boundaries are 0.0, false otherwise
    */
   public boolean areBoundariesAllZero()
   {
     for (double boundary : boundaries) {
       if (boundary != 0.0) {
         return false;
       }
     }
     return true;
   }

   private void mergeBoundariesAllZero(VariableWidthHistogram otherHistogram)
   {
    if (otherHistogram.areBoundariesAllZero()) {
      return;
    }
     int otherHistNumBuckets = otherHistogram.getNumBuckets();
     double[] otherHistBoundaries = otherHistogram.getBoundaries();
     double[] otherHistCounts = otherHistogram.getCounts();
     if (numBuckets <= otherHistNumBuckets) {
      numBuckets = otherHistNumBuckets;
      for (int i = 0; i < numBuckets-1; i++) {
        boundaries[i] = otherHistBoundaries[i];
        counts[i] = otherHistCounts[i];
      }
      counts[numBuckets-1] = otherHistCounts[otherHistNumBuckets-1];
     } else {
      for (int i = 0; i < numBuckets-1; i++) {
        boundaries[i] = otherHistBoundaries[i];
        counts[i] = otherHistCounts[i];
      }
      for (int i = numBuckets-1; i < otherHistNumBuckets; i++) {
        counts[numBuckets-1] += otherHistCounts[i];
      }
     } 
   }
 
   /**
    * Encode the serialized form generated by toBytes() in Base64, used as the JSON serialization format.
    *
    * @return Base64 serialization
    */
   @JsonValue
   public String toBase64()
   {
     byte[] asBytes = toBytes();
     return StringUtils.fromUtf8(StringUtils.encodeBase64(asBytes));
   }
 
   /**
    * Serialize the histogram, with two possible encodings chosen based on the number of filled buckets:
    *
    * Full: Store the histogram buckets as an array of bucket counts, with size numBuckets
    * Sparse: Store the histogram buckets as a list of (bucketNum, count) pairs.
    */
   public byte[] toBytes()
   {
     readWriteLock.readLock().lock();
 
     try {
        int size = getStorageSize(numBuckets);
        ByteBuffer buf = ByteBuffer.allocate(size);
        writeByteBuffer(buf);
        return buf.array();
     }
     finally {
       readWriteLock.readLock().unlock();
     }
   }
 
   /**
    * Helper method for toBytes
    *
    * @param buf Destination buffer
    */
   private void writeByteBuffer(ByteBuffer buf)
   {
     buf.putDouble(count);
     buf.putInt(numBuckets);
     buf.putDouble(max);
     buf.putDouble(min);
 
     buf.asDoubleBuffer().put(boundaries);
     buf.position(buf.position() + Double.BYTES * boundaries.length);
     buf.asDoubleBuffer().put(counts);
     buf.position(buf.position() + Double.BYTES * counts.length);
   }

   /**
    * Deserialize a Base64-encoded histogram, used when deserializing from JSON (such as when transferring query results)
    * or when ingesting a pre-computed histogram.
    *
    * @param encodedHistogram Base64-encoded histogram
    * @return Deserialized object
    */
   public static VariableWidthHistogram fromBase64(String encodedHistogram)
   {
     byte[] asBytes = StringUtils.decodeBase64(encodedHistogram.getBytes(StandardCharsets.UTF_8));
     return fromBytes(asBytes);
   }
 
   /**
    * General deserialization method for VariableWidthHistogram.
    *
    * @param bytes
    * @return
    */
   public static VariableWidthHistogram fromBytes(byte[] bytes)
   {
     ByteBuffer buf = ByteBuffer.wrap(bytes);
     return fromByteBuffer(buf);
   }
 
   /**
    * Deserialization helper method
    *
    * @param buf Source buffer containing serialized histogram
    * @return Deserialized object
    */
  public static VariableWidthHistogram fromByteBuffer(ByteBuffer buf)
  {
    double count = buf.getDouble();
    int numBuckets = buf.getInt();
    double max = buf.getDouble();
    double min = buf.getDouble();
    double[] boundaries = new double[numBuckets - 1];
    buf.asDoubleBuffer().get(boundaries);
    buf.position(buf.position() + Double.BYTES * boundaries.length);
    double[] counts = new double[numBuckets];
    buf.asDoubleBuffer().get(counts);
    buf.position(buf.position() + Double.BYTES * counts.length);
    return new VariableWidthHistogram(
        numBuckets,
        boundaries,
        counts,
        0,
        count,
        max,
        min
    );
  }
 
   /**
    * Compute the size in bytes of a full-encoding serialized histogram, without the serialization header
    *
    * @param numBuckets number of buckets
    * @return full serialized size in bytes
    */
   public static int getStorageSize(int numBuckets)
   {
     return FIXED_SIZE_FIELDS_SIZE +
            Double.BYTES * (numBuckets - 1) +
            Double.BYTES * numBuckets;
   }
 
   @VisibleForTesting
   public int getNonEmptyBucketCount()
   {
     int count = 0;
     for (int i = 0; i < numBuckets; i++) {
       if (counts[i] != 0) {
         count++;
       }
     }
     return count;
   }
 }
 