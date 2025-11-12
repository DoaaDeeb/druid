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
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VariableWidthHistogram
{
  private static final Logger log = new Logger(VariableWidthHistogram.class);
  
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
  public static final int FIXED_SIZE_FIELDS_SIZE = Double.BYTES +      // count
                                               Integer.BYTES +     // maxNumBuckets
                                               Integer.BYTES +     // numBuckets
                                               Long.BYTES +        // missingValueCount
                                               Double.BYTES +      // max
                                               Double.BYTES;       // min
 
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
   private int maxNumBuckets;
   private double[] boundaries;
   private double[] counts;
   private long missingValueCount = 0;
   private double count = 0;
   private double max = Double.NEGATIVE_INFINITY;
   private double min = Double.POSITIVE_INFINITY;
 
   public VariableWidthHistogram(int maxNumBuckets)
   {
     this.readWriteLock = new ReentrantReadWriteLock(true);
     this.maxNumBuckets = maxNumBuckets;
   }

   protected VariableWidthHistogram(
    int maxNumBuckets,
    long missingValueCount,
    double count,
    double max,
    double min
   )
   {
     this.maxNumBuckets = maxNumBuckets;
     this.missingValueCount = missingValueCount;
     this.count = count;
     this.max = max;
     this.min = min;
     this.readWriteLock = new ReentrantReadWriteLock(true);
   }
 
   @VisibleForTesting
   protected VariableWidthHistogram(
    int maxNumBuckets,
    int numBuckets,
    double[] boundaries,
    double[] counts,
    long missingValueCount,
    double count,
    double max,
    double min
   )
   {
     this.maxNumBuckets = maxNumBuckets;
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
        maxNumBuckets,
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
      log.info("[VWH-COMBINE] combineHistogram called with null otherHistogram");
      return;
    }

    log.info("[VWH-COMBINE] Before combine: this[numBuckets=%d, count=%s, min=%s, max=%s, boundaries=%s] other[numBuckets=%d, count=%s, min=%s, max=%s, boundaries=%s]",
             numBuckets, count, min, max, Arrays.toString(boundaries),
             otherHistogram.getNumBuckets(), otherHistogram.getCount(), otherHistogram.getMin(), otherHistogram.getMax(), Arrays.toString(otherHistogram.getBoundaries()));

    readWriteLock.writeLock().lock();
    otherHistogram.getReadWriteLock().readLock().lock();

    try {
      missingValueCount += otherHistogram.getMissingValueCount();

      if (numBuckets == otherHistogram.getNumBuckets() &&
          Arrays.equals(boundaries, otherHistogram.getBoundaries())) {
        log.info("[VWH-COMBINE] Using combineHistogramSameBuckets");
        combineHistogramSameBuckets(otherHistogram);
      } else {
        log.info("[VWH-COMBINE] Using combineHistogramDifferentBuckets");
        combineHistogramDifferentBuckets(otherHistogram);
      }
      
      log.info("[VWH-COMBINE] After combine: this[count=%s, min=%s, max=%s, counts=%s]",
               count, min, max, Arrays.toString(counts));
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
     double[] otherCounts = otherHistogram.getCounts();
     for (int i = 0; i < numBuckets; i++) {
       counts[i] += otherCounts[i];
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
    if (otherHistogram.getNumBuckets() == 0) {
      return;
     }

    if (numBuckets == 0) {
      mergeZeroBuckets(otherHistogram);
     return;
    }
    // simpleInterpolateMerge(otherHistogram);
   }
 
   /**
    * Combines this histogram with another histogram by "rebucketing" the other histogram to match our bucketing scheme,
    * assuming that the original input values are uniformly distributed within each bucket in the other histogram.
    *
    * Suppose we have the following histograms:
    *
    *    |   0   |   0   |   0   |   0   |   0   |   0   |
    *  -inf      1       3       4       6       10      inf
    *
    *    |   6   |   7   |   0   |   3   |
    *  -inf      2       6       8      inf
    *
    * We will preserve the bucketing scheme of our histogram, and determine what cut-off points within the other
    * histogram align with our bucket boundaries.
    *
    * Using this example, we would effectively rebucket the second histogram to the following:
    *
    *    |   6   |   1.75   |   1.75   |   3.5   |   0   |   3   |
    *  -inf      1          3          4         6       10      inf
    *
    *
    * @param otherHistogram other histogram to be merged
    */
  private void simpleInterpolateMerge(VariableWidthHistogram otherHistogram)
  {

    double[] otherBoundaries = otherHistogram.getBoundaries();
    double[] otherCounts = otherHistogram.getCounts();
    int otherNumBuckets = otherHistogram.getNumBuckets();
    double otherMin = otherHistogram.getMin();
    double otherMax = otherHistogram.getMax();

    int i = 0;
    int j = 0;

    double upperBound, lowerBound, otherUpperBound, otherLowerBound;
    double overlap, bucketWidth, overlapProportion = 0;

    while (i < numBuckets && j < otherNumBuckets) {
      lowerBound = (i == 0) ? min : boundaries[i - 1];
      upperBound = (i == numBuckets - 1) ? max : boundaries[i];
      otherLowerBound = (j == 0) ? otherMin : otherBoundaries[j - 1];
      otherUpperBound = (j == otherNumBuckets - 1) ? otherMax : otherBoundaries[j];
      
      overlap = Math.min(upperBound, otherUpperBound) - Math.max(lowerBound, otherLowerBound);
      bucketWidth = otherUpperBound - otherLowerBound;
      overlapProportion = overlap / bucketWidth;

      log.info("i=%d, j=%d: [%f,%f] x [%f,%f] -> overlap=%f, width=%f, prop=%f",
               i, j, upperBound, lowerBound, otherUpperBound, otherLowerBound,
               overlap, bucketWidth, overlapProportion);

      if ((overlap  == Double.POSITIVE_INFINITY && bucketWidth == Double.POSITIVE_INFINITY) 
       || (overlap  == Double.NEGATIVE_INFINITY && bucketWidth == Double.NEGATIVE_INFINITY)) {
        counts[i] += otherCounts[j];
      } else if (overlapProportion > 0) {
        counts[i] += otherCounts[j] * overlapProportion;
      }

      if (otherUpperBound < upperBound) {
        j++;
      } else {
        i++;
      }
    }

    while (j < otherNumBuckets) {
      log.info("Adding remaining bucket from other histogram: j=%d, otherCount=%f, count=%f", j, otherCounts[j], counts[numBuckets - 1]);
      if (overlapProportion > 0) {
        counts[numBuckets - 1] += otherCounts[j] * (1 - overlapProportion);
        overlapProportion = 0;
      } else {
        counts[numBuckets - 1] += otherCounts[j];
      }
      j++;
    }

    if (i < numBuckets) {
      log.info("Adding remaining bucket from this histogram: i=%d, count=%f", i, counts[i]);
      for (int k = 0; k < otherNumBuckets; k++) {
        log.info("Adding bucket from other histogram: k=%d, count=%f", k, otherCounts[k]);
        counts[i] += otherCounts[k];
      }
      
    }

    // if (j < otherNumBuckets) {
    //   // Other histogram is entirely to the RIGHT - add remaining buckets to the LAST bucket
    //   log.info("Other histogram is entirely to the right, adding remaining buckets to last bucket");
    //   while (j < otherNumBuckets) {
    //     log.info("Adding remaining bucket from other histogram: j=%d, count=%f", j, otherCounts[j]);
    //     counts[numBuckets - 1] += otherCounts[j];
    //     j++;
    //   }
    // } else if (i < numBuckets && j == otherNumBuckets) {
    //   // Check if nothing was added from the other histogram (all buckets had negative overlap)
    //   // This means the other histogram was entirely to the LEFT - add all its buckets to the FIRST bucket
    //   log.info("Other histogram is entirely to the left, adding all buckets to first bucket");
    //   for (int k = 0; k < otherNumBuckets; k++) {
    //     log.info("Adding bucket from other histogram: k=%d, count=%f", k, otherCounts[k]);
    //     counts[0] += otherCounts[k];
    //   }
    // }
  

    

    // Update aggregate statistics
    count += otherHistogram.getCount();
    max = Math.max(max, otherHistogram.getMax());
    min = Math.min(min, otherHistogram.getMin());
  }


 private void mergeZeroBuckets(VariableWidthHistogram otherHistogram)
 {
   log.info("[VWH-MERGE-ZERO] mergeBoundariesAllZero called: this.boundaries=%s, other.boundaries=%s",
            Arrays.toString(boundaries), Arrays.toString(otherHistogram.getBoundaries()));
   
  if (otherHistogram.getNumBuckets() == 0) {
    log.info("[VWH-MERGE-ZERO] other histogram also has all-zero boundaries, returning");
    return;
  }
  
   log.info("[VWH-MERGE-ZERO] Copying from other histogram: numBuckets=%d->%d",
            numBuckets, otherHistogram.getNumBuckets());
   
   int otherNumBuckets = otherHistogram.getNumBuckets();
   double[] otherBoundaries = otherHistogram.getBoundaries();
   double[] otherCounts = otherHistogram.getCounts();
   if (otherNumBuckets <= maxNumBuckets) {
    numBuckets = otherNumBuckets;
    counts = Arrays.copyOf(otherCounts, numBuckets);
    boundaries = Arrays.copyOf(otherBoundaries, numBuckets-1);
   } else {
    numBuckets = maxNumBuckets;
    boundaries = new double[numBuckets-1];
    counts = new double[numBuckets];
    for (int i = 0; i < numBuckets-1; i++) {
      boundaries[i] = otherBoundaries[i];
      counts[i] = otherCounts[i];
    }
    for (int i = numBuckets-1; i < otherNumBuckets; i++) {
      counts[numBuckets-1] += otherCounts[i];
    }
   }
   
   // Update aggregate statistics
   count = otherHistogram.getCount();
   missingValueCount += otherHistogram.getMissingValueCount();
   max = otherHistogram.getMax();
   min = otherHistogram.getMin();
   
   log.info("[VWH-MERGE-ZERO] After merge: boundaries=%s, counts=%s, count=%s, min=%s, max=%s",
            Arrays.toString(boundaries), Arrays.toString(counts), count, min, max);
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
   * Convert the histogram to a Map for JSON representation.
   * This is useful for query results when you want structured JSON instead of base64.
   *
   * @return Map containing histogram fields
   */
  public java.util.Map<String, Object> toMap()
  {
    readWriteLock.readLock().lock();
    try {
      java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
      map.put("numBuckets", numBuckets);
      map.put("boundaries", boundaries);
      map.put("counts", counts);
      map.put("missingValueCount", missingValueCount);
      map.put("count", count);
      map.put("max", max);
      map.put("min", min);
      return map;
    }
    finally {
      readWriteLock.readLock().unlock();
    }
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
    buf.putInt(maxNumBuckets);
    buf.putInt(numBuckets);
    buf.putLong(missingValueCount);
    buf.putDouble(max);
    buf.putDouble(min);

    if (boundaries != null && boundaries.length > 0) {
      buf.asDoubleBuffer().put(boundaries);
      buf.position(buf.position() + Double.BYTES * boundaries.length);
    }
    if (counts != null && counts.length > 0) {
      buf.asDoubleBuffer().put(counts);
      buf.position(buf.position() + Double.BYTES * counts.length);
    }
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
    int maxNumBuckets = buf.getInt();
    int numBuckets = buf.getInt();
    long missingValueCount = buf.getLong();
    double max = buf.getDouble();
    double min = buf.getDouble();
    if (numBuckets == 0) {
      return new VariableWidthHistogram(
        maxNumBuckets,
        missingValueCount,
        count,
        max,
        min
        );
    }
    double[] boundaries = new double[numBuckets - 1];
    buf.asDoubleBuffer().get(boundaries);
    buf.position(buf.position() + Double.BYTES * boundaries.length);
    double[] counts = new double[numBuckets];
    buf.asDoubleBuffer().get(counts);
    buf.position(buf.position() + Double.BYTES * counts.length);
    return new VariableWidthHistogram(
        maxNumBuckets,
        numBuckets,
        boundaries,
        counts,
        missingValueCount,
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
    if (numBuckets == 0) {
      return FIXED_SIZE_FIELDS_SIZE;
    }
    return FIXED_SIZE_FIELDS_SIZE +
           Double.BYTES * (numBuckets - 1) +  // boundaries array
           Double.BYTES * numBuckets;          // counts array
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
 