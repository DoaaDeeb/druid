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

 import org.apache.druid.segment.data.ObjectStrategy;
 import org.junit.Assert;
 import org.junit.Test;
 
 public class VariableWidthHistogramTest
 {

  protected VariableWidthHistogram buildEmptyHistogram(
    int maxNumBuckets
   )
   {
     VariableWidthHistogram h = new VariableWidthHistogram(
        maxNumBuckets
     );
     return h;
   }
 
   protected VariableWidthHistogram buildHistogram(
    int numBuckets,
    double[] boundaries,
    double[] counts,
    long missingValueCount,
    double count,
    double max,
    double min
   )
   {
     VariableWidthHistogram h = new VariableWidthHistogram(
        numBuckets,
         numBuckets,
         boundaries,
         counts,
         missingValueCount,
         count,
         max,
         min
     );
     return h;
   }

  @Test
  public void testMergeSameBuckets()
  {
    VariableWidthHistogram h = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        15,
        29,
        3
    );

    VariableWidthHistogram h2 = buildHistogram(
       6,
       new double[]{4, 10, 12, 18, 20},
       new double[]{3, 1, 2, 2, 1, 9},
       0,
       14,
       30,
       1
    );

   h.combineHistogram(h2);
   Assert.assertEquals(6, h.getNumBuckets());
   Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
   Assert.assertArrayEquals(new double[]{6, 2, 4, 4, 2, 19}, h.getCounts(), 0.01);
   Assert.assertEquals(0, h.getMissingValueCount());
   Assert.assertEquals(29, h.getCount(), 0.01);
   Assert.assertEquals(1, h.getMin(), 0.01);
   Assert.assertEquals(30, h.getMax(), 0.01);

  }
 
  @Test
  public void testMergeNoOverlapRight()
  {
    VariableWidthHistogram h = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        25,
        1
    );

    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{30, 40, 60, 70, 100},
        new double[]{10, 0, 30, 10, 21, 33},
        0,
        104,
        130,
        29
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{3, 1, 2, 2, 1, 114}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(130, h.getMax(), 0.01);
  }

  @Test
  public void testMergeNoOverlapRightInf()
  {
    VariableWidthHistogram h = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        25,
        Double.NEGATIVE_INFINITY
    );

    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{30, 40, 60, 70, 100},
        new double[]{10, 0, 30, 10, 21, 33},
        0,
        104,
        Double.POSITIVE_INFINITY,
        29
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{3, 1, 2, 2, 1, 114}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
  }

  @Test
  public void testMergeNoExplicitOverlapRightInf()
  {
    VariableWidthHistogram h = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        Double.POSITIVE_INFINITY,
        1
    );

    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{30, 40, 60, 70, 100},
        new double[]{10, 0, 30, 10, 21, 33},
        0,
        104,
        110,
        Double.NEGATIVE_INFINITY
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{13, 1, 2, 2, 1, 104}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
  }

  @Test
  public void testMergeNoExplicitOverlapRightInf2()
  {
    VariableWidthHistogram h = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY
    );

    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{30, 40, 60, 70, 100},
        new double[]{10, 0, 30, 10, 21, 33},
        0,
        104,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{13, 1, 2, 2, 1, 104}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
  }
 
  @Test
  public void testMergeNoOverlapLeft()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      130,
      29
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        25,
        1
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{29, 0, 30, 10, 21, 33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(130, h.getMax(), 0.01);
  }
  
  @Test
  public void testMergeNoOverlapLeft2()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      130,
      25
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        25,
        1
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{29, 0, 30, 10, 21, 33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(130, h.getMax(), 0.01);
  }

  @Test
  public void testMergeOverlapLeft()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      130,
      23
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        25,
        1
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{29, 0, 30, 10, 21, 33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(130, h.getMax(), 0.01);
  }

  @Test
  public void testMergeOverlapLeft2()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      130,
      20
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        30,
        1
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{29, 0, 30, 10, 21, 33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(130, h.getMax(), 0.01);
  }

  @Test
  public void testMergeOverlapLeft3()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      130,
      20
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        30,
        1
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{29, 0, 30, 10, 21, 33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(130, h.getMax(), 0.01);
  }
  
  @Test
  public void testMergeNoOverlapLeftInf()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      Double.POSITIVE_INFINITY,
      29
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        25,
        Double.NEGATIVE_INFINITY
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{29, 0, 30, 10, 21, 33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
  }
 

  @Test
  public void testMergeNoExplicitOverlapLeftInf()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{30, 40, 60, 70, 100},
      new double[]{10, 0, 30, 10, 21, 33},
      0,
      104,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{30, 40, 60, 70, 100}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{19, 0, 30, 10, 21, 43}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(123, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
  }
 
   @Test
   public void testMergeSameBucketsRightOverlap()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{4, 10, 12, 18, 20},
      new double[]{4, 6, 8, 10, 12, 14},
      0,
      54,
      32,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{12, 18, 20, 32, 40},
        new double[]{10, 2, 4, 6, 8, 10},
        0,
        40,
        45,
        10
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{4, 6, 18, 12, 16, 38}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(94, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(45, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeSameBucketsRightOverlap2()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{4, 10, 12, 18, 20},
      new double[]{4, 6, 8, 10, 12, 14},
      0,
      54,
      32,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{12, 18, 20, 26, 40},
        new double[]{10, 2, 4, 6, 8, 10},
        0,
        40,
        45,
        10
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{4, 6, 18, 12, 16, 38}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(94, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(45, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeSameBucketsRightOverlapInf()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{4, 10, 12, 18, 20},
      new double[]{4, 6, 8, 10, 12, 14},
      0,
      54,
      32,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{12, 18, 20, 32, 40},
        new double[]{10, 2, 4, 6, 8, 10},
        0,
        40,
        Double.POSITIVE_INFINITY,
        10
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{4, 10, 12, 18, 20}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{4, 6, 18, 12, 16, 38}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(94, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeSameBucketsLeftOverlap()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{12, 18, 20, 32, 40},
      new double[]{10, 2, 4, 6, 8, 10},
      0,
      40,
      45,
      10
  );
    VariableWidthHistogram h2 = buildHistogram(
      6,
      new double[]{4, 10, 12, 18, 20},
      new double[]{4, 6, 8, 10, 12, 14},
      0,
      54,
      32,
      1
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{12, 18, 20, 32, 40}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{28, 12, 16, 20, 8, 10}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(94, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(45, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeSameBucketsLeftOverlap2()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{12, 18, 20, 32, 40},
      new double[]{10, 2, 4, 6, 8, 10},
      0,
      40,
      45,
      10
  );
    VariableWidthHistogram h2 = buildHistogram(
      6,
      new double[]{4, 11, 12, 18, 20},
      new double[]{4, 6, 8, 10, 12, 14},
      0,
      54,
      32,
      1
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{12, 18, 20, 32, 40}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{28, 12, 16, 20, 8, 10}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(94, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(45, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeSameBucketsLeftOverlapInf()
   {
    VariableWidthHistogram h = buildHistogram(
        6,
        new double[]{12, 18, 20, 32, 40},
        new double[]{10, 2, 4, 6, 8, 10},
        0,
        40,
        Double.POSITIVE_INFINITY,
        10
    );
      VariableWidthHistogram h2 = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{4, 6, 8, 10, 12, 14},
        0,
        54,
        32,
        Double.NEGATIVE_INFINITY
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{12, 18, 20, 32, 40}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{28, 12, 16, 20, 8, 10}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(94, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }
 
   
   @Test
   public void testMergeSameBucketsContainsOther()
   {
    VariableWidthHistogram h = buildHistogram(
      7,
      new double[]{10, 20, 35, 45, 65, 90},
      new double[]{10, 2, 4, 6, 8, 10, 12},
      0,
      52,
      100,
      5
  );
    VariableWidthHistogram h2 = buildHistogram(
      3,
      new double[]{35, 45},
      new double[]{1,2,3},
      0,
      6,
      65,
      20
  );

    h.combineHistogram(h2);
    Assert.assertEquals(7, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{10, 20, 35, 45, 65, 90}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{10, 2, 5, 8, 11, 10, 12}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(58, h.getCount(), 0.01);
    Assert.assertEquals(5, h.getMin(), 0.01);
    Assert.assertEquals(100, h.getMax(), 0.01);
   }

   @Test
   public void testMergeSameBucketsContainsOtherInf()
   {
    VariableWidthHistogram h = buildHistogram(
      7,
      new double[]{10, 20, 35, 45, 65, 90},
      new double[]{10, 2, 4, 6, 8, 10, 12},
      0,
      52,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
      3,
      new double[]{35, 45},
      new double[]{1,2,3},
      0,
      6,
      65,
      20
  );

    h.combineHistogram(h2);
    Assert.assertEquals(7, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{10, 20, 35, 45, 65, 90}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{10, 2, 5, 8, 11, 10, 12}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(58, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeSameBucketsContainedByOther()
   {
    VariableWidthHistogram h = buildHistogram(
      3,
      new double[]{35, 45},
      new double[]{1,2,3},
      0,
      6,
      65,
      20
  );
    VariableWidthHistogram h2 = buildHistogram(
      7,
      new double[]{10, 20, 35, 45, 65, 90},
      new double[]{10, 2, 4, 6, 8, 10, 12},
      0,
      52,
      100,
      5
  );

    h.combineHistogram(h2);
    Assert.assertEquals(3, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{35, 45}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{17,8,33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(58, h.getCount(), 0.01);
    Assert.assertEquals(5, h.getMin(), 0.01);
    Assert.assertEquals(100, h.getMax(), 0.01);
   }

   @Test
   public void testMergeSameBucketsContainedByOtherInf()
   {
    VariableWidthHistogram h = buildHistogram(
      3,
      new double[]{35, 45},
      new double[]{1,2,3},
      0,
      6,
      65,
      20
  );
    VariableWidthHistogram h2 = buildHistogram(
      7,
      new double[]{10, 20, 35, 45, 65, 90},
      new double[]{10, 2, 4, 6, 8, 10, 12},
      0,
      52,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );

    h.combineHistogram(h2);
    Assert.assertEquals(3, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{35, 45}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{17,8,33}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(58, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }
 
   @Test
   public void testMergeDifferentBuckets2()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
      9,
      new double[]{2, 8, 9, 14, 17, 20, 25, 30},
      new double[]{1, 6, 1, 5, 3, 3, 5, 5, 2},
      0,
      31,
      32,
      1
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 24}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(52, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(32, h.getMax(), 0.01);
   }

   @Test
   public void testMergeDifferentBucketsInf()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
      9,
      new double[]{2, 8, 9, 14, 17, 20, 25, 30},
      new double[]{1, 6, 1, 5, 3, 3, 5, 5, 2},
      0,
      31,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 24}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(52, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }

   @Test
   public void testMergeDifferentBucketsInf2()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
      9,
      new double[]{2, 8, 9, 14, 17, 20, 25, 30},
      new double[]{1, 6, 1, 5, 3, 3, 5, 5, 2},
      0,
      31,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 24}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(52, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }

   @Test
   public void testMergeDifferentBucketsInf3()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
      9,
      new double[]{2, 8, 9, 14, 17, 20, 25, 30},
      new double[]{1, 6, 1, 5, 3, 3, 5, 5, 2},
      0,
      31,
      32,
      1
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 24}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(52, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01);
   }
 
 
   @Test
   public void testMergeDifferentBuckets()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
      10,
      new double[]{2, 3, 5, 7, 8, 10, 12, 14, 19},
      new double[]{1, 1, 2, 2, 1, 2, 2, 2, 5, 3},
      0,
      21,
      22,
      1
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 14}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(42, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(22, h.getMax(), 0.01);
   }
 
  @Test
  public void testMergeDifferentBucketsRightOverlap()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
      6,
      new double[]{9, 10, 13, 17, 25},
      new double[]{1, 1, 3, 4, 8, 4},
      0,
      21,
      29,
      8
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{1, 3, 2, 5, 10, 21}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(42, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01); 
  }

  @Test
  public void testMergeDifferentBucketsRightOverlapInf()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
      6,
      new double[]{9, 10, 13, 17, 25},
      new double[]{1, 1, 3, 4, 8, 4},
      0,
      21,
      Double.POSITIVE_INFINITY,
      8
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{1, 3, 2, 5, 10, 21}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(42, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01); 
  }
 
  @Test
  public void testMergeDifferentBucketsLeftOverlapInf()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{9, 10, 13, 17, 25},
      new double[]{1, 1, 3, 4, 8, 4},
      0,
      21,
      Double.POSITIVE_INFINITY,
      8
  );
    VariableWidthHistogram h2 = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 12},
      new double[]{1, 3, 2, 3, 2, 3},
      0,
      14,
      15,
      Double.NEGATIVE_INFINITY
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{9, 10, 13, 17, 25}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{9, 2, 6, 6, 8, 4}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(35, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01); 
  }

  @Test
  public void testMergeDifferentBucketsLeftOverlap()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{9, 10, 13, 17, 25},
      new double[]{1, 1, 3, 4, 8, 4},
      0,
      21,
      29,
      8
  );
    VariableWidthHistogram h2 = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 12},
      new double[]{1, 3, 2, 3, 2, 3},
      0,
      14,
      15,
      1
  );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{9, 10, 13, 17, 25}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{9, 2, 6, 6, 8, 4}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(35, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01); 
  }
 
  @Test
  public void testMergeDifferentBucketsContainsOther()
  {
    VariableWidthHistogram h = buildHistogram(
      8,
      new double[]{9, 10, 13, 17, 20, 22, 25},
      new double[]{1, 1, 3, 4, 3, 2, 3, 4},
      0,
      21,
      29,
      8
  );
    VariableWidthHistogram h2 = buildHistogram(
      5,
      new double[]{14, 15, 18, 19},
      new double[]{2, 1, 3, 1, 2},
      0,
      9,
      21,
      12
  );

    h.combineHistogram(h2);
    Assert.assertEquals(8, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{9, 10, 13, 17, 20, 22, 25}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{1, 1, 4, 8, 6, 3, 3, 4}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(30, h.getCount(), 0.01);
    Assert.assertEquals(8, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01); 
  }

  @Test
  public void testMergeDifferentBucketsContainsOtherInf()
  {
    VariableWidthHistogram h = buildHistogram(
      8,
      new double[]{9, 10, 13, 17, 20, 22, 25},
      new double[]{1, 1, 3, 4, 3, 2, 3, 4},
      0,
      21,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );
    VariableWidthHistogram h2 = buildHistogram(
      5,
      new double[]{14, 15, 18, 19},
      new double[]{2, 1, 3, 1, 2},
      0,
      9,
      21,
      12
  );

    h.combineHistogram(h2);
    Assert.assertEquals(8, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{9, 10, 13, 17, 20, 22, 25}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{1, 1, 4, 8, 6, 3, 3, 4}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(30, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01); 
  }
 
  @Test
  public void testMergeDifferentBucketsContainedByOther()
  {
    VariableWidthHistogram h = buildHistogram(
      5,
      new double[]{14, 15, 18, 19},
      new double[]{2, 1, 3, 1, 2},
      0,
      9,
      21,
      12
  );
    VariableWidthHistogram h2 = buildHistogram(
      8,
      new double[]{9, 10, 13, 17, 20, 22, 25},
      new double[]{1, 1, 3, 4, 3, 2, 3, 4},
      0,
      21,
      29,
      8
  );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{14, 15, 18, 19}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{8, 2, 6, 2, 12}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(30, h.getCount(), 0.01);
    Assert.assertEquals(8, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01); 
  }

  @Test
  public void testMergeDifferentBucketsContainedByOtherInf()
  {
    VariableWidthHistogram h = buildHistogram(
      5,
      new double[]{14, 15, 18, 19},
      new double[]{2, 1, 3, 1, 2},
      0,
      9,
      21,
      12
  );
    VariableWidthHistogram h2 = buildHistogram(
      8,
      new double[]{9, 10, 13, 17, 20, 22, 25},
      new double[]{1, 1, 3, 4, 3, 2, 3, 4},
      0,
      21,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
  );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{14, 15, 18, 19}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{8, 2, 6, 2, 12}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(30, h.getCount(), 0.01);
    Assert.assertEquals(Double.NEGATIVE_INFINITY, h.getMin(), 0.01);
    Assert.assertEquals(Double.POSITIVE_INFINITY, h.getMax(), 0.01); 
  }
 
  @Test
  public void testCombineBase64()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
      10,
      new double[]{2, 3, 5, 7, 8, 10, 12, 14, 19},
      new double[]{1, 1, 2, 2, 1, 2, 2, 2, 5, 3},
      0,
      21,
      22,
      1
  );

    h.combine(h2.toBase64Proto());
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 14}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(42, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(22, h.getMax(), 0.01);
  }
 
  @Test
  public void testCombineAnotherHistogram()
  {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    VariableWidthHistogram h2 = buildHistogram(
      10,
      new double[]{2, 3, 5, 7, 8, 10, 12, 14, 19},
      new double[]{1, 1, 2, 2, 1, 2, 2, 2, 5, 3},
      0,
      21,
      22,
      1
  );

    h.combine(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertArrayEquals(new double[]{2, 5, 7, 10, 15}, h.getBoundaries(), 0.01);
    Assert.assertArrayEquals(new double[]{2, 6, 4, 6, 10, 14}, h.getCounts(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(42, h.getCount(), 0.01);
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(22, h.getMax(), 0.01);
  }

  @Test
  public void testCombineEmptyHistogramEqualNumBuckets()
  {
   VariableWidthHistogram h = buildEmptyHistogram(
     10
 );

   VariableWidthHistogram h2 = buildHistogram(
     10,
     new double[]{2, 3, 5, 7, 8, 10, 12, 14, 19},
     new double[]{1, 1, 2, 2, 1, 2, 2, 2, 5, 3},
     0,
     21,
     22,
     1
 );

   h.combineHistogram(h2);
   Assert.assertEquals(10, h.getNumBuckets());
   Assert.assertArrayEquals(new double[]{2, 3, 5, 7, 8, 10, 12, 14, 19}, h.getBoundaries(), 0.01);
   Assert.assertArrayEquals(new double[]{1, 1, 2, 2, 1, 2, 2, 2, 5, 3}, h.getCounts(), 0.01);
   Assert.assertEquals(0, h.getMissingValueCount());
   Assert.assertEquals(21, h.getCount(), 0.01);
   Assert.assertEquals(1, h.getMin(), 0.01);
   Assert.assertEquals(22, h.getMax(), 0.01);
  }

  @Test
  public void testCombineEmptyHistogramLessNumBuckets()
  {
   VariableWidthHistogram h = buildEmptyHistogram(
     10
 );

   VariableWidthHistogram h2 = buildHistogram(
     5,
     new double[]{2, 3, 5, 7},
     new double[]{1, 1, 2, 2, 1},
     0,
     7,
     10,
     1
 );

   h.combineHistogram(h2);
   Assert.assertEquals(5, h.getNumBuckets());
   Assert.assertArrayEquals(new double[]{2, 3, 5, 7}, h.getBoundaries(), 0.01);
   Assert.assertArrayEquals(new double[]{1, 1, 2, 2, 1}, h.getCounts(), 0.01);
   Assert.assertEquals(0, h.getMissingValueCount());
   Assert.assertEquals(7, h.getCount(), 0.01);
   Assert.assertEquals(1, h.getMin(), 0.01);
   Assert.assertEquals(10, h.getMax(), 0.01);
  }

  @Test
  public void testCombineEmptyHistogramMoreNumBuckets()
  {
   VariableWidthHistogram h = buildEmptyHistogram(
     3
 );

   VariableWidthHistogram h2 = buildHistogram(
     5,
     new double[]{2, 3, 5, 7},
     new double[]{1, 1, 2, 2, 1},
     0,
     7,
     10,
     1
 );

   h.combineHistogram(h2);
   Assert.assertEquals(3, h.getNumBuckets());
   Assert.assertArrayEquals(new double[]{2, 3}, h.getBoundaries(), 0.01);
   Assert.assertArrayEquals(new double[]{1, 1, 5}, h.getCounts(), 0.01);
   Assert.assertEquals(0, h.getMissingValueCount());
   Assert.assertEquals(7, h.getCount(), 0.01);
   Assert.assertEquals(1, h.getMin(), 0.01);
   Assert.assertEquals(10, h.getMax(), 0.01);
  }

   @Test
   public void testMissing()
   {
    VariableWidthHistogram h = buildHistogram(
      6,
      new double[]{2, 5, 7, 10, 15},
      new double[]{1, 3, 2, 3, 5, 7},
      0,
      21,
      22,
      1
  );
    h.incrementMissing();
    h.incrementMissing();
    Assert.assertEquals(2, h.getMissingValueCount());

    VariableWidthHistogram h2 = buildHistogram(
      10,
      new double[]{2, 3, 5, 7, 8, 10, 12, 14, 19},
      new double[]{1, 1, 2, 2, 1, 2, 2, 2, 5, 3},
      0,
      21,
      22,
      1
  );
    h2.incrementMissing();
    h2.incrementMissing();
    h2.incrementMissing();

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getMissingValueCount());
   }

   @Test
  public void testSerdeHistogram()
  {
    VariableWidthHistogram h = buildHistogram(
       6,
       new double[]{4, 10, 12, 18, 20},
       new double[]{3, 1, 2, 2, 1, 10},
       0,
       15,
       30,
       1
    );
    byte[] bytes = h.toBytesProto();
    String asBase64 = h.toBase64Proto();

    VariableWidthHistogram fromBytes = VariableWidthHistogram.fromBytesProto(bytes);
    Assert.assertEquals(h, fromBytes);

    VariableWidthHistogram fromBase64 = VariableWidthHistogram.fromBase64Proto(asBase64);
    Assert.assertEquals(h, fromBase64);
  }

   @Test
   public void testObjectStrategyReadRetainsBufferReference()
   {
     VariableWidthHistogramSerde serde = new VariableWidthHistogramSerde();
     ObjectStrategy<?> strategy = serde.getObjectStrategy();
     Assert.assertFalse(strategy.readRetainsBufferReference());
   }

  @Test
  public void testDecodeBase64String() throws Exception
  {
    String base64String = "CAoQChpIAAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEAAAAAAAAAUQAAAAAAAABhAAAAAAAAAHEAAAAAAAAAgQAAAAAAAACJAIlAAAAAAAADwPwAAAAAAAABAAAAAAAAACEAAAAAAAAAQQAAAAAAAABRAAAAAAAAAGEAAAAAAAAAcQAAAAAAAACBAAAAAAAAAIkAAAAAAAAA+QDEAAAAAAMBSQDkAAAAAAABJQA==";
    
    // Decode the base64 string into a histogram
    VariableWidthHistogram histogram = VariableWidthHistogram.fromBase64Proto(base64String);
    
    // Print out the histogram details
    System.out.println("=== Decoded Histogram ===");
    System.out.println("Number of buckets: " + histogram.getNumBuckets());
    System.out.println("Total count: " + histogram.getCount());
    System.out.println("Min value: " + histogram.getMin());
    System.out.println("Max value: " + histogram.getMax());
    System.out.println("Missing value count: " + histogram.getMissingValueCount());
    
    System.out.println("\nBoundaries:");
    double[] boundaries = histogram.getBoundaries();
    for (int i = 0; i < boundaries.length; i++) {
      System.out.println("  Boundary[" + i + "]: " + boundaries[i]);
    }
    
    System.out.println("\nCounts:");
    double[] counts = histogram.getCounts();
    for (int i = 0; i < counts.length; i++) {
      System.out.println("  Count[" + i + "]: " + counts[i]);
    }
    
    System.out.println("\nHistogram as Map:");
    System.out.println(histogram.toMap());
    
    System.out.println("\nHistogram toString:");
    System.out.println(histogram.toString());
    
    // Basic assertions
    Assert.assertNotNull(histogram);
    Assert.assertEquals(10, histogram.getNumBuckets());
  }
 }
 