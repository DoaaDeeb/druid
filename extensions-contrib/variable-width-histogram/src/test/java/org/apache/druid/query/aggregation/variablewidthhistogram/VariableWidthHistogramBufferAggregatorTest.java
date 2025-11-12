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

 import org.apache.druid.jackson.DefaultObjectMapper;
 import org.apache.druid.java.util.common.granularity.Granularities;
 import org.apache.druid.query.Druids;
 import org.apache.druid.query.aggregation.BufferAggregator;
 import org.apache.druid.query.aggregation.TestObjectColumnSelector;
 import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
 import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
 import org.apache.druid.query.timeseries.TimeseriesQuery;
 import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
 import org.apache.druid.segment.column.ColumnType;
 import org.apache.druid.segment.column.RowSignature;
 import org.junit.Assert;
 import org.junit.Test;
 
 import java.nio.ByteBuffer;
 
 public class VariableWidthHistogramBufferAggregatorTest
 {

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
   private void aggregateBuffer(TestObjectColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position)
   {
     agg.aggregate(buf, position);
     selector.increment();
   }
 
   @Test
   public void testBufferAggregate()
   {

    VariableWidthHistogram h1 = buildHistogram(
        5,
        new double[]{10, 20, 30, 40},
        new double[]{1, 2, 1, 1, 1},
        0,
        6,
        45,
        2
    );
    VariableWidthHistogram h2 = buildHistogram(
        5,
        new double[]{10, 20, 30, 40},
        new double[]{1, 1, 0, 2, 0},
        0,
        4,
        40,
        5
    );
    VariableWidthHistogram[] histograms = {h1, h2};

    final TestObjectColumnSelector<VariableWidthHistogram> selector = new TestObjectColumnSelector<>(histograms);
 
     VariableWidthHistogramAggregatorFactory factory = new VariableWidthHistogramAggregatorFactory(
         "test",
         "test",
         5,
         false
     );
 
     VariableWidthHistogramBufferAggregator agg = new VariableWidthHistogramBufferAggregator(
         selector,
         5
     );
 
     ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
     int position = 0;
 
     agg.init(buf, position);
     //noinspection ForLoopReplaceableByForEach
     for (int i = 0; i < histograms.length; i++) {
       aggregateBuffer(selector, agg, buf, position);
     }
 
     VariableWidthHistogram h = ((VariableWidthHistogram) agg.get(buf, position));
 
     Assert.assertEquals(5, h.getNumBuckets());
     Assert.assertArrayEquals(new double[]{10, 20, 30, 40}, h.getBoundaries(), 0.01);
     Assert.assertArrayEquals(new double[]{2, 3, 1, 3, 1}, h.getCounts(), 0.01);
     Assert.assertEquals(0, h.getMissingValueCount());
     Assert.assertEquals(10, h.getCount(), 0.01);
     Assert.assertEquals(2, h.getMin(), 0.01);
     Assert.assertEquals(45, h.getMax(), 0.01);
   }
 
   @Test
   public void testFinalize() throws Exception
   {
     DefaultObjectMapper objectMapper = new DefaultObjectMapper();
 
     VariableWidthHistogram h1 = buildHistogram(
        5,
        new double[]{10, 20, 30, 40},
        new double[]{1, 2, 1, 1, 1},
        0,
        6,
        45,
        2
    );
    VariableWidthHistogram h2 = buildHistogram(
        5,
        new double[]{10, 20, 30, 40},
        new double[]{1, 1, 0, 2, 0},
        0,
        4,
        40,
        5
    );
    VariableWidthHistogram[] histograms = {h1, h2};

    final TestObjectColumnSelector<VariableWidthHistogram> selector = new TestObjectColumnSelector<>(histograms);
 
     VariableWidthHistogramAggregatorFactory humanReadableFactory = new VariableWidthHistogramAggregatorFactory(
         "test",
         "test",
         5,
         false
     );
 
     VariableWidthHistogramAggregatorFactory binaryFactory = new VariableWidthHistogramAggregatorFactory(
         "test",
         "test",
         5,
         true
     );
 
     VariableWidthHistogramAggregator agg = new VariableWidthHistogramAggregator(
         selector,
         5
     );
     for (int i = 0; i < histograms.length; i++) {
       agg.aggregate();
       selector.increment();
     }
 
    Object finalizedObjectHumanReadable = humanReadableFactory.finalizeComputation(agg.get());
    String finalStringHumanReadable = objectMapper.writeValueAsString(finalizedObjectHumanReadable);
    Assert.assertEquals(
        "{\"numBuckets\":5,\"boundaries\":[10.0,20.0,30.0,40.0],\"counts\":[2.0,3.0,1.0,3.0,1.0],\"missingValueCount\":0,\"count\":10.0,\"max\":45.0,\"min\":2.0}",
        finalStringHumanReadable
    );

    Object finalizedObjectBinary = binaryFactory.finalizeComputation(agg.get());
    String finalStringBinary = objectMapper.writeValueAsString(finalizedObjectBinary);
    Assert.assertEquals(
        "\"QCQAAAAAAAAAAAAFAAAABQAAAAAAAAAAQEaAAAAAAABAAAAAAAAAAEAkAAAAAAAAQDQAAAAAAABAPgAAAAAAAEBEAAAAAAAAQAAAAAAAAABACAAAAAAAAD/wAAAAAAAAQAgAAAAAAAA/8AAAAAAAAA==\"",
        finalStringBinary
    );

   }
 
   @Test
   public void testResultArraySignature()
   {
     final TimeseriesQuery query =
         Druids.newTimeseriesQueryBuilder()
               .dataSource("test")
               .intervals("2000/3000")
               .granularity(Granularities.HOUR)
               .aggregators(
                   new VariableWidthHistogramAggregatorFactory("hist1", "col", 5, false),
                   new VariableWidthHistogramAggregatorFactory("hist2", "col", 5, true)
               )
               .postAggregators(
                   new FieldAccessPostAggregator("hist1-access", "hist1"),
                   new FinalizingFieldAccessPostAggregator("hist1-finalize", "hist1"),
                   new FieldAccessPostAggregator("hist2-access", "hist2"),
                   new FinalizingFieldAccessPostAggregator("hist2-finalize", "hist2")
               )
               .build();
 
     Assert.assertEquals(
         RowSignature.builder()
                     .addTimeColumn()
                     .add("hist1", null)
                     .add("hist2", VariableWidthHistogramAggregator.TYPE)
                     .add("hist1-access", VariableWidthHistogramAggregator.TYPE)
                     .add("hist1-finalize", ColumnType.STRING)
                     .add("hist2-access", VariableWidthHistogramAggregator.TYPE)
                     .add("hist2-finalize", VariableWidthHistogramAggregator.TYPE)
                     .build(),
         new TimeseriesQueryQueryToolChest().resultArraySignature(query)
     );
   }
 
   @Test
   public void testWithName()
   {
     VariableWidthHistogramAggregatorFactory factory = new VariableWidthHistogramAggregatorFactory(
         "test",
         "test",
         5,
         false
     );
     Assert.assertEquals(factory, factory.withName("test"));
     Assert.assertEquals("newTest", factory.withName("newTest").getName());
   }
 }
 