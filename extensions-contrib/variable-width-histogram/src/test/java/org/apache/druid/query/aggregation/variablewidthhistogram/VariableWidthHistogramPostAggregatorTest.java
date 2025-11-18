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

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VariableWidthHistogramPostAggregatorTest extends InitializedNullHandlingTest
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

  @Test
  public void testCountPostAggregatorCompute()
  {
    VariableWidthHistogram vwh = buildHistogram(
        6,
        new double[]{4, 10, 12, 18, 20},
        new double[]{3, 1, 2, 2, 1, 10},
        0,
        19,
        29,
        3
    );

    Map<String, Object> metricValues = new HashMap<>();
    metricValues.put("price", vwh);

    CountPostAggregator countPostAggregator = new CountPostAggregator(
        "count",
        "price"
    );
    
    Assert.assertEquals(19.0, ((Number) countPostAggregator.compute(metricValues)).doubleValue(), 0.01);
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(new VariableWidthHistogramAggregatorFactory("vwHisto", "col", 10, false))
              .postAggregators(
                  new CountPostAggregator("count", "vwHisto")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("vwHisto", null)
                    .add("count", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}

 