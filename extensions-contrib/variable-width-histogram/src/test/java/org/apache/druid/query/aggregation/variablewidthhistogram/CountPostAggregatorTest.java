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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class CountPostAggregatorTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws Exception
  {
    CountPostAggregator there =
        new CountPostAggregator("count", "test_field");

    DefaultObjectMapper mapper = new DefaultObjectMapper();
    CountPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        CountPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
    Assert.assertEquals(there.getDependentFields(), andBackAgain.getDependentFields());
  }

  @Test
  public void testComparator()
  {
    final String aggName = "doubleWithNulls";
    Map<String, Object> metricValues = new HashMap<>();

    CountPostAggregator count = new CountPostAggregator("count", aggName);
    Comparator<Object> comp = count.getComparator();
    
    // Empty histogram (count = 0)
    VariableWidthHistogram histo1 = new VariableWidthHistogram(10);
    metricValues.put(aggName, histo1);
    Object before = count.compute(metricValues);

    // Histogram with data (count = 5)
    VariableWidthHistogram histo2 = new VariableWidthHistogram(
        3,
        3,
        new double[]{4, 10},
        new double[]{2, 1, 2},
        0,
        5,
        12,
        3
    );
    metricValues.put(aggName, histo2);
    Object after = count.compute(metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg =
        new CountPostAggregator("count", "test_field");

    Assert.assertEquals(
        "CountPostAggregator{fieldName='test_field'}",
        postAgg.toString()
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(CountPostAggregator.class)
                  .withNonnullFields("name", "fieldName")
                  .usingGetClass()
                  .verify();
  }
}

