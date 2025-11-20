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

import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedRow;

import java.util.Map;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
 
import java.io.ByteArrayInputStream;
 import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
 import java.util.ArrayList;
 import java.util.Collection;
 import java.util.List;
 
 /**
  *
  */
@RunWith(Parameterized.class)
public class VariableWidthHistogramAggregationTest extends InitializedNullHandlingTest
{
  private AggregationTestHelper groupByHelper;
  private AggregationTestHelper topNHelper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public VariableWidthHistogramAggregationTest(final GroupByQueryConfig config)
  {
    VariableWidthHistogramDruidModule.registerSerde();
    groupByHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(new VariableWidthHistogramDruidModule().getJacksonModules()),
        config,
        tempFolder
    );
    topNHelper = AggregationTestHelper.createTopNQueryAggregationTestHelper(
        Lists.newArrayList(new VariableWidthHistogramDruidModule().getJacksonModules()),
        tempFolder
    );
  }
 
 @Parameterized.Parameters(name = "{0}")
 public static Collection<?> constructorFeeder()
 {
   final List<Object[]> constructors = new ArrayList<>();
   for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
     constructors.add(new Object[]{config});
   }
   return constructors;
 }
 
  @After
  public void teardown() throws IOException
  {
    groupByHelper.close();
    topNHelper.close();
  }
 
  @Test
  public void testIngestWithNullsIgnoredAndQuery() throws Exception
  {
    MapBasedRow row = ingestAndQuery();
    Object rawResult = row.getRaw("index_vwh");
    
    Assert.assertNotNull("Result should not be null", rawResult);
    Assert.assertTrue("Result should be a Map", rawResult instanceof Map);
    
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) rawResult;
    Assert.assertEquals(10, result.get("numBuckets"));
    Assert.assertEquals(81.0, (Double) result.get("count"), 0.01);
    Assert.assertEquals(200.0, (Double) result.get("max"), 0.01);
    Assert.assertEquals(0.0, (Double) result.get("min"), 0.01);
    Assert.assertEquals(2L, result.get("missingValueCount"));
    
    // Assert boundaries
    double[] boundaries = (double[]) result.get("boundaries");
    Assert.assertNotNull("boundaries should not be null", boundaries);
    Assert.assertArrayEquals(new double[]{10, 20, 30, 40, 50, 60, 70, 80, 90}, boundaries, 0.01);
    
    // Assert counts
    double[] counts = (double[]) result.get("counts");
    Assert.assertNotNull("counts should not be null", counts);
    Assert.assertArrayEquals(new double[]{4.0, 6.0, 9.0, 12.0, 15.0, 12.0, 9.0, 6.0, 4.0, 4.0}, counts, 0.01);
  }
 
   private MapBasedRow ingestAndQuery() throws Exception
   {
     String ingestionAgg = VariableWidthHistogramAggregator.TYPE_NAME;
 
     String metricSpec = "[{"
                         + "\"type\": \"" + ingestionAgg + "\","
                         + "\"name\": \"index_vwh\","
                        + "\"fieldName\": \"index_vwh\","
                        + "\"maxNumBuckets\": 10"
                         + "}]";
 
     String parseSpec = "{"
                        + "\"type\" : \"string\","
                        + "\"parseSpec\" : {"
                        + "    \"format\" : \"json\","
                        + "    \"timestampSpec\" : {"
                        + "        \"column\" : \"timestamp\","
                        + "        \"format\" : \"auto\""
                        + "},"
                        + "    \"dimensionsSpec\" : {"
                        + "        \"dimensions\": [],"
                       + "        \"dimensionExclusions\" : [\"index_vwh\"],"
                        + "        \"spatialDimensions\" : []"
                        + "    },"
                       + "    \"columns\": [\"timestamp\", \"d1\", \"d2\", \"d3\", \"d4\", \"index_vwh\"]"
                        + "  }"
                        + "}";
 
     String query = "{"
                    + "\"queryType\": \"groupBy\","
                    + "\"dataSource\": \"test_datasource\","
                    + "\"granularity\": \"ALL\","
                    + "\"dimensions\": [],"
                    + "\"aggregations\": ["
                    + "  {"
                   + "   \"type\": \"variableWidthHistogram\","
                    + "   \"name\": \"index_vwh\","
                    + "   \"fieldName\": \"index_vwh\","
                    + "   \"maxNumBuckets\": 10"
                    + "  }"
                    + "],"
                    + "\"intervals\": [ \"1970/2050\" ]"
                    + "}";
 
    // Create test data with pre-computed histograms
    String[][] dimensionValues = {
        {"val1", "val2", "val3", "val4"},
        {"val5", "val6", "val7", "val8"},
        {"val9", "val10", "val11", "val12"}
    };

    double[][][] histogramData = {
        // First histogram: boundaries, counts, [missingCount, totalCount, max, min]
        // 10 buckets = 9 boundaries + 10 counts
        {
            {10, 20, 30, 40, 50, 60, 70, 80, 90},     // 9 boundaries
            {1, 2, 3, 4, 5, 4, 3, 2, 1, 1},           // 10 counts
            {0, 26, 100, 0}                            // metadata
        },
        // Second histogram (same boundaries)
        {
            {10, 20, 30, 40, 50, 60, 70, 80, 90, 100},   // 10 boundaries
            {2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 1},           // 11 counts
            {0, 36, 200, 0}                              // metadata
        },
        // Third histogram (same boundaries)
        {
            {10, 20, 30, 40, 50, 60, 70, 80, 90},     // 9 boundaries
            {1, 1, 2, 3, 4, 3, 2, 1, 1, 1},           // 10 counts
            {0, 19, 100, 0}                            // metadata
        }
    };

    String[][] nullHistogramCountDimensionValues = {
        {"val1", "val2", "val3", "val4"},
        {"val13", "val14", "val15", "val16"},
    };

     Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        createTestDataStream(dimensionValues, histogramData, nullHistogramCountDimensionValues),
         parseSpec,
         metricSpec,
         0,
         Granularities.NONE,
         50000,
         query
     );
 
     return seq.toList().get(0).toMapBasedRow((GroupByQuery) groupByHelper.readQuery(query));
   }

  /**
   * Creates an InputStream of JSON data with pre-computed histograms for testing.
   * 
   * @param dimensionValues Array of dimension values for each row [row][d1, d2, d3, d4]
   * @param histogramData Array of histogram data for each row [row][boundaries, counts, metadata]
   * @return InputStream containing JSONL formatted data
   */
  private InputStream createTestDataStream(
      String[][] dimensionValues,
      double[][][] histogramData,
      String[][] nullHistogramCountDimensionValues
  ) {
    StringBuilder jsonData = new StringBuilder();
    String timestamp = "2011-04-15T00:00:00.000Z";
    
    for (int i = 0; i < dimensionValues.length; i++) {
      String[] dims = dimensionValues[i];
      
      // Create histogram programmatically
      VariableWidthHistogram histogram = createHistogram(histogramData[i]);
      String base64Histogram = histogram.toBase64Proto();
      
      jsonData.append(StringUtils.format(
          "{\"timestamp\":\"%s\",\"d1\":\"%s\",\"d2\":\"%s\",\"d3\":\"%s\",\"d4\":\"%s\",\"index_vwh\":\"%s\"}\n",
          timestamp,
          dims[0], dims[1], dims[2], dims[3],
          base64Histogram
      ));
    }

    for (int i = 0; i < nullHistogramCountDimensionValues.length; i++) {
      String[] dims = nullHistogramCountDimensionValues[i];
      jsonData.append(StringUtils.format(
          "{\"timestamp\":\"%s\",\"d1\":\"%s\",\"d2\":\"%s\",\"d3\":\"%s\",\"d4\":\"%s\"}\n",
        timestamp,
        dims[0], dims[1], dims[2], dims[3]
      ));
    }
    
    return new ByteArrayInputStream(jsonData.toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Creates a VariableWidthHistogram from structured data.
   * 
   * @param data [0] = boundaries array, [1] = counts array, [2] = [missingValueCount, count, max, min]
   * @return VariableWidthHistogram instance
   */
  private VariableWidthHistogram createHistogram(double[][] data) {
    int numBuckets = data[1].length;
    double[] boundaries = data[0];
    double[] counts = data[1];
    long missingValueCount = (long) data[2][0];
    double count = data[2][1];
    double max = data[2][2];
    double min = data[2][3];
    
    return buildHistogram(numBuckets, boundaries, counts, missingValueCount, count, max, min);
  }

  /**
   * Helper to build a VariableWidthHistogram for testing.
   */
  private VariableWidthHistogram buildHistogram(
      int numBuckets,
      double[] boundaries,
      double[] counts,
      long missingValueCount,
      double count,
      double max,
      double min
  ) {
    return new VariableWidthHistogram(
        numBuckets,
        numBuckets,
        boundaries,
        counts,
        missingValueCount,
        count,
        max,
        min
    );
  }

  @Test
  public void testTopNQueryWithVariableWidthHistogram() throws Exception
  {
    String metricSpec = "[{"
                        + "\"type\": \"" + VariableWidthHistogramAggregator.TYPE_NAME + "\","
                        + "\"name\": \"index_vwh\","
                        + "\"fieldName\": \"index_vwh\","
                        + "\"maxNumBuckets\": 10"
                        + "}]";

    String parseSpec = "{"
                       + "\"type\" : \"string\","
                       + "\"parseSpec\" : {"
                       + "    \"format\" : \"json\","
                       + "    \"timestampSpec\" : {"
                       + "        \"column\" : \"timestamp\","
                       + "        \"format\" : \"auto\""
                       + "},"
                       + "    \"dimensionsSpec\" : {"
                       + "        \"dimensions\": [\"d1\", \"d2\", \"d3\", \"d4\"],"
                       + "        \"dimensionExclusions\" : [\"index_vwh\"],"
                       + "        \"spatialDimensions\" : []"
                       + "    },"
                       + "    \"columns\": [\"timestamp\", \"d1\", \"d2\", \"d3\", \"d4\", \"index_vwh\"]"
                       + "  }"
                       + "}";

    String query = "{"
                   + "\"queryType\": \"topN\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"granularity\": \"ALL\","
                   + "\"dimension\": \"d1\","
                   + "\"metric\": \"index_vwh\","
                   + "\"threshold\": 3,"
                   + "\"aggregations\": ["
                   + "  {"
                   + "   \"type\": \"variableWidthHistogram\","
                   + "   \"name\": \"index_vwh\","
                   + "   \"fieldName\": \"index_vwh\","
                   + "   \"maxNumBuckets\": 10"
                   + "  }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    // Create test data with different dimension values
    String[][] dimensionValues = {
        {"dimension1", "val2", "val3", "val4"},
        {"dimension1", "val6", "val7", "val8"},
        {"dimension1", "val10", "val11", "val12"}
    };

    double[][][] histogramData = {
        // First histogram for dimension1
        {
            {10, 20, 30, 40, 50, 60, 70, 80, 90},
            {1, 2, 3, 4, 5, 4, 3, 2, 1, 1},
            {0, 26, 100, 0}
        },
        // Second histogram for dimension1 (will be merged with first)
        {
            {10, 20, 30, 40, 50, 60, 70, 80, 90},
            {1, 1, 2, 3, 4, 3, 2, 1, 1, 1},
            {0, 19, 90, 5}
        },
        // Histogram for dimension2
        {
            {10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
            {2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 1},
            {0, 36, 200, 0}
        }
    };

    String[][] nullHistogramCountDimensionValues = {
        {"dimension1", "val14", "val15", "val16"}
    };

    Sequence<Result<TopNResultValue>> resultSeq = topNHelper.createIndexAndRunQueryOnSegment(
        createTestDataStream(dimensionValues, histogramData, nullHistogramCountDimensionValues),
        parseSpec,
        metricSpec,
        0,
        Granularities.NONE,
        50000,
        query
    );

    List<Result<TopNResultValue>> results = resultSeq.toList();
    Assert.assertNotNull("Results should not be null", results);
    Assert.assertFalse("Results should not be empty", results.isEmpty());

    Result<TopNResultValue> result = results.get(0);
    List<org.apache.druid.query.topn.DimensionAndMetricValueExtractor> topNValues = result.getValue().getValue();
    
    Assert.assertNotNull("TopN values should not be null", topNValues);
    Assert.assertTrue("Should have at least one result", topNValues.size() >= 1);
    
    // Verify that the first result has a histogram
    org.apache.druid.query.topn.DimensionAndMetricValueExtractor firstResult = topNValues.get(0);
    Assert.assertNotNull("First result should have d1 dimension", firstResult.getDimensionValue("d1"));
    
    Object histResult = firstResult.getMetric("index_vwh");
    Assert.assertNotNull("Histogram result should not be null", histResult);
    
    // The result should be a Map representation of the histogram
    Assert.assertTrue("Histogram result should be a Map", histResult instanceof Map);
    @SuppressWarnings("unchecked")
    Map<String, Object> histResultMap = (Map<String, Object>) histResult;
    Assert.assertEquals(10, histResultMap.get("numBuckets"));
    Assert.assertEquals(81.0, (Double) histResultMap.get("count"), 0.01);
    Assert.assertEquals(200.0, (Double) histResultMap.get("max"), 0.01);
    Assert.assertEquals(0.0, (Double) histResultMap.get("min"), 0.01);
    Assert.assertEquals(1L, histResultMap.get("missingValueCount"));
    
    // Assert boundaries
    double[] boundaries = (double[]) histResultMap.get("boundaries");
    Assert.assertNotNull("boundaries should not be null", boundaries);
    Assert.assertArrayEquals(new double[]{10, 20, 30, 40, 50, 60, 70, 80, 90}, boundaries, 0.01);
    
    // Assert counts
    double[] counts = (double[]) histResultMap.get("counts");
    Assert.assertNotNull("counts should not be null", counts);
    Assert.assertArrayEquals(new double[]{4.0, 6.0, 9.0, 12.0, 15.0, 12.0, 9.0, 6.0, 4.0, 4.0}, counts, 0.01);
  }
}
 