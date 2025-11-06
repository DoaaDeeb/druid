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

 import com.fasterxml.jackson.annotation.JsonCreator;
 import com.fasterxml.jackson.annotation.JsonProperty;
 import com.fasterxml.jackson.annotation.JsonTypeName;
 import org.apache.druid.java.util.common.StringUtils;
 import org.apache.druid.query.aggregation.AggregateCombiner;
 import org.apache.druid.query.aggregation.Aggregator;
 import org.apache.druid.query.aggregation.AggregatorFactory;
 import org.apache.druid.query.aggregation.AggregatorUtil;
 import org.apache.druid.query.aggregation.BufferAggregator;
 import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
 import org.apache.druid.query.cache.CacheKeyBuilder;
 import org.apache.druid.segment.ColumnInspector;
 import org.apache.druid.segment.ColumnSelectorFactory;
 import org.apache.druid.segment.ColumnValueSelector;
 import org.apache.druid.segment.column.ColumnCapabilities;
 import org.apache.druid.segment.column.ColumnType;
 
 import javax.annotation.Nullable;
 import java.util.Collections;
 import java.util.Comparator;
 import java.util.List;
 import java.util.Objects;
 
 @JsonTypeName(VariableWidthHistogramAggregator.TYPE_NAME)
 public class VariableWidthHistogramAggregatorFactory extends AggregatorFactory
 {
 
  private final String name;
  private final String fieldName;
  private final int numBuckets;
  private final boolean finalizeAsBase64Binary;
 
  @JsonCreator
  public VariableWidthHistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("finalizeAsBase64Binary") @Nullable Boolean finalizeAsBase64Binary
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.numBuckets = numBuckets;
    this.finalizeAsBase64Binary = finalizeAsBase64Binary == null ? false : finalizeAsBase64Binary;
  }
 
   @Override
   public Aggregator factorize(ColumnSelectorFactory metricFactory)
   {
     return new VariableWidthHistogramAggregator(
         metricFactory.makeColumnValueSelector(fieldName),
         numBuckets
     );
   }
 
   @Override
   public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
   {
     return new VariableWidthHistogramBufferAggregator(
         metricFactory.makeColumnValueSelector(fieldName),
         numBuckets
     );
   }
 
   @Override
   public boolean canVectorize(ColumnInspector columnInspector)
   {
     ColumnCapabilities capabilities = columnInspector.getColumnCapabilities(fieldName);
     return capabilities != null && capabilities.isNumeric();
   }
 
   @Override
   public Comparator getComparator()
   {
     return VariableWidthHistogramAggregator.COMPARATOR;
   }
 
   @Nullable
   @Override
   public Object combine(@Nullable Object lhs, @Nullable Object rhs)
   {
     if (lhs == null) {
       if (rhs == null) {
         return null;
       } else {
         return rhs;
       }
     } else {
       ((VariableWidthHistogram) lhs).combineHistogram((VariableWidthHistogram) rhs);
       return lhs;
     }
   }
 
   @Override
   public AggregateCombiner makeAggregateCombiner()
   {
     return new ObjectAggregateCombiner()
     {
       private final VariableWidthHistogram combined = new VariableWidthHistogram(numBuckets);
 
       @Override
       public void reset(ColumnValueSelector selector)
       {
         combined.reset();
         fold(selector);
       }
 
       @Override
       public void fold(ColumnValueSelector selector)
       {
         VariableWidthHistogram other = (VariableWidthHistogram) selector.getObject();
         combined.combineHistogram(other);
       }
 
       @Override
       public VariableWidthHistogram getObject()
       {
         return combined;
       }
 
       @Override
       public Class<VariableWidthHistogram> classOfObject()
       {
         return VariableWidthHistogram.class;
       }
     };
   }
 
   @Override
   public AggregatorFactory getCombiningFactory()
   {
     return new VariableWidthHistogramAggregatorFactory(
         name,
         name,
         numBuckets,
         finalizeAsBase64Binary
     );
   }
 
   @Override
   public AggregatorFactory getMergingFactory(AggregatorFactory other)
   {
     return new VariableWidthHistogramAggregatorFactory(
         name,
         name,
         numBuckets,
         finalizeAsBase64Binary
     );
   }
 
   @Override
   public Object deserialize(Object object)
   {
     if (object instanceof String) {
       byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8((String) object));
       final VariableWidthHistogram fbh = VariableWidthHistogram.fromBytes(bytes);
       return fbh;
     } else {
       return object;
     }
   }
 
   @Nullable
   @Override
   public Object finalizeComputation(@Nullable Object object)
   {
     if (object == null) {
       return null;
     }
 
     if (finalizeAsBase64Binary) {
       return object;
     } else {
       return object.toString();
     }
   }
 
   @JsonProperty
   @Override
   public String getName()
   {
     return name;
   }
 
   @Override
   public List<String> requiredFields()
   {
     return Collections.singletonList(fieldName);
   }
 
   /**
    * actual type is {@link VariableWidthHistogram}
    */
   @Override
   public ColumnType getIntermediateType()
   {
     return VariableWidthHistogramAggregator.TYPE;
   }
 
   /**
    * actual type is {@link VariableWidthHistogram} if {@link #finalizeAsBase64Binary} is set
    */
   @Override
   public ColumnType getResultType()
   {
     return finalizeAsBase64Binary ? VariableWidthHistogramAggregator.TYPE : ColumnType.STRING;
   }
 
  @Override
  public int getMaxIntermediateSize()
  {
    return VariableWidthHistogram.getStorageSize(numBuckets);
  }
 
   @Override
   public AggregatorFactory withName(String newName)
   {
     return new VariableWidthHistogramAggregatorFactory(
         newName,
         getFieldName(),
         getNumBuckets(),
         isFinalizeAsBase64Binary()
     );
   }
 
   @Override
   public byte[] getCacheKey()
   {
     final CacheKeyBuilder builder = new CacheKeyBuilder(AggregatorUtil.VARIABLE_WIDTH_HIST_CACHE_TYPE_ID)
         .appendString(fieldName)
         .appendBoolean(finalizeAsBase64Binary);
 
     return builder.build();
   }
 
   @JsonProperty
   public String getFieldName()
   {
     return fieldName;
   }

   @JsonProperty
   public int getNumBuckets()
   {
     return numBuckets;
   }

   @JsonProperty
   public boolean isFinalizeAsBase64Binary()
   {
     return finalizeAsBase64Binary;
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
     VariableWidthHistogramAggregatorFactory that = (VariableWidthHistogramAggregatorFactory) o;
     return Objects.equals(getName(), that.getName()) &&
            Objects.equals(getFieldName(), that.getFieldName()) &&
            isFinalizeAsBase64Binary() == that.isFinalizeAsBase64Binary();
   }
 
   @Override
   public int hashCode()
   {
     return Objects.hash(
         getName(),
         getFieldName(),
         isFinalizeAsBase64Binary()
     );
   }
 
   @Override
   public String toString()
   {
     return "VariableWidthHistogramAggregatorFactory{" +
            "name='" + name + '\'' +
            ", fieldName='" + fieldName + '\'' +
            ", finalizeAsBase64Binary=" + finalizeAsBase64Binary +
            '}';
   }
 }
 