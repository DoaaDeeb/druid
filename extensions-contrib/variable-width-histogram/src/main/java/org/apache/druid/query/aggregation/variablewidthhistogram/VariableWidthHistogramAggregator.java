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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;

public class VariableWidthHistogramAggregator implements Aggregator
{
  private static final Logger log = new Logger(VariableWidthHistogramAggregator.class);
  
  public static final String TYPE_NAME = "variableWidthHistogram";
  public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);
 
   public static final Comparator COMPARATOR = new Comparator()
   {
     @Override
     public int compare(Object o, Object o1)
     {
       double count1 = ((VariableWidthHistogram) o).getCount();
       double count2 = ((VariableWidthHistogram) o1).getCount();
       return Double.compare(count1, count2);
     }
   };

  private final BaseObjectColumnValueSelector selector;
 
  private VariableWidthHistogram histogram;

  public VariableWidthHistogramAggregator(
      BaseObjectColumnValueSelector selector,
      int maxNumBuckets
  )
   {
     this.selector = selector;
     this.histogram = new VariableWidthHistogram(maxNumBuckets);
   }
 
  @Override
  public void aggregate()
  {
    Object val = selector.getObject();
    
    if (val == null) {
      log.info("[VWH-AGG] aggregate: selector returned null");
    } else if (val instanceof VariableWidthHistogram) {
      VariableWidthHistogram vwh = (VariableWidthHistogram) val;
      log.info("[VWH-AGG] aggregate: got VariableWidthHistogram[numBuckets=%d, count=%s, min=%s, max=%s]",
               vwh.getNumBuckets(), vwh.getCount(), vwh.getMin(), vwh.getMax());
    } else {
      log.info("[VWH-AGG] aggregate: got unexpected type: %s, value=%s", val.getClass(), val);
    }
    
    histogram.combine(val);
    
    log.info("[VWH-AGG] aggregate: after combine, histogram[count=%s, min=%s, max=%s]",
             histogram.getCount(), histogram.getMin(), histogram.getMax());
  }
 
   @Nullable
   @Override
   public Object get()
   {
     return histogram;
   }
 
   @Override
   public float getFloat()
   {
     throw new UnsupportedOperationException("VariableWidthHistogramAggregator does not support getFloat()");
   }
 
   @Override
   public long getLong()
   {
     throw new UnsupportedOperationException("VariableWidthHistogramAggregator does not support getLong()");
   }
 
   @Override
   public double getDouble()
   {
     throw new UnsupportedOperationException("VariableWidthHistogramAggregator does not support getDouble()");
   }
 
   @Override
   public boolean isNull()
   {
     return false;
   }
 
   @Override
   public void close()
   {
 
   }
 }
 