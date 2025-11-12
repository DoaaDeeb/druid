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

import com.google.common.collect.Ordering;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class VariableWidthHistogramSerde extends ComplexMetricSerde
{
  private static final Logger log = new Logger(VariableWidthHistogramSerde.class);
  
  private static Ordering<VariableWidthHistogram> comparator = new Ordering<VariableWidthHistogram>()
   {
     @Override
     public int compare(
         VariableWidthHistogram arg1,
         VariableWidthHistogram arg2
     )
     {
       return VariableWidthHistogramAggregator.COMPARATOR.compare(arg1, arg2);
     }
   }.nullsFirst();
 
   @Override
   public String getTypeName()
   {
     return VariableWidthHistogramAggregator.TYPE_NAME;
   }
 
   @Override
   public ComplexMetricExtractor getExtractor()
   {
     return new ComplexMetricExtractor()
     {
       @Override
       public Class<VariableWidthHistogram> extractedClass()
       {
         return VariableWidthHistogram.class;
       }
 
       @Nullable
       @Override
       public Object extractValue(InputRow inputRow, String metricName)
       {
         throw new UnsupportedOperationException("extractValue without an aggregator factory is not supported.");
       }
 
      @Override
      public VariableWidthHistogram extractValue(InputRow inputRow, String metricName, AggregatorFactory agg)
      {
        Object rawValue = inputRow.getRaw(metricName);

        VariableWidthHistogramAggregatorFactory aggregatorFactory = (VariableWidthHistogramAggregatorFactory) agg;

        if (rawValue == null) {
          log.info("[VWH-SERDE] extractValue: rawValue is null for metric=%s, creating empty histogram", metricName);
          VariableWidthHistogram vwh = new VariableWidthHistogram(aggregatorFactory.getMaxNumBuckets());
          vwh.incrementMissing();
          return vwh;
        } else if (rawValue instanceof VariableWidthHistogram) {
          log.info("[VWH-SERDE] extractValue: rawValue is already VariableWidthHistogram for metric=%s, count=%s", 
                   metricName, ((VariableWidthHistogram) rawValue).getCount());
          return (VariableWidthHistogram) rawValue;
        } else if (rawValue instanceof String) {
          try {
           String base64Str = (String) rawValue;
           log.info("[VWH-SERDE] extractValue: parsing base64 string for metric=%s, length=%d, preview=%s", 
                    metricName, base64Str.length(), base64Str.substring(0, Math.min(50, base64Str.length())));
           VariableWidthHistogram vwh = VariableWidthHistogram.fromBase64(base64Str);
           log.info("[VWH-SERDE] extractValue: parsed histogram for metric=%s, numBuckets=%d, count=%s, min=%s, max=%s", 
                    metricName, vwh.getNumBuckets(), vwh.getCount(), vwh.getMin(), vwh.getMax());
           return vwh;
         } catch (ParseException pe) {
           throw new ISE("Failed to parse histogram from base64 string: %s", rawValue);
         }
       } else {
         log.warn("[VWH-SERDE] extractValue: Unknown type for metric=%s: %s", metricName, rawValue.getClass());
         throw new UnsupportedOperationException("Unknown type: " + rawValue.getClass());
       }
     }
    };
  }
 
   @Override
   public ObjectStrategy getObjectStrategy()
   {
     return new ObjectStrategy<VariableWidthHistogram>()
     {
       @Override
       public Class<? extends VariableWidthHistogram> getClazz()
       {
         return VariableWidthHistogram.class;
       }
 
       @Override
       public VariableWidthHistogram fromByteBuffer(ByteBuffer buffer, int numBytes)
       {
         buffer.limit(buffer.position() + numBytes);
         VariableWidthHistogram vwh = VariableWidthHistogram.fromByteBuffer(buffer);
         return vwh;
       }
 
       @Override
       public byte[] toBytes(VariableWidthHistogram h)
       {
         if (h == null) {
           return new byte[]{};
         }
 
         return h.toBytes();
       }
 
       @Override
       public int compare(VariableWidthHistogram o1, VariableWidthHistogram o2)
       {
         return comparator.compare(o1, o2);
       }
 
       @Override
       public boolean readRetainsBufferReference()
       {
         return false;
       }
     };
   }
 }
 