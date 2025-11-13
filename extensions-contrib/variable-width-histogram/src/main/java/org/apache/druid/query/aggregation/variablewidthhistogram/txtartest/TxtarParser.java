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

package org.apache.druid.query.aggregation.variablewidthhistogram.txtartest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal parser for the txtar archive format.
 * 
 * Based on golang.org/x/tools/txtar
 * 
 * The txtar format is:
 * - Zero or more comment lines
 * - A sequence of file entries, each starting with "-- FILENAME --"
 * - File content follows until the next file marker
 */
public class TxtarParser
{
  private static final String MARKER = "-- ";
  private static final String MARKER_END = " --";
  private static final String NEWLINE_MARKER = "\n-- ";

  /**
   * An Archive is a collection of files.
   */
  public static class Archive
  {
    private final String comment;
    private final List<File> files;

    public Archive(String comment, List<File> files)
    {
      this.comment = comment;
      this.files = files;
    }

    public String getComment()
    {
      return comment;
    }

    public List<File> getFiles()
    {
      return files;
    }

    public File getFile(String name)
    {
      for (File f : files) {
        if (f.getName().equals(name)) {
          return f;
        }
      }
      return null;
    }
  }

  /**
   * A File is a single file in an archive.
   */
  public static class File
  {
    private final String name;
    private final byte[] data;

    public File(String name, byte[] data)
    {
      this.name = name;
      this.data = data;
    }

    public String getName()
    {
      return name;
    }

    public byte[] getData()
    {
      return data;
    }

    public String getDataAsString()
    {
      return new String(data, StandardCharsets.UTF_8);
    }
  }

  /**
   * Parse a txtar file from the given path.
   */
  public static Archive parseFile(Path path) throws IOException
  {
    byte[] data = Files.readAllBytes(path);
    return parse(data);
  }

  /**
   * Parse a txtar archive from byte data.
   */
  public static Archive parse(byte[] data)
  {
    List<File> files = new ArrayList<>();
    
    MarkerResult result = findFileMarker(data, 0);
    String comment = new String(result.before, StandardCharsets.UTF_8);
    
    while (result.name != null && !result.name.isEmpty()) {
      MarkerResult nextResult = findFileMarker(data, result.afterStart);
      files.add(new File(result.name, nextResult.before));
      result = nextResult;
    }
    
    return new Archive(comment, files);
  }

  private static class MarkerResult
  {
    final byte[] before;
    final String name;
    final int afterStart;

    MarkerResult(byte[] before, String name, int afterStart)
    {
      this.before = before;
      this.name = name;
      this.afterStart = afterStart;
    }
  }

  /**
   * Find the next file marker in data starting at offset.
   * Returns the data before the marker, the file name, and the position after the marker.
   */
  private static MarkerResult findFileMarker(byte[] data, int offset)
  {
    int i = offset;
    
    while (i < data.length) {
      // Check if we're at a marker
      MarkerCheck check = isMarker(data, i);
      if (check.name != null && !check.name.isEmpty()) {
        // Extract data before marker
        byte[] before = new byte[i - offset];
        System.arraycopy(data, offset, before, 0, before.length);
        return new MarkerResult(before, check.name, check.afterStart);
      }
      
      // Look for next potential marker (newline followed by "-- ")
      int nextNewline = indexOf(data, (byte) '\n', i);
      if (nextNewline < 0) {
        // No more markers, return remaining data
        byte[] remaining = new byte[data.length - offset];
        System.arraycopy(data, offset, remaining, 0, remaining.length);
        return new MarkerResult(fixNL(remaining), "", data.length);
      }
      
      i = nextNewline + 1; // Move past the newline
    }
    
    // No marker found, return all remaining data
    byte[] remaining = new byte[data.length - offset];
    System.arraycopy(data, offset, remaining, 0, remaining.length);
    return new MarkerResult(fixNL(remaining), "", data.length);
  }

  private static class MarkerCheck
  {
    final String name;
    final int afterStart;

    MarkerCheck(String name, int afterStart)
    {
      this.name = name;
      this.afterStart = afterStart;
    }
  }

  /**
   * Check if data at offset begins with a file marker line.
   */
  private static MarkerCheck isMarker(byte[] data, int offset)
  {
    // Check for "-- " prefix
    if (!startsWith(data, offset, MARKER.getBytes(StandardCharsets.UTF_8))) {
      return new MarkerCheck("", offset);
    }
    
    // Find end of line
    int lineEnd = indexOf(data, (byte) '\n', offset);
    int afterStart;
    byte[] line;
    
    if (lineEnd >= 0) {
      line = new byte[lineEnd - offset];
      System.arraycopy(data, offset, line, 0, line.length);
      afterStart = lineEnd + 1;
    } else {
      line = new byte[data.length - offset];
      System.arraycopy(data, offset, line, 0, line.length);
      afterStart = data.length;
    }
    
    // Check for " --" suffix
    if (!endsWith(line, MARKER_END.getBytes(StandardCharsets.UTF_8)) ||
        line.length < MARKER.length() + MARKER_END.length()) {
      return new MarkerCheck("", offset);
    }
    
    // Extract and trim filename
    int nameStart = MARKER.length();
    int nameEnd = line.length - MARKER_END.length();
    String name = new String(line, nameStart, nameEnd - nameStart, StandardCharsets.UTF_8).trim();
    
    return new MarkerCheck(name, afterStart);
  }

  /**
   * If data is empty or ends in newline, return data.
   * Otherwise return data with a final newline added.
   */
  private static byte[] fixNL(byte[] data)
  {
    if (data.length == 0 || data[data.length - 1] == '\n') {
      return data;
    }
    byte[] result = new byte[data.length + 1];
    System.arraycopy(data, 0, result, 0, data.length);
    result[data.length] = '\n';
    return result;
  }

  private static boolean startsWith(byte[] data, int offset, byte[] prefix)
  {
    if (offset + prefix.length > data.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (data[offset + i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  private static boolean endsWith(byte[] data, byte[] suffix)
  {
    if (suffix.length > data.length) {
      return false;
    }
    int offset = data.length - suffix.length;
    for (int i = 0; i < suffix.length; i++) {
      if (data[offset + i] != suffix[i]) {
        return false;
      }
    }
    return true;
  }

  private static int indexOf(byte[] data, byte b, int fromIndex)
  {
    for (int i = fromIndex; i < data.length; i++) {
      if (data[i] == b) {
        return i;
      }
    }
    return -1;
  }
}

