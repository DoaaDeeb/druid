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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.aggregation.variablewidthhistogram.VariableWidthHistogram;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone test program for validating interoperability between Go and Java implementations
 * of VariableWidthHistogram serialization.
 * 
 * <p>This program reads .txtar test files from a Go repository and validates that the Java
 * implementation produces identical binary serialization (base64-encoded) as the Go implementation.
 * This ensures data compatibility when histograms are marshaled/unmarshaled across systems using
 * both implementations.</p>
 * 
 * <p><b>Background:</b> VariableWidthHistogram is implemented in both:
 * <ul>
 *   <li>Java: Apache Druid's variable-width-histogram extension (this codebase)</li>
 *   <li>Go: Augtera's variablewidthhistogram package (separate repository)</li>
 * </ul>
 * Both implementations must produce identical binary serialization for interoperability.</p>
 * 
 * <p><b>Test Methodology:</b>
 * <ol>
 *   <li>Reads .txtar test files from the Go repository's test data directory</li>
 *   <li>Each .txtar file contains:
 *     <ul>
 *       <li>histogram.data - JSON representation of a histogram</li>
 *       <li>base64.expected - Expected base64-encoded binary serialization (from Go)</li>
 *     </ul>
 *   </li>
 *   <li>Deserializes JSON into Java VariableWidthHistogram</li>
 *   <li>Marshals to base64 using Java's toBase64() method</li>
 *   <li>Compares with expected output from Go implementation</li>
 *   <li>Reports PASS/FAIL for each test case</li>
 * </ol>
 * </p>
 * 
 * <p><b>Usage:</b></p>
 * <pre>
 *   # With default directory (requires access to Go repository)
 *   java -jar druid-variable-width-histogram-jar-with-dependencies.jar
 *   
 *   # With custom directory
 *   java -jar druid-variable-width-histogram-jar-with-dependencies.jar -d /path/to/txtar/files
 * </pre>
 * 
 * <p>Exit code 0 indicates all tests passed, 1 indicates failures.</p>
 * 
 * @see VariableWidthHistogram
 * @see TxtarParser
 */
public class VariableWidthHistogramTxtarTest
{
  private static final String DEFAULT_TXTAR_DIR = 
      System.getProperty("user.home") + "/dev/mercury/common/go/src/augtera/variablewidthhistogram/testdata/variablehistomarshal/";
  
  private static final String HISTOGRAM_DATA_FILE = "histogram.data";
  private static final String BASE64_EXPECTED_FILE = "base64.expected";

  public static void main(String[] args)
  {
    String txtarDir = DEFAULT_TXTAR_DIR;
    
    // Parse command line arguments
    for (int i = 0; i < args.length; i++) {
      if ("-d".equals(args[i]) && i + 1 < args.length) {
        txtarDir = args[i + 1];
        i++; // Skip next argument
      } else if ("-h".equals(args[i]) || "--help".equals(args[i])) {
        printUsage();
        System.exit(0);
      }
    }
    
    System.out.println("VariableWidthHistogram Interoperability Test");
    System.out.println("=============================================");
    System.out.println("Testing Go <-> Java serialization compatibility");
    System.out.println("Reading test files from: " + txtarDir);
    System.out.println();
    
    Path txtarPath = Paths.get(txtarDir);
    if (!Files.exists(txtarPath)) {
      System.err.println("Error: Directory does not exist: " + txtarDir);
      System.exit(1);
    }
    
    if (!Files.isDirectory(txtarPath)) {
      System.err.println("Error: Not a directory: " + txtarDir);
      System.exit(1);
    }
    
    List<Path> txtarFiles = findTxtarFiles(txtarPath);
    if (txtarFiles.isEmpty()) {
      System.err.println("Error: No .txtar files found in: " + txtarDir);
      System.exit(1);
    }
    
    int passed = 0;
    int failed = 0;
    
    for (Path txtarFile : txtarFiles) {
      String testName = txtarFile.getFileName().toString();
      if (testName.endsWith(".txtar")) {
        testName = testName.substring(0, testName.length() - 6);
      }
      
      try {
        boolean success = runTest(testName, txtarFile);
        if (success) {
          passed++;
        } else {
          failed++;
        }
      } catch (Exception e) {
        System.out.println("FAIL: " + testName);
        System.out.println("  Error: " + e.getMessage());
        e.printStackTrace(System.out);
        System.out.println();
        failed++;
      }
    }
    
    System.out.println("=============================================");
    System.out.println("Results: " + passed + " passed, " + failed + " failed");
    
    if (failed > 0) {
      System.exit(1);
    }
  }
  
  private static void printUsage()
  {
    System.out.println("VariableWidthHistogram Interoperability Test");
    System.out.println();
    System.out.println("Tests that Java and Go implementations produce identical serialization.");
    System.out.println();
    System.out.println("Usage: java -jar druid-variable-width-histogram-jar-with-dependencies.jar [OPTIONS]");
    System.out.println();
    System.out.println("Options:");
    System.out.println("  -d <directory>  Directory containing .txtar test files");
    System.out.println("                  Default: " + DEFAULT_TXTAR_DIR);
    System.out.println("  -h, --help      Show this help message");
  }
  
  private static List<Path> findTxtarFiles(Path directory)
  {
    List<Path> txtarFiles = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.txtar")) {
      for (Path entry : stream) {
        if (Files.isRegularFile(entry)) {
          txtarFiles.add(entry);
        }
      }
    } catch (IOException e) {
      System.err.println("Error reading directory: " + e.getMessage());
    }
    return txtarFiles;
  }
  
  private static boolean runTest(String testName, Path txtarFile) throws Exception
  {
    System.out.println("Running: " + testName);
    
    // Parse txtar file
    TxtarParser.Archive archive = TxtarParser.parseFile(txtarFile);
    
    // Extract required files
    TxtarParser.File histogramDataFile = archive.getFile(HISTOGRAM_DATA_FILE);
    TxtarParser.File base64ExpectedFile = archive.getFile(BASE64_EXPECTED_FILE);
    
    if (histogramDataFile == null) {
      throw new RuntimeException("Missing required file: " + HISTOGRAM_DATA_FILE);
    }
    
    if (base64ExpectedFile == null) {
      throw new RuntimeException("Missing required file: " + BASE64_EXPECTED_FILE);
    }
    
    // Deserialize histogram data from JSON
    ObjectMapper mapper = new ObjectMapper();
    HistogramData histogramData = mapper.readValue(histogramDataFile.getData(), HistogramData.class);
    
    // Convert to VariableWidthHistogram
    VariableWidthHistogram histogram = histogramData.toVariableWidthHistogram();
    
    // Marshal to base64
    String actualBase64 = histogram.toBase64().trim();
    
    // Get expected base64
    String expectedBase64 = base64ExpectedFile.getDataAsString().trim();
    
    // Compare
    if (actualBase64.equals(expectedBase64)) {
      System.out.println("  PASS");
      System.out.println();
      return true;
    } else {
      System.out.println("  FAIL: Base64 mismatch");
      System.out.println("  Expected: " + expectedBase64);
      System.out.println("  Actual:   " + actualBase64);
      System.out.println();
      
      // Also try to unmarshal both and compare to provide more debugging info
      try {
        VariableWidthHistogram fromExpected = VariableWidthHistogram.fromBase64(expectedBase64);
        System.out.println("  Expected histogram unmarshals successfully:");
        printHistogram(fromExpected);
      } catch (Exception e) {
        System.out.println("  Error unmarshaling expected base64: " + e.getMessage());
      }
      
      try {
        VariableWidthHistogram fromActual = VariableWidthHistogram.fromBase64(actualBase64);
        System.out.println("  Actual histogram unmarshals successfully:");
        printHistogram(fromActual);
      } catch (Exception e) {
        System.out.println("  Error unmarshaling actual base64: " + e.getMessage());
      }
      
      System.out.println();
      return false;
    }
  }
  
  private static void printHistogram(VariableWidthHistogram h)
  {
    System.out.println("    numBuckets: " + h.getNumBuckets());
    System.out.println("    count: " + h.getCount());
    System.out.println("    min: " + h.getMin());
    System.out.println("    max: " + h.getMax());
    System.out.println("    missingValueCount: " + h.getMissingValueCount());
    
    double[] boundaries = h.getBoundaries();
    if (boundaries != null && boundaries.length > 0) {
      System.out.print("    boundaries: [");
      for (int i = 0; i < boundaries.length; i++) {
        if (i > 0) System.out.print(", ");
        System.out.print(boundaries[i]);
      }
      System.out.println("]");
    }
    
    double[] counts = h.getCounts();
    if (counts != null && counts.length > 0) {
      System.out.print("    counts: [");
      for (int i = 0; i < counts.length; i++) {
        if (i > 0) System.out.print(", ");
        System.out.print(counts[i]);
      }
      System.out.println("]");
    }
  }
}

