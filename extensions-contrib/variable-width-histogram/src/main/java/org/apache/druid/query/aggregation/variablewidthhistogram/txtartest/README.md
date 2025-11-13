# VariableWidthHistogram Txtar Test Tool

## Purpose

This directory contains a standalone Java program for validating interoperability between the Go and Java implementations of VariableWidthHistogram serialization.

## Background

The VariableWidthHistogram implementation exists in both:
- **Java**: Apache Druid's variable-width-histogram extension (this codebase)
- **Go**: Augtera's variablewidthhistogram package (separate repository)

Both implementations must produce **identical binary serialization** to ensure data compatibility when histograms are marshaled/unmarshaled across systems.

## How It Works

The test program:
1. Reads `.txtar` test files from the Go repository's test data directory
2. Each `.txtar` file contains:
   - `histogram.data` - JSON representation of a histogram
   - `base64.expected` - The expected base64-encoded binary serialization
3. Deserializes the JSON into a Java `VariableWidthHistogram`
4. Marshals it to base64 using Java's `toBase64()` method
5. Compares the result with the expected output from the Go implementation
6. Reports PASS/FAIL for each test case

## Usage

### Building

From the `variable-width-histogram` directory:

```bash
mvn clean package
```

This creates an executable JAR: `target/druid-variable-width-histogram-33.0.0-jar-with-dependencies.jar`

### Running

With default test directory (requires access to the Go repository):

```bash
java -jar target/druid-variable-width-histogram-33.0.0-jar-with-dependencies.jar
```

With custom test directory:

```bash
java -jar target/druid-variable-width-histogram-33.0.0-jar-with-dependencies.jar -d /path/to/txtar/files
```

Show help:

```bash
java -jar target/druid-variable-width-histogram-33.0.0-jar-with-dependencies.jar -h
```

### Default Test Data Location

By default, the program looks for test files in:
```
~/dev/mercury/common/go/src/augtera/variablewidthhistogram/testdata/variablehistomarshal/
```

This assumes you have the Go repository cloned locally alongside the Java repository.

## Why Txtar?

The `.txtar` format is a simple text-based archive format from the Go ecosystem that:
- Is human-readable and easy to edit
- Diffs nicely in version control
- Contains multiple related test files in a single file
- Has no external dependencies

The format is simple:
```
optional comment lines
-- filename1 --
file1 content
-- filename2 --
file2 content
```

## Files in This Directory

- **TxtarParser.java** - Minimal parser for the txtar archive format (ported from Go)
- **HistogramData.java** - JSON DTO for deserializing histogram test data
- **VariableWidthHistogramTxtarTest.java** - Main program that runs the tests
- **README.md** - This file

## Adding New Test Cases

New test cases should be added to the Go repository's test data directory:
```
augtera/variablewidthhistogram/testdata/variablehistomarshal/
```

Create a new `.txtar` file following the existing format, and both the Go tests (`go test`) and this Java tool will automatically pick it up.

## Exit Codes

- `0` - All tests passed
- `1` - One or more tests failed or an error occurred

This allows integration into CI/CD pipelines to ensure ongoing compatibility between implementations.

