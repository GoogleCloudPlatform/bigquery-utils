# SQL Extraction

![SQL Extraction](https://github.com/GoogleCloudPlatform/bigquery-utils/workflows/SQL%20Extraction/badge.svg)

This directory contains a command-line application that can
- Locate raw SQL query strings and their usage within code
- Extract and return the SQL query strings from the code

It can be used to locate embedded SQL query strings in your repository while migrating to a new database.

## Usage

```
Usage: sql_extraction [OPTIONS] [FILEPATHS]...

  Command line application to extract raw SQL query strings and their usage
  within code.

  Sample Usages:

  > sql_extraction path/to/file.java
  > sql_extraction -r path/to/directory path/to/another/directory path/to/file.java
  > sql_extraction --include="*.java" path/to/directory
  > sql_extraction -r --include="*.java" --include="*.cs" .
  > sql_extraction -r --exclude="*.cs" /

Options:
  -R, -r, --recursive  Scan files in subdirectories recursively
  --include GLOB       Search only files whose base name matches GLOB
  --exclude GLOB       Skip files whose base name matches GLOB
  --pretty             Pretty-print output JSON
  --progress           Print progress to STDERR
  --threshold FLOAT    Minimum confidence value (inclusive between 0 and 1)
                       required for a detected query to be included in final
                       output (default is 0.5)
  --parallel           Analyze multiple files in parallel
  --debug, --verbose   Print debug logs to STDERR
  -h, --help           Show this message and exit

Arguments:
  FILEPATHS  File and directory paths to analyze
```

## Building

To run:
```
./gradlew run --args="[ARGUMENTS]"
```

To build:
```
./gradlew build
```

To clean build artifacts:
```
./gradlew clean
```

To run tests:
```
./gradlew test
```
