# Query Verification

This directory contains the code for a command-line tool to verify migrated queries.

## Usage
```
usage: query_verification -q <PATH> <PATH> [-d <PATHS>] [-s <PATH> <PATH>]
       [-h]
 -q,--query <PATH> <PATH>    First argument is the path to the migrated
                             query file. Second argument is the path to
                             the original query file and only required
                             when data is provided.
 -d,--data <PATHS>           Paths for table data in CSV format. File
                             names should be formatted as
                             "[dataset].[table].csv".
 -s,--schema <PATH> <PATH>   First argument is the path to the migrated
                             schema path. Second argument is the path to
                             the original schema query and is optional.
                             Referenced files should be DDL statements or
                             in JSON format.
 -h,--help                   Print this help screen.
 ```

Build:
```
mvn install:install-file
-DgroupId=com.teradata.jdbc
-DartifactId=terajdbc4
-Dversion=17.00.00.02
-Dpackaging=jar
-Dfile=[PATH_TO_TERADATA_JDBC_DRIVER]
```
```
mvn install
```

Run:
```
mvn exec:java -Dexec.args="[args]"
```

Test:
```
mvn test
```