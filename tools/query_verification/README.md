# Query Verification

This directory contains the code for a command-line tool to verify migrated queries.

## Usage
```
usage: query_verification -q <PATH> <PATH> [-d <PATH>] [-s <PATH> <PATH>] [-h]
 -q,--query <PATH> <PATH>    First argument is the path to the migrated
                             query file. Second argument is the path to
                             the original query file and only required
                             when data is provided.
 -d,--data <PATH>            Path for table data in CSV format.
 -s,--schema <PATH> <PATH>   First argument is the path to the migrated
                             schema path. Second argument is the path to
                             the original schema query and is optional.
                             Referenced files should be in a JSON format.
 -h,--help                   Print this help screen.
 ```

Build:
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