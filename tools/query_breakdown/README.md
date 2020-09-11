# Query Breakdown
This directory contains the code for the CLI / Backend version of the Query Breakdown tool
## Objective
The objective of this project is to create a tool which, given SQL queries and a parser, 
identifies and minimizes the portion of the queries that cannot be parsed by the parser. 
By doing so, this tool will be able to help us quickly identify which features are unsupported by 
the parser in the input queries.

## Usage
```
Usage: query_breakdown -i <PATH> [-j] [-l <INTEGER>] [-r <INTEGER>]
-i, --inputFile, PATH: this command specifies the path to the file containing queries to be 
                       inputted into the tool. It is therefore mandatory

-j, --json: this command specifies whether the program should output the results in a json format. 

-l, --limit, INTEGER: this command specifies the path to an integer that the tool takes as a 
                      limit for the number of milliseconds a tool can spend on a query, 
                      thereby controlling the overall runtime.  

-r, --replacement, INTEGER: this command specifies the number of replacements that can be 
                            recommended by the ReplacementLogic class, thereby controlling 
                            the runtime and performance.
```

## Building
To build: 
```
mvn install
```
To run: 
```
mvn exec:java -Dexec.args="[args]"
```

To test:
```
mvn test
``` 
To create an executable jar file (in target folder):
```
mvn clean compile assembly:single
```

## Input Format
The input must be a txt file containing queries that are separated by semicolons. Different queries
should be in different lines (a line cannot contain two queries). 

## Output Format
The Query Breakdown tool will outpuut the results in three ways: 

CLI (Command Line Interface)
```
Unparseable Portion: Start Line x1, End Line x2, Start Column y1, End Column y2, Deletion
Unparseable Portion: Start Line n1, End Line n2, Start Column m1, End Column m2, Replacement: replaced A with B
... 
Percentage of Parseable Components: 00.0%
Runtime: 0.0 seconds
```
JSON
```
[{“error_position”: {“startLine”: x1, “startColumn”: y1, “endLine”: x2, “endColumn”: y2}, 
“error_type”: “DELETION”}, 
{“error_position”: {“startLine”: n1, “startColumn”: m1, “endLine”: n2, “endColumn”: m2}, 
“error_type”: “REPLACEMENT”, 
“replacedFrom”: “A”, 
“replacedTo”: “B”},
...
{“performance”: “00.0”},
{“runtime”: “0.0”}]
```
output.txt file
```$xslt
Original Query: abc

Resulting Query: xyz

Original Query: ijk

Resulting Query: the entire query can be parsed without error

...
```