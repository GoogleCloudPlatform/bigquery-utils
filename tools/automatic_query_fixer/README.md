# AutomaticQueryFixer

This directory contains a command-line application that can automatically fix multiple common errors
of BigQuery SQL queries without changing the semantics of queries. It will output the fixed query 
to the CLI if the query is fixed successfully. It will print the error message if it fails to fix the 
query.

## Prerequisite
Before building the query fixer, please go to `zetasql_helper` directory at the parent directory (`tools`)
and check if the docker image is compiled. If you don't have the ZetaSQL Helper Docker image, please follow
the [instruction](https://github.com/GoogleCloudPlatform/bigquery-utils/tree/master/tools/zetasql_helper)
to compile the Docker image.

## Build the Query Fixer
Every command should be run in the current directory (auto_query_fixer) unless it is specified by the description.

Build the query fixer:
```
./gradlew build
./gradlew installdist
```

## Run the Query Fixer
Run this at Linux and Mac
```
./build/install/AutomaticQueryFixer/bin/AutomaticQueryFixer <args>
```
or at Windows
```
./build/install/AutomaticQueryFixer/bin/AutomaticQueryFixer.bat <args>
```

The details for `<args>` can be seen at the [**Input Flag**](#Input-Flag) section.

If you would like to examine the test cases, please run the tests:
```
./gradlew test
```

### Example Query to fix
The project prepares a few incorrect sql to fix. Please try out with the following command. If you 
are using windows, replace `AutomaticQueryFixer` with `AutomaticQueryFixer.bat`.

```bash
./build/install/AutomaticQueryFixer/bin/AutomaticQueryFixer -m ua  -p <project-id> -q "./examples/syntax_error.sql"
./build/install/AutomaticQueryFixer/bin/AutomaticQueryFixer -m ua  -p <project-id> -q "./examples/semantic_error1.sql"
./build/install/AutomaticQueryFixer/bin/AutomaticQueryFixer -m ua  -p <project-id> -q "./examples/semantic_error2.sql"
./build/install/AutomaticQueryFixer/bin/AutomaticQueryFixer -m ua  -p <project-id> -q "./examples/semantic_error3.sql"
```

The project ID should be the one of your own Google Cloud Platform.


## Input Flag
```
Usage: AutomaticQueryFixer -opt <value> --long-opt <value> "query"

  Introduction:
  A command-line tool automatically fixing multiple common errors
  of BigQuery SQL queries.

  Sample Usages:
  > AutomaticQueryFixer -m auto -p "<your gcp project id>" "<your query>"
  > AutomaticQueryFixer -m sg -c "path/to/credentials.json" -o json -p "<your gcp project id>" "<your query>"
  > AutomaticQueryFixer -m ua -o natural -p "<your gcp project id>" -q "path/to/sql_file"


Options:
 -c,--credentials <arg>   The path to the credential file of the service
                          account connecting to BigQuery. Otherwise, the
                          default application-login credential will be
                          used.
 -m,--mode <arg>          Interactive Mode. The available mode are "auto"
                          (default), "ua/user-assistance" and
                          "sg/suggestion". Please see the README file for
                          the detailed description.
 -o,--output <arg>        The format to output fix results. The available
                          formats are "natural" (default) and "json"
 -p,--project-id <arg>    The ID of project where queries will be
                          performed. This field is required if the project
                          is not specified in credential
 -q,--query-file <arg>    The directory of the query file. If query has
                          been provided as an argument, this will be
                          ignored.
```

### Credential
`-c` is used to specify the path to your Google Cloud Platform (GCP) Credentials. If you don't know to create one, 
please click [here](https://cloud.google.com/iam/docs/creating-managing-service-account-keys). After you download your 
GCP credentials, specify its path to the query fixer by using `-c "path/to/credentials"`.

Besides service account, you could use your user account's credentials to start the query fixer. First, download the 
[gcloud](https://cloud.google.com/sdk/install), then call this command:

```bash
gcloud auth application-default login
```

A user credentials will be created at a default directory. Then leave the `-c` flag empty, and the query fixer should
be able to detect your user credentials and connect to the BigQuery server.

### Interactive Mode
`-m` flag can choose the mode of the query fixer. There are three modes: Auto, User-assisted, and Suggestion mode. They
decide the involvement of users. 

#### Auto mode
It is activated by `-m auto`. Auto mode means no interaction. It continually tries to fix every error in a query 
until the query is correct or unable to be fixed. If an error can be fixed in multiple ways, then the first method 
will be selected. In this mode, the query fixer takes the input query and outputs the final fix results 
without interacting with users during the fix process.

Here is an example of Auto Mode:
```text
Input query:
SELECT DIV(created_data, 1000000000), street_number
FROM `bigquery-public-data.austin_311.311_requests`
where  MOD(street_number, 1000) > 100
LIMIT 10

The query has an error: Not found: Table bigquery-public-data:austin_311.311_requests was not found in location US
It is fixed by the approach: Replace the table name `bigquery-public-data:austin_311.311_requests`
Action: Change to `bigquery-public-data.austin_311.311_request`

Fixed query:
SELECT DIV(created_data, 1000000000), street_number
FROM bigquery-public-data.austin_311.311_request
where  MOD(street_number, 1000) > 100
LIMIT 10
-----------------------------------

The query has an error: No matching signature for function MOD for argument types: STRING, INT64. Supported signatures: MOD(INT64, INT64); MOD(NUMERIC, NUMERIC) at [3:8]
It is fixed by the approach: Update the function `MOD`.
Action: Change to MOD(SAFE_CAST(street_number AS INT64), 1000)

Fixed query:
SELECT DIV(created_data, 1000000000), street_number
FROM bigquery-public-data.austin_311.311_request
where  MOD(SAFE_CAST(street_number AS INT64), 1000) > 100
LIMIT 10
-----------------------------------

...

The input query is valid. No errors to fix.
```

#### User-assisted mode
It is activated by `-m ua`. It does not interact with users until it needs an instruction from the users. 
It continually fixes errors in a query until the query is correct or unable to be fixed. However, If an error 
can be fixed in multiple ways, then the program will request users to choose one of the fix options 
by entering their number.

Here is an example that asks a user to choose one option:

```text
The query has an error: No matching signature for function MOD for argument types: STRING, INT64. Supported signatures: MOD(INT64, INT64); MOD(NUMERIC, NUMERIC) at [3:8]
It can be fixed by the approach: Update the function `MOD`.

1. Action: Change to MOD(SAFE_CAST(street_number AS INT64), 1000)
   Fixed query:
SELECT DIV(created_data, 1000000000), street_number
FROM bigquery-public-data.austin_311.311_request
where  MOD(SAFE_CAST(street_number AS INT64), 1000) > 100
LIMIT 10

2. Action: Change to MOD(SAFE_CAST(street_number AS NUMERIC), SAFE_CAST(1000 AS NUMERIC))
   Fixed query:
SELECT DIV(created_data, 1000000000), street_number
FROM bigquery-public-data.austin_311.311_request
where  MOD(SAFE_CAST(street_number AS NUMERIC), SAFE_CAST(1000 AS NUMERIC)) > 100
LIMIT 10

Enter the option you use to fix the query:
```

#### Suggestion mode
It is activated by `-m sg`. This mode only fixes one error of an input query if an error exists. 
Then outputs all the Fix Result to users. This mode is useful if a user would like to review individual 
fixes before the query fixer corrects the next error based on the previous result. This mode can also be 
integrated with frontend to provide fixing suggestions in a UI.

Here is an example of the suggestion mode:
```text
Input query:
SELECT DIV(created_data, 1000000000), street_number
FROM `bigquery-public-data.austin_311.311_requests`
where  MOD(street_number, 1000) > 100
LIMIT 10

The query has an error: Not found: Table bigquery-public-data:austin_311.311_requests was not found in location US
It can be fixed by the approach: Replace the table name `bigquery-public-data:austin_311.311_requests`

1. Action: Change to `bigquery-public-data.austin_311.311_request`
   Fixed query:
SELECT DIV(created_data, 1000000000), street_number
FROM bigquery-public-data.austin_311.311_request
where  MOD(street_number, 1000) > 100
LIMIT 10
```

### Output Format
`-o` represents the output format in the program input flag. It has “natural language” and “json” format.

#### Natural Language format
This is the default version, which can be explicitly turned on by `-o natural`. It prints all the fixing results 
in natural language.  It is primarily used to interact with users when they use the query fixer directly.

#### JSON format
This mode can be turned on by `-o json`.This format is allowed when the query fixer is in Auto or Suggestion mode. 
It prints out all the fixing results in JSON format. Here is an example:

```json
{
  "query": "SELECT DIV(created_data, 1000000000), street_number\nFROM `bigquery-public-data.austin_311.311_requests`\nwhere  MOD(street_number, 1000) \u003e 100\nLIMIT 10",
  "status": "ERROR_FIXED",
  "options": [
    {
      "action": "Change to `bigquery-public-data.austin_311.311_request`",
      "fixedQuery": "SELECT DIV(created_data, 1000000000), street_number\nFROM bigquery-public-data.austin_311.311_request\nwhere  MOD(street_number, 1000) \u003e 100\nLIMIT 10"
    }
  ],
  "error": "Not found: Table bigquery-public-data:austin_311.311_requests was not found in location US",
  "approach": "Replace the table name `bigquery-public-data:austin_311.311_requests`",
  "errorPosition": {
    "row": 2,
    "column": 6
  },
  "isConfident": true,
  "failureDetail": null
}
```

Suggestion mode will output a single Structure while Auto mode will output a list of structure. 
Each structure represents one fixing result.

## Project Structure
```text
├── libs
│   └── zetasql_helper_client.jar
└── src
    ├── main
    │   ├── java.com.google.cloud.bigquery.utils.queryfixer
    │   │   ├── AutomaticQueryFixer.java
    │   │   ├── QueryFixerMain.java
    │   │   ├── cmd
    │   │   ├── entity
    │   │   ├── errors
    │   │   ├── exception
    │   │   ├── fixer
    │   │   ├── service
    │   │   ├── tokenizer
    │   │   └── util
    │   └── resources
    └── test
        ├── java.com.google.cloud.bigquery.utils.queryfixer
        └── resources
```

Inside `src/main/java.com.google.cloud.bigquery.utils.queryfixer`:

`QueryFixerMain.java` is the entrance of the command line tool.

`AutomaticQueryFixer.java` is the interface of the Automatic Query Fixer. If the query fixer is used
as a library, you should import this class.

`cmd` directory stores the all the classes and logic regarding command line interaction.

`entity` directory defines the Entities used in the query fixer.

`errors` directory defines the classes of BigQuery errors. When a BigQuery exception is caught from the
BigQuery server, it should be structured as one of the error classes in this directory.

`exception` directory defines the Exception thrown by the query fixer.

`fixer` directory defines the fixers for the BigQuery errors. Each error in `errors` directoyry should have
its corresponding fixer in this directory.

`service` directory contains the service provided by external source, like `BigQueryService`.

`tokenizer` directory contains the implementation of tokenizers.

`util` directory stores helper functions.

\-\-\-
<br>

Inside `lib` directory:

`zetasql_helper_client.jar` is the client to connect with the BigQuery Helper server.

### How to add new fix methods?

Suppose we are `src/main/java.com.google.cloud.bigquery.utils.queryfixer` directory.

* Define the error class of the BigQuery error you would like to fix, Please refer to other
 error classes in `errors` directory.
 
* Define the parsing logic (information extraction) of your error class in `errors/SqlErrorFactory`.
The `getError` method in this factory converts a `BigQueryException` to the instance of an error class.

* Define the corresponding `Fixer` class in `fixer` directory. Please refer to other fixer classes in 
this directory.

* Implement the `fix` method of your fixer class.

* Implement the logic to get a fixer from an BigQuery error in `fixer/FixerFactory` class. It should be 
implemented inside `getFixer` method.

* Write the unit tests like other fixers.
