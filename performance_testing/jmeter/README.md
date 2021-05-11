# Using JMeter for BigQuery Performance Testing

## Before You Start

Make sure you've completed the following prerequisite steps before running the
provided JMeter test plans

* Install
  [Java 8+ Oracle JDK](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
  from Oracle page
* Download the
  [Simba BigQuery JDBC Driver](https://cloud.google.com/bigquery/providers/simba-drivers)
* Download the latest
  [JMeter Binary](https://jmeter.apache.org/download_jmeter.cgi)

## Which JMeter Test Plan Do I Use?

### [bigquery_jdbc_sampler.jmx](bigquery_jdbc_sampler.jmx) (Runs queries using JDBC driver)

#### Pros

* **Long-running job polling** - The JDBC request sampler is necessary for tests
  where queries run longer than 4 minutes and where a consistent concurrency
  level must be maintained. The JDBC driver will poll the query job until it is
  finished before submitting a new query, ensuring that JMeter active threads
  exactly match active BigQuery query jobs.
* **Simpler query format** - The JDBC request sampler does not require you to
  form a JSON configuration object to submit the query to the API. This
  eliminates JSON errors as a source of problems.
    * Unescaped double quotes are allowed in SQL queries - You do not have to
      escape double quotes in your SQL queries as is required in the HTTP
      sampler.

#### Cons

* **JDBC overhead latency** - The JDBC driver has some overhead latency
  associated with it versus directly calling the REST API. Use the
  BigQuery-provided
  [INFORMATION_SCHEMA.JOBS_BY*](https://cloud.google.com/bigquery/docs/information-schema-jobs)
  view to exclusively measure query runtime without any other latencies like
  network.
* **BigQuery job labels unsupported** - You cannot currently set labels for jobs
  submitted by the JDBC driver. In order to get a similar effect to labeling,
  you'll need to include something like a JSON object in a comment in each
  query, that can be parsed when querying the
  [INFORMATION_SCHEMA.JOBS_BY*](https://cloud.google.com/bigquery/docs/information-schema-jobs)
  view.
* **Response rows must be returned** - The JDBC driver does not support an
  option to return 0 results. The MaxResults JDBC config should therefore be set
  to 1, since the default setting of 0 instructs the JDBC driver to return all
  rows.

### [bigquery_http_sampler.jmx](bigquery_http_sampler.jmx) (Runs queries using REST API)

#### Pros

* **Fully configurable job options, including job labels** - The HTTP request
  sampler allows you to specify the raw JSON request body which can include any
  supported BigQuery options. In particular, it's very useful to include query
  labels, since these will be present in the
  [jobs metadata schema](https://cloud.google.com/bigquery/docs/information-schema-jobs#schema)
  in the labels field.
* **Faster Performance** - Since JMeter is making REST calls directly to the
  BigQuery API, the performance is faster than having to invoke BigQuery API via
  the Java JDBC driver.

#### Cons

* **Default 1 hour maximum lifetime for access tokens** - The HTTP request
  sampler uses an access token (which you provide as a command-line parameter at
  startup) to authenticate with BigQuery. The default maximum lifetime of a
  Google access token is 1 hour (3,600 seconds). However, you can extend the
  maximum lifetime to 12 hours by
  [modifying the organization policy](https://cloud.google.com/resource-manager/docs/organization-policy/restricting-service-accounts#extend_oauth_ttl)
  . JMeter calls to BigQuery APIs will start failing if your JMeter test runs
  longer than your access token’s maximum lifetime.
* **JSON body configuration** - You need to configure the API request payload
  using JSON, and the JSON object configuration is easy to break. A stray quote
  or a missing comma can make your query fail in ways that are hard to
  troubleshoot.
    * **Queries must have all double quotes escaped** - Since the SQL queries
      you pass to JMeter are values inside the HTTP request JSON body, you must
      escape all double quotes that appear in the SQL query with a backslash. (
      e.g. SELECT \”Hello World\” )
* **4min Max Timeout** - If a query runs for longer than 4 minutes, it can
  appear to be done. If you intend to use JMeter's data to characterize the
  runtime of your queries, this is a critical consideration. The results will be
  wrong if you have queries that are long-running.

## Running the JMeter Test Plan

The JMeter test plans provided in this repo are designed to be run with very few
modifications. You should first test-run them this way before adding in more
changes to simplify troubleshooting if any issues are encountered.

### [run_jmeter_jdbc_sampler.sh](run_jmeter_jdbc_sampler.sh) (**Runs

bigquery_jdbc_sampler.jmx**)

1. Replace the bash script placeholders with your own values, depending on
   whether you use JDBC or HTTP as shown below:
    * `-Jproject_id=`*YOUR_PROJECT_ID*
    * `-Jdefault_dataset_id=`*YOUR_DATASET_ID*
      
      > Note: `-Jdefault_dataset_id` specifies the default dataset id to assume
      > for any unqualified table names in the queries. If not set, all table
      > names in the query string must be qualified in the format
      > `'datasetId.tableId'`.

    * `-Juser.classpath=`*/path/to/your/SimbaJDBCDriverforGoogleBigQuery*
1. Ensure proper authentication is set up for either service account or user
   account authentication:
    * Service account authentication: \
      `export GOOGLE_APPLICATION_CREDENTIALS=`*/path/to/your/private_key.json*
    * User account authentication: \
      `gcloud auth application-default login`
1. Run the bash helper script to begin the JMeter test
    * `bash run_jmeter_jdbc_sampler.sh`

### [run_jmeter_http_sampler.sh](run_jmeter_http_sampler.sh) (**Runs

bigquery_http_sampler.jmx**)

1. Replace the bash script placeholders shown below with your own values:
    * `-Jproject_id=`*YOUR_PROJECT_ID*
    * `-Jdefault_dataset_id=`*YOUR_DATASET_ID*

      > Note: `-Jdefault_dataset_id` specifies the default dataset id to assume 
      > for any unqualified table names in the queries. If not set, all table
      > names in the query string must be qualified in the format
      > `'datasetId.tableId'`.

1. Ensure proper authentication is set up
    * Service account authentication: \
      `gcloud auth activate-service-account --key-file=`*
      /path/to/your/private_key.json*
    * User account authentication: \
      `gcloud auth login`
1. Run the bash helper script to begin the JMeter test
    * `bash run_jmeter_http_sampler.sh`

## Inspecting the JMeter Test Plans

The best method of viewing and understand the JMeter test plans is to open then
in JMeter's GUI mode as shown below:

* `./apache-jmeter-5.3/bin/jmeter -t bigquery_jdbc_sampler.jmx`
* `./apache-jmeter-5.3/bin/jmeter -t bigquery_http_sampler.jmx`
