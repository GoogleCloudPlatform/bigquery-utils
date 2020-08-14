# SQL Crawler

This directory contains the code to run a universal, unsupervised SQL web crawler. The user provides a starting target URL from which to begin crawling, and has the option to set the maximum depth or size of the crawler.

## Usage
To run the crawler, run the following command from within the unsupervised_dataset folder:

```
python3 run_crawler.py <starting URLs>
```

Multiple URLs should be separated by a space. There is also the option to set the maximum depth and size of the crawler, as shown below.

```
python3 run_crawler.py <starting URLs> --max_depth 5 --max_size 500
```

If these are not specified, the default is a depth of 3 and size of 100. After the crawler runs to completion, a log containing the websites will be saved in a subdirectory named "SQLCrawler/Logs". The queries themselves will be in a CSV file in a subdirectory titled "SQLCrawler/Queries".

### Google Cloud Integration

Output can also be saved to Google BigQuery or Cloud Storage. To enable these, first download the [BigQuery](https://cloud.google.com/bigquery/docs/reference/libraries#installing_the_client_library) and [GCS](https://cloud.google.com/storage/docs/reference/libraries#installing_the_client_library) client libraries, and set up Google Cloud [authentication](https://cloud.google.com/docs/authentication/production).

To upload to BigQuery, the user must provide the project ID and the name of the dataset where they would like the queries to be loaded. For example, if the project ID is “sql_project” and the dataset name is “query_dataset”, the user can have their output saved to BigQuery with the --bigquery flag shown below.

```
python3 run_crawler.py <starting URLs> --bigquery sql_project.query_dataset
```

The user can use similar syntax to save the CSV to Google Cloud Storage, providing the project ID and bucket name as shown below.

```
python3 run_crawler.py https://website1.com --cloud_storage sql_project.query_bucket
```

If these are not specified, the default is a depth of 3 and size of 100. After the crawler runs to completion, a log containing the websites will be saved in a subdirectory named "SQLCrawler/Logs". The queries themselves will be in a subdirectory titled "SQLCrawler/Queries".

# SQL Classifier

This directory contains the code for the SQL dialect classifier built on top of Calcite. To build, navigate to the directory and run:
```
mvn package
```

This should build a jar with dependencies. Run this jar and provide a CSV file with queries as the first command line argument, which will classify the queries in the file.
