# Project Analysis
Project level analysis enables us to understand key metrics such as slot_time, bytes_scanned, bytes_shuffled  and bytes_spilled on a daily basis within a project. The metrics are examined as averages, medians and p80s. This enables us to understand at a high level what jobs within a project consume 80% of the time and 50% of the time daily.

## [project_analysis.sql](project_analysis.sql)

The script only considers "QUERY" jobs
The script provides the metrics for a range of 30 days

Replace the following placeholders:\
`<LIST_OF_PROJECT_IDS>`: with the actual list of projects that needs to be analyzed\
`<dest_project_id>`: name of the GCP project used for storing the BQ optimization data\
`<dest_dataset>`: name of the dataset used for storing the BQ optimization data

# Table Read Patterns

## Enable BigQuery Clustering/Partitioning Recommender Tool

```bash
# The following script retrieves all distinct projects from the JOBS_BY_ORGANIZATION view
# and then enables the recommender API for each project.
for proj in $(bq query --nouse_legacy_sql --format=csv "SELECT DISTINCT project_id FROM \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION" | sed 1d); do
  gcloud services --project="${proj}" enable recommender.googleapis.com &
done
```

## [table_read_patterns.sql](table_read_patterns.sql)
This script will create a table called, `optimization_workshop.table_read_patterns`, and populate it with usage data to help you determine:
* Which tables (when queried) are resulting in high slot consumption.
* Which tables are most frequently queried.



### Examples

* Top 10 tables with highest slot consumption

    ```sql
    SELECT *
    FROM optimization_workshop.table_read_patterns
    ORDER BY total_slot_ms DESC
    LIMIT 10
    ```

* Top 10 most frequently queried tables

    ```sql
    SELECT *
    FROM optimization_workshop.table_read_patterns
    ORDER BY num_occurences DESC
    LIMIT 10
    ```

## [largest_tables_without_partitioning_or_clustering.sql](largest_freq_read_tables_without_partitioning_or_clustering.sql)

This script creates a table named, `largest_tables_without_part_clust`,
that contains a list of the largest tables which are:
  - not partitioned
  - not clustered
  - neither partitioned nor clustered

## [largest_freq_read_tables_without_partitioning_or_clustering.sql](largest_freq_read_tables_without_partitioning_or_clustering.sql)

**Note:** This script depends on the `table_read_patterns` table so you must first run the [tables_read_patters.sql](table_read_patterns.sql) script.

This script creates a table named, `largest_freq_read_tables_without_part`
that contains a list of the most frequently read tables which are:
  - not partitioned
  - not clustered
  - neither partitioned nor clustered

## [frequent_daily_table_dml.sql](frequent_daily_table_dml.sql)

This script creates a table named, `frequent_daily_table_dml`, that contains tables that have had the highest quantity of DML statements run against them in the past 30 days.

### Examples

* Top 10 tables with the most DML statements in a day

  ```sql
  SELECT
    table_id,
    table_url,
    ANY_VALUE(dml_execution_date HAVING MAX daily_dml_per_table) AS sample_dml_execution_date,
    ANY_VALUE(job_urls[OFFSET(0)] HAVING MAX daily_dml_per_table) AS sample_dml_job_url,
    MAX(daily_dml_per_table) max_daily_table_dml,
  FROM optimization_workshop.frequent_daily_table_dml
  GROUP BY table_id, table_url
  ORDER BY max_daily_table_dml DESC
  LIMIT 10;
  ```

# Query Patterns

## [top_bytes_scanning_queries_by_hash.sql](top_bytes_scanning_queries_by_hash.sql)

This script creates a table named, top_bytes_scanning_queries_by_hash, 
which contains the top 200 most expensive queries by total bytes scanned.
Queries are grouped by their normalized query pattern, which ignores
comments, parameter values, UDFs, and literals in the query text.
This allows us to group queries that are logically the same, but
have different literals. 

For example, the following queries would be grouped together because the date literal filters are ignored:
  ```
  SELECT * FROM my_table WHERE date = '2020-01-01';
  SELECT * FROM my_table WHERE date = '2020-01-02';
  SELECT * FROM my_table WHERE date = '2020-01-03';
  ```

## [query_performance_insights.sql](query_performance_insights.sql)

This script retrieves the top 100 queries that have had performance insights
generated for them in the past 30 days.
