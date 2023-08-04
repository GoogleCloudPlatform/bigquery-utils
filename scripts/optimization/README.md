# Optimzation Scripts

This folder contains scripts that (when executed) create tables with information to help you optimize your BigQuery tables, views, and queries. The scripts are broken down by categories as shown below:

* Project Analysis
  * [Daily project metrics](#daily-project-metrics)
* Table Analysis
  * [BigQuery Clustering/Partitioning Recommender Tool](#bigquery-clusteringpartitioning-recommender-tool)
  * [Tables with high slot consumption](#tables-with-high-slot-consumption)
  * [Tables without partitioning or clustering](#tables-without-partitioning-or-clustering)
  * [Frequently read tables without partitioning or clustering](#frequently-read-tables-without-partitioning-or-clustering)
  * [Tables receiving high quantity of daily DML statements](#tables-receiving-high-quantity-of-daily-dml-statements)
* Query Analysis
  * [Queries grouped by hash](#queries-grouped-by-hash)
  * [Queries with performance insights](#queries-with-performance-insights)



# Project Analysis
Project level analysis enables us to understand key metrics such as slot_time, bytes_scanned, bytes_shuffled  and bytes_spilled on a daily basis within a project. The metrics are examined as averages, medians and p80s. This enables us to understand at a high level what jobs within a project consume 80% of the time and 50% of the time daily.

## Daily project metrics

The [daily_project_analysis.sql](daily_project_analysis.sql) script creates a table called,
`daily_project_analysis` of daily slot consumption metrics (for a 30day period) for all your projects.

### Examples

* Top 100 tables with highest slot consumption

    ```sql
    SELECT *
    FROM optimization_workshop.daily_project_analysis
    ORDER BY total_slot_ms DESC
    LIMIT 100
    ```

# Table Analysis

## BigQuery Clustering/Partitioning Recommender Tool

### Enable using gcloud

```bash
# The following script retrieves all distinct projects from the JOBS_BY_ORGANIZATION view
# and then enables the recommender API for each project.
for proj in $(bq query --nouse_legacy_sql --format=csv "SELECT DISTINCT project_id FROM \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION" | sed 1d); do
  gcloud services --project="${proj}" enable recommender.googleapis.com &
done
```

### Enable using Terraform

```hcl
resource "google_project_service" "recommender_service" {
  project = "your-project"
  service = "recommender.googleapis.com"
}
```

## Tables with high slot consumption 

The [table_read_patterns.sql](table_read_patterns.sql) script creates a table called, `table_read_patterns`, and populate it with usage data to help you determine:
* Which tables (when queried) are resulting in high slot consumption.
* Which tables are most frequently queried.

### Examples

* Top 100 tables with highest slot consumption

    ```sql
    SELECT *
    FROM optimization_workshop.table_read_patterns
    ORDER BY total_slot_ms DESC
    LIMIT 100
    ```

* Top 100 most frequently queried tables

    ```sql
    SELECT *
    FROM optimization_workshop.table_read_patterns
    ORDER BY num_occurrences DESC
    LIMIT 100
    ```

## Tables without partitioning or clustering

The [tables_without_partitioning_or_clustering.sql](tables_without_partitioning_or_clustering.sql) script creates a table named, `tables_without_part_clust`,
that contains a list of tables which meet any of the following conditions:
  - not partitioned
  - not clustered
  - neither partitioned nor clustered

### Example queries

* Top 100 largest tables without partitioning or clustering

    ```sql
    SELECT *
    FROM optimization_workshop.tables_without_part_clust
    ORDER BY logical_gigabytes DESC
    LIMIT 100
    ```

## Frequently read tables without partitioning or clustering

**Note:** The [freq_read_tables_without_partitioning_or_clustering.sql](freq_read_tables_without_partitioning_or_clustering.sql) script depends on the `table_read_patterns` table so you must first run the [tables_read_patters.sql](table_read_patterns.sql) script.

The [freq_read_tables_without_partitioning_or_clustering.sql](freq_read_tables_without_partitioning_or_clustering.sql) script creates a table named, `freq_read_tables_without_part`
that contains a list of the most frequently read tables which meet any of the following conditions:
  - not partitioned
  - not clustered
  - neither partitioned nor clustered

## Tables receiving high quantity of daily DML statements

The [frequent_daily_table_dml.sql](frequent_daily_table_dml.sql) script creates a table named, `frequent_daily_table_dml`, that contains tables that have had more than 24 daily DML statements run against them in the past 30 days.

### Examples

* Top 100 tables with the most DML statements per table in a day

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
  LIMIT 100;
  ```

# Query Analysis

## Queries grouped by hash

The [queries_grouped_by_hash.sql](queries_grouped_by_hash.sql) script creates a table named, 
`queries_grouped_by_hash`. This table groups queries by their normalized query pattern, which ignores
comments, parameter values, UDFs, and literals in the query text.
This allows us to group queries that are logically the same, but
have different literals. 

For example, the following queries would be grouped together because the date literal filters are ignored:
  
```sql
SELECT * FROM my_table WHERE date = '2020-01-01';
SELECT * FROM my_table WHERE date = '2020-01-02';
SELECT * FROM my_table WHERE date = '2020-01-03';
```

### Examples

* Top 100 tables with highest bytes processed

  ```sql
  SELECT *
  FROM optimization_workshop.queries_grouped_by_hash
  ORDER BY Total_Gigabytes_Processed 
  LIMIT 100
  ```

* Top 100 tables with highest bytes processed

  ```sql
  SELECT *
  FROM optimization_workshop.queries_grouped_by_hash
  ORDER BY Total_Slot_Hours 
  LIMIT 100
  ```

## Queries with performance insights

The [query_performance_insights.sql](query_performance_insights.sql) script creates a table named, `query_performance_insights` retrieves all queries that have had performance insights
generated for them in the past 30 days.

### Examples

* Top 100 queries with most query stage performance insights

  ```sql
  SELECT *
  FROM optimization_workshop.query_performance_insights
  ORDER BY (
    num_stages_with_slot_contention 
    + num_stages_with_insufficient_shuffle_quota
    + ARRAY_LENGTH(records_read_diff_percentages)
  ) DESC
  LIMIT 100
  ```