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

# Query Patterns

## 