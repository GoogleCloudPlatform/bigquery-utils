# Stored Procedures

This directory contains [scripting](https://cloud.google.com/bigquery/docs/reference/standard-sql/scripting)
examples which mimic the behavior of features in a traditional database. Each stored procedure within this
directory will be automatically synchronized to the `bqutil` project within the
`procedure` dataset for reference in queries.

For example, if you'd like to reference the `get_next_ids` function within your query,
you can reference it like the following:
```sql
DECLARE next_ids ARRAY<INT64> DEFAULT [];
CALL bqutil.procedure.get_next_ids(10, next_ids);
```
## Using the stored procedures

All stored procedures within this repository are available under the `bqutil` project on
publicly shared datasets. Queries can then reference the shared procedures in the US multi-region via
`bqutil.procedure.<procedure_name>()`.

Procedures within this repository are also deployed publicly into every other region that [BigQuery supports](https://cloud.google.com/bigquery/docs/locations). 
In order to use a procedure in your desired location outside of the US multi-region, you can reference it via a dataset with a regional suffix:

`bqutil.procedure_<region>.<procedure_name>()`.

For example, `GetNextIds` can be referenced in various locations:

```
CALL bqutil.procedure_eu.GetNextIds()            ## eu multi-region

CALL bqutil.procedure_europe_west1.GetNextIds()  ## europe-west1 region

CALL bqutil.procedure_asia_south1.GetNextIds()   ## asia-south1 region

```

Note: Region suffixes added to dataset names replace `-` with `_` in order to comply with BigQuery dataset naming rules.


## Stored Procedures

* [bh_multiple_tests](#bh_multiple_tests-pvalue_table_name-string-pvalue_column_name-string-n_rows-int64-temp_table_name-string-)
* [bqml_generate_embeddings](#bqml_generate_embeddings-source_table-string-target_table-string-ml_model-string-content_column-string-key_columns-array-options_string-string)
* [bqml_generate_text](#bqml_generate_text-source_table-string-target_table-string-ml_model-string-prompt_column-string-key_columns-array-options_string-string)
* [chi_square](#chi_squaretable_name-string-independent_var-string-dependent_var-string-out-result-structx-float64-dof-float64-p-float64)
* [get_next_ids](#get_next_idsid_count-int64-out-next_ids-array)
* [linear_regression](#linear_regression-table_name-string-independent_var-string-dependent_var-string-out-result-structa-float64-b-float64-r-float64-)


## Documentation

### [get_next_ids(id_count INT64, OUT next_ids ARRAY<INT64>)](definitions/get_next_ids.sqlx)
Generates next ids and inserts them into a sample table. This implementation prevents against race condition.
```sql
BEGIN
  DECLARE next_ids ARRAY<INT64> DEFAULT [];
  CALL bqutil.procedure.get_next_ids(10, next_ids);
  SELECT FORMAT('IDs are: %t', next_ids);
END;

IDs are: [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
```

### [chi_square(table_name STRING, independent_var STRING, dependent_var STRING, OUT result STRUCT<x FLOAT64, dof FLOAT64, p FLOAT64>)](definitions/chi_square.sqlx)
Performs a chi-square statistical test from an input table. It generates a structure containg the chi-square statistics, the degrees of freedom, and the pvalue of the test.
```sql
 BEGIN
    DECLARE result STRUCT<x FLOAT64, dof FLOAT64, p FLOAT64>;

    CREATE TEMP TABLE categorical (animal STRING, toy STRING) AS
        SELECT 'dog' AS animal, 'ball' as toy
        UNION ALL SELECT 'dog', 'ball'
        UNION ALL SELECT 'dog', 'ball'
        UNION ALL SELECT 'dog', 'ball'
        UNION ALL SELECT 'dog', 'yarn'
        UNION ALL SELECT 'dog', 'yarn'
        UNION ALL SELECT 'cat', 'ball'
        UNION ALL SELECT 'cat', 'yarn'
        UNION ALL SELECT 'cat', 'yarn'
        UNION ALL SELECT 'cat', 'yarn'
        UNION ALL SELECT 'cat', 'yarn'
        UNION ALL SELECT 'cat', 'yarn'
        UNION ALL SELECT 'cat', 'yarn';

  CALL bqutil.procedure.chi_square('categorical', 'animal', 'toy', result);
  SELECT result ;
END
```
Output:
| result.x  | result.dof | result.p |
|---|---|---|
| 3.7452380952380966 | 1.0  |  0.052958181867438725 |

### [bh_multiple_tests( pvalue_table_name STRING, pvalue_column_name STRING, n_rows INT64, temp_table_name STRING )](definitions/bh_multiple_tests.sqlx)
Adjust p values using the Benjamini-Hochberg multipletests method, additional details in doi:10.1098/rsta.2009.0127

```sql
BEGIN
   CREATE TEMP TABLE Pvalues AS
      SELECT 0.001 as pval
      UNION ALL SELECT 0.008
      UNION ALL SELECT 0.039
      UNION ALL SELECT 0.041
      UNION ALL SELECT 0.042
      UNION ALL SELECT 0.06
      UNION ALL SELECT 0.074
      UNION ALL SELECT 0.205;

   CALL bqutil.procedure.bh_multiple_tests('Pvalues','pval',8, 'bh_multiple_tests_results');
   SELECT * FROM bh_multiple_tests_results;
END;
```
Output:
| pval | pval_adj |
|---|---|
| 0.008 | 0.032 |
| 0.039 | 0.06720000000000001 |
| 0.041 | 0.06720000000000001 |
| 0.042 | 0.06720000000000001 |
| 0.06 | 0.08 |
| 0.074 | 0.08457142857142856 |
| 0.205 | 0.205 |
   
### [linear_regression (table_name STRING, independent_var STRING, dependent_var STRING, OUT result STRUCT<a FLOAT64, b FLOAT64, r FLOAT64> )](definitions/linear_regression.sqlx)
Run a standard linear regression on table data. Expects a table and two columns: the independent variable and the dependent variable. The output is a STRUCT with the slope (`a`), the intercept (`b`) and the correlation value (`r`).

> Input data

The unit test for this procedure builds a TEMP table to contain the classic [Iris flower data set](https://en.wikipedia.org/wiki/Iris_flower_data_set). This dataset contains 150 data points, not all shown below. The sample call demonstrates how to access the output.

```sql
-- a unit test of linear_regression
BEGIN
  DECLARE result STRUCT<a FLOAT64, b FLOAT64, r FLOAT64>;
  CREATE TEMP TABLE iris (sepal_length FLOAT64, sepal_width FLOAT64, petal_length FLOAT64, petal_width FLOAT64, species STRING)
  AS
  SELECT 5.1 AS sepal_length,
       3.5 AS sepal_width,
       1.4 AS petal_length,
       0.2 AS petal_width,
       'setosa' AS species
     UNION ALL SELECT 4.9,3.0,1.4,0.2,'setosa'
     UNION ALL SELECT 4.7,3.2,1.3,0.2,'setosa'
     ...
     UNION ALL SELECT 6.5,3.0,5.2,2.0,'virginica'
     UNION ALL SELECT 6.2,3.4,5.4,2.3,'virginica'
     UNION ALL SELECT 5.9,3.0,5.1,1.8,'virginica';
```

```sql
CALL bqutil.procedure.linear_regression('iris', 'sepal_width', 'petal_width', result);

  -- We round to 11 decimals here because there appears to be some inconsistency in the function, likely due to floating point errors and the order of aggregation
  ASSERT ROUND(result.a, 11) = 3.11519268710;
  ASSERT ROUND(result.b, 11) = -0.62754617565;
  ASSERT ROUND(result.r, 11) = -0.35654408961;
END;
```

Output:

`This assertion was successful`

### [bqml_generate_embeddings (source_table STRING, target_table STRING, ml_model STRING, content_column STRING, key_columns ARRAY<STRING>, options_string STRING)](definitions/bqml_generate_embeddings.sqlx)

Iteratively executes the [BQML.GENERATE_EMBEDDING](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-embedding) function to ensure all source table rows are embedded in the destination table, handling potential retryable errors gracefully along the way. Any rows already present in the destination table are ignored, so this procedure is safe to call multiple times.

This approach improves the robustness of your embedding generation process by automatically retrying failed batches, ensuring complete data coverage in the destination table.

#### Function parameters

| Parameter | Description | 
| ----------- | ----------- | 
| `source_table` | The full path of the BigQuery table containing the text data to be embedded. Path format - "project.dataset.table" or "dataset.table" |
| `destination_table` | The full path of the BigQuery table where the generated embeddings will be stored. This table will be created if it does not exist.|
| `model` | The full path of the embedding model to be used. | 
| `content_column` | The name of the column in the `source_table` containing the text to be embedded. |
| `key_columns` | An array of column names in the `source_table` that uniquely identify each row. '*' is not a valid value. |
| `options` | A JSON string containing additional optional parameters for the embedding generation process. Set to '{}' if you want to use defaults for all options parameters. |

The options JSON encodes additional optional arguments for the procedure. Each parameter must be set as a key-value pair in the JSON.

| Parameter | Default Value | Description |
|---|---|---|
| `batch_size` | 4500000 | The number of rows to process in each child job during the procedure. A larger value will reduce the overhead of multiple child jobs, but needs to be small enough to complete in a single job run. A reasonable starting value is the Vertex QPM quota * 500 |
| `termination_time_secs` | 82800 (23 hours) | The maximum time (in seconds) the script should run before terminating. |
| `source_filter` | 'TRUE' | An optional filter applied as a WHERE clause to the source table before processing. |
| `projection_columns` | ARRAY[] | An array of column names to select from the source table into the destination table. '*' is not a valid value. |
| `ml_options` | 'STRUCT()' | A JSON string representing additional options for the ML operation. Must be of the form 'STRUCT(...) |

A sample fully-filled JSON option string would look like: 
```
"""{
  "batch_size": 50000,
  "termination_time_secs": 43200,
  "source_filter": "LENGTH(text) < 1000",
  "projection_columns": ["type", "text"],
  "ml_options": "STRUCT('RETRIEVAL_DOCUMENT' as task_type)"
}"""
```

#### Example usage

```sql
BEGIN
  -- Assumes dataset and model are already created

  CREATE OR REPLACE TABLE sample.hacker AS
  SELECT * FROM `bigquery-public-data.hacker_news.full`
  WHERE text IS NOT NULL
  LIMIT 1000;

  CALL `bqutil.procedure.bqml_generate_embeddings`(
      "sample.hacker",                  -- source_table
      "sample.hacker_results",          -- destination_table (it will be created if it doesn't exist)
      "sample.embedding_model",         -- model
      "text",                           -- content column
      ["id"],                           -- key columns
      '{}'                              -- optional arguments encoded as a JSON string
  );

  ASSERT (SELECT COUNT(*) FROM `sample.hacker_results`) = 1000;
END;
```

Output:

`This assertion was successful`


### [bqml_generate_text (source_table STRING, target_table STRING, ml_model STRING, prompt_column STRING, key_columns ARRAY<STRING>, options_string STRING)](definitions/bqml_generate_text.sqlx)

*This procedure is still in draft mode and is subject to changes*

Iteratively executes the [BQML.GENERATE_TEXT](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-generate-text) function to ensure all source table prompts have responses in the destination table, handling potential retryable errors gracefully along the way. Any rows already present in the destination table are ignored, so this procedure is safe to call multiple times.

This approach improves the robustness of your text generation process by automatically retrying failed batches, ensuring complete data coverage in the destination table.

#### Function parameters

| Parameter | Description | 
| ----------- | ----------- | 
| `source_table` | The full path of the BigQuery table containing the text data to be embedded. Path format - "project.dataset.table" or "dataset.table" |
| `destination_table` | The full path of the BigQuery table where the generated text will be stored. This table will be created if it does not exist.|
| `model` | The full path of the text model to be used. | 
| `prompt_column` | The name of the column in the `source_table` containing the text prompts. |
| `key_columns` | An array of column names in the `source_table` that uniquely identify each row. '*' is not a valid value. |
| `options` | A JSON string containing additional optional parameters for the text generation process. Set to '{}' if you want to use defaults for all options parameters. |

The options JSON encodes additional optional arguments for the procedure. Each parameter must be set as a key-value pair in the JSON.

| Parameter | Default Value | Description |
|---|---|---|
| `batch_size` | 1000 | The number of rows to process in each child job during the procedure. A larger value will reduce the overhead of multiple child jobs, but needs to be small enough to complete in a single job run. A reasonable starting value is the Vertex QPM quota * 100 |
| `termination_time_secs` | 82800 (23 hours) | The maximum time (in seconds) the script should run before terminating. |
| `source_filter` | 'TRUE' | An optional filter applied as a WHERE clause to the source table before processing. |
| `projection_columns` | ARRAY[] | An array of column names to select from the source table into the destination table. '*' is not a valid value. |
| `ml_options` | 'STRUCT()' | A JSON string representing additional options for the ML operation. Must be of the form 'STRUCT(...) |

A sample fully-filled JSON option string would look like: 
```
"""{
  "batch_size": 50,
  "termination_time_secs": 43200,
  "source_filter": "LENGTH(text) < 1000",
  "projection_columns": ["type", "text"],
  "ml_options": "STRUCT(0.2 as temperature)"
}"""
```

#### Example usage

```sql
BEGIN
  -- Assumes dataset and model are already created

  CREATE OR REPLACE TABLE sample.hacker AS
  SELECT * FROM `bigquery-public-data.hacker_news.full`
  WHERE text IS NOT NULL
  LIMIT 100;

  CALL `bqutil.procedure.bqml_generate_text`(
      "sample.hacker",                  -- source_table
      "sample.hacker_results",          -- destination_table (it will be created if it doesn't exist)
      "sample.text_model",              -- model
      "text",                           -- content column
      ["id"],                           -- key columns
      '{}'                              -- optional arguments encoded as a JSON string
  );

  ASSERT (SELECT COUNT(*) FROM `sample.hacker_results`) = 100;
END;
```

Output:

`This assertion was successful`