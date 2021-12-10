# Stored Procedures

This directory contains [scripting](https://cloud.google.com/bigquery/docs/reference/standard-sql/scripting)
examples which mimic the behavior of features in a traditional database. Each stored procedure within this
directory will be automatically synchronized to the `bqutil` project within the
`procedure` dataset for reference in queries.

For example, if you'd like to reference the `GetNextIds` function within your query,
you can reference it like the following:
```sql
DECLARE next_ids ARRAY<INT64> DEFAULT [];
CALL bqutil.procedure.GetNextIds(10, next_ids);
```

## Stored Procedures

* [GetNextIds](#getnextidsid_count-int64-out-next_ids-array)
* [chi_square](#chi_squaretable_name-string-independent_var-string-dependent_var-string-out-result-structx-float64-dof-float64-p-float64)
* [bh_multiple_tests](#bh_multiple_tests-pvalue_table_name-string-pvalue_column_name-string-n_rows-int64-temp_table_name-string-)
* [linear_regression](#linear_regression-table_name-string-independent_var-string-dependent_var-string-out-result-structa-float64-b-float64-r-float64-)

## Documentation

### [GetNextIds(id_count INT64, OUT next_ids ARRAY<INT64>)](get_next_id.sql)
Generates next ids and inserts them into a sample table. This implementation prevents against race condition.
```sql
BEGIN
  DECLARE next_ids ARRAY<INT64> DEFAULT [];
  CALL bqutil.procedure.GetNextIds(10, next_ids);
  SELECT FORMAT('IDs are: %t', next_ids);
END;

IDs are: [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
```

### [chi_square(table_name STRING, independent_var STRING, dependent_var STRING, OUT result STRUCT<x FLOAT64, dof FLOAT64, p FLOAT64>)](chi_square.sql)
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

### [bh_multiple_tests( pvalue_table_name STRING, pvalue_column_name STRING, n_rows INT64, temp_table_name STRING )](bh_multiple_tests.sql)
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
   
### [linear_regression (table_name STRING, independent_var STRING, dependent_var STRING, OUT result STRUCT<a FLOAT64, b FLOAT64, r FLOAT64> )](linear_regression.sql)
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