### Structify UDF Command Line Generator

Command line utility to generate a BigQuery standard SQL Structify UDF.
A structify UDF converts a struct with repeated columns into a flat array of
structs with the respective columns as member fields of the struct.

#### Inputs
- Columns list: Space separated list of column names
- UDF name: Default UDF name is 'structify'. Can be overriden with this flag.
- Output path: Optional parameter to write the generated UDF to a file.

#### Outputs
- The generated UDF is printed to STDOUT and written to file if the output path
parameter is passed. This output can be now copied and used as a temporary UDF
in ad hoc queries that need arrays to be structified.

#### Usage
```sh
python structify_generator.py [-h] --columns COLUMNS [COLUMNS ...]
                              [--output_path OUTPUT_PATH]
                              [--udf_name UDF_NAME]
```

#### Examples

1. Use default name for generated UDF:
python structify_generator.py --columns a b c
```sql
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION structify(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b),  c_t AS (SELECT ROW_NUMBER() OVER() idx, c FROM UNNEST(col.c) c)
    (SELECT AS STRUCT a, b, c
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx FULL JOIN c_t ON b_t.idx = c_t.idx
    )
  )
);
```
2. Override default name for generted UDF:
python structify_generator.py --columns a b c --udf_name custom_name
```sql
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION custom_name(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b),  c_t AS (SELECT ROW_NUMBER() OVER() idx, c FROM UNNEST(col.c) c)
    (SELECT AS STRUCT a, b, c
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx FULL JOIN c_t ON b_t.idx = c_t.idx
    )
  )
);
```
3. Write output to a file:
```sh
python structify_generator.py --columns y m d --output_path /tmp/udf.sql
```

> __Note: If the output file exists, it will be overwritten.__

#### Sample use the output of the structify generator in BigQuery queries

```sql
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION structify(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b),  c_t AS (SELECT ROW_NUMBER() OVER() idx, c FROM UNNEST(col.c) c)
    (SELECT AS STRUCT a, b, c
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx FULL JOIN c_t ON b_t.idx = c_t.idx
    )
  )
);

WITH ex AS (
  SELECT AS VALUE STRUCT(['A', 'B', 'C'] as a, [4, 5] as b, [.6] as c) struct_field
  UNION ALL
  SELECT AS VALUE STRUCT(['X'] as a, [6, 7] as b, [.9, .10, .11] as c) struct_field
)
SELECT r.a, r.b, r.c
FROM ex 
  JOIN UNNEST(structify(ex)) r
;
```

Output:

Row | a | b | c
--- | - | - | -
1   | A | 4 | 0.6
2   | B | 5 | null
3   | C | null | null
4   | X | 6 | 0.9
5   | null | 7 | 0.1
6   | null | null | 0.11

### Running the python tests
```bash
python3 -m unittest discover .
```
