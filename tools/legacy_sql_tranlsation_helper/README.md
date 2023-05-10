# BigQuery Legacy SQL translation

## Introduction
This repo is for BigQuery Legacy SQL Translation usage. It contains:
1. patternScan.py: A pattern scan code to detect which parts in the Legacy SQL needed to be translated. 
2. translatorCommaJoin.py: A Comma Join to Union All translation script. It queries Information Schema to cast `null` to a data type if a column doesn't exist in a given table.
3. Other utility scripts.

## Step 1: Scan query for translation recommendations
### Run patternScan.py
1. To scan and recommend which part of the legacy SQL may need to be translated
2. Please replace the `legacy_sql` with your SQL string, or replace `file_path` with your SQL file path.
3. Sample input: `sample_sql/sample1.sql`
4. Sample output: `"Legacy Table Name detected"`

### Pattern Scan Scope
1. Comma Join.
2. Flatten
3. Time Decorator
4. Legacy Table Name
5. x % y
6. LEFT()
7. RIGHT()
8. CONTAINS()
9. DATE()
10. DATE_ADD()
11. TIMESTAMP(UTC_USEC_TO_HOUR(TIMESTAMP_TO_USEC()))
12. UTC_USEC_TO_HOUR()
13. INTEGER()
14. DATEDIFF()
15. STRFTIME_UTC_USEC()
16. UTC_USEC_TO_DAY()
17. IS_NULL()
18. REGEXP_MATCH()
19. USEC_TO_TIMESTAMP()
20. TIMESTAMP_TO_USEC()
21. SEC_TO_TIMESTAMP()
22. TIMESTAMP_TO_MSEC()
23. INSTR()
24. GROUP_CONCAT_UNQUOTED()
25. GROUP_CONCAT()
26. NOW()
27. UNIQUE()
28. TABLE_DATE_RANGE()
29. hash()
30. STRING()



## Step 2: Translate each component based on the patterns found in the previous step
### Translation 1: Translate `COMMA JOIN` using translatorCommaJoin.py
1. This is the function to Translate Legacy SQL COMMA JOIN into UNION ALL. Specifically, it examines what columns do not exist, and it put `cast(null as a datatype)` as a placeholder.
2. **Prerequisite:** 
   1. The input SQL should only include column names in the select statement, without functions. 
   2. No WHERE clause in the input SQL. 
   3. No comma in the end of column list.
3. Please replace the `legacy_sql` with your SQL string.
4. Sample input: `sample_sql/sample1.sql`
5. Sample output: `sample_sql/sample2.sql`

### Translation 2: Translate `FLATTEN`
1. `FLATTEN` is replaced by `UNNEST`
2. Multiple parentheses need to be replaced by comma between each FLATTEN layer
3. Give each unnested column an alias
4. Replace all unnested column with the alias in the rest of the query
5. Only need to unnest array columns and using original column names as non-array column reference
6. Sample input: `sample_sql/sample3.sql` 
7. Sample input: `sample_sql/sample4.sql`

### Translation 3: Translate `Time Decorator`
1. Legacy SQL example: 
   ```
   from  [my_project:my_dataset.mytable_20230401@1234567891-1234567895]```
2. Standard SQL example:
   ```
   FROM
   APPENDS(TABLE `my_project.my_dataset.mytable_20230401`, TIMESTAMP_MILLIS(1234567891), TIMESTAMP_MILLIS(1234567895+1));
   ```
3. Note: "Upper bound is exclusive, so you need to add +1. Feature is currently under preview, so some issues might exist, please plan accordingly."

### Translation 4: Translate `GROUP_CONCAT`
1. Legacy SQL example:
      ```
      SELECT
      GROUP_CONCAT(x)
      FROM (
      SELECT
      'a"b' AS x),
      (
      SELECT
      'cd' AS x);```
2. Standard SQL example:
      ```
      CREATE TEMP FUNCTION quote_string(x STRING)
      AS (IF(STRPOS(x, '"')=0, x, CONCAT('"', REPLACE(x, '"', '""'), '"')));
      SELECT
      STRING_AGG(quote_string(x))
      FROM (
      SELECT
      'a"b' AS x
      UNION ALL
      SELECT
      'cd' AS x);```

### Translation 5: Translate `TABLE_DATE_RANGE`
1. Legacy SQL example:
      ```
      SELECT
      ROUND(AVG(col1),1) AS AVG_COL_1
      FROM
      TABLE_DATE_RANGE([mydataset.mytable_],
      TIMESTAMP("2016-05-01"),
      TIMESTAMP("2016-05-09"))
      ```
2. Standard SQL example:
      ```
      SELECT
      ROUND(AVG(col1),1) AS AVG_COL_1
      FROM
      `mydataset.mytable_*`
      WHERE
      _TABLE_SUFFIX BETWEEN '20160501' AND '20160509'
      ```
### Translation 6: Translate other basic functions
Based on the recommendations on Step1, translate the following items:
1. Comma Join.
   * See above steps
2. Flatten
   * See above steps
3. Time Decorator
   * See above steps
4. Legacy Table Name
   * [project:dataset.table] --> \`project.dataset.table\`
5. x % y
   * x % y --> MOD(x,y)
6. LEFT()
   * LEFT(s, len) --> SUBSTR(s, 0, len)
7. RIGHT()
   * RIGHT(s, len) --> SUBSTR(s, -len)
8. CONTAINS()
   * CONTAINS 'string' --> LIKE '%string%'
9. DATE()
   * DATE(xxxxxxxx) --> DATE(xxxx-xx-xx)
10. DATE_ADD()
    * DATE_ADD(TIMESTAMP('20230211'), 8, 'HOUR') --> DATE_ADD(TIMESTAMP('2023-02-11'), INTERVAL 8 HOUR)
11. TIMESTAMP(UTC_USEC_TO_HOUR(TIMESTAMP_TO_USEC()))
    * TIMESTAMP(UTC_USEC_TO_HOUR(TIMESTAMP_TO_USEC(event_time))) --> TIMESTAMP_TRUNC(event_time, HOUR, "UTC")
12. UTC_USEC_TO_HOUR()
    * UTC_USEC_TO_HOUR(123456789) --> UNIX_MICROS(TIMESTAMP_TRUNC(TIMESTAMP_MICROS(123456789), HOUR))
13. INTEGER()
    * INTEGER(x) --> SAFE_CAST(x as INT64)
14. DATEDIFF()
    * DATEDIFF(t1, t2) --> TIMESTAMP_DIFF(t1, t2, DAY)
15. STRFTIME_UTC_USEC()
    * STRFTIME_UTC_USEC(t, fmt)	--> FORMAT_TIMESTAMP(fmt, t)
16. UTC_USEC_TO_DAY()
    * UTC_USEC_TO_DAY(t) --> TIMESTAMP_TRUNC(t, DAY)
17. IS_NULL()
    * IS_NULL(x) --> x IS NULL
18. REGEXP_MATCH()
    * REGEXP_MATCH() --> REGEXP_CONTAINS()
19. USEC_TO_TIMESTAMP()
    * USEC_TO_TIMESTAMP() --> TIMESTAMP_MICROS()
20. TIMESTAMP_TO_USEC()
    * TIMESTAMP_TO_USEC() --> UNIX_MICROS()
21. SEC_TO_TIMESTAMP()
    * SEC_TO_TIMESTAMP() --> TIMESTAMP_SECONDS()
22. TIMESTAMP_TO_MSEC()
    * TIMESTAMP_TO_MSEC() --> UNIX_MILLIS()
23. INSTR()
    * INSTR --> STRPOS
24. GROUP_CONCAT_UNQUOTED()
    * GROUP_CONCAT_UNQUOTED --> STRING_AGG()
25. GROUP_CONCAT()
    * See above steps
26. NOW()
    * NOW() --> CURRENT_TIMESTAMP()
27. UNIQUE() 
    * UNIQUE()--> DISTINCT()
28. TABLE_DATE_RANGE()
    * See above steps
29. hash()
    * hash --> FARM_FINGERPRINT
    * Note that samples using the new sharding function won’t be identical; 
30. STRING()
    * STRING(bool_column) --> CAST(CAST(bool_column AS INT64) AS STRING)



## Step 3: Validation
Based on validation result, adjusting the translation accordingly.

## References:
[Migrating to GoogleSQL](https://cloud.google.com/bigquery/docs/reference/standard-sql/migrating-from-legacy-sql)