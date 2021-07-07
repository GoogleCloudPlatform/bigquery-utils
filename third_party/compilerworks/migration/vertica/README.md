# Vertica to BigQuery Migration UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Vertica. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`vertica` dataset for reference in queries.

## UDFs

* [regexp_extract_with_occurrence](#regexp_extract_with_occurrencehaystack-string-needle-string-position-int64-occurrence-int64)
* [regexp_extract_with_occurrence_and_flags](#regexp_extract_with_occurrence_and_flagshaystack-string-needle-string-position-int64-occurrence-int64-mode-string)


## Documentation


### [regexp_extract_with_occurrence(haystack STRING, needle STRING, position INT64, occurrence INT64)](regexp_extract_with_occurrence.sql)
The regexp_extract_with_occurrence function extracts the nth occurrence of a regex match within a string at a certain specified position. The analogous Vertica function is [regex_substr](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/RegularExpressions/REGEXP_SUBSTR.htm)
```sql
Select regexp_substr_with_occurrence ("Try `function(x)` or `function(y)`", "`(.+?)`", 2, 2);
Select regexp_substr_with_occurrence ("Try `function(x)` or `function(y)`", "`(.+?)`", 12, 2);
Select regexp_substr_with_occurrence ("Try `function(x)` or `function(y)`", "`(.+?)`", 20, 1)

function(x), or ,function(y)
```


### [regexp_extract_with_occurrence_and_flags(haystack STRING, needle STRING, position INT64, occurrence INT64, mode STRING)](regexp_extract_with_occurrence_and_flags.sql)
The regexp_extract_with_occurrence_and_flag function extracts the nth occurrence of a regex match within a string at a certain specified position using a specific regex flag. The analogous Vertica function is [regex_substr](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/RegularExpressions/REGEXP_SUBSTR.htm). Regex flags information can be found [here](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions#advanced_searching_with_flags)
```sql
Select regexp_extract_with_occurrence_and_flags ("Blah AABBBC", "ab+c", 3, 1, 'i')
Select regexp_extract_with_occurrence_and_flags ("Blah AABBBC", "ab+c", 3, 1, '')

ABBBC, null
```

