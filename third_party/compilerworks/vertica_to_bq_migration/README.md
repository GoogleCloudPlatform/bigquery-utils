# Vertica to BigQuery Migration UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Vertica. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`vertica_to_bq` dataset for reference in queries.

## UDFs

* [lower_case_ascii_only](#lower_case_ascii_onlystring-string-position-int64-length-int64)
* [substrb](#substrbstring-string-position-int64-length-int64)


## Documentation

### [lower_case_ascii_only(string STRING)](lower_case_ascii_only.sql)
This function lower cases all ascii characters in string which imitates the Vertica function (lowerb)[https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/String/LOWERB.htm]. Note that BigQuery's (lower)[https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lower] will lower case all non ascii characters.
```sql
   SELECT bqutil.vertica_to_bq.lower_case_ascii_only('ÉTUDIANT'), lower('ÉTUDIANT'); 
   SELECT bqutil.vertica_to_bq.lower_case_ascii_only('Student'), lower('Student');

Étudiant, étudiant, student, student
```


### [substrb(string STRING, position INT64, part INT64)](substr_of_bytes.sql)
This function treats a string as an array of bytes and takes the substring based on the octets. The function imitates the Vertica function (substrb)[https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/String/SUBSTRB.htm]. Note that BigQuery's (substr)[https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr] does not treat the string a bytes.

```sql
   SELECT bqutil.vertica_to_bq.substrb('soupçon', 5, 2), substrb('soupçon', 5, 2); 
   SELECT bqutil.vertica_to_bq.substrb('something', 1, 2), substr('something', 1, 2);

ç, ço, so, so
```
