# Vertica UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Vertica. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`ve` dataset for reference in queries.

## UDFs

* [lowerb](#lowerbstring-string)
* [substrb](#substrbstring-string-position-int64-length-int64)
* [upperb](#upperbstring-string)


## Documentation

### [lowerb(string STRING)](lowerb.sqlx)
This function lower cases all ascii characters in string which imitates the Vertica function [lowerb](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/String/LOWERB.htm). Note that BigQuery's [lower](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#lower) will lower case all non ascii characters.
```sql
   SELECT ve.lowerb('ÉTUDIANT'), lower('ÉTUDIANT'); 
   SELECT ve.lowerb('Student'), lower('Student');

Étudiant, étudiant, student, student
```


### [substrb(string STRING, position INT64, length INT64)](substrb.sqlx)
This function treats a string as an array of bytes and takes the substring based on the octets. The function imitates the Vertica function [substrb](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/String/SUBSTRB.htm). Note that BigQuery's [substr](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#substr) does not treat the string as bytes.

```sql
   SELECT ve.substrb('soupçon', 5, 2), substr('soupçon', 5, 2); 
   SELECT ve.substrb('something', 1, 2), substr('something', 1, 2);

ç, ço, so, so
```

### [upperb(string STRING)](upperb.sqlx)
This function upper cases all ascii characters in string which imitates the Vertica function [lowerb](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/String/UPPERB.htm). Note that BigQuery's [upper](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#upper) will upper case all non ascii characters.
```sql
   SELECT ve.upperb('étudiant'), upper('étudiant'); 
   SELECT ve.upperb('Student'), upper('Student');

éTUDIANT, ÉTUDIANT, STUDENT, STUDENT
```
