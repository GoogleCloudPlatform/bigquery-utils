# Vertica to BigQuery Migration UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Vertica.

## UDFs

* [next_day](#next_daydate_value-date-day_name-string)
* [regexp_extract_with_occurrence](#regexp_extract_with_occurrencehaystack-string-needle-string-position-int64-occurrence-int64)
* [regexp_extract_with_occurrence_and_flags](#regexp_extract_with_occurrence_and_flagshaystack-string-needle-string-position-int64-occurrence-int64-mode-string)




## Documentation

### [next_day(date_value DATE, day_name STRING)](../common/next_day.sql)
This function calculates the next occurance of a specific day based off an initial date value. The function is based off of the Vertica [function](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/Date-Time/NEXT_DAY.htm) 
```sql
   SELECT next_day(Date('2016-04-29'),'mon'); 
   SELECT next_day(Date('2016-04-29'),'fri');

2016-05-02, 2016-05-06
```


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


