# Vertica to BigQuery Migration UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Vertica. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`vertica_to_bq` dataset for reference in queries.

## UDFs

* [next_day](#next_daydate_value-date-day_name-string)


## Documentation


### [next_day(date_value DATE, day_name STRING)](next_day.sql)
This function calculates the next occurance of a specific day based off an initial date value. The function is based off of the Vertica (function)[https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/Date-Time/NEXT_DAY.htm] 
```sql
   SELECT bqutil.vertica_to_bq.next_day(Date('2016-04-29'),'mon'); 
   SELECT bqutil.vertica_to_bq.next_day(Date('2016-04-29'),'fri');

2016-05-02, 2016-05-06
```
