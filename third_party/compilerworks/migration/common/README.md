# Migration UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions across various data warehouses. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`common` dataset for reference in queries.

## UDFs

* [next_day](#next_daydate_value-date-day_name-string)


## Documentation


### [next_day(date_value DATE, day_name STRING)](next_day.sql)
This function calculates the next occurance of a specific day based off an initial date value. The function is based off of the Vertica [function](https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Functions/Date-Time/NEXT_DAY.htm), Oracle [function](https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions093.htm) and IBM [function](https://www.ibm.com/docs/en/informix-servers/14.10?topic=functions-next-day-function).
```sql
   SELECT bqutil.common.next_day(Date('2016-04-29'),'mon'); 
   SELECT bqutil.common.next_day(Date('2016-04-29'),'fri');

2016-05-02, 2016-05-06
```
