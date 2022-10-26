# Teradata Views

This directory contains views that mimic the behavior of tables / views in Teradata.

## SYS_CALENDAR.CALENDAR

[sys_calendar.sql](sys_calendar.sql) - Emulation for the SYS_CALENDAR.CALENDAR table from Teradata. The columns of this view are described below.

column|type|description
------|----|------------
calendar_date | DATE | the date
day_of_week | INT64 | 1-7 day of week
day_of_month | INT64 | 1-n day of month
day_of_year | INT64 | 1-365 day of year
day_of_calendar | INT64 | 1-n day since the beginning of the calendar
weekday_of_month | INT64 | 1 if the date is a Monday and the first Monday of the month, 2 if it's a Monday and the second Monday of the month, 4 for the fourth Tuesday, etc.
week_of_month | INT64 | 0-n week of the month. The first partial week is zero. The first full week is 1. If the month starts on Sunday, there is no week 0.
week_of_year | INT64 | 0-53 week of the year
week_of_calendar | INT64 | 0-n week of the calendar
month_of_quarter | INT64 | 1-3 month of the quarter
month_of_year | INT64 | 1-12 month
month_of_calendar | INT64 | 1-n month since the beginning of the calendar
quarter_of_year | INT64 | 1-4 quarter of the year
quarter_of_calendar | INT64 | 1-n quarter since the beginning of the calendar
year_of_calendar | INT64 | 1-n the year
