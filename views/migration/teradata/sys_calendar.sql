/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Emulation for Teradata's SYS_CALENDAR.CALENDAR table */
CREATE OR REPLACE VIEW SYS_CALENDAR.CALENDAR (
    /* the date */
    calendar_date,
    /* 1-7 day of week */
    day_of_week,
    /* 1-n day of month */
    day_of_month,
    /* 1-365 day of year */
    day_of_year,
    /* 1-n day since the beginning of the calendar */
    day_of_calendar,
    /* 1 if the date is a Monday and the first Monday of the month, 2 if it's a
     * Monday and the second Monday of the month, 4 for the fourth Tuesday, etc.
     */
    weekday_of_month,
    /* 0-n week of the month. The first partial week is zero.
     * The first full week is 1. If the month starts on Sunday, there is no
     * week 0. */
    week_of_month,
    /* 0-53 week of the year */
    week_of_year,
    /* 0-n week of the calendar */
    week_of_calendar,
    /* 1-3 month of the quarter */
    month_of_quarter,
    /* 1-12 month */
    month_of_year,
    /* 1-n month since the beginning of the calendar */
    month_of_calendar,
    /* 1-4 quarter of the year */
    quarter_of_year,
    /* 1-n quarter since the beginning of the calendar */
    quarter_of_calendar,
    /* 1-n the year */
    year_of_calendar
  ) AS SELECT
    r AS calendar_date,
    EXTRACT(DAYOFWEEK FROM r) AS day_of_week,
    EXTRACT(DAY FROM r) as day_of_month,
    EXTRACT(DAYOFYEAR FROM r) AS day_of_year,
    DATE_DIFF(r, DATE(1, 1, 1), DAY) AS day_of_calendar,
    CAST(FLOOR((EXTRACT(DAY FROM r) - 1) / 7) AS INT64) + 1 AS weekday_of_month,
    EXTRACT(WEEK FROM r) - EXTRACT(WEEK FROM DATE_TRUNC(r, MONTH))
        + IF(EXTRACT(DAYOFWEEK FROM DATE_TRUNC(r, MONTH)) = 1, 1, 0)
        AS week_of_month,
    EXTRACT(WEEK FROM r) as week_of_year,
    DATE_DIFF(r, DATE(1, 1, 1), WEEK) as week_of_calendar,
    MOD(EXTRACT(MONTH FROM r) - 1, 3) + 1 as month_of_quarter,
    EXTRACT(MONTH FROM r) AS month_of_year,
    DATE_DIFF(r, DATE(1, 1, 1), MONTH) AS month_of_calendar,
    EXTRACT(QUARTER FROM r) AS quarter_of_year,
    4 * EXTRACT(YEAR FROM r) + EXTRACT(QUARTER FROM r) AS quarter_of_calendar,
    EXTRACT(YEAR FROM r) as year_of_calendar
  FROM
    UNNEST(GENERATE_DATE_ARRAY('1900-1-1', '2100-1-1', INTERVAL 1 DAY)) AS r;
