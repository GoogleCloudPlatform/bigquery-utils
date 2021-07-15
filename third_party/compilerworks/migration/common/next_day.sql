/*
 * Copyright 2013-2020 CompilerWorks
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


 /* Returns the date of the first weekday (second arugment) that is later than the date specified by the first argument */
CREATE OR REPLACE FUNCTION cw_next_day(date_value DATE, day_name STRING) RETURNS DATE AS (
    (WITH t AS (SELECT
       CASE lower(substr(day_name, 1, 3))
       WHEN 'sun' THEN 1
       WHEN 'mon' THEN 2
       WHEN 'tue' THEN 3
       WHEN 'wed' THEN 4
       WHEN 'thu' THEN 5
       WHEN 'fri' THEN 6
       WHEN 'sat' THEN 7
       ELSE NULL END AS tgt,
       extract(dayofweek FROM date_value) AS src
    ) SELECT CASE WHEN src < tgt
             THEN date_add(date_value, INTERVAL (tgt - src) DAY)
             ELSE date_add(date_value, INTERVAL (tgt + 7 - src) DAY) END
             FROM t)
);
