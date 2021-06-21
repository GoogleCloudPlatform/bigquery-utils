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

CREATE OR REPLACE FUNCTION cw_udf.cw_instr4(source STRING, search STRING, position INT64, ocurrence INT64) RETURNS INT64 AS (
   case when position > 0 then
      (SELECT length(string_agg(x, '')) + length(search) * (ocurrence - 1) + position
       FROM unnest(split(substr(source, position), search)) as x with offset occ
       WHERE occ < ocurrence)
   when position < 0 then
     (SELECT length(string_agg(x, '')) + length(search) * (count(occ)) - (length(search) - 1)
       FROM unnest(array_reverse(split(substr(source, 1, length(source) + position + length(search)), search))) as x with offset occ
       WHERE occ >= ocurrence)
   else
      0
   end
);

CREATE OR REPLACE FUNCTION cw_udf.cw_initcap(s STRING) RETURNS STRING AS (
  (SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(part, 1, 1)), LOWER(SUBSTR(part, 2))), ' ')
   FROM UNNEST(SPLIT(s, ' ')) AS part)
);

CREATE OR REPLACE FUNCTION cw_udf.cw_otranslate(s STRING, key STRING, value STRING) RETURNS STRING AS (
  (SELECT
     STRING_AGG(
       IFNULL(
         (SELECT SPLIT(value, '')[SAFE_OFFSET((
            SELECT o2 FROM UNNEST(SPLIT(key, '')) AS k WITH OFFSET o2
            WHERE k = c ORDER BY o2 ASC LIMIT 1))]
         ),
         c),
       '' ORDER BY o1)
   FROM UNNEST(SPLIT(s, '')) AS c WITH OFFSET o1
  )
);


/* Formats the interval as 'day hour:minute:second */
CREATE OR REPLACE FUNCTION cw_udf.cw_stringify_interval (x INT64) RETURNS STRING AS
(
    concat(
       CASE WHEN x >= 0 THEN '+' ELSE '-' END,
       format('%04d', div(abs(x), 24*60*60)), ' ', -- day
       format('%02d', div(mod(abs(x), 24*60*60), 60*60)), ':', -- hour
       format('%02d', div(mod(abs(x), 60*60), 60)), ':', -- minute
       format('%02d', mod(abs(x), 60)) -- second
    )
);

/* Internal function */
CREATE OR REPLACE FUNCTION cw_udf.cw_regex_mode (mode STRING) RETURNS STRING
LANGUAGE js AS """
  var m = '';
  if (mode == 'i' || mode == 'm')
    m += mode;
  else if (mode == 'n')
    m += 's';
  m += 'g';
  return m;
""";

/* Implements regexp_substr/4 (haystack, needle, position, occurence) */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_substr_4 (h STRING, n STRING, p INT64, o INT64) RETURNS STRING AS
(
    regexp_extract_all(substr(h, p), n)[safe_ordinal(o)]
);

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_substr_generic (str STRING, regexp STRING, p INT64, o INT64, mode STRING) RETURNS STRING
LANGUAGE js AS """
  if (str == null || regexp == null || p == null || o == null || mode == null) return null;
  var r = new RegExp(regexp, mode);
  var m = str.substring(p - 1).match(r);
  if (m == null) return null;
  return m[o - 1];
""";

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_substr_5 (h STRING, n STRING, p INT64, o INT64, mode STRING) RETURNS STRING AS
(
    cw_udf.cw_regexp_substr_generic(h, n, p, o, cw_udf.cw_regex_mode(mode))
);

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_instr_2(haystack STRING, needle STRING) RETURNS INT64 AS (
  CASE WHEN REGEXP_CONTAINS(haystack, needle) THEN
    LENGTH(REGEXP_REPLACE(haystack, CONCAT('(.*?)', needle, '(.*)'), '\\1')) + 1
  WHEN needle IS NULL OR haystack IS NULL THEN
    NULL
  ELSE 0
  END
);

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_instr_3(haystack STRING, needle STRING, start INT64) RETURNS INT64 AS (
  CASE WHEN REGEXP_CONTAINS(substr(haystack, start), needle) THEN
    LENGTH(REGEXP_REPLACE(substr(haystack, start), CONCAT('(.*?)', needle, '(.*)'), '\\1')) + 1 + start
  WHEN needle IS NULL OR haystack IS NULL or start IS NULL THEN
    NULL
  ELSE 0
  END
);

/* Implements regexp_instr/4 (haystack, needle, position, occurence) */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_instr_4 (haystack STRING, regexp STRING, p INT64, o INT64) RETURNS INT64
LANGUAGE js AS """
  if (haystack == null || regexp == null || o == null) return null;
  p = p -1;
  o = o -1;
  var str = haystack.substring(p);
  var a;
  var r = new RegExp(regexp, 'g');
  var count = 0;
  while ((a = r.exec(str)) !== null && count++ < o) {
    }
  if (a == null)
     return 0;
  return a.index+1+Number(p);
""";

/* Implements regexp_instr/6 (haystack, needle, position, occurence, returnopt)
    // else:
    // case 'c': default
    // case 'l': not supported
    // case 'x': not supported
*/
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_instr_generic (haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64, mode STRING) RETURNS INT64
LANGUAGE js AS """
  if (haystack == null || regexp == null || p == null || o == null || returnopt == null || mode == null) return null;
  p = p -1;
  o = o -1;
  var str = haystack.substring(p);
  var a;
  var r = new RegExp(regexp, mode);
  var count = 0;
  while ((a = r.exec(str)) !== null && count++ < o) {
    }
  if (a == null)
     return 0;
  if (returnopt == 0)
    return a.index+1+Number(p);
  else
    return a.index+1+Number(p)+a[0].length;
""";

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_instr_6 (haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64, mode STRING) RETURNS INT64 AS
(
   cw_udf.cw_regexp_instr_generic(haystack, regexp, p, o, returnopt, cw_udf.cw_regex_mode(mode))
);

/* Generic regexp_replace, which is the 6-args version with regexp_mode already decoded */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_replace_generic (haystack STRING, regexp STRING, replacement STRING, offset INT64, occurrence INT64, mode STRING) RETURNS STRING
LANGUAGE js AS """
  if (haystack == null || regexp == null || replacement == null || offset == null || occurrence == null || mode == null) return null;
  replacement = replacement.replace('\\\\', '$');
  let regExp = new RegExp(regexp, mode);
  if (occurrence == 0)
      return haystack.replace(regExp, replacement);
  const start = offset - 1;
  const index = occurrence - 1;
  let relevantString = haystack.substring(start);
  let a, count = 0;
  while ((a = regExp.exec(relevantString)) !== null && count++ < index) {
      relevantString = relevantString.substr(regExp.lastIndex);
      regExp.lastIndex = 0;
  }
  if (a == null) return haystack;
  const prefix = haystack.substr(0, haystack.length - relevantString.length);
  if (mode.indexOf('g') >= 0) {
      mode = mode.replace('g', '');
      regExp = new RegExp(regexp, mode);
  }
  return prefix + relevantString.replace(regExp, replacement);
""";

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_replace_4 (haystack STRING, regexp STRING, replacement STRING, offset INT64) RETURNS STRING AS
(
   cw_udf.cw_regexp_replace_generic(haystack, regexp, replacement, offset, 0, cw_udf.cw_regex_mode(''))
);

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_replace_5 (haystack STRING, regexp STRING, replacement STRING, offset INT64, occurrence INT64) RETURNS STRING AS
(
   cw_udf.cw_regexp_replace_generic(haystack, regexp, replacement, offset, occurrence, cw_udf.cw_regex_mode(''))
);

CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_replace_6 (haystack STRING, regexp STRING, replacement STRING, p INT64, o INT64, mode STRING) RETURNS STRING AS
(
   cw_udf.cw_regexp_replace_generic(haystack, regexp, replacement, p, o, cw_udf.cw_regex_mode(''))
);

/* Implements regexp_instr/5 (haystack, needle, position, occurence, returnopt) */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_instr_5 (haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64) RETURNS INT64 AS
(
   cw_udf.cw_regexp_instr_generic(haystack, regexp, p, o, returnopt, cw_udf.cw_regex_mode(''))
);

/* Like presto ARRAY_MIN */
CREATE OR REPLACE FUNCTION cw_udf.cw_array_min(arr ANY TYPE) AS (
   ( SELECT MIN(x) FROM UNNEST(arr) AS x )
);

/* Similar to MEDIAN in Teradata */
CREATE OR REPLACE FUNCTION cw_udf.cw_array_median(arr ANY TYPE) RETURNS FLOAT64 AS (
  ( SELECT PERCENTILE_CONT(x, 0.5) OVER() FROM UNNEST(arr) AS x LIMIT 1 )
);

/* Like presto ARRAY_MAX */
CREATE OR REPLACE FUNCTION cw_udf.cw_array_max(arr ANY TYPE) AS (
   ( SELECT MAX(x) FROM UNNEST(arr) AS x )
);

/* Like presto ARRAY_DISTINCT */
CREATE OR REPLACE FUNCTION cw_udf.cw_array_distinct(arr ANY TYPE) AS (
   ARRAY( SELECT DISTINCT x FROM UNNEST(arr) AS x )
);

/* Returns the date of the first weekday (second arugment) that is later than the date specified by the first argument */
CREATE OR REPLACE FUNCTION cw_udf.cw_next_day(date_value DATE, day_name STRING) RETURNS DATE AS (
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

/* Emulate teradata NVP function - extract a value from a key-value separated string */
CREATE OR REPLACE FUNCTION cw_udf.cw_td_nvp(haystack STRING, needle STRING, pairsep STRING, valuesep STRING, occurence INT64) RETURNS STRING AS (
   NULLIF(ARRAY(SELECT kv[SAFE_OFFSET(1)] FROM (
        SELECT SPLIT(pairs, valuesep) AS kv, o FROM UNNEST(SPLIT(haystack, pairsep)) AS pairs WITH OFFSET o
   ) t WHERE kv[SAFE_OFFSET(0)] = needle ORDER BY o ASC)[SAFE_ORDINAL(occurence)], '')
);

/* Emulate Presto from_base function - convert string from given base to decimal */
CREATE OR REPLACE FUNCTION cw_udf.cw_from_base(number STRING, base INT64) RETURNS INT64 AS (
    (WITH chars AS (
        SELECT IF(ch >= 48 AND ch <= 57, ch - 48, IF(ch >= 65 AND ch <= 90, ch - 65 + 10, ch - 97 + 10)) pos, offset + 1 AS idx
        FROM UNNEST(TO_CODE_POINTS(number)) AS ch WITH OFFSET
    )
    SELECT SAFE_CAST(SUM(pos*CAST(POW(base, CHAR_LENGTH(number) - idx) AS NUMERIC)) AS INT64) from_base FROM chars)
);

/* Emulate Presto to_base function - convert decimal number to number with given base */
CREATE OR REPLACE FUNCTION cw_udf.cw_to_base(number INT64, base INT64) RETURNS STRING AS (
    (WITH chars AS (
        SELECT MOD(CAST(FLOOR(ABS(number)/POW(base, (FLOOR(LOG(ABS(number))/LOG(base)) + 1) - idx)) AS INT64), base) ch, idx
            from UNNEST(GENERATE_ARRAY(1, CAST(FLOOR(LOG(ABS(number))/LOG(base)) AS INT64) + 1)) idx
    )
    SELECT CONCAT(CASE WHEN number < 0 THEN '-' ELSE '' END,
        CODE_POINTS_TO_STRING(ARRAY_AGG(if(ch >= 0 AND ch <= 9, ch + 48, ch + 97 - 10) ORDER BY idx))) to_base FROM chars)
);

/* Implements Presto ARRAYS_OVERLAP */
CREATE OR REPLACE FUNCTION cw_udf.cw_array_overlap(x ANY TYPE, y ANY TYPE) RETURNS BOOL AS(
    CASE WHEN EXISTS(SELECT 1 FROM UNNEST(ARRAY_CONCAT(x,y)) as z WHERE z IS NULL) THEN NULL
         ELSE EXISTS(SELECT 1 FROM UNNEST(x) as u JOIN UNNEST(y) as v ON u=v)
    END
);

/* Implements Snowflake ARRAY_COMPACT */
CREATE OR REPLACE FUNCTION cw_udf.cw_array_compact(a ANY TYPE) AS (
    ARRAY(SELECT v FROM UNNEST(a) v WHERE v IS NOT NULL)
);

/* Kludge for interval translation - for now day->sec only! */
CREATE OR REPLACE FUNCTION cw_udf.cw_runtime_parse_interval_seconds(ival STRING) RETURNS INT64 AS (
    CASE WHEN ival IS NULL THEN NULL
         WHEN ARRAY_LENGTH(SPLIT(ival,' ')) <> 2 THEN NULL
         WHEN SPLIT(ival,' ')[OFFSET(1)] NOT IN ('day','DAY') THEN NULL
         ELSE 86400 * SAFE_CAST(SPLIT(ival,' ')[OFFSET(0)] AS INT64) END
);

/* url encode a string */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_encode(path STRING) RETURNS STRING
LANGUAGE js AS """
  if (path == null) return null;
  try {
    return encodeURIComponent(path);
  } catch (e) {
    return path;
  }
""";

/* url decode a string */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_decode(path STRING) RETURNS STRING
LANGUAGE js AS """
  if (path == null) return null;
  try {
    return decodeURIComponent(path);
  } catch (e) {
    return path;
  }
""";

/* Extract the host from a url, return "" (empty string) if no host is found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_host(url STRING) RETURNS STRING AS (
  NET.HOST(url)
);

/* Extract the protocol from a url, return "" (empty string) if no protocol is found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_protocol(url STRING) RETURNS STRING AS (
  (WITH a AS (
   SELECT STRPOS(url, "://") AS v
  ) SELECT IF(a.v <= 0, "", SUBSTR(url,1,a.v-1)) FROM a)
);

/* Extract the path from a url, returns "" (empty string) if no path is found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_path(url STRING) RETURNS STRING
LANGUAGE js AS """
  var queryPos = url.indexOf('?');
  if (queryPos >= 0)
    url = url.slice(0,queryPos);
  else {
    var fragPos = url.indexOf('#');
    if (fragPos >= 0)
      url = url.slice(0,fragPos);
  }
  var protPos = url.indexOf("//");
  if (protPos >= 0)
    url = url.slice(protPos+2);
  var pathPos = url.indexOf("/");
  if (pathPos < 0)
    return "";
  url = url.slice(pathPos);
  return url;
""";

/* Extract the port from a url, returns null if no port is found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_port(url STRING) RETURNS INT64
LANGUAGE js AS """
  var protPos = url.indexOf("//");
  if (protPos >= 0)
    url = url.slice(protPos+2);
  var pathPos = url.indexOf("/");
  if (pathPos > 0)
    url = url.slice(0,pathPos);
  var portPos = url.lastIndexOf(":");
  if (portPos < 0)
    return null;
  url = url.slice(portPos+1);
  var port = parseInt(url);
  return isNaN(port) ? null : port;
""";

/* Extract the query from a url, returns "" (empty string) if no query is found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_query(url STRING) RETURNS STRING AS (
  COALESCE(SUBSTR(SPLIT(REGEXP_EXTRACT(url, '[^\\?]+(\\?.*)?'),'#')[OFFSET(0)],2),'')
);

/* Extract the fragment from a url, returns "" (empty string) if no fragment is found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_fragment(url STRING) RETURNS STRING AS (
  COALESCE(REGEXP_EXTRACT(url,'#(.+)'),'')
);

/* Extract the value of a query param from a url, returns null if the parameter isn't found. */
CREATE OR REPLACE FUNCTION cw_udf.cw_url_extract_parameter(url STRING, pname STRING) RETURNS STRING AS (
  SPLIT(REGEXP_EXTRACT(url, CONCAT('[?&]',pname,'=([^&]+).*$')),'#')[OFFSET(0)]
);

/* Returns the first substring matched by the regular expression `regexp` in `str`. */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_extract(str STRING, regexp STRING) RETURNS STRING
LANGUAGE js AS """
  var r = new RegExp(regexp);
  var a = str.match(r);
  return a[0];
""";

/* Finds the first occurrence of the regular expression `regexp` in `str` and returns the capturing group number `groupn` */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_extract_n(str STRING, regexp STRING, groupn INT64) RETURNS STRING
LANGUAGE js AS """
  var r = new RegExp(regexp);
  var a = str.match(r);
  if (!a) return null;
  var groupn = groupn || 0;
  if (groupn >= a.length) {
    throw new Error("Pattern has " + (a.length-1) + " groups. Cannot access group "+groupn);
  }
  return a[groupn];
""";

/* Returns the substring(s) matched by the regular expression `regexp` in `str`. */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_extract_all(str STRING, regexp STRING) RETURNS ARRAY<STRING>
LANGUAGE js AS """
  var r = new RegExp(regexp, "g");
  return str.match(r);
""";

/* Finds all occurrences of the regular expression `regexp` in `str` and returns the capturing group number `groupn`. */
CREATE OR REPLACE FUNCTION cw_udf.cw_regexp_extract_all_n(str STRING, regexp STRING, groupn INT64) RETURNS ARRAY<STRING>
LANGUAGE js AS """
  var r = new RegExp(regexp, 'g');
  var o = [];
  while ((a = r.exec(str)) !== null) {
    if (groupn >= a.length) {
      throw new Error("Pattern has " + (a.length-1) + " groups. Cannot access group "+groupn);
    }
    o.push(a[groupn]);
  }
  return o;
""";

/* Determine if value exists in json (a string containing a JSON array). */
CREATE OR REPLACE FUNCTION cw_udf.cw_json_array_contains_str(json STRING, needle STRING) RETURNS BOOL
LANGUAGE js AS """
  if (json == null || needle == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.indexOf(needle) >= 0;
""";

/* Same as cw_json_array_contains_str(STRING, STRING) UDF but with needle = number */
CREATE OR REPLACE FUNCTION cw_udf.cw_json_array_contains_num(json STRING, needle FLOAT64) RETURNS BOOL
LANGUAGE js AS """
  if (json == null || needle == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.indexOf(needle) >= 0;
""";

/* Same as cw_json_array_contains_str(STRING, STRING) UDF but with needle = boolean */
CREATE OR REPLACE FUNCTION cw_udf.cw_json_array_contains_bool(json STRING, needle BOOL) RETURNS BOOL
LANGUAGE js AS """
  if (json == null || needle == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.indexOf(needle) >= 0;
""";

/* Returns the element at the specified index into the json_array. The index is zero-based */
CREATE OR REPLACE FUNCTION cw_udf.cw_json_array_get(json STRING, loc FLOAT64) RETURNS STRING
LANGUAGE js AS """
  if (json == null || loc == null)
    return null;
  var parsedJson = JSON.parse(json);
  if (loc < 0)
    loc = parsedJson.length + loc
  if (loc < 0 && loc >= parsedJson.length)
    return null
  return JSON.stringify(parsedJson[loc]);
""";

/* Returns the array length of json (a string containing a JSON array) */
CREATE OR REPLACE FUNCTION cw_udf.cw_json_array_length(json STRING) RETURNS INT64
LANGUAGE js AS """
  if (json == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.length;
""";



/** Emulate Vertica LOWERB function, which lowercases only ASCII characters within a given string. */
CREATE OR REPLACE FUNCTION cw_udf.cw_lower_case_ascii_only(str STRING) RETURNS STRING AS (
    (WITH chars AS (
        SELECT ch FROM UNNEST(TO_CODE_POINTS(str)) AS ch
    )
    SELECT CONCAT(CODE_POINTS_TO_STRING(ARRAY_AGG(if(ch >= 65 and ch <= 90, ch + 32, ch)))) from chars
));

/** Emulate Vertica SUBSTRB function, which treats the multibyte character string as a string of octets (bytes). */
CREATE OR REPLACE FUNCTION cw_udf.cw_substrb(str STRING, startpos INT64 /* 1-based */, extent INT64 /* 1-based */) RETURNS STRING AS (
    (WITH octets AS (
        SELECT oct FROM UNNEST(TO_CODE_POINTS(CAST(str as bytes))) AS oct WITH OFFSET off WHERE (off+1) >= startpos and (off+1) < startpos+extent ORDER BY off
    )
    SELECT CAST(CODE_POINTS_TO_BYTES(ARRAY_AGG(oct)) AS STRING) FROM octets
));
