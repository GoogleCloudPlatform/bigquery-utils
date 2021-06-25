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

CREATE OR REPLACE FUNCTION cw_instr4(source STRING, search STRING, position INT64, ocurrence INT64) RETURNS INT64 AS (
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

CREATE OR REPLACE FUNCTION cw_initcap(s STRING) RETURNS STRING AS (
  (SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(part, 1, 1)), LOWER(SUBSTR(part, 2))), ' ')
   FROM UNNEST(SPLIT(s, ' ')) AS part)
);

CREATE OR REPLACE FUNCTION cw_otranslate(s STRING, key STRING, value STRING) RETURNS STRING AS (
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
CREATE OR REPLACE FUNCTION cw_stringify_interval (x INT64) RETURNS STRING AS
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
CREATE OR REPLACE FUNCTION cw_regex_mode (mode STRING) RETURNS STRING
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
CREATE OR REPLACE FUNCTION cw_regexp_substr_4 (h STRING, n STRING, p INT64, o INT64) RETURNS STRING AS
(
    regexp_extract_all(substr(h, p), n)[safe_ordinal(o)]
);

CREATE OR REPLACE FUNCTION cw_regexp_substr_generic (str STRING, regexp STRING, p INT64, o INT64, mode STRING, g INT64) RETURNS STRING
LANGUAGE js AS """
  if (str == null || regexp == null || p == null || o == null || mode == null) return null;
  var r = new RegExp(regexp, mode);
  var m = str.substring(p - 1).matchAll(r);
  if (m == null) return null;
  var co = 1;
  for(var cm of m) {
    if (co == o) {
      if (g < cm.length) {
        return cm[g];
      } else {
        return null;
      }
    }
    co++;
  }
  return null;
""";

/* Implements regexp_substr/5 (haystack, needle, position, occurence, mode) */
CREATE OR REPLACE FUNCTION cw_regexp_substr_5 (h STRING, n STRING, p INT64, o INT64, mode STRING) RETURNS STRING AS
(
    cw_regexp_substr_generic(h, n, p, o, cw_regex_mode(mode), 0)
);

/* Implements regexp_substr/6 (haystack, needle, position, occurence, mode, captured_subexp) */
CREATE OR REPLACE FUNCTION cw_regexp_substr_6 (h STRING, n STRING, p INT64, o INT64, mode STRING, g INT64) RETURNS STRING AS
(
    cw_regexp_substr_generic(h, n, p, o, cw_regex_mode(mode), g)
);

CREATE OR REPLACE FUNCTION cw_regexp_instr_2(haystack STRING, needle STRING) RETURNS INT64 AS (
  CASE WHEN REGEXP_CONTAINS(haystack, needle) THEN
    LENGTH(REGEXP_REPLACE(haystack, CONCAT('(.*?)', needle, '(.*)'), '\\1')) + 1
  WHEN needle IS NULL OR haystack IS NULL THEN
    NULL
  ELSE 0
  END
);

CREATE OR REPLACE FUNCTION cw_regexp_instr_3(haystack STRING, needle STRING, start INT64) RETURNS INT64 AS (
  CASE WHEN REGEXP_CONTAINS(substr(haystack, start), needle) THEN
    LENGTH(REGEXP_REPLACE(substr(haystack, start), CONCAT('(.*?)', needle, '(.*)'), '\\1')) + 1 + start
  WHEN needle IS NULL OR haystack IS NULL or start IS NULL THEN
    NULL
  ELSE 0
  END
);

/* Implements regexp_instr/4 (haystack, needle, position, occurence) */
CREATE OR REPLACE FUNCTION cw_regexp_instr_4 (haystack STRING, regexp STRING, p INT64, o INT64) RETURNS INT64
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
CREATE OR REPLACE FUNCTION cw_regexp_instr_generic (haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64, mode STRING) RETURNS INT64
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

CREATE OR REPLACE FUNCTION cw_regexp_instr_6 (haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64, mode STRING) RETURNS INT64 AS
(
   cw_regexp_instr_generic(haystack, regexp, p, o, returnopt, cw_regex_mode(mode))
);

/* Generic regexp_replace, which is the 6-args version with regexp_mode already decoded */
CREATE OR REPLACE FUNCTION cw_regexp_replace_generic (haystack STRING, regexp STRING, replacement STRING, offset INT64, occurrence INT64, mode STRING) RETURNS STRING
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

CREATE OR REPLACE FUNCTION cw_regexp_replace_4 (haystack STRING, regexp STRING, replacement STRING, offset INT64) RETURNS STRING AS
(
   cw_regexp_replace_generic(haystack, regexp, replacement, offset, 0, cw_regex_mode(''))
);

CREATE OR REPLACE FUNCTION cw_regexp_replace_5 (haystack STRING, regexp STRING, replacement STRING, offset INT64, occurrence INT64) RETURNS STRING AS
(
   cw_regexp_replace_generic(haystack, regexp, replacement, offset, occurrence, cw_regex_mode(''))
);

CREATE OR REPLACE FUNCTION cw_regexp_replace_6 (haystack STRING, regexp STRING, replacement STRING, p INT64, o INT64, mode STRING) RETURNS STRING AS
(
   cw_regexp_replace_generic(haystack, regexp, replacement, p, o, cw_regex_mode(''))
);

/* Implements regexp_instr/5 (haystack, needle, position, occurence, returnopt) */
CREATE OR REPLACE FUNCTION cw_regexp_instr_5 (haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64) RETURNS INT64 AS
(
   cw_regexp_instr_generic(haystack, regexp, p, o, returnopt, cw_regex_mode(''))
);

/* Like presto ARRAY_MIN */
CREATE OR REPLACE FUNCTION cw_array_min(arr ANY TYPE) AS (
   ( SELECT MIN(x) FROM UNNEST(arr) AS x )
);

/* Similar to MEDIAN in Teradata */
CREATE OR REPLACE FUNCTION cw_array_median(arr ANY TYPE) RETURNS FLOAT64 AS (
  ( SELECT PERCENTILE_CONT(x, 0.5) OVER() FROM UNNEST(arr) AS x LIMIT 1 )
);

/* Like presto ARRAY_MAX */
CREATE OR REPLACE FUNCTION cw_array_max(arr ANY TYPE) AS (
   ( SELECT MAX(x) FROM UNNEST(arr) AS x )
);

/* Like presto ARRAY_DISTINCT */
CREATE OR REPLACE FUNCTION cw_array_distinct(arr ANY TYPE) AS (
   ARRAY( SELECT DISTINCT x FROM UNNEST(arr) AS x )
);

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

/* Emulate teradata NVP function - extract a value from a key-value separated string */
CREATE OR REPLACE FUNCTION cw_td_nvp(haystack STRING, needle STRING, pairsep STRING, valuesep STRING, occurence INT64) RETURNS STRING AS (
   NULLIF(ARRAY(SELECT kv[SAFE_OFFSET(1)] FROM (
        SELECT SPLIT(pairs, valuesep) AS kv, o FROM UNNEST(SPLIT(haystack, pairsep)) AS pairs WITH OFFSET o
   ) t WHERE kv[SAFE_OFFSET(0)] = needle ORDER BY o ASC)[SAFE_ORDINAL(occurence)], '')
);

/* Emulate Presto from_base function - convert string from given base to decimal */
CREATE OR REPLACE FUNCTION cw_from_base(number STRING, base INT64) RETURNS INT64 AS (
    (WITH chars AS (
        SELECT IF(ch >= 48 AND ch <= 57, ch - 48, IF(ch >= 65 AND ch <= 90, ch - 65 + 10, ch - 97 + 10)) pos, offset + 1 AS idx
        FROM UNNEST(TO_CODE_POINTS(number)) AS ch WITH OFFSET
    )
    SELECT SAFE_CAST(SUM(pos*CAST(POW(base, CHAR_LENGTH(number) - idx) AS NUMERIC)) AS INT64) from_base FROM chars)
);

/* Emulate Presto to_base function - convert decimal number to number with given base */
CREATE OR REPLACE FUNCTION cw_to_base(number INT64, base INT64) RETURNS STRING AS (
    (WITH chars AS (
        SELECT MOD(CAST(FLOOR(ABS(number)/POW(base, (FLOOR(LOG(ABS(number))/LOG(base)) + 1) - idx)) AS INT64), base) ch, idx
            from UNNEST(GENERATE_ARRAY(1, CAST(FLOOR(LOG(ABS(number))/LOG(base)) AS INT64) + 1)) idx
    )
    SELECT CONCAT(CASE WHEN number < 0 THEN '-' ELSE '' END,
        CODE_POINTS_TO_STRING(ARRAY_AGG(if(ch >= 0 AND ch <= 9, ch + 48, ch + 97 - 10) ORDER BY idx))) to_base FROM chars)
);

/* Implements Presto ARRAYS_OVERLAP */
CREATE OR REPLACE FUNCTION cw_array_overlap(x ANY TYPE, y ANY TYPE) RETURNS BOOL AS(
    CASE WHEN EXISTS(SELECT 1 FROM UNNEST(ARRAY_CONCAT(x,y)) as z WHERE z IS NULL) THEN NULL
         ELSE EXISTS(SELECT 1 FROM UNNEST(x) as u JOIN UNNEST(y) as v ON u=v)
    END
);

/* Implements Snowflake ARRAY_COMPACT */
CREATE OR REPLACE FUNCTION cw_array_compact(a ANY TYPE) AS (
    ARRAY(SELECT v FROM UNNEST(a) v WHERE v IS NOT NULL)
);

/* Kludge for interval translation - for now day->sec only! */
CREATE OR REPLACE FUNCTION cw_runtime_parse_interval_seconds(ival STRING) RETURNS INT64 AS (
    CASE WHEN ival IS NULL THEN NULL
         WHEN ARRAY_LENGTH(SPLIT(ival,' ')) <> 2 THEN NULL
         WHEN SPLIT(ival,' ')[OFFSET(1)] NOT IN ('day','DAY') THEN NULL
         ELSE 86400 * SAFE_CAST(SPLIT(ival,' ')[OFFSET(0)] AS INT64) END
);

/* url encode a string */
CREATE OR REPLACE FUNCTION cw_url_encode(path STRING) RETURNS STRING
LANGUAGE js AS """
  if (path == null) return null;
  try {
    return encodeURIComponent(path);
  } catch (e) {
    return path;
  }
""";

/* url decode a string */
CREATE OR REPLACE FUNCTION cw_url_decode(path STRING) RETURNS STRING
LANGUAGE js AS """
  if (path == null) return null;
  try {
    return decodeURIComponent(path);
  } catch (e) {
    return path;
  }
""";

/* Extract the host from a url, return "" (empty string) if no host is found. */
CREATE OR REPLACE FUNCTION cw_url_extract_host(url STRING) RETURNS STRING AS (
  NET.HOST(url)
);

/* Extract the protocol from a url, return "" (empty string) if no protocol is found. */
CREATE OR REPLACE FUNCTION cw_url_extract_protocol(url STRING) RETURNS STRING AS (
  (WITH a AS (
   SELECT STRPOS(url, "://") AS v
  ) SELECT IF(a.v <= 0, "", SUBSTR(url,1,a.v-1)) FROM a)
);

/* Extract the path from a url, returns "" (empty string) if no path is found. */
CREATE OR REPLACE FUNCTION cw_url_extract_path(url STRING) RETURNS STRING
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
CREATE OR REPLACE FUNCTION cw_url_extract_port(url STRING) RETURNS INT64
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
CREATE OR REPLACE FUNCTION cw_url_extract_query(url STRING) RETURNS STRING AS (
  COALESCE(SUBSTR(SPLIT(REGEXP_EXTRACT(url, '[^\\?]+(\\?.*)?'),'#')[OFFSET(0)],2),'')
);

/* Extract the fragment from a url, returns "" (empty string) if no fragment is found. */
CREATE OR REPLACE FUNCTION cw_url_extract_fragment(url STRING) RETURNS STRING AS (
  COALESCE(REGEXP_EXTRACT(url,'#(.+)'),'')
);

/* Extract the value of a query param from a url, returns null if the parameter isn't found. */
CREATE OR REPLACE FUNCTION cw_url_extract_parameter(url STRING, pname STRING) RETURNS STRING AS (
  SPLIT(REGEXP_EXTRACT(url, CONCAT('[?&]',pname,'=([^&]+).*$')),'#')[OFFSET(0)]
);

/* Returns the first substring matched by the regular expression `regexp` in `str`. */
CREATE OR REPLACE FUNCTION cw_regexp_extract(str STRING, regexp STRING) RETURNS STRING
LANGUAGE js AS """
  var r = new RegExp(regexp);
  var a = str.match(r);
  return a[0];
""";

/* Finds the first occurrence of the regular expression `regexp` in `str` and returns the capturing group number `groupn` */
CREATE OR REPLACE FUNCTION cw_regexp_extract_n(str STRING, regexp STRING, groupn INT64) RETURNS STRING
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
CREATE OR REPLACE FUNCTION cw_regexp_extract_all(str STRING, regexp STRING) RETURNS ARRAY<STRING>
LANGUAGE js AS """
  var r = new RegExp(regexp, "g");
  return str.match(r);
""";

/* Finds all occurrences of the regular expression `regexp` in `str` and returns the capturing group number `groupn`. */
CREATE OR REPLACE FUNCTION cw_regexp_extract_all_n(str STRING, regexp STRING, groupn INT64) RETURNS ARRAY<STRING>
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
CREATE OR REPLACE FUNCTION cw_json_array_contains_str(json STRING, needle STRING) RETURNS BOOL
LANGUAGE js AS """
  if (json == null || needle == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.indexOf(needle) >= 0;
""";

/* Same as cw_json_array_contains_str(STRING, STRING) UDF but with needle = number */
CREATE OR REPLACE FUNCTION cw_json_array_contains_num(json STRING, needle FLOAT64) RETURNS BOOL
LANGUAGE js AS """
  if (json == null || needle == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.indexOf(needle) >= 0;
""";

/* Same as cw_json_array_contains_str(STRING, STRING) UDF but with needle = boolean */
CREATE OR REPLACE FUNCTION cw_json_array_contains_bool(json STRING, needle BOOL) RETURNS BOOL
LANGUAGE js AS """
  if (json == null || needle == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.indexOf(needle) >= 0;
""";

/* Returns the element at the specified index into the json_array. The index is zero-based */
CREATE OR REPLACE FUNCTION cw_json_array_get(json STRING, loc FLOAT64) RETURNS STRING
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
CREATE OR REPLACE FUNCTION cw_json_array_length(json STRING) RETURNS INT64
LANGUAGE js AS """
  if (json == null)
    return null;
  var parsedJson = JSON.parse(json);
  return parsedJson.length;
""";

/* Implements MySQL SUBSTRING_INDEX */
CREATE OR REPLACE FUNCTION cw_substring_index(str STRING, sep STRING, idx INT64) RETURNS STRING
LANGUAGE js AS """
  if (str === null || sep === null || idx === null) return null;
  if (sep == "") return "";
  var arr = str.split(sep);
  var oth = arr.splice(idx, arr.length - idx);
  return (idx >= 0 ? arr : oth).join(sep);
""";

/* Implements teradata's editdistance without weightages */
CREATE OR REPLACE FUNCTION cw_editdistance( a STRING, b STRING ) RETURNS INT64
LANGUAGE js AS """

    if ( a == null || b == null ) {
        return null;
    }

    var work = new Array( a.length + 1 )
    for ( var i = 0 ; i <= a.length ; i ++ ) {
        work[i] = new Array ( b.length + 1 )
    }


    for ( var i = 0 ; i < a.length + 1 ; i ++ )
        work[i][0] = i;

    for ( var j = 0 ; j < b.length + 1 ; j ++ )
        work[0][j] = j;


    for ( var i = 1 ; i < a.length + 1 ; i ++ ) {
        for ( var j = 1 ; j < b.length + 1 ; j ++ ) {
            var cost = a[i-1] == b[j-1] ? 0 : 1;
            work[i][j] = Math.min(
                work[i-1] [j]   + 1,
                work[i]   [j-1] + 1,
                work[i-1] [j-1] + cost )
            if ( i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] ) {
                work[i][j] = Math.min(
                    work[i]  [j],
                    work[i-2][j-2] + 1 );
            }
        }
    }
    return work[a.length][b.length];
""";

CREATE OR REPLACE FUNCTION cw_round_half_even(n BIGNUMERIC, d INT64) RETURNS NUMERIC AS (
  CAST(
    POW(10, -d) * SIGN(n) *
    CASE WHEN CAST(TRUNC(n * POW(10, d)) AS INT64) & 1 = 0 -- even number
       THEN CEIL(ABS(n * POW(10, d)) - 0.5) -- round towards
       ELSE FLOOR(ABS(n * POW(10, d)) + 0.5) -- round away
    END
    AS NUMERIC)
);

CREATE OR REPLACE FUNCTION cw_round_half_even_bignumeric(n BIGNUMERIC, d INT64) RETURNS BIGNUMERIC AS (
  CAST(
    POW(10, -d) * SIGN(n) *
    CASE WHEN CAST(TRUNC(n * POW(10, d)) AS INT64) & 1 = 0 -- even number
       THEN CEIL(ABS(n * POW(10, d)) - 0.5) -- round towards
       ELSE FLOOR(ABS(n * POW(10, d)) + 0.5) -- round away
    END
    AS BIGNUMERIC)
);

CREATE OR REPLACE FUNCTION cw_getbit(bits INT64, index INT64) RETURNS INT64 AS (
       (bits & (1 << index)) >> index
);

CREATE OR REPLACE FUNCTION cw_setbit(bits INT64, index INT64) RETURNS INT64 AS (
       bits | (1 << index)
);

/** Emulate Vertica LOWERB function, which lowercases only ASCII characters within a given string. */
CREATE OR REPLACE FUNCTION cw_lower_case_ascii_only(str STRING) RETURNS STRING AS (
    (WITH chars AS (
        SELECT ch FROM UNNEST(TO_CODE_POINTS(str)) AS ch
    )
    SELECT CONCAT(CODE_POINTS_TO_STRING(ARRAY_AGG(if(ch >= 65 and ch <= 90, ch + 32, ch)))) from chars
));

/** Emulate Vertica SUBSTRB function, which treats the multibyte character string as a string of octets (bytes). */
CREATE OR REPLACE FUNCTION cw_substrb(str STRING, startpos INT64 /* 1-based */, extent INT64 /* 1-based */) RETURNS STRING AS (
    (WITH octets AS (
        SELECT oct FROM UNNEST(TO_CODE_POINTS(CAST(str as bytes))) AS oct WITH OFFSET off WHERE (off+1) >= startpos and (off+1) < startpos+extent ORDER BY off
    )
    SELECT SAFE_CONVERT_BYTES_TO_STRING(CODE_POINTS_TO_BYTES(ARRAY_AGG(oct))) FROM octets
));

CREATE OR REPLACE FUNCTION cw_twograms(t STRING) RETURNS ARRAY<STRING> AS (
   (WITH splt as (SELECT regexp_extract_all(t, r'(\S+)') as a)
    SELECT array_agg(concat(a[offset(i)], ' ', a[offset(i+1)])) from splt join unnest(generate_array(0, array_length(a) - 2, 1)) as i
));

CREATE OR REPLACE FUNCTION cw_threegrams(t STRING) RETURNS ARRAY<STRING> AS (
   (WITH splt as (SELECT regexp_extract_all(t, r'(\S+)') as a)
    SELECT array_agg(concat(a[offset(i)], ' ', a[offset(i+1)], ' ', a[offset(i+2)])) from splt join unnest(generate_array(0, array_length(a) - 3, 1)) as i
));


CREATE OR REPLACE FUNCTION cw_nvp2json1 (nvp STRING) RETURNS STRING AS (
         --concat('{"',replace(replace(nvp,'&','","'),'=','":"'),'"}')
         --JSON_EXTRACT(concat('{"',replace(replace(nvp,'&','","'),'=','":"'),'"}'),'$')
         JSON_EXTRACT(concat('{"',replace(replace(replace(replace(nvp,'\\','\\\\'),'"','\\"'),'&','","'),'=','":"'),'"}'),'$')
);

CREATE OR REPLACE FUNCTION cw_nvp2json3 (nvp STRING,name_delim STRING, val_delim STRING) RETURNS STRING AS (
         --concat('{"',replace(replace(nvp,name_delim,'","'),val_delim,'":"'),'"}')
         -- JSON_EXTRACT(concat('{"',replace(replace(nvp,name_delim,'","'),val_delim,'":"'),'"}'),'$')
         JSON_EXTRACT(concat('{"',replace(replace(replace(replace(nvp,'\\','\\\\'),'"','\\"'),name_delim,'","'),val_delim,'":"'),'"}'),'$')
);

CREATE OR REPLACE FUNCTION cw_nvp2json4 (nvp STRING, name_delim STRING, val_delim STRING, ignore_char STRING) RETURNS STRING AS (
         --concat('{"',replace(replace(translate(nvp,ignore_char,''),name_delim,'","'),val_delim,'":"'),'"}')
         --JSON_EXTRACT(concat('{"',replace(replace(translate(nvp,ignore_char,''),name_delim,'","'),val_delim,'":"'),'"}'),'$')
         JSON_EXTRACT(concat('{"',replace(replace(replace(replace(translate(nvp,ignore_char,''),'\\','\\\\'),'"','\\"') ,name_delim,'","'),val_delim,'":"'),'"}'),'$')
);

CREATE OR REPLACE FUNCTION cw_strtok(text string, delim string)
RETURNS array<struct<tokennumber int64, token string>>
LANGUAGE js AS """
    var ret = [ ]
    var token = ""
    var tokennumber = 1;

    if ( text === null || delim === null )
        return ret;

    var isDelim = function ( ch ) {
        return delim.indexOf(ch) >= 0;
    };
    var wrap_up = function( ) {
        if ( token != '' ) {
            ret.push({ tokennumber, token});
            token = "";
            tokennumber++;
        }
    };

    for ( var j = 0 ; j < text.length ; j ++ ) {
        var c = text.charAt(j);
        if ( isDelim(c) ) {
            wrap_up();
        } else {
            token += c;
        }
    }
    wrap_up();
    return ret;
""";

CREATE OR REPLACE FUNCTION cw_regexp_split(text string, delim string, flags string)
RETURNS array<struct<tokennumber int64, token string>>
LANGUAGE js AS """
    var idx = 0;
    var nxt = function() {
        idx ++;
        return idx;
    };
    var fix_flags = function(mode) {
        var m = '';
        if (mode == 'i' || mode == 'm')
            m += mode;
        else if (mode == 'n')
            m += 's';
        m += 'g';
        return m;
    };

    return text.
        split( new RegExp(delim,fix_flags(flags))).
        filter( x => x != '').
        map( x => ({ tokennumber : nxt(), token: x }) );
""";

CREATE OR REPLACE FUNCTION cw_csvld(text string, comma string, quote string,len INT64)
RETURNS array<string>
LANGUAGE js AS """

    var ret = []
    var index = 0;
    /* Can't return NULL from ARRAY */
    for( var j = 0 ; j < len ; j ++ ) ret.push('');
    if ( text === null )
        return ret;
    var state = 0;
    const STATE_TOP    = 0;
    const STATE_QUOTE  = 1;
    const STATE_ESCAPE = 2;
    var start_new = function () { index ++;  };
    var append = function(c) {  ret[index] += c; };

    for ( var j = 0 ; j < text.length ; j ++ ) {
        var c = text.charAt(j);
        switch ( state ) {
        case STATE_TOP:
            if ( c == quote ) state = STATE_QUOTE;
            else if ( c == comma ) start_new();
            else append(c);
            break;
        case STATE_QUOTE:
            if ( c == quote ) state = STATE_ESCAPE;
            else append(c);
            break;
        case STATE_ESCAPE:
            if ( c == quote ) {
                state = STATE_QUOTE;
                append(quote)
            } else if ( c == comma ) {
                state = STATE_TOP;
                start_new();
            } else {
                state = STATE_TOP;
                append(c);
            }
	    break;
        default: /* impossible condition */
            break;
        }
    }
    return ret;
""";

CREATE OR REPLACE FUNCTION cw_json_enumerate_array(text string)
RETURNS array<struct<ordinal int64, jsonvalue string>>
LANGUAGE js AS """

    if ( text === null  )
      return [];
    var idx = 0;
    var nxt = function() {
        idx ++;
        return idx;
    };
    return  JSON.parse(text).map( x => ({ ordinal: nxt(), jsonvalue: JSON.stringify(x)}));
""";

-- ts_pattern_match is function that returns range of matched pattern
-- in given UID, SID (user session)
CREATE OR REPLACE FUNCTION cw_ts_pattern_match(evSeries ARRAY<STRING>, regexpParts ARRAY<STRING>)
RETURNS ARRAY<STRUCT<pattern_id INT64, start INT64, stop INT64>>
LANGUAGE js AS """

var chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
var charIdx = 0;
var name2char = {};
var eventRegExp = '';

for( nameWithQuantifierOrAlt of regexpParts ) {
    var match = nameWithQuantifierOrAlt.match(/([a-zA-Z0-9]*)([^a-zA-Z0-9]*)/)
    if( !match ) {
        throw new Error("Invalid Event name" + nameWithQuantifierOrAlt);
    }
    var name = match[1];
    var quantifiers = match[2];
    if ( name ) {
        var ch = name2char[name];
        if ( ! ch ) {
            if( charIdx == 52) {
                throw new Error("More than 52 events are not supported")
            }
            ch = chars.charAt(charIdx++)
            name2char[name] = ch;
        }
        eventRegExp += ch + quantifiers;
    } else { // is  Alt('|')
        eventRegExp += quantifiers; // quantifier match the PCRE, [*+?]/[*?], |
    }
};

var eventHaystack = ''
for ( ev of evSeries ) {
    var ch = name2char[ev];
    if ( ch )
        eventHaystack += ch;
    else if ( ev == '?' ) {
        eventHaystack += ev;
    } else {
        throw new Error("Unknown event in event stream : " + evSeries)
    }
};

var out = [];
var k = 1;
for(var matchedEvent of eventHaystack.matchAll(new RegExp(eventRegExp, 'g'))) {
    out.push({ 'pattern_id' : k++ , 'start' : matchedEvent.index + 1, 'stop' : matchedEvent.index + matchedEvent[0].length})
}
return out;
""";
