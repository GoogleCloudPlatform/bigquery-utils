# Community UDFs

This directory contains community contributed [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
to extend BigQuery for more specialized usage patterns. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`fn` dataset for reference in queries.

For example, if you'd like to reference the `int` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.fn.int(1.684)
```

## UDFs
* [azimuth_to_geog_point](#azimuth_to_geog_pointinput_lat-float64-input_lon-float64-azimuth-float64-distance-float64)
* [bignumber_add](#bignumber_addfirst-string-second-string)
* [bignumber_avg](#bignumber_avgnumbers-array)
* [bignumber_div](#bignumber_divfirst-string-second-string)
* [bignumber_eq](#bignumber_gtfirst-string-second-string)
* [bignumber_gt](#bignumber_gtfirst-string-second-string)
* [bignumber_gte](#bignumber_gtefirst-string-second-string)
* [bignumber_lt](#bignumber_gtfirst-string-second-string)
* [bignumber_lte](#bignumber_gtefirst-string-second-string)
* [bignumber_mul](#bignumber_mulfirst-string-second-string)
* [bignumber_sub](#bignumber_subfirst-string-second-string)
* [bignumber_sum](#bignumber_sumnumbers-array)
* [chisquare_cdf](#chisquare_cdfh-float64-dof-float64)
* [corr_pvalue](#corr_pvaluer-float64-n-int64)
* [csv_to_struct](#csv_to_structstrlist-string)
* [cw_array_compact](#cw_array_compacta-any-type)
* [cw_array_distinct](#cw_array_distinctarr-any-type)
* [cw_array_max](#cw_array_maxarr-any-type)
* [cw_array_median](#cw_array_medianarr-any-type)
* [cw_array_min](#cw_array_minarr-any-type)
* [cw_array_overlap](#cw_array_overlapx-any-type-y-any-type)
* [cw_array_stable_distinct](#cw_array_stable_distinctarr-any-type)
* [cw_comparable_format_bigint](#cw_comparable_format_bigintdata-array)
* [cw_comparable_format_bigint_t](#cw_comparable_format_bigint_tpart-int64)
* [cw_comparable_format_varchar](#cw_comparable_format_varchardata-array)
* [cw_comparable_format_varchar_t](#cw_comparable_format_varchar_tpart-string)
* [cw_convert_base](#cw_convert_basenumber-string-from_base-int64-to_base-int64)
* [cw_csvld](#cw_csvldtext-string-comma-string-quote-string-len-int64)
* [cw_disjoint_all_partitions_by_regexp](#cw_disjoint_all_partitions_by_regexphaystack-string-regex-string)
* [cw_disjoint_partition_by_regexp](#cw_disjoint_partition_by_regexpfirstrn-int64-haystack-string-regex-string)
* [cw_editdistance](#cw_editdistancea-string-b-string)
* [cw_error_number](cw_error_numbererrmsg-string)
* [cw_error_severity](cw_error_severityerrmsg-string)
* [cw_error_state](cw_error_stateerrmsg-string)
* [cw_find_in_list](#cw_find_in_listneedle-string-list-string)
* [cw_from_base](#cw_from_basenumber-string-base-int64)
* [cw_getbit](#cw_getbitbits-int64-index-int64)
* [cw_getbit_binary](#cw_getbit_binarybits-bytes-index-int64)
* [cw_initcap](#cw_initcaps-string)
* [cw_instr4](#cw_instr4source-string-search-string-position-int64-ocurrence-int64)
* [cw_json_array_contains_bool](#cw_json_array_contains_booljson-string-needle-bool)
* [cw_json_array_contains_num](#cw_json_array_contains_numjson-string-needle-float64)
* [cw_json_array_contains_str](#cw_json_array_contains_strjson-string-needle-string)
* [cw_json_array_get](#cw_json_array_getjson-string-loc-float64)
* [cw_json_array_length](#cw_json_array_lengthjson-string)
* [cw_json_enumerate_array](#cw_json_enumerate_arraytext-string)
* [cw_lower_case_ascii_only](#cw_lower_case_ascii_onlystr-string)
* [cw_map_create](#cw_map_createkeys-any-type-vals-any-type)
* [cw_map_get](#cw_map_getmaparray-any-type-inkey-any-type)
* [cw_map_parse](#cw_map_parsem-string-pd-string-kvd-string)
* [cw_months_between](#cw_months_betweenet-datetime-st-datetime)
* [cw_next_day](#cw_next_daydate_value-date-day_name-string)
* [cw_nvp2json1](#cw_nvp2json1nvp-string)
* [cw_nvp2json3](#cw_nvp2json3nvp-string-name_delim-string-val_delim-string)
* [cw_nvp2json4](#cw_nvp2json4nvp-string-name_delim-string-val_delim-string-ignore_char-string)
* [cw_otranslate](#cw_otranslates-string-key-string-value-string)
* [cw_overlapping_partition_by_regexp](#cw_overlapping_partition_by_regexpfirstrn-int64-haystack-string-regex-string)
* [cw_parse_timestamp](#cw_parse_timestamptimeString-string-formatString-string)
* [cw_period_intersection](#cw_period_intersectionp1-structlower-timestamp-upper-timestamp-p2-structlower-timestamp-upper-timestamp)
* [cw_period_ldiff](#cw_period_ldiffp1-structlower-timestamp-upper-timestamp-p2-structlower-timestamp-upper-timestamp)
* [cw_period_rdiff](#cw_period_rdiffp1-structlower-timestamp-upper-timestamp-p2-structlower-timestamp-upper-timestamp)
* [cw_regex_mode](#cw_regex_modemode-string)
* [cw_regexp_extract](#cw_regexp_extractstr-string-regexp-string)
* [cw_regexp_extract_all](#cw_regexp_extract_allstr-string-regexp-string)
* [cw_regexp_extract_all_start_pos](#cw_regexp_extract_all_start_posstr-string-regexp-string-position-int64)
* [cw_regexp_extract_all_n](#cw_regexp_extract_all_nstr-string-regexp-string-groupn-int64)
* [cw_regexp_extract_n](#cw_regexp_extract_nstr-string-regexp-string-groupn-int64)
* [cw_regexp_instr_2](#cw_regexp_instr_2haystack-string-needle-string)
* [cw_regexp_instr_3](#cw_regexp_instr_3haystack-string-needle-string-start-int64)
* [cw_regexp_instr_4](#cw_regexp_instr_3haystack-string-needle-string-p-int64-o-int64)
* [cw_regexp_instr_5](#cw_regexp_instr_5haystack-string-needle-string-p-int64-o-int64-returnopt-int64)
* [cw_regexp_instr_6](#cw_regexp_instr_6haystack-string-needle-string-p-int64-o-int64-returnopt-int64-mode-string)
* [cw_regexp_instr_generic](#cw_regexp_instr_generichaystack-string-needle-string-p-int64-o-int64-returnopt-int64-mode-string)
* [cw_regexp_replace_4](cw_regexp_replace_4haystack-string-regexpstring-replacement-string-offset-int64)
* [cw_regexp_replace_5](cw_regexp_replace_5haystack-string-regexpstring-replacement-string-offset-int64-occurrence-int64)
* [cw_regexp_replace_6](cw_regexp_replace_6haystack-string-regexpstring-replacement-string-p-int64-o-int64-mode-string)
* [cw_regexp_replace_generic](#cw_regexp_replace_generichaystack-string-regexp-string-replacement-string-offset-int64-occurrence-int64-mode-string)
* [cw_regexp_split](#cw_regexp_splittext-string-delim-string-flags-string)
* [cw_regexp_substr_4](#cw_regexp_substr_4h-string-n-string-p-int64-o-int64)
* [cw_regexp_substr_5](#cw_regexp_substr_5h-string-n-string-p-int64-o-int64-mode-string)
* [cw_regexp_substr_6](#cw_regexp_substr_6h-string-n-string-p-int64-o-int64-mode-string-g-string)
* [cw_regexp_substr_generic](#cw_regexp_substr_genericstr-string-regexp-string-p-int64-o-int64-mode-string-g-int64)
* [cw_round_half_even](#cw_round_half_evenn-bignumeric-d-int64)
* [cw_round_half_even_bignumeric](#cw_round_half_even_bignumericn-bignumeric-d-int64)
* [cw_runtime_parse_interval_seconds](#cw_runtime_parse_interval_secondsival-string)
* [cw_setbit](#cw_setbitbits-int64-index-int64)
* [cw_signed_leftshift_128bit](#cw_signed_leftshift_128bitvalue-bignumeric-n-bignumeric)
* [cw_signed_rightshift_128bit](#cw_signed_rightshift_128bitvalue-bignumeric-n-bignumeric)
* [cw_split_part_delimstr_idx](#cw_split_part_delimstr_idxvalue-string-delimiter-string-part-int64)
* [cw_stringify_interval](#cw_stringify_intervalx-int64)
* [cw_strtok](#cw_strtoktext-string-delim-string)
* [cw_substrb](#cw_substrbstr-string-startpos-int64-extent-int64)
* [cw_substring_index](#cw_substring_indexstr-string-sep-string-idx-int64)
* [cw_td_normalize_number](#cw_td_normalize_numberstr-string)
* [cw_td_nvp](#cw_td_nvphaystack-string-needle-string-pairsep-string-valuesep-string-occurence-int64)
* [cw_threegrams](#cw_threegramst-string)
* [cw_to_base](#cw_to_basenumber-int64-base-int64)
* [cw_ts_overlap_buckets](#cw_ts_overlap_bucketsincludemeets-boolean-arraystruct-st-timestamp-et-timestamp)
* [cw_ts_pattern_match](#cw_ts_pattern_matchevseries-array-regexpParts-array)
* [cw_twograms](#cw_twogramst-string)
* [cw_url_decode](#cw_url_decodepath-string)
* [cw_url_encode](#cw_url_encodepath-string)
* [cw_url_extract_authority](#cw_url_extract_authorityurl-string)
* [cw_url_extract_file](#cw_url_extract_fileurl-string)
* [cw_url_extract_fragment](#cw_url_extract_fragmenturl-string)
* [cw_url_extract_host](#cw_url_extract_hosturl-string)
* [cw_url_extract_parameter](#cw_url_extract_parameterurl-string-pname-string)
* [cw_url_extract_path](#cw_url_extract_pathurl-string)
* [cw_url_extract_port](#cw_url_extract_porturl-string)
* [cw_url_extract_protocol](#cw_url_extract_protocolurl-string)
* [cw_url_extract_query](#cw_url_extract_queryurl-string)
* [cw_width_bucket](#cw_width_bucketvalue_expression-float64-lower_bound-float64-upper_bound-float64-partition_count-int64)
* [day_occurrence_of_month](#day_occurrence_of_monthdate_expression-any-type)
* [degrees](#degreesx-any-type)
* [exif](#exifsrc_obj_ref-structuri-string-version-string-authorizer-string-details-json)
* [find_in_set](#find_in_setstr-string-strlist-string)
* [freq_table](#freq_tablearr-any-type)
* [from_binary](#from_binaryvalue-string)
* [from_hex](#from_hexvalue-string)
* [get_array_value](#get_array_valuek-string-arr-any-type)
* [getbit](#getbittarget_arg-int64-target_bit_arg-int64)
* [get_value](#get_valuek-string-arr-any-type)
* [gunzip](#gunzipgzipped-bytes)
* [int](#intv-any-type)
* [jaccard](#jaccard)
* [job_url](#job_urljob_id-string)
* [json_extract_keys](#json_extract_keys)
* [json_extract_key_value_pairs](#json_extract_key_value_pairs)
* [json_extract_values](#json_extract_values)
* [json_typeof](#json_typeofjson-string)
* [knots_to_mph](#knots_to_mphinput_knots-float64)
* [kruskal_wallis](#kruskal_wallisarraystructfactor-string-val-float64)
* [last_day](https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day)
* [levenshtein](#levenshteinsource-string-target-string-returns-int64)
* [linear_interpolate](#linear_interpolatepos-int64-prev-structx-int64-y-float64-next-structx-int64-y-float64)
* [linear_regression](#linear_regressionarraystructstructx-float64-y-float64)
* [mannwhitneyu](#mannwhitneyux-array-y-array-alt-string)
* [median](#medianarr-any-type)
* [meter_to_miles](#meters_to_milesinput_meters-float64)
* [miles_to_meters](#miles_to_metersinput_miles-float64)
* [mph_to_knots](#mph_to_knotsinput_mph-float64)
* [nautical_miles_conversion](#nautical_miles_conversioninput_nautical_miles-float64)
* [nlp_compromise_number](#nlp_compromise_numberstr-string)
* [nlp_compromise_people](#nlp_compromise_peoplestr-string)
* [normal_cdf](#normal_cdfx-float64-mean-float64-stdev-float64)
* [percentage_change](#percentage_changeval1-float64-val2-float64)
* [percentage_difference](#percentage_differenceval1-float64-val2-float64)
* [pi](#pi)
* [pvalue](#pvalueh-float64-dof-float64)
* [p_fisherexact](#p_fisherexacta-float64-b-float64-c-float64-d-float64)
* [radians](#radiansx-any-type)
* [random_int](#random_intmin-any-type-max-any-type)
* [random_string](#random_stringlength-int64)
* [random_value](#random_valuearr-any-type)
* [studentt_cdf](#studentt_cdfx-float64-dof-float64)
* [sure_cond](#sure_condvalue-string-cond-bool)
* [sure_like](#sure_likevalue-string-like_pattern-string)
* [sure_nonnull](#sure_nonnullvalue-any-type)
* [sure_range](#sure_rangevalue-any-type)
* [sure_values](#sure_valuesvalue-any-type-acceptable_value_array-any-type)
* [table_url](#table_urltable_id-string)
* [to_binary](#to_binaryx-int64)
* [to_hex](#to_hexx-int64)
* [translate](#translateexpression-string-characters_to_replace-string-characters_to_substitute-string)
* [ts_gen_keyed_timestamps](#ts_gen_keyed_timestampskeys-arraystring-tumble_seconds-int64-min_ts-timestamp-max_ts-timestamp)
* [ts_linear_interpolate](#ts_linear_interpolatepos-timestamp-prev-structx-timestamp-y-float64-next-structx-timestamp-y-float64)
* [ts_session_group](#ts_session_grouprow_ts-timestamp-prev_ts-timestamp-session_gap-int64)
* [ts_slide](#ts_slidets-timestamp-period-int64-duration-int64)
* [ts_tumble](#ts_tumbleinput_ts-timestamp-tumble_seconds-int64)
* [t_test](#t_testarrayarray)
* [typeof](#typeofinput-any-type)
* [url_decode](#url_decodetext-string-method-string)
* [url_encode](#url_encodetext-string-method-string)
* [url_keys](#url_keysquery-string)
* [url_param](#url_paramquery-string-p-string)
* [url_parse](#url_parseurlstring-string-parttoextract-string)
* [url_trim_query](#url_trim_queryurl-string-keys_to_trim-array)
* [week_of_month](#week_of_monthdate_expression-any-type)
* [xml_to_json](#xml_to_jsonxml-string)
* [xml_to_json_fpx](#xml_to_json_fpxxml-string)
* [y4md_to_date](#y4md_to_datey4md-string)
* [zeronorm](#zeronormx-any-type-meanx-float64-stddevx-float64)

## Documentation
### [azimuth_to_geog_point(input_lat FLOAT64, input_lon FLOAT64, azimuth FLOAT64, distance FLOAT64)](azimuth_to_geog_point.sqlx)
Takes an input latitude, longitude, azimuth, and distance (in miles) and returns the corresponding latitude and longitude as a BigQuery GEOGRAPHY point.
```sql
SELECT bqutil.fn.azimuth_to_geog_point(30.2672, 97.7431, 312.9, 1066.6);

POINT(81.4417483906444 39.9606210457152)
```

### [bignumber_add(first STRING, second STRING)](bignumber_add.sqlx)
Safely allows mathematical addition on numbers of any magnitude. Returns the result as a string.

```sql
SELECT bqutil.fn.bignumber_add(
  '99999999999999999999999999999999999999999999999999999999999999999999', '2348592348793428978934278932746531725371625376152367153761536715376')

"102348592348793428978934278932746531725371625376152367153761536715375"
```

### [bignumber_avg(numbers ARRAY<STRING>)](bignumber_avg.sqlx)
Safely allows calculating the average of numbers of any magnitude. Returns the result as a string.

```sql
SELECT bqutil.fn.bignumber_avg(
  '99999999999999999999999999999999999999999999999999999999999999999999', '33333333333333333333333333333333333333333333333333333333333333333333', '66666666666666666666666666666666666666666666666666666666666666666666')

"66666666666666666666666666666666666666666666666666666666666666666666"
```

### [bignumber_div(first STRING, second STRING)](bignumber_div.sqlx)
Safely allows mathematical division on numbers of any magnitude. Returns the result as a string.

```sql
SELECT bqutil.fn.bignumber_div(
  '99999999999999999999999999999999999999999999999999999999999999999999', '33333333333333333333333333333333333333333333333333333333333333333333')

"3"
```

### [bignumber_eq(first STRING, second STRING)](bignumber_eq.sqlx)
Safely allows equal comparison on numbers of any magnitude. Returns the result as a boolean.

```sql
SELECT bqutil.fn.bignumber_eq(
  '99999999999999999999999999999999999999999999999999999999999999999999', '99999999999999999999999999999999999999999999999999999999999999999999')

TRUE
```

### [bignumber_gt(first STRING, second STRING)](bignumber_gt.sqlx)
Safely allows greater than comparison on numbers of any magnitude. Returns the result as a boolean.

```sql
SELECT bqutil.fn.bignumber_gt(
  '99999999999999999999999999999999999999999999999999999999999999999999', '33333333333333333333333333333333333333333333333333333333333333333333')

TRUE
```

### [bignumber_gte(first STRING, second STRING)](bignumber_gte.sqlx)
Safely allows greater than or equal comparison on numbers of any magnitude. Returns the result as a boolean.

```sql
SELECT bqutil.fn.bignumber_gte(
  '99999999999999999999999999999999999999999999999999999999999999999999', '99999999999999999999999999999999999999999999999999999999999999999999')

TRUE
```

### [bignumber_lt(first STRING, second STRING)](bignumber_lt.sqlx)
Safely allows less than comparison on numbers of any magnitude. Returns the result as a boolean.

```sql
SELECT bqutil.fn.bignumber_lt(
  '33333333333333333333333333333333333333333333333333333333333333333333','99999999999999999999999999999999999999999999999999999999999999999999')

TRUE
```

### [bignumber_lte(first STRING, second STRING)](bignumber_lte.sqlx)
Safely allows less than or equal comparison on numbers of any magnitude. Returns the result as a boolean.

```sql
SELECT bqutil.fn.bignumber_lte(
  '99999999999999999999999999999999999999999999999999999999999999999999', '99999999999999999999999999999999999999999999999999999999999999999999')

TRUE
```

### [bignumber_mul(first STRING, second STRING)](bignumber_mul.sqlx)
Safely allows mathematical multiplication on numbers of any magnitude. Returns the result as a string.

```sql
SELECT bqutil.fn.bignumber_mul(
  '99999999999999999999999999999999999999999999999999999999999999999999', '893427328732842662772591830391462182598436547786876876876')

"89342732873284266277259183039146218259843654778687687687599999999999106572671267157337227408169608537817401563452213123123124"
```

### [bignumber_sub(first STRING, second STRING)](bignumber_sub.sqlx)
Safely allows mathematical subtraction on numbers of any magnitude. Returns the result as a string.

```sql
SELECT bqutil.fn.bignumber_sub(
  '99999999999999999999999999999999999999999999999999999999999999999999', '893427328732842662772591830391462182598436547786876876876')

"99999999999106572671267157337227408169608537817401563452213123123123"
```

### [bignumber_sum(numbers ARRAY<STRING>)](bignumber_sum.sqlx)
Safely allows calculating the total sum of numbers of any magnitude. Returns the result as a string.

```sql
SELECT bqutil.fn.bignumber_sum(
  '99999999999999999999999999999999999999999999999999999999999999999999', '893427328732842662772591830391462182598436547786876876876', '123456789123456789123456789123456789123456789123456789123456789123456789')

"123556789123457682550785521966119561715287180585639387560004576000333664"
```

### [csv_to_struct(strList STRING)](csv_to_struct.sqlx)
Take a list of comma separated key-value pairs and creates a struct.
Input:
strList: string that has map in the format a:b,c:d....
Output: struct for the above map.
```sql
WITH test_cases AS (
  SELECT NULL as s
  UNION ALL
  SELECT '' as s
  UNION ALL
  SELECT ',' as s
  UNION ALL
  SELECT ':' as s
  UNION ALL
  SELECT 'a:b' as s
  UNION ALL
  SELECT 'a:b,c:d' as s
  UNION ALL
  SELECT 'a:b' as s
)
SELECT key, value from test_cases as t, UNNEST(bqutil.fn.csv_to_struct(t.s)) s;
```

results:

| key | value |
|-----|-------|
| a   | b     |
| a   | b     |
| c   | d     |
| a   | b     |

### [cw_array_compact(a ANY TYPE)](cw_array_compact.sqlx)
Returns a compacted array with null values removed
```sql
SELECT bqutil.fn.cw_array_compact([1, 2, 3, null, 5]);

[1, 2, 3, 5]
```

### [cw_array_distinct(arr ANY TYPE)](cw_array_distinct.sqlx)
Returns distinct array.
```sql
SELECT bqutil.fn.cw_array_distinct([1, 2, 3, 4, 4, 5, 5]);

[1, 2, 3, 4, 5]
```

### [cw_array_max(arr ANY TYPE)](cw_array_max.sqlx)
Returns maximum of array.
```sql
SELECT bqutil.fn.cw_array_max([1, 2, 3, 4, 5, 6]);

6
```

### [cw_array_median(arr ANY TYPE)](cw_array_median.sqlx)
Returns median of array.
```sql
SELECT bqutil.fn.cw_array_median([1, 2, 3, 4, 5, 6]);

3.5
```

### [cw_array_min(arr ANY TYPE)](cw_array_min.sqlx)
Returns minimum of array.
```sql
SELECT bqutil.fn.cw_array_min([1, 2, 3, 4, 5]);

1
```

### [cw_array_overlap(x ANY TYPE, y ANY TYPE)](cw_array_overlap.sqlx)
Returns true if arrays are overlapped otherwise false.
```sql
SELECT bqutil.fn.cw_array_overlap([1, 2, 3], [4, 5, 6]);
SELECT bqutil.fn.cw_array_overlap([1, 2, 3], [2, 3, 4]);

false
true
```

### [cw_array_stable_distinct(arr ANY TYPE)](cw_array_stable_distinct.sqlx)
Returns distinct array with preserved elements order.
```sql
SELECT bqutil.fn.cw_array_stable_distinct([4, 1, 4, 9, 1, 10]);

[4, 1, 9, 10]
```

### [cw_comparable_format_bigint(data ARRAT<INT64>)](cw_comparable_format_bigint.sqlx)
Lexicographically '+' comes before '-' so we replace p(lus) and m(inus) and subtract LONG_MIN on negative values
```sql
SELECT bqutil.fn.cw_comparable_format_bigint([2, 8]);

p                  2 p                  8
```

### [cw_comparable_format_bigint_t(part INT64)](cw_comparable_format_bigint_t.sqlx)
Lexicographically '+' comes before '-' so we replace p(lus) and m(inus) and subtract LONG_MIN on negative values
```sql
SELECT bqutil.fn.cw_comparable_format_bigint_t(2);

p                  2
```

### [cw_comparable_format_varchar(data ARRAY<STRING>)](cw_comparable_format_varchar.sqlx)
Use hex to work around the separator problem (e.g. if separator = '-' then ['-', ''] and ['', '-'] both produce '--')
```sql
SELECT bqutil.fn.cw_comparable_format_varchar(["2", "8"]);

32 38
```

### [cw_comparable_format_varchar_t(part STRING)](cw_comparable_format_varchar_t.sqlx)
Use hex to work around the separator problem (e.g. if separator = '-' then ['-', ''] and ['', '-'] both produce '--')
```sql
SELECT bqutil.fn.cw_comparable_format_varchar_t("2");

32
```

### [cw_convert_base(number STRING, from_base INT64, to_base INT64)](cw_from_base.sqlx)
Convert string from given base to another base

```sql
SELECT bqutil.fn.cw_convert_base('001101011', 2, 10);
SELECT bqutil.fn.cw_convert_base('A', 16, 2);

107
1010
```


### [cw_csvld(text string, comma string, quote string,len INT64)](cw_csvld.sqlx)
Generates CSV array.
```sql
SELECT bqutil.fn.cw_csvld('Test#123', '#', '"', 2);

["Test", "123"]
```

### [cw_disjoint_all_partitions_by_regexp(haystack STRING, regex STRING)](cw_disjoint_all_partitions_by_regexp.sqlx)
Partitions rows into disjoint segments and returns all the partitions by matching row-sequence with the provided regex pattern.
```sql
SELECT bqutil.fn.cw_disjoint_all_partitions_by_regexp(1, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_all_partitions_by_regexp(1, 'A@1#B@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_all_partitions_by_regexp(1, 'B@1#B@2#B@3#B@4#A@5#', '(?:A@\\d+#)+(?:B@\\d+#)')

[(0, 1), (0, 2), (0, 3), (1, 4), (1, 5)]
[(0, 1), (0, 2), (1, 4), (1, 5)]
[]
```

### [cw_disjoint_partition_by_regexp(firstRn INT64, haystack STRING, regex STRING)](cw_disjoint_partition_by_regexp.sqlx)
Partitions rows into disjoint segments and returns a partition associated with the given row-number by matching row-sequence with the provided regex pattern.
```sql
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(1, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(2, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(3, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(4, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(5, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')

[1, 2, 3]
[]
[]
[4, 5]
[]
```

### [cw_editdistance(a STRING, b STRING)](cw_editdistance.sqlx)
Similar to teradata's editdistance without weightages
```sql
SELECT bqutil.fn.cw_editdistance('Jim D. Swain', 'Jim D. Swain');
SELECT bqutil.fn.cw_editdistance('Jim D. Swain', 'John Smith');

0
9
```

### [cw_error_number(errmsg string)](cw_error_number.sqlx)
Convert BQ generated error string to a number appropriate for other DBs
```sql
SELECT bqutil.fn.cw_error_number('Error Message');

1
```

### [cw_error_severity(errmsg string)](cw_error_severity.sqlx)
Convert BQ generated error string to a number appropriate for other DBs
```sql
SELECT bqutil.fn.cw_error_severity('Error Message');

1
```

### [cw_error_state(errmsg string)](cw_error_state.sqlx)
Convert BQ generated error string to a number appropriate for other DBs
```sql
SELECT bqutil.fn.cw_error_state('Error Message');

1
```

### [cw_find_in_list(needle STRING, list STRING)](cw_find_in_list.sqlx)
Find index of element in set.
```sql
SELECT bqutil.fn.cw_find_in_list("1", "[Test,1,2]");

2
```

### [cw_from_base(number STRING, base INT64)](cw_from_base.sqlx)
Convert string from given base to decimal
```sql
SELECT bqutil.fn.cw_from_base('001101011', 2);
SELECT bqutil.fn.cw_from_base('A', 16);

107
10
```

### [cw_getbit(bits INT64, index INT64)](cw_getbit.sqlx)
Return bit of INT64 input at given index, starting from 0 for the least significant bit.
```sql
SELECT bqutil.fn.cw_getbit(11, 100);
SELECT bqutil.fn.cw_getbit(11, 3);

0
1
```

### [cw_getbit_binary(bits BYTES, index INT64)](cw_getbit_binary.sqlx)
Return bit of BYTES input at given index, starting from 0 for the least significant bit.
```sql
SELECT bqutil.fn.cw_getbit_binary(b'\x0B', 100);
SELECT bqutil.fn.cw_getbit_binary(b'\x0B', 3);

0
1
```

### [cw_initcap(s STRING)](cw_initcap.sqlx)
Takes an input string and returns input string with first letter capital.
```sql
SELECT bqutil.fn.cw_initcap('teststr');
SELECT bqutil.fn.cw_initcap('test str');

Teststr
Test Str
```

### [cw_instr4(source STRING, search STRING, position INT64, ocurrence INT64)](cw_instr4.sqlx)
Takes an input source string, search string within source, position and number of occurrence. It returns index number of last occurrence staring position from position in source.
```sql
SELECT bqutil.fn.cw_instr4('TestStr123456Str', 'Str', 1, 2);

14
```

### [cw_json_array_contains_bool(json STRING, needle BOOL)](cw_json_array_contains_bool.sqlx)
Same as cw_json_array_contains_str(STRING, STRING) UDF but with needle = boolean
```sql
SELECT bqutil.fn.cw_json_array_contains_bool('[1, 2, 3, "valid", true]', true);
SELECT bqutil.fn.cw_json_array_contains_bool('[1, 2, 3, "valid", true]', false);

true
false
```

### [cw_json_array_contains_num(json STRING, needle FLOAT64)](cw_json_array_contains_num.sqlx)
Same as cw_json_array_contains_str(STRING, STRING) UDF but with needle = number.
```sql
SELECT bqutil.fn.cw_json_array_contains_num('[1, 2, 3, "valid"]', 1.0);
SELECT bqutil.fn.cw_json_array_contains_num('[1, 2, 3, "valid"]', 5.0);

true
false
```

### [cw_json_array_contains_str(json STRING, needle STRING)](cw_json_array_contains_str.sqlx)
Determine if value exists in json (a string containing a JSON array).
```sql
SELECT bqutil.fn.cw_json_array_contains_str('["name", "test", "valid"]', 'test');

true
```

### [cw_json_array_get(json STRING, loc FLOAT64)](cw_json_array_get.sqlx)
Returns the element at the specified index into the json_array. The index is zero-based
```sql
SELECT bqutil.fn.cw_json_array_get('[{"name": "test"}, {"name": "test1"}]', 1.0);

test1
```

### [cw_json_array_length(json STRING)](cw_json_array_length.sqlx)
Returns the array length of json (a string containing a JSON array)
```sql
SELECT bqutil.fn.cw_json_array_length('[{"name": "test"}, {"name": "test1"}]');

2
```

### [cw_json_enumerate_array(text STRING)](cw_json_enumerate_array.sqlx)
Takes input JSON array and flatten it.
```sql
SELECT bqutil.fn.cw_json_enumerate_array('[{"name":"Cameron"}, {"name":"John"}]');
```
results:
|   Row   |  f0_.ordinal   |  f0_.jsonvalue               |
|---------|----------------|------------------------------|
|    1    |       1        |     {"name":"Cameron"}       |
|         |       2        |     {"name":"John"}          |

### [cw_lower_case_ascii_only(str STRING)](cw_lower_case_ascii_only.sqlx)
Lowercases only ASCII characters within a given string.
```sql
SELECT bqutil.fn.cw_lower_case_ascii_only('TestStr123456#');

teststr123456#
```

### [cw_map_create(keys ANY TYPE, vals ANY TYPE)](cw_map_create.sqlx)
Given an array of keys and values, creates an array of struct containing matched <key,value> from each array. Number of elements in each array should be equal otherwise remaining values will be ignored.
```sql
SELECT bqutil.fn.cw_map_create([1, 2, 3], ['A', 'B', 'C']);
```

results:
|   Row   |  f0_.key    |  f0_.value  |
|---------|-------------|-------------|
|    1    |       1     |     A       |
|         |       2     |     B       |
|         |       3     |     C       |

### [cw_map_get(maparray ANY TYPE, inkey ANY TYPE)](cw_map_get.sqlx)
Given an array of struct and needle, searches an array to find struct whose key-field matches needle, then it returns the value-field in the given struct.
```sql
SELECT bqutil.fn.cw_map_get([STRUCT(1 as key, "ABC" as value)], 1);

ABC
```

### [cw_map_parse(m string, pd string, kvd string)](cw_map_parse.sqlx)
String to map convert.
```sql
SELECT bqutil.fn.cw_map_parse("a=1 b=42", " ", "=");

([STRUCT("a" AS key, "1" AS value),
STRUCT("b" AS key, "42" AS value)])
```

### [cw_months_between(et DATETIME, st DATETIME)](cw_months_between.sqlx)
Similar to Teradata and Netezza's months_between function
```sql
SELECT bqutil.fn.months_between(DATETIME '2005-03-01 10:34:56', DATETIME '2005-02-28 11:22:33');

0.12795698924731182795698924731182795699
```

### [cw_next_day(date_value DATE, day_name STRING)](cw_next_day.sqlx)
Returns the date of the first weekday (second arugment) that is later than the date specified by the first argument.
```sql
SELECT bqutil.fn.cw_next_day('2022-09-21', 'we');

2022-09-28
```

### [cw_nvp2json1(nvp STRING)](cw_nvp2json1.sqlx)
Convert an input string of name-value pairs to a JSON object.
```sql
SELECT bqutil.fn.cw_nvp2json1('name=google&occupation=engineer&hair=color');

{"name":"google","occupation":"engineer","hair":"color"}
```

### [cw_nvp2json3(nvp STRING,name_delim STRING, val_delim STRING)](cw_nvp2json3.sqlx)
Convert an input string of name-value pairs to a JSON object.
name_delim is delimiter for keys. val_delim is delimiter for key-value.
```sql
SELECT bqutil.fn.cw_nvp2json3('name=google&occupation=engineer&hair=color', '&', '=');

{"name":"google","occupation":"engineer","hair":"color"}
```

### [cw_nvp2json4(nvp STRING, name_delim STRING, val_delim STRING, ignore_char STRING)](cw_nvp2json4.sqlx)
Convert an input string of name-value pairs to a JSON object.
name_delim is delimiter for keys. val_delim is delimiter for key-value. ignore_char is to ignore and removed from output json.
```sql
SELECT bqutil.fn.cw_nvp2json4('name=google#1&occupation=engineer#2&hair=color#3', '&', '=', '#');

{"name":"google1","occupation":"engineer2","hair":"color3"}
```

### [cw_otranslate(s STRING, key STRING, value STRING)](cw_otranslate.sqlx)
Takes input source string with key and value. It returns source string with replacement of key with value.
```sql
SELECT bqutil.fn.cw_otranslate('Thin and Thick', 'Thk', 'Sp');

Spin and Spic
```

### [cw_overlapping_partition_by_regexp(firstRn INT64, haystack STRING, regex STRING)](cw_overlapping_partition_by_regexp.sqlx)
Partitions rows into overlapping segments by matching their sequence with the provided regex pattern.
```sql
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(1, 'A@1#A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(2, 'A@2#B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(3, 'B@3#A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(4, 'A@4#B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')
SELECT bqutil.fn.cw_disjoint_partition_by_regexp(5, 'B@5#', '(?:A@\\d+#)+(?:B@\\d+#)')

[1, 2, 3]
[2, 3]
[]
[4, 5]
[]
```

### [cw_parse_timestamp(timeString STRING, formatString STRING)](cw_parse_timestamp.sqlx)
Parses a timestamp string according to a specified format string. Returns a TIMESTAMP value.

```sql
SELECT bqutil.fn.cw_parse_timestamp('Y-m-d H:M:s', '2024-03-20 14:30:00');

2024-03-20 14:30:00 UTC
```

The function uses JavaScript library Moment to parse the timestamp string according to the format string. format string is Joda time format. Returns NULL if the input string cannot be parsed according to the format string.

Input	                  Example	           Description
YYYY	                  2014	             4 or 2 digit year. Note: Only 4 digit can be parsed on strict mode
YY	                    14	               2 digit year
Y	                      -25	               Year with any number of digits and sign
Q	                      1..4	             Quarter of year. Sets month to first month in quarter.
M MM	                  1..12	             Month number
MMM MMMM	              Jan..December	     Month name in locale set by moment.locale()
D DD	                  1..31	             Day of month
DDD DDDD	              1..365	           Day of year
X	                      1410715640.579	   Unix timestamp
H HH	                  0..23	             Hours (24 hour time)
h hh	                  1..12	             Hours (12 hour time used with a A.)
k kk	                  1..24	             Hours (24 hour time from 1 to 24)
a A	                    am pm	             Post or ante meridiem (Note the one character a p are also considered valid)
m mm	                  0..59	             Minutes
s ss	                  0..59	             Seconds
S SS SSS ... SSSSSSSSS	0..999999999	     Fractional seconds
Z ZZ	                  +12:00	           Offset from UTC as +-HH:mm, +-HHmm, or Z



### [cw_period_intersection(p1 STRUCT<lower TIMESTAMP, upper TIMESTAMP>, p2 STRUCT<lower TIMESTAMP, upper TIMESTAMP>)](cw_period_intersection.sqlx)
```sql
SELECT bqutil.fn.cw_period_intersection(
  STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper),
  STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper))

STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)
```

### [cw_period_ldiff(p1 STRUCT<lower TIMESTAMP, upper TIMESTAMP>, p2 STRUCT<lower TIMESTAMP, upper TIMESTAMP>)](cw_period_ldiff.sqlx)
```sql
SELECT bqutil.fn.cw_period_ldiff(
  STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper),
  STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper))

STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-13 00:00:00' AS upper)
```

### [cw_period_rdiff(p1 STRUCT<lower TIMESTAMP, upper TIMESTAMP>, p2 STRUCT<lower TIMESTAMP, upper TIMESTAMP>)](cw_period_rdiff.sqlx)
```sql
SELECT bqutil.fn.cw_period_rdiff(
  STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper),
  STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper))

STRUCT(TIMESTAMP '2001-11-14 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)
```

### [cw_regex_mode(mode STRING)](cw_regex_mode.sqlx)
Retrieve mode.
```sql
SELECT bqutil.fn.cw_regex_mode('i');
SELECT bqutil.fn.cw_regex_mode('m');
SELECT bqutil.fn.cw_regex_mode('n);

ig
mg
sg
```

### [cw_regexp_extract(str STRING, regexp STRING)](cw_regexp_extract.sqlx)
Extracts the first substring matched by the regular expression regexp in str, returns null if the regex doesn't have a match or either str or regexp is null.
```sql
SELECT bqutil.fn.cw_regexp_extract('TestStr123456#?%&', 'Str');
SELECT bqutil.fn.cw_regexp_extract('TestStr123456#?%&', 'StrX');
SELECT bqutil.fn.cw_regexp_extract(NULL, 'StrX');
SELECT bqutil.fn.cw_regexp_extract('TestStr123456#?%&', NULL);

Str
NULL
NULL
NULL
```

### [cw_regexp_extract_all(str STRING, regexp STRING)](cw_regexp_extract_all.sqlx)
Returns the substring(s) matched by the regular expression regexp in str, returns null if the regex doesn't have a match or either str or regexp is null.
```sql
SELECT bqutil.fn.cw_regexp_extract_all('TestStr123456', 'Str.*');
SELECT bqutil.fn.cw_regexp_extract_all('TestStr123456', 'StrX.*');
SELECT bqutil.fn.cw_regexp_extract_all(NULL, 'Str.*');
SELECT bqutil.fn.cw_regexp_extract_all('TestStr123456', NULL);

[Str123456]
NULL
NULL
NULL
```

### [cw_regexp_extract_all_n(str STRING, regexp STRING, groupn INT64)](cw_regexp_extract_all_n.sqlx)
Finds all occurrences of the regular expression regexp in str and returns the capturing group number groupn.
```sql
SELECT bqutil.fn.cw_regexp_extract_all_n('TestStr123456Str789', 'Str.*', 0);

Str123456Str789
```

### [cw_regexp_extract_all_start_pos(str STRING, regexp STRING, position INT64)](cw_regexp_extract_all_start_pos.sqlx)
Finds all occurrences of the regular expression regexp in str starting from position. Returns null if either str or regexp is null.
```sql
SELECT bqutil.fn.cw_regexp_extract_all_start_pos('TestStr123456Str789', 'Str.*', 2);
SELECT bqutil.fn.cw_regexp_extract_all_start_pos('TestStr123456Str789', 'Str.*', 12);

Str123456Str789
Str789
```

### [cw_regexp_extract_n(str STRING, regexp STRING, groupn INT64)](cw_regexp_extract_n.sqlx)
Finds the first occurrence of the regular expression regexp in str and returns the capturing group number groupn.
```sql
SELECT bqutil.fn.cw_regexp_extract_n('TestStr123456', 'Str', 0);

Str
```

### [cw_regexp_instr_2(haystack STRING, needle STRING)](cw_regexp_instr_2.sqlx)
Takes input haystack string with needle string. Returns starting index of needle.
```sql
SELECT bqutil.fn.cw_regexp_instr_2('TestStr123456', 'Str');
SELECT bqutil.fn.cw_regexp_instr_2('TestStr123456', '90');

5
0
```

### [cw_regexp_instr_3(haystack STRING, needle STRING, start INT64)](cw_regexp_instr_3.sqlx)
Takes input haystack string, needle string and starting positin from where search will start. Returns starting index of needle.
```sql
SELECT bqutil.fn.cw_regexp_instr_3('TestStr123456', 'Str', 0);
SELECT bqutil.fn.cw_regexp_instr_3('TestStr123456', 'Str', 6);

5
0
```

### [cw_regexp_instr_4(haystack STRING, needle STRING, start INT64)](cw_regexp_instr_4.sqlx)
Takes input haystack string, needle string, starting positin from where search will start and number of occurance. Returns starting index of last needle.
```sql
SELECT bqutil.fn.cw_regexp_instr_4('TestStr123456', 'Str', 1, 1);
SELECT bqutil.fn.cw_regexp_instr_4('TestStr123456Str', 'Str', 1, 2);
SELECT bqutil.fn.cw_regexp_instr_4('TestStr123456Str', 'Str', 1, 3);

5
14
0
```

### [cw_regexp_instr_5(haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64)](cw_regexp_instr_5.sqlx)
Takes input haystack string, needle string, starting position from where search will start, the 1-based number of match occurence which , and returnopt number. Returns end index +1 of last needle. Mode can be g for global search, i for case insensetive search and m for multiline search.
```sql
SELECT bqutil.fn.cw_regexp_instr_5('TestStr123456', '123', 1, 1, 1);

11
```

### [cw_regexp_instr_6(haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64, mode STRING)](cw_regexp_instr_6.sqlx)
Takes input haystack string, needle string, starting positin from where search will start, number of occurance, returnopt number and mode. Returns end index +1 of last needle. Mode can be g for global search, i for case insensetive search and m for multiline search.
```sql
SELECT bqutil.fn.cw_regexp_instr_6('TestStr123456', 'Str', 1, 1, 1, 'g');

8
```

### [cw_regexp_instr_generic(haystack STRING, regexp STRING, p INT64, o INT64, returnopt INT64, mode STRING)](cw_regexp_instr_generic.sqlx)
Takes input haystack string, needle string, starting positin from where search will start, number of occurance, returnopt number and mode. Returns end index +1 of last needle. Mode can be g for global search, i for case insensetive search and m for multiline search.
```sql
SELECT bqutil.fn.cw_regexp_instr_generic('TestStr123456', 'Str', 1, 1, 1, 'g');

8
```

### [cw_regexp_replace_4(haystack STRING, regexp STRING, replacement STRING, offset INT64)](cw_regexp_replace_4.sqlx)
Takes input haystack string, regular expression, replacement string and 1-based starting offset. It returns new string with replacement string matches accordingly regular expression.
```sql
SELECT bqutil.fn.cw_regexp_replace_4('TestStr123456', 'Str', 'Cad$', 1);

TestCad$123456
```

### [cw_regexp_replace_5(haystack STRING, regexp STRING, replacement STRING, offset INT64, occurrence INT64)](cw_regexp_replace_5.sqlx)
Takes input haystack string, regular expression, replacement string, 1-based starting offset, 1-based number of the occurence which we want to replace. It returns new string with replacement string matches accordingly regular expression.
```sql
SELECT bqutil.fn.cw_regexp_replace_5('TestStr123456', 'Str', 'Cad$', 1, 1);
SELECT bqutil.fn.cw_regexp_replace_5('TestStr123456Str', 'Str', 'Cad$', 1, 2);
SELECT bqutil.fn.cw_regexp_replace_5('TestStr123456Str', 'Str', 'Cad$', 1, 1);

TestCad$123456
TestStr123456Cad$
TestCad$123456Str
```

### [cw_regexp_replace_6(haystack STRING, regexp STRING, replacement STRING, p INT64, o INT64, mode STRING)](cw_regexp_replace_6.sqlx)
Takes input haystack string, regular expression, replacement string, 1-based starting offset, 1-based number of the occurence which we want to replace, and the mode. It returns new string with replacement string matches accordingly regular expression. Mode can be g for global search, i for case insensetive search and m for multiline search.
```sql
SELECT bqutil.fn.cw_regexp_replace_6('TestStr123456', 'Str', '$:#>', 1, 1, 'i');

Test$:#>123456
```

### [cw_regexp_replace_generic(haystack STRING, regexp STRING, replacement STRING, offset INT64, occurrence INT64, mode STRING)](cw_regexp_replace_generic.sqlx)
Generic regexp_replace, which is the 6-args version with regexp_mode already decoded
```sql
SELECT bqutil.fn.cw_regexp_replace_generic('TestStr123456', 'Str', '$:#>', 1, 1, 'i');

Test$:#>123456
```

### [cw_regexp_split(text string, delim string, flags string)](cw_regexp_split.sqlx)
Takes input string, delimiter and flags. It generates pair from string tokenizer. Flags works like Regex mode of javascript.
```sql
SELECT bqutil.fn.cw_regexp_split('Test#1', '#', 'i');

([STRUCT(CAST(1 AS INT64) AS tokennumber, "Test" AS token),
STRUCT(CAST(2 AS INT64) AS tokennumber, "1" AS token)])
```

### [cw_regexp_substr_4(h STRING, n STRING, p INT64, o INT64)](cw_regexp_substr_4.sqlx)
Takes input haystack string, needle string, position and occurence. It returns needle from the starting position if present with number of occurence time in haystack.
```sql
SELECT bqutil.fn.cw_regexp_substr_4('TestStr123456', 'Test', 1, 1);
SELECT bqutil.fn.cw_regexp_substr_4('TestStr123456Test', 'Test', 1, 2);
SELECT bqutil.fn.cw_regexp_substr_4('TestStr123456Test', 'Test', 1, 3);
SELECT bqutil.fn.cw_regexp_substr_4('Test123Str123Test', '(Test|Str)123', 1, 1);
SELECT bqutil.fn.cw_regexp_substr_4('Test123Str123Test', '(Test|Str)123', 1, 2);

Test
Test
null
Test123
Str123
```

### [cw_regexp_substr_5(h STRING, n STRING, p INT64, o INT64, mode STRING)](cw_regexp_substr_5.sqlx)
Takes input haystack string, needle string, position, occurence and mode. It returns needle from the starting position if present with number of occurence time in haystack. Mode can be g for global search, i for case insensetive search and m for multiline search.
```sql
SELECT bqutil.fn.cw_regexp_substr_5('TestStr123456', 'Test', 1, 1, 'g');
SELECT bqutil.fn.cw_regexp_substr_5('TestStr123456Test', 'test', 1, 2, 'i');
SELECT bqutil.fn.cw_regexp_substr_5('TestStr123456\nTest', 'Test', 1, 2, 'm');

Test
Test
Test
```

### [cw_regexp_substr_6(h STRING, n STRING, p INT64, o INT64, mode STRING, g INT64)](cw_regexp_substr_6.sqlx)
Takes input haystack string, needle string, position, occurence and mode. It returns needle from the starting position if present with number of occurence time in haystack. Mode can be g for global search, i for case insensetive search and m for multiline search.
```sql
SELECT bqutil.fn.cw_regexp_substr_6('TestStr123456', 'Test', 1, 1, 'g', 0);

Test
```

### [cw_regexp_substr_generic(str STRING, regexp STRING, p INT64, o INT64, mode STRING, g INT64)](cw_regexp_substr_generic.sqlx)
Generic regex based substring function.
```sql
SELECT bqutil.fn.cw_regexp_substr_generic('TestStr123456', 'Test', 1, 1, 'g', 0);

Test
```

### [cw_round_half_even(n BIGNUMERIC, d INT64)](cw_round_half_even.sqlx)
Round half even number
```sql
SELECT bqutil.fn.cw_round_half_even(10, 10);

10
```

### [cw_round_half_even_bignumeric(n BIGNUMERIC, d INT64)](cw_round_half_even_bignumeric.sqlx)
Round half even bignumeric number
```sql
SELECT bqutil.fn.cw_round_half_even_bignumeric(10, 10);

10
```

### [cw_runtime_parse_interval_seconds(ival STRING)](cw_runtime_parse_interval_seconds.sqlx)
Kludge for interval translation - for now day->sec only
```sql
SELECT bqutil.fn.cw_runtime_parse_interval_seconds(1 DAY);

86400
```

### [cw_setbit(bits INT64, index INT64)](cw_setbit.sqlx)
Set bit and return new bits
```sql
SELECT bqutil.fn.cw_setbit(1001, 2);

1005
```

### [cw_signed_leftshift_128bit(value BIGNUMERIC, n BIGNUMERIC)](cw_signed_leftshift_128bit.sqlx)
Performs a signed shift left on BIGNUMERIC as if it was a 128 bit integer.
```sql
- SELECT bqutil.fn.cw_signed_leftshift_128bit(NUMERIC '1', NUMERIC '3');
- SELECT bqutil.fn.cw_signed_leftshift_128bit(NUMERIC '1', NUMERIC '127');
- SELECT bqutil.fn.cw_signed_leftshift_128bit(NUMERIC '-5', NUMERIC '2');

- 8
- -170141183460469231731687303715884105728
- -20
```

### [cw_signed_rightshift_128bit(value BIGNUMERIC, n BIGNUMERIC)](cw_signed_rightshift_128bit.sqlx)
Performs a signed shift right on BIGNUMERIC as if it was a 128 bit integer.
```sql
- SELECT bqutil.fn.cw_signed_rightshift_128bit(NUMERIC '32', NUMERIC '3');
- SELECT bqutil.fn.cw_signed_rightshift_128bit(NUMERIC '7', NUMERIC '1');
- SELECT bqutil.fn.cw_signed_rightshift_128bit(NUMERIC '-7', NUMERIC '1');
- SELECT bqutil.fn.cw_signed_rightshift_128bit(NUMERIC '-1', NUMERIC '1');
- SELECT bqutil.fn.cw_signed_rightshift_128bit(NUMERIC '-1', NUMERIC '100');

- 4
- 3
- -4
- -1
- -1
```

### [cw_split_part_delimstr_idx(value STRING, delimiter STRING, part INT64)](cw_split_part_delimstr_idx.sqlx)
Extract a part from a string value delimited by a delimiter string.
Indexing start from 1. Negative offsets count from the end.

```SQL
- SELECT bqutil.fn.cw_split_part_delimstr_idx('foo bar baz', ' ', 3)
- SELECT bqutil.fn.cw_split_part_delimstr_idx('foo bar baz', ' ', -3)
- SELECT bqutil.fn.cw_split_part_delimstr_idx('foo bar baz', ' ', 4)

- bar
- foo
- NULL
```


### [cw_stringify_interval(x INT64)](cw_stringify_interval.sqlx)
Formats the interval as 'day hour:minute:second
```sql
SELECT bqutil.fn.cw_stringify_interval(86100);

+0000 23:55:00
```

### [cw_strtok(text string, delim string)](cw_strtok.sqlx)
Takes input string and delimiter. It generates pair from string tokenizer.
```sql
SELECT bqutil.fn.cw_strtok('Test#1', '#');

([STRUCT(CAST(1 AS INT64) AS tokennumber, "Test" AS token),
STRUCT(CAST(2 AS INT64) AS tokennumber, "1" AS token)])
```

### [cw_substrb(str STRING, startpos INT64, extent INT64)](cw_substrb.sqlx)
Treats the multibyte character string as a string of octets (bytes).
```sql
SELECT bqutil.fn.cw_substrb('TestStr123', 0, 3);

Te
```

### [cw_substring_index(str STRING, sep STRING, idx INT64)](cw_substring_index.sqlx)
Takes input string, seperater string and index number. It returns index element.
```sql
SELECT bqutil.fn.cw_substring_index('TestStr123456,Test123', ',', 1);

TestStr123456
```

### [#cw_td_normalize_number(str STRING)](#cw_td_normalize_number.sqlx)
Takes string representation of number, parses it according to Teradata rules and returns a normalized string, that is parseable by BigQuery.
```sql
SELECT bqutil.fn.cw_td_normalize_number('12:34:56');
SELECT bqutil.fn.cw_td_normalize_number('3.14e-1');
SELECT bqutil.fn.cw_td_normalize_number('00042-');
SELECT bqutil.fn.cw_td_normalize_number('Hello World!');

'123456'
'0.314'
'-42'
'ILLEGAL_NUMBER(Hello World!)'
```

### [cw_td_nvp(haystack STRING, needle STRING, pairsep STRING, valuesep STRING, occurence INT64)](cw_td_nvp.sqlx)
Extract a value from a key-value separated string
```sql
SELECT bqutil.fn.cw_td_nvp('entree:orange chicken#entree2:honey salmon', 'entree', '#', ':', 1);

orange chicken
```

### [cw_threegrams(t STRING)](cw_threegrams.sqlx)
Takes input string with space. Space delimiter words will repeat three times and generate array.
```sql
SELECT bqutil.fn.cw_threegrams('Test 1234 str abc');

["Test 1234 str", "1234 str abc"]
```

### [cw_to_base(number INT64, base INT64)](cw_to_base.sqlx)
Convert string from decimal to given base
```sql
SELECT bqutil.fn.cw_to_base(5, 2);
SELECT bqutil.fn.cw_to_base(10, 16);

101
a
```

### [cw_ts_overlap_buckets(includeMeets BOOLEAN, inputs ARRAY<STRUCT<st TIMESTAMP, et TIMESTAMP>>)](cw_ts_overlap_buckets.sqlx)
Merges two periods together if they overlap and returns unique id for each merged bucket. Coalesces meeting periods as well (not just overlapping periods) if includeMeets is true.
```sql
SELECT bqutil.fn.cw_ts_overlap_buckets(false, [STRUCT(TIMESTAMP("2008-12-25"), TIMESTAMP("2008-12-31")), STRUCT(TIMESTAMP("2008-12-26"), TIMESTAMP("2008-12-30"))]);
```

results:
|   Row   |  f0_.bucketNo   |  f0_.st                      |  f0_.et                 |
|---------|-----------------|------------------------------|-------------------------|
|    1    |       1         |     2008-12-25 00:00:00 UTC  | 2008-12-31 00:00:00 UTC |

### [cw_ts_pattern_match(evSeries ARRAY<STRING>, regexpParts ARRAY<STRING>)](cw_ts_pattern_match.sqlx)
ts_pattern_match is function that returns range of matched pattern in given UID, SID (user session)
```sql
SELECT bqutil.fn.cw_ts_pattern_match(['abc', 'abc'], ['abc']);
```

results:
|   Row   |  f0_.pattern_id   |  f0_.start      |  f0_.stop    |
|---------|-------------------|-----------------|--------------|
|    1    |       1           |     1           |   1          |
|         |       2           |     2           |   2          |

### [cw_twograms(t STRING)](cw_twograms.sqlx)
Takes input string with space. Space delimiter words will repeat two times and generate array.
```sql
SELECT bqutil.fn.cw_twograms('Test Str 123456 789');

["Test Str", "Str 123456", "123456 789"]
```

### [cw_url_decode(path STRING)](cw_url_decode.sqlx)
URL decode a string
```sql
SELECT bqutil.fn.cw_url_decode("%3F");
SELECT bqutil.fn.cw_url_decode("%2F");

?
/
```

### [cw_url_encode(path STRING)](cw_url_encode.sqlx)
URL encode a string
```sql
SELECT bqutil.fn.cw_url_encode("?");
SELECT bqutil.fn.cw_url_encode("/");

%3F
%2F
```

### [cw_url_extract_authority(url STRING)](cw_url_extract_authority.sqlx)
Extract the authority from a url, returns "" (empty string) if no authority is found.
```sql
SELECT bqutil.fn.cw_url_extract_authority('https://localhost:8080/test?key=val');

localhost:8080
```

### [cw_url_extract_file(url STRING)](cw_url_extract_file.sqlx)
Extract the file from a url, returns "" (empty string) string if no file is found.
```sql
SELECT bqutil.fn.cw_url_extract_file('https://www.test.com/collections-in-java#collectionmethods');

/collections-in-java
```

### [cw_url_extract_fragment(url STRING)](cw_url_extract_fragment.sqlx)
Extract the fragment from a url, returns "" (empty string) if no fragment is found.
```sql
SELECT bqutil.fn.cw_url_extract_fragment('https://www.test.com/collections-in-java#collectionmethods');

collectionmethods
```

### [cw_url_extract_host(url STRING)](cw_url_extract_host.sqlx)
Extract the host from a url, return "" (empty string) if no host is found.
```sql
SELECT bqutil.fn.cw_url_extract_host('https://google.com');

google.com
```

### [cw_url_extract_parameter(url STRING, pname STRING)](cw_url_extract_parameter.sqlx)
Extract the value of a query param from a url, returns null if the parameter isn't found.
```sql
SELECT bqutil.fn.cw_url_extract_parameter('https://www.test.com/collections-in-java&key=val#collectionmethods', 'key');

val
```

### [cw_url_extract_path(url STRING)](cw_url_extract_path.sqlx)
Extract the path from a url, returns "" (empty string) if no path is found.
```sql
SELECT bqutil.fn.cw_url_extract_path('https://www.test.com/collections-in-java#collectionmethods');

/collections-in-java
```

### [cw_url_extract_port(url STRING)](cw_url_extract_port.sqlx)
Extract the port from a url, returns null if no port is found.
```sql
SELECT bqutil.fn.cw_url_extract_port('https://localhost:8080/test?key=val');

8080
```

### [cw_url_extract_protocol(url STRING)](cw_url_extract_protocol.sqlx)
Extract the protocol from a url, return "" (empty string) if no protocol is found.
```sql
SELECT bqutil.fn.cw_url_extract_protocol('https://google.com/test?key=val');

https
```

### [cw_url_extract_query(url STRING)](cw_url_extract_query.sqlx)
Extract the query from a url, returns "" (empty string) if no query is found.
```sql
SELECT bqutil.fn.cw_url_extract_query('https://localhost:8080/test?key=val');

key=val
```

### [cw_width_bucket(value_expression FLOAT64, lower_bound FLOAT64, upper_bound FLOAT64, partition_count INT64)](cw_width_bucket.sqlx)
Emulates WIDTH_BUCKET present in many dialects.
```sql
- SELECT bqutil.fn.cw_width_bucket(4, 0, 10, 6);
- SELECT bqutil.fn.cw_width_bucket(4, 10, 0, 6);
- SELECT bqutil.fn.cw_width_bucket(15, 0, 10, 6);
- SELECT bqutil.fn.cw_width_bucket(-10, 0, 10, 6);

- 3
- 4
- 7
- 0
```

### [day_occurrence_of_month(date_expression ANY TYPE)](day_occurrence_of_month.sqlx)
Returns the nth occurrence of the weekday in the month for the specified date. The result is an INTEGER value between 1 and 5.
```sql
SELECT
  bqutil.fn.day_occurrence_of_month(DATE '2020-07-01'),
  bqutil.fn.day_occurrence_of_month(DATE '2020-07-08');

1 2
```

### [degrees(x ANY TYPE)](degrees.sqlx)
Convert radians values into degrees.

```sql
SELECT bqutil.fn.degrees(3.141592653589793) is_this_pi

180.0
```

### [exif(src_obj_ref STRUCT<uri STRING, version STRING, authorizer STRING, details JSON>)](exif.sqlx)
Extract EXIF data as JSON from the ObjectRef of GCS files. Now only support image types.

```sql
SELECT bqutil.mm.exif(
  OBJ.MAKE_REF("gs://<PATH>/<TO>/<YOUR_IMAGE_FILE>", "<BQ_LOCATION>.<YOUR_CONNECTION_ID>")
) AS output_json
```

results:

| output_json |
| ----------- |
| {"ExifOffset": 47, "Make": "MyCamera"} |


### [find_in_set(str STRING, strList STRING)](find_in_set.sqlx)
Returns the first occurance of str in strList where strList is a comma-delimited string.
Returns null if either argument is null.
Returns 0 if the first argument contains any commas.
For example, find_in_set('ab', 'abc,b,ab,c,def') returns 3.
Input:
str: string to search for.
strList: string in which to search.
Output: Position of str in strList
```sql
WITH test_cases AS (
  SELECT 'ab' as str, 'abc,b,ab,c,def' as strList
  UNION ALL
  SELECT 'ab' as str, 'mobile,tablet,mobile/tablet,phone,text' as strList
  UNION ALL
  SELECT 'mobile' as str, 'mobile,tablet,mobile/tablet,phone,text' as strList
  UNION ALL
  SELECT 'mobile,' as str, 'mobile,tablet,mobile/tablet,phone,text' as strList
)
SELECT bqutil.fn.find_in_set(str, strList) from test_cases
```

results:

| f0_  |
|------|
|    3 |
| NULL |
|    1 |
|    0 |



### [freq_table(arr ANY TYPE)](freq_table.sqlx)
Construct a frequency table (histogram) of an array of elements.
Frequency table is represented as an array of STRUCT(value, freq)

```sql
SELECT bqutil.fn.freq_table([1,2,1,3,1,5,1000,5]) ft
```

results:

|   Row   |  ft.value  |  ft.freq  |
|---------|------------|-----------|
|    1    |       1    |     3     |
|         |       2    |     1     |
|         |       3    |     1     |
|         |       5    |     2     |
|         |    1000    |     1     |


### [from_binary(value STRING)](from_binary.sqlx)
Returns a number in decimal form from its binary representation.

```sql
SELECT
  bqutil.fn.to_binary(x) AS binary,
  bqutil.fn.from_binary(bqutil.fn.to_binary(x)) AS x
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:

|                              binary                              |     x      |
|------------------------------------------------------------------|------------|
| 0000000000000000000000000000000000000000000000000000000000000001 |          1 |
| 0000000000000000000000000000000000000000000000011110001001000000 |     123456 |
| 0000000000000000000000000000001001001100101100000001011011101010 | 9876543210 |
| 1111111111111111111111111111111111111111111111111111110000010111 |      -1001 |


### [from_hex(value STRING)](from_hex.sqlx)
Returns a number in decimal form from its hexadecimal representation.

```sql
SELECT
  bqutil.fn.to_hex(x) AS hex,
  bqutil.fn.from_hex(bqutil.fn.to_hex(x)) AS x
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:

|       hex        |     x      |
|------------------|------------|
| 0000000000000001 |          1 |
| 000000000001e240 |     123456 |
| 000000024cb016ea | 9876543210 |
| fffffffffffffc17 |      -1001 |


### [get_array_value(k STRING, arr ANY TYPE)](get_array_value.sqlx)
Given a key and a map, returns the ARRAY type value.
This is same as get_value except it returns an ARRAY type.
This can be used when the map has multiple values for a given key.
```sql
WITH test AS (
  SELECT ARRAY(
    SELECT STRUCT('a' AS key, 'aaa' AS value) AS s
    UNION ALL
    SELECT STRUCT('b' AS key, 'bbb' AS value) AS s
    UNION ALL
    SELECT STRUCT('a' AS key, 'AAA' AS value) AS s
    UNION ALL
    SELECT STRUCT('c' AS key, 'ccc' AS value) AS s
  ) AS a
)
SELECT bqutil.fn.get_array_value('b', a), bqutil.fn.get_array_value('a', a), bqutil.fn.get_array_value('c', a) from test;
```

results:

|   f0_   |      f1_      |   f2_   |
|---------|---------------|---------|
| ["bbb"] | ["aaa","AAA"] | ["ccc"] |


### [getbit(target_arg INT64, target_bit_arg INT64)](getbit.sqlx)
Given an INTEGER value, returns the value of a bit at a specified position. The position of the bit starts from 0.

```sql
SELECT bqutil.fn.getbit(23, 2), bqutil.fn.getbit(23, 3), bqutil.fn.getbit(null, 1)

1 0 NULL
```

### [get_value(k STRING, arr ANY TYPE)](get_value.sqlx)
Given a key and a list of key-value maps in the form [{'key': 'a', 'value': 'aaa'}], returns the SCALAR type value.

```sql
WITH test AS (
  SELECT ARRAY(
    SELECT STRUCT('a' AS key, 'aaa' AS value) AS s
    UNION ALL
    SELECT STRUCT('b' AS key, 'bbb' AS value) AS s
    UNION ALL
    SELECT STRUCT('c' AS key, 'ccc' AS value) AS s
  ) AS a
)
SELECT bqutil.fn.get_value('b', a), bqutil.fn.get_value('a', a), bqutil.fn.get_value('c', a) from test;
```

results:

| f0_ | f1_ | f2_ |
|-----|-----|-----|
| bbb | aaa | ccc |


### [gunzip(gzipped BYTES)](gunzip.sqlx)
Given compressed BYTES using the `DEFLATE` (a.k.a. `gzip`) algorithm, this method will return the decompressed value as BYTES.

```sql
SELECT CAST(bqutil.fn.gunzip(FROM_BASE64("H4sIAOL4JWgAA8tIzcnJVyjPL8pJAQCFEUoNCwAAAA==")) AS STRING)
```

results:

| f0_ |
|-----|
| hello world |


### [int(v ANY TYPE)](int.sqlx)
Convience wrapper which can be used to convert values to integers in place of
the native `CAST(x AS INT64)`.

```sql
SELECT bqutil.fn.int(1) int1
  , bqutil.fn.int(2.5) int2
  , bqutil.fn.int('7') int3
  , bqutil.fn.int('7.8') int4

1, 2, 7, 7
```

Note that CAST(x AS INT64) rounds the number, while this function truncates it. In many cases, that's the behavior users expect.

### [jaccard()](jaccard.sqlx)
Accepts two string and returns the distance using Jaccard algorithm.
```sql
SELECT
       bqutil.fn.jaccard('thanks', 'thaanks'),
       bqutil.fn.jaccard('thanks', 'thanxs'),
       bqutil.fn.jaccard('bad demo', 'abd demo'),
       bqutil.fn.jaccard('edge case', 'no match'),
       bqutil.fn.jaccard('Special. Character?', 'special character'),
       bqutil.fn.jaccard('', ''),
1, 0.71, 1.0, 0.25, 0.67, 0.0
```

### [job_url(job_id STRING)](job_url.sqlx)
Generates a deep link to the BigQuery console for a given job_id in the form: `project:location.job_id`.
```sql
SELECT bqutil.fn.job_url("my_project:us.my_job_id")

https://console.cloud.google.com/bigquery?project=my_project&j=bq:us:my_job_id
```

### [json_extract_keys()](json_extract_keys.sqlx)
Returns all keys in the input JSON as an array of string
Returns NULL if invalid JSON string is passed,

```sql
SELECT bqutil.fn.json_extract_keys(
  '{"foo" : "cat", "bar": "dog", "hat": "rat"}'
) AS keys_array

foo
bar
hat
```

### [json_extract_key_value_pairs()](json_extract_key_value_pairs.sqlx)
Returns all key/values pairs in the input JSON as an array
of STRUCT<key STRING, value STRING>
Returns NULL if invalid JSON string is passed,


```sql
SELECT * FROM UNNEST(
  bqutil.fn.json_extract_key_value_pairs(
    '{"foo" : "cat", "bar": [1,2,3], "hat": {"qux": true}}'
  )
)

key,value
foo,"cat"
bar,[1,2,3]
hat,{"qux":true}
```

### [json_extract_values()](json_extract_values.sqlx)
Returns all values in the input JSON as an array of string
Returns NULL if invalid JSON string is passed,


```sql
SELECT bqutil.fn.json_extract_values(
  '{"foo" : "cat", "bar": "dog", "hat": "rat"}'
) AS keys_array

cat
dog
rat
```

### [json_typeof(json string)](json_typeof.sqlx)

Returns the type of JSON value. It emulates [`json_typeof` of PostgreSQL](https://www.postgresql.org/docs/12/functions-json.html).

```sql
SELECT
       bqutil.fn.json_typeof('{"foo": "bar"}'),
       bqutil.fn.json_typeof(TO_JSON_STRING(("foo", "bar"))),
       bqutil.fn.json_typeof(TO_JSON_STRING([1,2,3])),
       bqutil.fn.json_typeof(TO_JSON_STRING("test")),
       bqutil.fn.json_typeof(TO_JSON_STRING(123)),
       bqutil.fn.json_typeof(TO_JSON_STRING(TRUE)),
       bqutil.fn.json_typeof(TO_JSON_STRING(FALSE)),
       bqutil.fn.json_typeof(TO_JSON_STRING(NULL)),

object, array, string, number, boolean, boolean, null
```

### [knots_to_mph(input_knots FLOAT64)](knots_to_mph.sqlx)
Converts knots to miles per hour
```sql
SELECT bqutil.fn.knots_to_mph(37.7);

43.384406
```

### [levenshtein(source STRING, target STRING) RETURNS INT64](levenshtein.sqlx)
Returns an integer number indicating the degree of similarity between two strings (0=identical, 1=single character difference, etc.)

```sql
SELECT
  source,
  target,
  bqutil.fn.levenshtein(source, target) distance,
FROM UNNEST([
  STRUCT('analyze' AS source, 'analyse' AS target),
  STRUCT('opossum', 'possum'),
  STRUCT('potatoe', 'potatoe'),
  STRUCT('while', 'whilst'),
  STRUCT('aluminum', 'alumininium'),
  STRUCT('Connecticut', 'CT')
]);
```

Row | source      | target      | distance
--- | ----------- | ----------- | ---------
1   |	analyze     | analyse     | 1
2   | opossum     | possum      | 1
3   | potatoe     | potatoe     | 0
4   | while       | whilst      | 2
5   | aluminum    | alumininium | 3
6   | Connecticut | CT          | 10

> This function is based on the [Levenshtein distance algorithm](https://en.wikipedia.org/wiki/Levenshtein_distance) which determines the minimum number of single-character edits (insertions, deletions or substitutions) required to change one source string into another target one.


### [linear_interpolate(pos INT64, prev STRUCT<x INT64, y FLOAT64>, next STRUCT<x INT64, y FLOAT64>)](linear_interpolate.sqlx)
Interpolate the current positions value from the preceding and folllowing coordinates

```sql
SELECT
  bqutil.fn.linear_interpolate(2, STRUCT(0 AS x, 0.0 AS y), STRUCT(10 AS x, 10.0 AS y)),
  bqutil.fn.linear_interpolate(2, STRUCT(0 AS x, 0.0 AS y), STRUCT(20 AS x, 10.0 AS y))
```

results:

| f0_ | f1_ |
|-----|-----|
| 2.0 | 1.0 |


### [median(arr ANY TYPE)](median.sqlx)
Get the median of an array of numbers.

```sql
SELECT bqutil.fn.median([1,1,1,2,3,4,5,100,1000]) median_1
  , bqutil.fn.median([1,2,3]) median_2
  , bqutil.fn.median([1,2,3,4]) median_3

3.0, 2.0, 2.5
```

### [meters_to_miles(input_meters FLOAT64)](meters_to_miles.sqlx)
Converts meters to miles
```sql
SELECT bqutil.fn.meters_to_miles(5000.0);

3.1068559611866697
```

### [miles_to_meters(input_miles FLOAT64)](miles_to_meters.sqlx)
Converts miles to meters
```sql
SELECT bqutil.fn.miles_to_meters(2.73);

4393.50912
```

### [mph_to_knots(input_mph FLOAT64)](mph_to_knots.sqlx)
Converts miles per hour to knots
```sql
SELECT bqutil.fn.mph_to_knots(75.5);

65.607674794487224
```

### [nautical_miles_conversion(input_nautical_miles FLOAT64)](nautical_miles_conversion.sqlx)
Converts nautical miles to miles
```sql
SELECT bqutil.fn.nautical_miles_conversion(1.12);

1.2888736
```


### [nlp_compromise_number(str STRING)](nlp_compromise_number.sqlx)
Parse numbers from text.

```sql
SELECT bqutil.fn.nlp_compromise_number('one hundred fifty seven')
  , bqutil.fn.nlp_compromise_number('three point 5')
  , bqutil.fn.nlp_compromise_number('2 hundred')
  , bqutil.fn.nlp_compromise_number('minus 8')
  , bqutil.fn.nlp_compromise_number('5 million 3 hundred 25 point zero 1')

157, 3.5, 200, -8, 5000325.01
```


### [nlp_compromise_people(str STRING)](nlp_compromise_people.sqlx)
Extract names out of text.

```sql
SELECT bqutil.fn.nlp_compromise_people(
  "hello, I'm Felipe Hoffa and I work with Elliott Brossard - who thinks Jordan Tigani will like this post?"
) names

["felipe hoffa", "elliott brossard", "jordan tigani"]
```


### [percentage_change(val1 FLOAT64, val2 FLOAT64)](percentage_change.sqlx)
Calculate the percentage change (increase/decrease) between two numbers.

```sql
SELECT bqutil.fn.percentage_change(0.2, 0.4)
  , bqutil.fn.percentage_change(5, 15)
  , bqutil.fn.percentage_change(100, 50)
  , bqutil.fn.percentage_change(-20, -45)
```

results:

| f0_ | f1_ |  f2_  |   f3_   |
|-----|-----|-------|---------|
| 1.0 | 2.0 |  -0.5 |  -1.125 |


### [percentage_difference(val1 FLOAT64, val2 FLOAT64)](percentage_difference.sqlx)
Calculate the percentage difference between two numbers.

```sql
SELECT bqutil.fn.percentage_difference(0.2, 0.8)
  , bqutil.fn.percentage_difference(4.0, 12.0)
  , bqutil.fn.percentage_difference(100, 200)
  , bqutil.fn.percentage_difference(1.0, 1000000000)
```

results:

| f0_ | f1_ |   f2_   | f3_ |
|-----|-----|---------|-----|
| 1.2 | 1.0 |  0.6667 | 2.0 |

### [pi()](pi.sqlx)
Returns the value of pi.

```sql
SELECT bqutil.fn.pi() this_is_pi

3.141592653589793
```

### [radians(x ANY TYPE)](radians.sqlx)
Convert degree values into radian.

```sql
SELECT bqutil.fn.radians(180) is_this_pi

3.141592653589793
```


### [random_int(min ANY TYPE, max ANY TYPE)](random_int.sqlx)
Generate random integers between the min and max values.

```sql
SELECT bqutil.fn.random_int(0,10) randint, COUNT(*) c
FROM UNNEST(GENERATE_ARRAY(1,1000))
GROUP BY 1
ORDER BY 1
```


### [random_value(arr ANY TYPE)](random_value.sqlx)
Returns a random value from an array.

```sql
SELECT
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe']),
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe']),
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe'])

'tino', 'julie', 'jordan'
```

### [sure_cond(value STRING, cond BOOL)](sure_cond.sqlx)

If `cond` is `FALSE` the function cause error.

```sql
SELECT
  `bqutil.fn.sure_cond`(x, x > 0)
FROM UNNEST([1, 2, 3, 4]) as x
```

### [sure_like(value STRING, like_pattern STRING)](sure_like.sqlx)

If argument `value` is matched by `like_pattern`, the function returns `value` as-is.
Otherwise it causes error.

```sql
SELECT
  `bqutil.fn.sure_like`("[some_pattern]", "[%]") = "hoge";
```

### [sure_nonnull(value ANY TYPE)](sure_nonnull.sqlx)

If non-NULL argument is passed, the function returns input `value` as-is.
However if NULL value is passed, it causes error.

```sql
SELECT
  bqutil.fn.sure_nonnull(1),
  bqutil.fn.sure_nonnull("string"),
  bqutil.fn.sure_nonnull([1, 2, 3]),
```

### [sure_range(value ANY TYPE)](sure_range.sqlx)

Returns true if value is between lower_bound and upper_bound, inclusive.

```sql
SELECT
  bqutil.fn.sure_range(1, 1, 10) == 1,
  bqutil.fn.sure_range("b", "a", "b") == "b",
```

### [sure_values(value ANY TYPE, acceptable_value_array ANY TYPE)](sure_values.sqlx)

If argument `value` is in `acceptable_value_array` or NULL, the function returns input `value` as-is.
Otherwise it causes error.

```sql
SELECT
  `bqutil.fn.sure_values`("hoge", ["hoge", "fuga"]) = "hoge",
  `bqutil.fn.sure_values`(  NULL, ["hoge", "fuga"]) is NULL
```

### [table_url(table_id STRING)](table_url.sqlx)
Generates a deep link to the BigQuery console for a table or view
in the form: "project.dataset.table"
```sql
SELECT bqutil.fn.table_url("bigquery-public-data.new_york_citibike.citibike_trips")

https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=new_york_citibike&t=citibike_trips&page=table
```

### [to_binary(x INT64)](to_binary.sqlx)
Returns a binary representation of a number.

```sql
SELECT
  x,
  bqutil.fn.to_binary(x) AS binary
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:

|     x      |                              binary                              |
|------------|------------------------------------------------------------------|
|          1 | 0000000000000000000000000000000000000000000000000000000000000001 |
|     123456 | 0000000000000000000000000000000000000000000000011110001001000000 |
| 9876543210 | 0000000000000000000000000000001001001100101100000001011011101010 |
|      -1001 | 1111111111111111111111111111111111111111111111111111110000010111 |


### [to_hex(x INT64)](to_hex.sqlx)
Returns a hexadecimal representation of a number.

```sql
SELECT
  x,
  bqutil.fn.to_hex(x) AS hex
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:
|     x      |       hex        |
|------------|------------------|
|          1 | 0000000000000001 |
|     123456 | 000000000001e240 |
| 9876543210 | 000000024cb016ea |
|      -1001 | fffffffffffffc17 |


### [random_string(length INT64)](random_string.sqlx)
Returns a random string of specified length. Individual characters are chosen uniformly at random from the following pool of characters: 0-9, a-z, A-Z.

```sql
SELECT
  bqutil.fn.random_string(5),
  bqutil.fn.random_string(7),
  bqutil.fn.random_string(10)

'mb3AP' 'aQG5XYB' '0D5WFVQuq6'
```


### [translate(expression STRING, characters_to_replace STRING, characters_to_substitute STRING)](translate.sqlx)
For a given expression, replaces all occurrences of specified characters with specified substitutes. Existing characters are mapped to replacement characters by their positions in the `characters_to_replace` and `characters_to_substitute` arguments. If more characters are specified in the `characters_to_replace` argument than in the `characters_to_substitute` argument, the extra characters from the `characters_to_replace` argument are omitted in the return value.
```sql
SELECT bqutil.fn.translate('mint tea', 'inea', 'osin')

most tin
```

### [ts_gen_keyed_timestamps(keys ARRAY<STRING>, tumble_seconds INT64, min_ts TIMESTAMP, max_ts TIMESTAMP)](ts_gen_keyed_timestamps.sqlx)
Generate a timestamp array associated with each key

```sql
SELECT *
FROM
  UNNEST(bqutil.fn.ts_gen_keyed_timestamps(['abc', 'def'], 60, TIMESTAMP '2020-01-01 00:30:00', TIMESTAMP '2020-01-01 00:31:00))
```

| series_key | tumble_val
|------------|-------------------------|
| abc        | 2020-01-01 00:30:00 UTC |
| def        | 2020-01-01 00:30:00 UTC |
| abc        | 2020-01-01 00:31:00 UTC |
| def        | 2020-01-01 00:31:00 UTC |


### [ts_linear_interpolate(pos TIMESTAMP, prev STRUCT<x TIMESTAMP, y FLOAT64>, next STRUCT<x TIMESTAMP, y FLOAT64>)](ts_linear_interpolation.sqlx)
Interpolate the positions value using timestamp seconds as the x-axis

```sql
select bqutil.fn.ts_linear_interpolate(
  TIMESTAMP '2020-01-01 00:30:00',
  STRUCT(TIMESTAMP '2020-01-01 00:29:00' AS x, 1.0 AS y),
  STRUCT(TIMESTAMP '2020-01-01 00:31:00' AS x, 3.0 AS y)
)
```

| f0_ |
|-----|
| 2.0 |

### [ts_session_group(row_ts TIMESTAMP, prev_ts TIMESTAMP, session_gap INT64)](ts_session_group.sqlx)
Function to compare two timestamp as being within the same session window. A timestamp in the same session window as its previous timestamp will evaluate as NULL, otherwise the current row's timestamp is returned.  The "LAST_VALUE(ts IGNORE NULLS)" window function can then be used to stamp all rows with the starting timestamp for the session window.

```sql
--5 minute (300 seconds) session window
WITH ticks AS (
  SELECT 'abc' as key, 1.0 AS price, CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP) AS ts
  UNION ALL
  SELECT 'abc', 2.0, CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 'abc', 3.0, CAST('2020-01-01 01:05:01 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 'abc', 4.0, CAST('2020-01-01 01:09:01 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 'abc', 5.0, CAST('2020-01-01 01:24:01 UTC' AS TIMESTAMP)
)
SELECT
  * EXCEPT(session_group),
  LAST_VALUE(session_group IGNORE NULLS)
    OVER (PARTITION BY key ORDER BY ts ASC) AS session_group
FROM (
  SELECT
    *,
    bqutil.fn.ts_session_group(
      ts,
      LAG(ts) OVER (PARTITION BY key ORDER BY ts ASC),
      300
    ) AS session_group
  FROM ticks
)
```

| key | price | ts                      |  sesssion_group         |
|-----|-------|-------------------------|-------------------------|
| abc | 1.0   | 2020-01-01 01:04:59 UTC | 2020-01-01 01:04:59 UTC |
| abc | 2.0   | 2020-01-01 01:05:00 UTC | 2020-01-01 01:04:59 UTC |
| abc | 3.0   | 2020-01-01 01:05:01 UTC | 2020-01-01 01:04:59 UTC |
| abc | 4.0   | 2020-01-01 01:09:01 UTC | 2020-01-01 01:04:59 UTC |
| abc | 5.0   | 2020-01-01 01:24:01 UTC | 2020-01-01 01:24:01 UTC |


### [ts_slide(ts TIMESTAMP, period INT64, duration INT64)](ts_slide.sqlx)
Calculate the sliding windows the ts parameter belongs to.

```sql
-- show a 15 minute window every 5 minutes and a 15 minute window every 10 minutes
WITH ticks AS (
  SELECT 1.0 AS price, CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP) AS ts
  UNION ALL
  SELECT 2.0, CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 3.0, CAST('2020-01-01 01:05:01 UTC' AS TIMESTAMP)
)
SELECT
  price,
  ts,
  bqutil.fn.ts_slide(ts, 300, 900) as _5_15,
  bqutil.fn.ts_slide(ts, 600, 900) as _10_15,
FROM ticks
```

| price | ts                      | _5_15.window_start      | _5_15.window_end        | _5_15.window_start      | _5_15.window_end        |
|-------|-------------------------|-------------------------|-------------------------|-------------------------|-------------------------|
| 1.0   | 2020-01-01 01:04:59 UTC | 2020-01-01 00:50:00 UTC | 2020-01-01 01:05:00 UTC | 2020-01-01 00:50:00 UTC | 2020-01-01 01:05:00 UTC |
|       |                         | 2020-01-01 00:55:00 UTC | 2020-01-01 01:10:00 UTC | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |
|       |                         | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |                         |                         |
| 2.0   | 2020-01-01 01:05:00 UTC | 2020-01-01 00:55:00 UTC | 2020-01-01 01:10:00 UTC | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |
|       |                         | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |                         |                         |
|       |                         | 2020-01-01 01:05:00 UTC | 2020-01-01 01:20:00 UTC |                         |                         |
| 3.0   | 2020-01-01 01:05:01 UTC | 2020-01-01 00:55:00 UTC | 2020-01-01 01:10:00 UTC | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |
|       |                         | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |                         |                         |
|       |                         | 2020-01-01 01:05:00 UTC | 2020-01-01 01:20:00 UTC |                         |                         |



### [ts_tumble(input_ts TIMESTAMP, tumble_seconds INT64)](ts_tumble.sqlx)
Calculate the [tumbling window](https://cloud.google.com/dataflow/docs/concepts/streaming-pipelines#tumbling-windows) the input_ts belongs in

```sql
SELECT
  fn.ts_tumble(TIMESTAMP '2020-01-01 00:17:30', 900) AS min_15,
  fn.ts_tumble(TIMESTAMP '2020-01-01 00:17:30', 600) AS min_10,
  fn.ts_tumble(TIMESTAMP '2020-01-01 00:17:30', 60) As min_1
```

| min_15                  | min_10                  |                         |
|-------------------------|-------------------------|-------------------------|
| 2020-01-01 00:15:00 UTC | 2020-01-01 00:10:00 UTC | 2020-01-01 00:17:00 UTC |

Consider using the built-in [TIMESTAMP_BUCKET](https://cloud.google.com/bigquery/docs/reference/standard-sql/time-series-functions#timestamp_bucket) function instead.

### [typeof(input ANY TYPE)](typeof.sqlx)

Return the type of input or 'UNKNOWN' if input is unknown typed value.

```sql
SELECT
  bqutil.fn.typeof(""),
  bqutil.fn.typeof(b""),
  bqutil.fn.typeof(1.0),
  bqutil.fn.typeof(STRUCT()),

STRING, BINARY, FLOAT64, STRUCT
```

### [url_decode(text STRING, method STRING)](url_decode.sqlx)
Return decoded string of inputs "text" in "method" function.

```sql
SELECT NULL as method, bqutil.fn.url_decode("https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A", NULL) as value
UNION ALL SELECT "decodeURIComponent" as method, bqutil.fn.url_encode("https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A", "decodeURIComponent") as value
UNION ALL SELECT "decodeURI" as method, bqutil.fn.url_decode("https://example.com/?id=%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A", "decodeURI") as value
UNION ALL SELECT "unescape" as method, bqutil.fn.url_decode("https%3A//example.com/%3Fid%3D%u3042%u3044%u3046%u3048%u304A", "unescape") as value
```

| method | value |
|:-------|:------|
| NULL | https://example.com/?id=あいうえお |
| decodeURIComponent | https://example.com/?id=あいうえお |
| decodeURI | https://example.com/?id=あいうえお |
| unescape | https://example.com/?id=あいうえお |

### [url_encode(text STRING, method STRING)](url_encode.sqlx)
Return encoded string of inputs "text" in "method" function.

```sql
SELECT NULL as method, bqutil.fn.url_encode("https://example.com/?id=あいうえお", NULL) as value
UNION ALL SELECT "encodeURIComponent" as method, bqutil.fn.url_encode("https://example.com/?id=あいうえお", "encodeURIComponent") as value
UNION ALL SELECT "encodeURI" as method, bqutil.fn.url_encode("https://example.com/?id=あいうえお", "encodeURI") as value
UNION ALL SELECT "escape" as method, bqutil.fn.url_encode("https://example.com/?id=あいうえお", "escape") as value
```

| method | value |
|:-------|:------|
| NULL | https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A |
| encodeURIComponent | https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A |
| encodeURI | https://example.com/?id=%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A |
| escape | https%3A//example.com/%3Fid%3D%u3042%u3044%u3046%u3048%u304A |

### [url_keys(query STRING)](url_keys.sqlx)
Get an array of url param keys.

```sql
SELECT bqutil.fn.url_keys(
  'https://www.google.com/search?q=bigquery+udf&client=chrome')

["q", "client"]
```


### [url_param(query STRING, p STRING)](url_param.sqlx)
Get the value of a url param key.

```sql
SELECT bqutil.fn.url_param(
  'https://www.google.com/search?q=bigquery+udf&client=chrome', 'client')

"chrome"
```


### [url_parse(urlString STRING, partToExtract STRING)](url_parse.sqlx)

Returns the specified part from the URL. Valid values for partToExtract include HOST, PATH, QUERY, REF, PROTOCOL
For example, url_parse('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST') returns 'facebook.com'.
```sql
WITH urls AS (
  SELECT 'http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' as url
  UNION ALL
  SELECT 'rpc://facebook.com/' as url
)
SELECT bqutil.fn.url_parse(url, 'HOST'), bqutil.fn.url_parse(url, 'PATH'), bqutil.fn.url_parse(url, 'QUERY'), bqutil.fn.url_parse(url, 'REF'), bqutil.fn.url_parse(url, 'PROTOCOL') from urls
```

results:

|     f0_      |     f1_     |       f2_        | f3_  | f4_  |
|--------------|-------------|------------------|------|------|
| facebook.com | path1/p.php | k1=v1&k2=v2#Ref1 | Ref1 | http |
| facebook.com | NULL        | NULL             | NULL | rpc  |


### [url_trim_query(url STRING, keys_to_trim ARRAY<STRING>)](url_trim_query.sqlx)

Returns a URL with specified keys removed from the
[URL's query component](https://en.wikipedia.org/wiki/Query_string).
The keys to be removed are provided as an ARRAY<STRING> input argument.

```sql
SELECT bqutil.fn.url_trim_query(
  "https://www.example.com/index.html?goods_id=G1002&utm_id=ads&gclid=abc123",
  ["utm_id", "gclid"]
)
UNION ALL SELECT bqutil.fn.url_trim_query(
  "https://www.example.com/index.html?goods_id=G1002&utm_id=ads&gclid=abc123",
  ["utm_id", "gclid", "goods_id"]
)
```

results:

|     f0_      |
|--------------|
| https://www.example.com/index.html?goods_id=G1002 |
| https://www.example.com/index.html |



### [week_of_month(date_expression ANY TYPE)](week_of_month.sqlx)
Returns the number of weeks from the beginning of the month to the specified date. The result is an INTEGER value between 1 and 5, representing the nth occurrence of the week in the month. The value 0 means the partial week.

```sql
SELECT
  bqutil.fn.week_of_month(DATE '2020-07-01'),
  bqutil.fn.week_of_month(DATE '2020-07-08');

0 1
```

### [xml_to_json(xml STRING)](xml_to_json.sqlx)
Converts XML to JSON using the open source
txml JavaScript library which is 2-3 times faster than the fast-xml-parser library. \
NULL input is returned as NULL output. \
Empty string input is returned as empty JSON object.

* [txml repo](https://github.com/TobiasNickel/tXml)
* [Benchmark details of comparison with fast-xml-parser](https://github.com/tobiasnickel/fast-xml-parser#benchmark)

```sql
SELECT bqutil.fn.xml_to_json(
  '<xml foo="FOO"><bar><baz>BAZ</baz></bar></xml>'
) AS output_json
```

results:

| output_json |
| ----------- |
| {"xml":[{"_attributes":{"foo":"FOO"},"bar":[{"baz":["BAZ"]}]}]} |

### [xml_to_json_fpx(xml STRING)](xml_to_json_fpx.sqlx)
Converts XML to JSON using the open source
fast-xml-parser JavaScript library. \
NULL input is returned as NULL output. \
Empty string input is returned as empty JSON object.

* [fast-xml-parser repo](https://github.com/NaturalIntelligence/fast-xml-parser)
* [List of options you can pass to the XMLParser object](https://github.com/NaturalIntelligence/fast-xml-parser/blob/master/docs/v4/2.XMLparseOptions.md)

```sql
SELECT bqutil.fn.xml_to_json_fpx(
  '<xml foo="FOO"><bar><baz>BAZ</baz></bar></xml>'
) as output_json
```
results:

| output_json |
| ----------- |
| {"xml":{"@_foo":"FOO","bar":{"baz":"BAZ"}}} |

### [y4md_to_date(y4md STRING)](y4md_to_date.sqlx)
Convert a STRING formatted as a YYYYMMDD to a DATE

```sql
SELECT bqutil.fn.y4md_to_date('20201220')

"2020-12-20"
```


### [zeronorm(x ANY TYPE, meanx FLOAT64, stddevx FLOAT64)](zeronorm.sqlx)
Normalize a variable so that it has zero mean and unit variance.

```sql
with r AS (
  SELECT 10 AS x
  UNION ALL SELECT 20
  UNION ALL SELECT 30
  UNION ALL SELECT 40
  UNION ALL SELECT 50
),
stats AS (
  SELECT AVG(x) AS meanx, STDDEV(x) AS stddevx
  FROM r
)
SELECT x, bqutil.fn.zeronorm(x, meanx, stddevx) AS zeronorm
FROM r, stats;
```

returns:

| Row | x | zeronorm |
| --- | -- | ------- |
| 1	| 10 | -12.649110640673518 |
| 2	| 20 | -6.324555320336759 |
| 3	| 30 | 0.0 |
| 4	| 40 | 6.324555320336759 |
| 5	| 50 | 12.649110640673518 |



<br/>
<br/>
<br/>

# StatsLib: Statistical UDFs

This section details the subset of community contributed [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
that extend BigQuery and enable more specialized Statistical Analysis usage patterns.
Each UDF detailed below will be automatically synchronized to the `fn` dataset
within the `bqutil` project for reference in your queries.

For example, if you'd like to reference the `int` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.fn.int(1.684)
```

## UDFs
* [corr_pvalue](#corr_pvaluer-float64-n-int64)
* [kruskal_wallis](#kruskal_wallisarraystructfactor-string-val-float64)
* [linear_regression](#linear_regressionarraystructstructx-float64-y-float64)
* [pvalue](#pvalueh-float64-dof-float64)
* [p_fisherexact](#p_fisherexacta-float64-b-float64-c-float64-d-float64)
* [mannwhitneyu](#mannwhitneyux-array-y-array-alt-string)
* [t_test](#t_testarrayarray)

## Documentation

### [corr_pvalue(r FLOAT64, n INT64)](corr_pvalue.sqlx)
The returns the p value of the computed correlation coefficient based on the t-distribution.
Input:
r: correlation value.
n: number of samples.
Output:
The p value of the correlation coefficient.
```sql
WITH test_cases AS (
    SELECT  0.9 AS r, 25 n
    UNION ALL
    SELECT -0.5, 40
    UNION ALL
    SELECT 1.0, 50
    UNION ALL
    SELECT -1.0, 50
)
SELECT bqutil.fn.corr_pvalue(r,n) AS p
FROM test_cases
```

results:

| p |
|-----|
| 1.443229117741041E-9 |
| 0.0010423414457657223 |
| 0.0 |
| 0.0 |
-----

### [kruskal_wallis(ARRAY(STRUCT(factor STRING, val FLOAT64))](kruskal_wallis.sqlx)
Takes an array of struct where each struct (point) represents a measurement, with a group label and a measurement value

The [Kruskal–Wallis test by ranks](https://en.wikipedia.org/wiki/Kruskal%E2%80%93Wallis_one-way_analysis_of_variance), Kruskal–Wallis H test (named after William Kruskal and W. Allen Wallis), or one-way ANOVA on ranks is a non-parametric method for testing whether samples originate from the same distribution. It is used for comparing two or more independent samples of equal or different sample sizes. It extends the Mann–Whitney U test, which is used for comparing only two groups. The parametric equivalent of the Kruskal–Wallis test is the one-way analysis of variance (ANOVA).

* Input: array: struct <factor STRING, val FLOAT64>
* Output: struct<H FLOAT64, p-value FLOAT64, DOF FLOAT64>
```sql
DECLARE data ARRAY<STRUCT<factor STRING, val FLOAT64>>;

set data = [
('a',1.0),
('b',2.0),
('c',2.3),
('a',1.4),
('b',2.2),
('c',5.5),
('a',1.0),
('b',2.3),
('c',2.3),
('a',1.1),
('b',7.2),
('c',2.8)
];


SELECT `bqutil.fn.kruskal_wallis`(data) AS results;
```

results:

| results.H	| results.p	| results.DoF	|
|-----------|-----------|-------------|
| 3.4230769 | 0.1805877 | 2           |
-----

### [linear_regression(ARRAY(STRUCT(STRUCT(X FLOAT64, Y FLOAT64))](linear_regression.sqlx)
Takes an array of STRUCT X, Y and returns _a, b, r_ where _Y = a*X + b_, and _r_ is the "goodness of fit measure.

The [Linear Regression](https://en.wikipedia.org/wiki/Linear_regression), is a linear approach to modelling the relationship between a scalar response and one or more explanatory variables (also known as dependent and independent variables).

* Input: array: struct <X FLOAT64, Y FLOAT64>
* Output: struct<a FLOAT64,b FLOAT64, r FLOAT64>
*
```sql
DECLARE data ARRAY<STRUCT<X STRING, Y FLOAT64>>;
set data = [ (5.1,2.5), (5.0,2.0), (5.7,2.6), (6.0,2.2), (5.8,2.6), (5.5,2.3), (6.1,2.8), (5.5,2.5), (6.4,3.2), (5.6,3.0)];
SELECT `bqutil.fn.linear_regression`(data) AS results;
```

results:


| results.a          	| results.b	         | results.r	       |
|---------------------|--------------------|-------------------|
| -0.4353361094588436 | 0.5300416418798544 | 0.632366563565354 |
-----

### [pvalue(H FLOAT64, dof FLOAT64)](pvalue.sqlx)
Takes _H_ and _dof_ and returns _p_ probability value.

The [chisquare_cdf](https://jstat.github.io/distributions.html#jStat.chisquare.cdf) is NULL Hypothesis probability of the Kruskal-Wallis (KW) test. This is obtained to be the CDF of the chisquare with the _H_ value and the Degrees of Freedom (_dof_) of the KW problem.

* Input: H FLOAT64, dof FLOAT64
* Output: p FLOAT64

```sql
SELECT `bqutil.fn.chisquare_cdf`(.3,2) AS results;
```

results:


| results         	|
|-------------------|
|0.8607079764250578 |
-----

### [p_fisherexact(a FLOAT64, b FLOAT64, c FLOAT64, d FLOAT64)](p_fisherexact.sqlx)
Computes the p value of the Fisher exact test (https://en.wikipedia.org/wiki/Fisher%27s_exact_test), implemented in JavaScript.

- **Input:** a,b,c,d : values of 2x2 contingency table ([ [ a, b ] ;[ c , d ] ] (type FLOAT64).
- **Output:** The p value of the test (type: FLOAT64)

Example
```SQL
WITH mydata as (
SELECT
    90.0        as a,
    27.0        as b,
    17.0        as c,
    50.0  as d
)
SELECT
    `bqutil.fn.p_fisherexact`(a,b,c,d) as pvalue
FROM
   mydata
```

Output:
| pvalue |
|---|
| 8.046828829103659E-12 |
-----

### [mannwhitneyu(x ARRAY<FLOAT64>, y ARRAY<FLOAT64>, alt STRING)](mannwhitneyu.sqlx)
Computes the U statistics and the p value of the Mann–Whitney U test (https://en.wikipedia.org/wiki/Mann%E2%80%93Whitney_U_test). This test is also called the Mann–Whitney–Wilcoxon (MWW), Wilcoxon rank-sum test, or Wilcoxon–Mann–Whitney test

- **Input:** x,y :arrays of samples, both should be one-dimensional (type: ARRAY<FLOAT64> ), alt: defines the alternative hypothesis, the following options are available: 'two-sided', 'less', and 'greater'.
- **Output:** structure of the type struct<U FLOAT64, p FLOAT64> where U is the statistic and p is the p value of the test.

Example
```
WITH mydata AS (
  SELECT
    [2, 4, 6, 2, 3, 7, 5, 1.] AS x,
    [8, 10, 11, 14, 20, 18, 19, 9. ] AS y
)
SELECT `bqutil.fn.mannwhitneyu`(y, x, 'two-sided') AS test
FROM mydata
```

Output:
| test.U | test.p |
|---|---|
| 0.0 | 9.391056991171487E-4 |

-----
### [t_test(ARRAY<FLOAT64>,ARRAY<FLOAT64>)](t_test.sqlx)

Runs the Student's T-test. Well known test to compare populations. Example taken from here: [Sample](https://www.jmp.com/en_ch/statistics-knowledge-portal/t-test/two-sample-t-test.html)

Sample Query:

```SQL
DECLARE pop1 ARRAY<FLOAT64>;
DECLARE pop2 ARRAY<FLOAT64>;

SET pop1 = [13.3,6.0,20.0,8.0,14.0,19.0,18.0,25.0,16.0,24.0,15.0,1.0,15.0];
SET pop2 = [22.0,16.0,21.7,21.0,30.0,26.0,12.0,23.2,28.0,23.0] ;

SELECT `bqutil.fn.t_test`(pop1, pop2) AS actual_result_rows;

```

Results:

| Row	| actual_result_rows.t_value | actual_result_rows.dof|
|-----|----------------------------|-----------------------|
| 1	| 2.8957935572829476 | 21


-----
### [normal_cdf(x FLOAT64, mean FLOAT64, stdev FLOAT64)](normal_cdf.sqlx)

Returns the value of x in the cdf of the Normal distribution with parameters mean and std (standard deviation).

Sample Query:

```SQL
SELECT `bqutil.fn.normal_cdf`(1.1, 1.7, 2.0) as normal_cdf;
```

Results:

| Row	| normal_cdf |
|-----|-------------------|
| 1	| 0.3820885778110474 |

---

### [studentt_cdf(x FLOAT64, dof FLOAT64)](studentt_cdf.sqlx)

Returns the value of x in the cdf of the Student's T distribution with dof degrees of freedom.

Sample Query:

```SQL
SELECT `bqutil.fn.studentt_cdf`(1.0, 2.0) as studentt_cdf;
```

Results:

| Row | studentt_cdf      |
| --- | ----------------- |
| 1   | 0.788675134594813 |
