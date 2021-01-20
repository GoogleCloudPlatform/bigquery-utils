package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import java.text.MessageFormat;

/** A static class storing the data types and their cast operations. */
public class TypeCast {

  public static final String ANY = "ANY";
  public static final String INT64 = "INT64";
  public static final String FLOAT64 = "FLOAT64";
  public static final String NUMERIC = "NUMERIC";
  public static final String BOOLEAN = "BOOLEAN";
  public static final String STRING = "STRING";
  public static final String BYTES = "BYTES";
  public static final String TIMESTAMP = "TIMESTAMP";
  public static final String DATE = "DATE";
  public static final String DATETIME = "DATETIME";
  public static final String TIME = "TIME";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";

  // Cast template to cast between data types.
  // The first String is target data type, second String is the original data type,
  // and the last String is the cast template. {1} here means using the first argument
  // of the input.
  public static final Table<String, String, String> castTable =
      ImmutableTable.<String, String, String>builder()
          .put(INT64, ANY, "SAFE_CAST({1} AS INT64)")
          .put(INT64, TIMESTAMP, "UNIX_MICROS({1})")
          .put(FLOAT64, ANY, "SAFE_CAST({1} AS FLOAT64)")
          .put(FLOAT64, TIMESTAMP, "SAFE_CAST(UNIX_MICROS({1}) AS FLOAT64)")
          .put(NUMERIC, ANY, "SAFE_CAST({1} AS NUMERIC)")
          .put(NUMERIC, TIMESTAMP, "SAFE_CAST(UNIX_MICROS({1}) AS NUMERIC)")
          .put(BOOLEAN, ANY, "SAFE_CAST({1} AS BOOL)")
          .put(STRING, ANY, "SAFE_CAST({1} AS STRING)")
          .put(BYTES, ANY, "SAFE_CAST({1} AS BYTES)")
          .put(TIMESTAMP, ANY, "SAFE_CAST({1} AS TIMESTAMP)")
          .put(TIMESTAMP, INT64, "TIMESTAMP_MICROS({1})")
          .put(TIME, ANY, "SAFE_CAST({1} AS TIME)")
          .put(TIME, INT64, "TIME(TIMESTAMP_MICROS(${1}))")
          .put(DATETIME, ANY, "SAFE_CAST({1} AS DATETIME)")
          .put(DATETIME, INT64, "DATETIME(TIMESTAMP_MICROS({1}))")
          .put(DATE, ANY, "SAFE_CAST({1} AS DATE)")
          .put(DATE, INT64, "DATE(TIMESTAMP_MICROS({1}))")
          .build();

  /**
   * Get the cast operation template given the target data type.
   *
   * @param toType target data type
   * @return cast operation template
   */
  public static String getCastTemplate(String toType) {
    return getCastTemplate(toType, ANY);
  }

  /**
   * Get the cast operation template given the target data type, source data type, and the
   * placeholder of the input. The placeholder (e.g. {1}, {2}) determines which input argument will
   * be put into this cast function template.
   *
   * @param toType target data type
   * @param fromType source data type
   * @param placeHolder which input argument should be put into this template
   * @return cast operation template
   */
  public static String getCastTemplate(String toType, String fromType, int placeHolder) {
    String template = getCastTemplate(toType, fromType);
    return MessageFormat.format(template, /*{0}*/ "", /*{1}*/ String.format("{%d}", placeHolder));
  }

  /**
   * Get the cast operation template given the target data type and source data type.
   *
   * @param toType target data type
   * @param fromType source data type
   * @return cast operation template
   */
  public static String getCastTemplate(String toType, String fromType) {
    if (castTable.contains(toType, fromType)) {
      return castTable.get(toType, fromType);
    }

    if (castTable.contains(toType, ANY)) {
      return castTable.get(toType, ANY);
    }

    return String.format("SAFE_CAST({1} as %s)", fromType.toUpperCase());
  }
}
