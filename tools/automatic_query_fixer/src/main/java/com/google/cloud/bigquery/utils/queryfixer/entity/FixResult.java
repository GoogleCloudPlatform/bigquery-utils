package com.google.cloud.bigquery.utils.queryfixer.entity;

import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySqlError;
import lombok.Builder;
import lombok.Value;

import java.util.List;

/** A value class represents the result of fixing an error in a query. */
@Builder
@Value
public class FixResult {

  /** Query to be fixed. */
  String query;

  /** Status of fixing the error. It is either ERROR_FIXED, NO_ERROR. or Failure. */
  Status status;

  /** A list of options to fix this error. */
  List<FixOption> options;

  /** The error message of the error to be fixed */
  String error;

  /**
   * An overview on how the query fixer will fix this error. The details of the fixing will be
   * presented in the options field.
   */
  String approach;

  /** The position at the query where the error occurs. */
  Position errorPosition;

  /** Is query fixer confident about this fixing */
  Boolean isConfident;

  /** The detail why the query fails to be fixed. It is only not null when the Status is FAILURE. */
  String failureDetail;

  /**
   * Create a Failure FixResult indicating that a {@link BigQuerySqlError} can not be fixed.
   *
   * @param query query to be fixed
   * @param error un-fixable error
   * @param failureDetail reason why this fix is failed.
   * @return FixResult with FAILURE Status
   */
  public static FixResult failure(String query, BigQuerySqlError error, String failureDetail) {
    return FixResult.builder()
        .status(Status.FAILURE)
        .query(query)
        .error(error.getErrorSource().getMessage())
        .errorPosition(error.getErrorPosition())
        .failureDetail(failureDetail)
        .build();
  }

  /**
   * Create a Failure FixResult indicating that a {@link BigQuerySqlError} can not be fixed.
   *
   * @param query query to be fixed
   * @param error un-fixable error
   * @return FixResult with FAILURE Status
   */
  public static FixResult failure(String query, BigQuerySqlError error) {
    return failure(query, error, null);
  }

  /**
   * Create a Success FixResult with the details on fixing a {@link BigQuerySqlError} in a query.
   *
   * @param query query to be fixed
   * @param approach approach to fix the error
   * @param options detailed options to fix the error by the given approach.
   * @param error error to fix
   * @param isConfident whether the query fixer confident on this fix.
   * @return FixResult with ERROR_FIXED Status
   */
  public static FixResult success(
      String query,
      String approach,
      List<FixOption> options,
      BigQuerySqlError error,
      boolean isConfident) {
    return FixResult.builder()
        .status(Status.ERROR_FIXED)
        .query(query)
        .options(options)
        .approach(approach)
        .error(error.getErrorSource().getMessage())
        .errorPosition(error.getErrorPosition())
        .isConfident(isConfident)
        .build();
  }

  /**
   * Create a FixResult indicating a query has no error.
   *
   * @param query query to be fixed
   * @return FixResult with NO_ERROR Status
   */
  public static FixResult noError(String query) {
    return FixResult.builder().status(Status.NO_ERROR).query(query).build();
  }

  /**
   * Create a Failure FixResult caused by an infinite loop in a fix process.
   *
   * @param query query to be fixed
   * @return FixResult with FAILURE
   */
  public static FixResult infiniteLoop(String query) {
    return FixResult.builder()
        .status(Status.FAILURE)
        .query(query)
        .failureDetail(
            "The query has been fixed before in this process, which indicates that an infinite loop exist in the fix process.")
        .build();
  }

  public enum Status {
    NO_ERROR,
    ERROR_FIXED,
    FAILURE
  }
}
