package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;

import lombok.Getter;

/**
 * A class to represent the "Unrecognized Name" errors. The unrecognized names here actually mean
 * the unrecognized columns. The errors look like one of the two forms:
 *
 * <ol>
 *   <li>Unrecognized name: [column] at [x:y]
 *   <li>Unrecognized name: [column]; Did you mean [suggestion]? at [x:y]
 * </ol>
 *
 * where [column] is the incorrect column, [suggestion] is one of the correct column candidates, and
 * [x:y] is the row and column numbers.
 */
@Getter
public class UnrecognizedColumnError extends BigQuerySemanticError {

  private final String columnName;
  private final String suggestion;

  public UnrecognizedColumnError(
      String columnName, Position errorPosition, String suggestion, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.columnName = columnName;
    this.suggestion = suggestion;
  }

  public UnrecognizedColumnError(
      String columnName, Position errorPosition, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.columnName = columnName;
    this.suggestion = null;
  }

  public boolean hasSuggestion() {
    return suggestion != null;
  }
}
