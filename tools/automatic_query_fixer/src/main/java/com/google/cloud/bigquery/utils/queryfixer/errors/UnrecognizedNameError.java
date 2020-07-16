package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;

import lombok.Getter;

/**
 * A class to represent the "Unrecognized Name" errors. The unrecognized names here actually mean
 * the unrecognized columns. The errors look like one of the two forms: (1) Unrecognized name:
 * [column] at [x:y] or (2) Unrecognized name: [column]; Did you mean [suggestion]? at [x:y], where
 * [column] is the incorrect column, [suggestion] is one of the correct column candidates, and [x:y]
 * is the row and column numbers.
 */
@Getter
public class UnrecognizedNameError extends BigQuerySemanticError {

  private final String unrecognizedName;
  private final String suggestion;

  public UnrecognizedNameError(
      String unrecognizedName, Position errPos, String suggestion, BigQueryException errorSource) {
    super(errPos, errorSource);
    this.unrecognizedName = unrecognizedName;
    this.suggestion = suggestion;
  }

  public UnrecognizedNameError(
      String unrecognizedName, Position errPos, BigQueryException errorSource) {
    super(errPos, errorSource);
    this.unrecognizedName = unrecognizedName;
    this.suggestion = null;
  }

  public boolean hasSuggestion() {
    return suggestion != null;
  }
}
