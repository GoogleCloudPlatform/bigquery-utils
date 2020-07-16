package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;

import lombok.Getter;

/** A class to represent the "Function Not Found" errors. The errors look like one of the two forms: (1) Function not
 * found: [function] at [x:y] or (2) Function not found: [function]; Did you mean [suggestion]? at [x:y], where
 * [function] is the incorrect function, [suggestion] is one of the correct function candidates, and [x:y] is the row
 * and column numbers. */
@Getter
public class FunctionNotFoundError extends BigQuerySemanticError {

  private final String incorrectFunctionName;
  private final String suggestion;


  public FunctionNotFoundError(String incorrectFunctionName, Position errPos, String suggestion,
      BigQueryException errorSource) {
    super(errPos, errorSource);
    this.incorrectFunctionName = incorrectFunctionName;
    this.suggestion = suggestion;
  }

  public FunctionNotFoundError(String incorrectFunctionName, Position errPos, BigQueryException errorSource) {
    super(errPos, errorSource);
    this.incorrectFunctionName = incorrectFunctionName;
    this.suggestion = null;
  }

  public boolean hasSuggestion() {
    return suggestion != null;
  }
}
