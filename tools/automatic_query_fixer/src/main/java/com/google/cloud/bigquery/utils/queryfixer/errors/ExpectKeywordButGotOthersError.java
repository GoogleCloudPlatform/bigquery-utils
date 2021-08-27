package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import lombok.Getter;

/**
 * A class representing "Expected [X] but got [Y] at [Z]" syntax errors. [X] is either keyword or
 * 'end of input'. [Y] is the actual token in a query. [Z] is the error position, with a format of
 * [row:col].
 *
 * <p>For example, the following query
 *
 * <pre>
 *     Select max(foo) from table group bar
 * </pre>
 *
 * could lead to an error "Syntax error: Expected keyword BY but got identifier "bar" at [1:34]".
 */
@Getter
public class ExpectKeywordButGotOthersError extends BigQuerySyntaxError {

  private final String expectedKeyword;

  public static final String END_OF_INPUT = "end of input";

  public ExpectKeywordButGotOthersError(
      String expectedKeyword, Position errorPosition, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.expectedKeyword = expectedKeyword;
  }
}
