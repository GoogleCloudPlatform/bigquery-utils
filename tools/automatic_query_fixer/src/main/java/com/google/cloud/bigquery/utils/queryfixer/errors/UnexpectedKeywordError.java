package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import lombok.Getter;

/**
 * A class representing "Unexpected Keyword [X] at [Y]". [X] is the keyword that cannot be parsed by
 * a parser, and [Y] is the error location with a format of [row:col]. This error will occur when
 * the parse expects an expression but keywords are provided. One possible cause is that a column
 * has the same name as a keyword, so the parser considers it as a keyword rather than a column.
 *
 * <p>For example, the following query
 *
 * <pre>
 *     SELECT hash FROM `bigquery-public-data.crypto_bitcoin.blocks`
 * </pre>
 *
 * would lead to an error "Syntax error: Unexpected keyword HASH at [1:8]".
 */
@Getter
public class UnexpectedKeywordError extends BigQuerySyntaxError {

  private final String keyword;

  public UnexpectedKeywordError(
      String keyword, Position errorPosition, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.keyword = keyword;
  }
}
