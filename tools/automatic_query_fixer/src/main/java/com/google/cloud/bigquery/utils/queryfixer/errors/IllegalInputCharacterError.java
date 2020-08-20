package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import lombok.Getter;

/**
 * A class representing "Illegal Input Character" syntax error. It was an error caused by ZetaSQL
 * tokenizer, indicating that it cannot tokenize at the current position likely due to some
 * characters not meeting its tokenizing rules. For example, $ is not allowed in a keyword or
 * identifier, so if an identifier contains a $ symbol, then the tokenizer will throw this error.
 *
 * <p>For example, the following query
 *
 * <pre>
 *     SELECT unique_key FROM `bigquery-public-data.austin_incidents`.incidents_2008$ LIMIT 10
 * </pre>
 *
 * could lead to an error "Syntax error: Illegal input character "$" at [1:78]".
 */
@Getter
public class IllegalInputCharacterError extends BigQuerySyntaxError {

  private final String illegalCharacter;

  public IllegalInputCharacterError(
      String illegalCharacter, Position errorPosition, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.illegalCharacter = illegalCharacter;
  }
}
