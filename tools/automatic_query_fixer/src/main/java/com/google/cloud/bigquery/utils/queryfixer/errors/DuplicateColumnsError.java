package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import lombok.Getter;

/**
 * A class representing "Duplicate column names in the result are not supported. Found duplicate(s):
 * [X]" errors. [X] is the duplicate columns.
 *
 * <p>For example, the following query
 *
 * <pre>
 *     SELECT status, status FROM `bigquery-public-data.austin_311.311_request`
 * </pre>
 *
 * could lead to an error "Duplicate column names in the result are not supported. Found
 * duplicate(s): status".
 */
@Getter
public class DuplicateColumnsError extends BigQuerySemanticError {

  private final String duplicate;

  public DuplicateColumnsError(String duplicate, BigQueryException errorSource) {
    super(null, errorSource);
    this.duplicate = duplicate;
  }
}
