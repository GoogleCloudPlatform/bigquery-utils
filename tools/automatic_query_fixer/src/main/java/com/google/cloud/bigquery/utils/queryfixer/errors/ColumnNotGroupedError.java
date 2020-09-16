package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import lombok.Getter;

/**
 * A class representing "SELECT list expression references column [X] which is neither grouped nor
 * aggregated at [Z]" errors. [X] is the ungrouped column. [Z] is the error position, with a format
 * of [row:col].
 *
 * <p>For example, the following query
 *
 * <pre>
 *     SELECT status, count(*) FROM `bigquery-public-data.austin_311.311_request`
 * </pre>
 *
 * could lead to an error "SELECT list expression references column status which is neither grouped
 * nor aggregated at [1:8]".
 */
@Getter
public class ColumnNotGroupedError extends BigQuerySemanticError {

  private final String missingColumn;

  public ColumnNotGroupedError(
      String missingColumn, Position errorPosition, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.missingColumn = missingColumn;
  }
}
