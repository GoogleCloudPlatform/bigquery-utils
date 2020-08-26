package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySqlError;
import com.google.cloud.bigquery.utils.queryfixer.errors.FunctionNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.errors.TableNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.errors.UnrecognizedColumnError;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import lombok.AllArgsConstructor;

/** A factory to yield the fixers for BigQuery SQL Error. */
@AllArgsConstructor
public class FixerFactory {

  private final QueryTokenProcessor queryTokenProcessor;
  private final BigQueryService bigQueryService;

  /**
   * Get the corresponding fixer based on the type of {@link BigQuerySqlError}. If an error does not
   * have any fixers, a null pointer will be returned.
   *
   * @param query the query with error.
   * @param error the BigQuery SQL error.
   * @return the corresponding fixer or null pointer.
   */
  public IFixer getFixer(String query, BigQuerySqlError error) {

    if (error instanceof TableNotFoundError) {
      return new TableNotFoundFixer(
          query, (TableNotFoundError) error, bigQueryService, queryTokenProcessor);
    }

    if (error instanceof UnrecognizedColumnError) {
      return new UnrecognizedColumnFixer(
          query, (UnrecognizedColumnError) error, queryTokenProcessor);
    }

    if (error instanceof FunctionNotFoundError) {
      FunctionNotFoundError functionError = (FunctionNotFoundError) error;
      return new FunctionNotFoundFixer(query, functionError, queryTokenProcessor);
    }

    return null;
  }
}
