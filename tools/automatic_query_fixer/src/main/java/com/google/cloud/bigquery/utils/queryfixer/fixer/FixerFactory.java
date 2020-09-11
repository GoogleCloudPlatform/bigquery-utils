package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.errors.*;
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
      return new TableNotFoundFixer(query, (TableNotFoundError) error, bigQueryService);
    }

    if (error instanceof UnrecognizedColumnError) {
      return new UnrecognizedColumnFixer(
          query, (UnrecognizedColumnError) error, queryTokenProcessor);
    }

    if (error instanceof FunctionNotFoundError) {
      FunctionNotFoundError functionError = (FunctionNotFoundError) error;
      return new FunctionNotFoundFixer(query, functionError, queryTokenProcessor);
    }

    if (error instanceof NoMatchingSignatureError) {
      NoMatchingSignatureError noMatchError = (NoMatchingSignatureError) error;
      return new NoMatchingSignatureFixer(query, noMatchError);
    }

    if (error instanceof UnexpectedKeywordError) {
      return new UnexpectedKeywordFixer(query, (UnexpectedKeywordError) error, queryTokenProcessor);
    }

    if (error instanceof IllegalInputCharacterError) {
      return new IllegalInputCharacterFixer(query, (IllegalInputCharacterError) error);
    }

    if (error instanceof ExpectKeywordButGotOthersError) {
      ExpectKeywordButGotOthersError expectKeywordError = (ExpectKeywordButGotOthersError) error;

      // The parser probably expects multiple keywords, but the error messages only provide the last
      // option, which usually is END_OF_INPUT. Therefore, if this happens, we can only search near
      // the error position and see if an identifier may be converted to a keyword.
      if (ExpectKeywordButGotOthersError.END_OF_INPUT.equals(
          expectKeywordError.getExpectedKeyword())) {

        return new NearbyTokenFixer(
            query, expectKeywordError, queryTokenProcessor, bigQueryService);
      }

      return new ExpectKeywordButGotOthersFixer(query, expectKeywordError, queryTokenProcessor);
    }

    if (error instanceof DuplicateColumnsError) {
      return new DuplicateColumnsFixer(query, (DuplicateColumnsError) error);
    }

    if (error instanceof ColumnNotGroupedError) {
      return new ColumnNotGroupedFixer(query, (ColumnNotGroupedError) error);
    }

    return null;
  }
}
