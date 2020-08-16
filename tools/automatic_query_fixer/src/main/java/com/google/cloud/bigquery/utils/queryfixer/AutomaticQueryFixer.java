package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySqlError;
import com.google.cloud.bigquery.utils.queryfixer.errors.SqlErrorFactory;
import com.google.cloud.bigquery.utils.queryfixer.fixer.FixerFactory;
import com.google.cloud.bigquery.utils.queryfixer.fixer.IFixer;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.CalciteTokenizer;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.Tokenizer;

/**
 * The entity to perform fixing logic. It uses dry-run to identify an error, extracts a fixer based
 * on the error, fix the error, and return the fix result.
 */
public class AutomaticQueryFixer {

  private final BigQueryService service;
  private final SqlErrorFactory errorFactory;
  private final FixerFactory fixerFactory;

  public AutomaticQueryFixer(BigQueryOptions options) {
    service = new BigQueryService(options);
    errorFactory = new SqlErrorFactory();
    QueryTokenProcessor tokenProcessor = buildQueryTokenProcessor();
    fixerFactory = new FixerFactory(tokenProcessor, service);
  }

  public FixResult fix(String query) {
    BigQueryException exception = service.catchExceptionFromDryRun(query);
    if (exception == null) {
      return FixResult.noError();
    }

    BigQuerySqlError sqlError = errorFactory.getError(exception);
    if (sqlError == null) {
      return fixNotSupport(exception);
    }

    IFixer fixer = fixerFactory.getFixer(query, sqlError);
    if (fixer == null) {
      return fixNotSupport(sqlError);
    }

    return fixer.fix();
  }

  private QueryTokenProcessor buildQueryTokenProcessor() {
    BigQueryParserFactory parserFactory = new BigQueryParserFactory();
    Tokenizer tokenizer = new CalciteTokenizer(parserFactory);
    return new QueryTokenProcessor(tokenizer);
  }

  private FixResult fixNotSupport(BigQueryException exception) {
    return FixResult.builder()
        .status(FixResult.Status.FAILURE)
        .approach("The error cannot be fixed automatically.")
        .error(exception.getMessage())
        .build();
  }

  private FixResult fixNotSupport(BigQuerySqlError error) {
    return FixResult.builder()
        .status(FixResult.Status.FAILURE)
        .approach("The error cannot be fixed automatically.")
        .error(error.getMessage())
        .errorPosition(error.getErrorPosition())
        .build();
  }
}
