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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
      return FixResult.noError(query);
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

  /**
   * The query fixer continuously fixes the query based on the dry-run message. It will
   * automatically apply the {@link com.google.cloud.bigquery.utils.queryfixer.entity.FixOption} to
   * fix the query. If multiple options are available, the first option will be chosen. It will be
   * terminated only when the latest {@link FixResult} has NO_ERROR or FAILURE status, or the newly
   * fixed query was identical to the one fixed before (i.e. A cycle exists).
   *
   * @param query query to fix
   * @return List of fix results that represent all the fixes applied to this query.
   */
  public List<FixResult> autoFix(String query) {
    List<FixResult> results = new ArrayList<>();
    // A set to store all the fixed queries. It is used to detect whether a cycle exists when
    // automatically fixing queries.
    Set<String> fixedQueries = new HashSet<>();

    while (true) {
      if (fixedQueries.contains(query)) {
        results.add(FixResult.infiniteLoop(query));
        break;
      }

      FixResult result = fix(query);
      fixedQueries.add(query);
      results.add(result);

      // result.getOptions().isEmpty() is equivalent to the status equals NO_ERROR or FAILURE.
      if (result.getOptions() == null || result.getOptions().isEmpty()) {
        break;
      }
      query = result.getOptions().get(0).getFixedQuery();
    }
    return results;
  }

  public List<FixResult> autoFixUntilUncertainOptions(String query) {
    List<FixResult> results = new ArrayList<>();
    // A set to store all the fixed queries. It is used to detect whether a cycle exists when
    // automatically fixing queries.
    Set<String> fixedQueries = new HashSet<>();

    while (true) {
      if (fixedQueries.contains(query)) {
        results.add(FixResult.infiniteLoop(query));
        break;
      }

      FixResult result = fix(query);
      fixedQueries.add(query);
      results.add(result);

      // If there is no fix options or multiple of them, break the loop.
      if (result.getOptions() == null || result.getOptions().size() != 1) {
        break;
      }
      query = result.getOptions().get(0).getFixedQuery();
    }
    return results;
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
