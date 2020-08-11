package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySqlError;
import com.google.cloud.bigquery.utils.queryfixer.errors.SqlErrorFactory;
import com.google.cloud.bigquery.utils.queryfixer.fixer.FixerFactory;
import com.google.cloud.bigquery.utils.queryfixer.fixer.IFixer;
import com.google.cloud.bigquery.utils.queryfixer.fixer.TableNotFoundFixer;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.CalciteTokenizer;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class FixerTest {

  private static final String TABLE_2017 = "survey_2017";
  private static final String TABLE_2018 = "survey_2018";
  private static final String TABLE_2019 = "survey_2019";
  private static final String TABLE_2020 = "survey_2020";

  private SqlErrorFactory errorFactory;
  private FixerFactory fixerFactory;

  @Mock private BigQueryService bigQueryServiceMock;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    errorFactory = new SqlErrorFactory();
    QueryTokenProcessor tokenProcessor =
        new QueryTokenProcessor(new CalciteTokenizer(new BigQueryParserFactory()));
    fixerFactory = new FixerFactory(tokenProcessor, bigQueryServiceMock);
  }

  @Test
  public void fixTableNotFound() {
    setupBigQueryService_mockListTableNames();
    String query =
        String.format("Select max(foo) from %s group by bar limit 10", fullMockTable(TABLE_2017));
    String message =
        String.format(
            "Not found: Table bigquery-public-data:mock.%s was not found in location US",
            TABLE_2017);
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof TableNotFoundFixer);

    FixResult result = fixer.fix();
    assertEquals(2, result.getOptions().size());
    List<String> tables =
        result.getOptions().stream().map(FixOption::getDescription).collect(Collectors.toList());

    assertThat(tables, contains(fullMockTable(TABLE_2018), fullMockTable(TABLE_2019)));

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(22, result.getErrorPosition().getColumn());
  }

  private String fullMockTable(String table) {
    return "bigquery-public-data.mock." + table;
  }

  private BigQuerySqlError buildError(String message) {
    BigQueryError bigQueryError = new BigQueryError("", "", message);
    BigQueryException exception = new BigQueryException(400, message, bigQueryError);
    return errorFactory.getError(exception);
  }

  private void setupBigQueryService_mockListTableNames() {
    when(bigQueryServiceMock.listTableNames(any(String.class), any(String.class)))
        .thenReturn(ImmutableList.of(TABLE_2018, TABLE_2019, TABLE_2020));
  }
}
