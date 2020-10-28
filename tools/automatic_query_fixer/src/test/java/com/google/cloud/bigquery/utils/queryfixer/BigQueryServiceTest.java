package com.google.cloud.bigquery.utils.queryfixer;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class BigQueryServiceTest {

  private BigQueryService service;

  private static final String FIELD = "corpus";
  private static final String TABLE_NOT_FOUND =
      "Not found: Table bigquery-public-data:samples.shakespearex was not found in location US";
  private static final String TABLE_1 = "311_request";
  private static final String TABLE_2 = "311_service_requests";

  @Mock BigQuery bigQueryMock;
  @Mock Job jobMock;
  @Mock JobStatistics.QueryStatistics queryStatisticsMock;
  @Mock Page<Table> tablePageMock;
  @Mock Table tableMock1;
  @Mock Table tableMock2;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    service = new BigQueryService("");
    injectBigQueryMockIntoService();
  }

  @Test
  public void dryRun_success() {
    setupBigQueryMock_returnSchema();
    String query =
        "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus limit 1000";
    Job job = service.dryRun(query);
    JobStatistics.QueryStatistics statistics = job.getStatistics();
    FieldList fields = statistics.getSchema().getFields();
    assertEquals(FIELD, fields.get(0).getName());
  }

  @Test
  public void dryRun_tableNotFound() {
    setupBigQueryMock_throwException();
    String query = "SELECT corpus FROM `bigquery-public-data.samples.shakespearex`";
    BigQueryException exception = service.catchExceptionFromDryRun(query);
    assertNotNull(exception);
    assertEquals(TABLE_NOT_FOUND, exception.getMessage());
  }

  @Test
  public void listTables() {
    setupBigQueryMock_returnTablePageMock();
    String project = "bigquery-public-data";
    String dataset = "austin_311";
    List<String> tables = service.listTableNames(project, dataset);
    assertEquals(2, tables.size());
    assertThat(tables, contains(TABLE_1, TABLE_2));
  }

  /**
   * Mock the connection to the BigQuery server. Use reflection to inject the fake connection into
   * the {@link BigQueryService}.
   */
  private void injectBigQueryMockIntoService() {
    try {
      FieldUtils.writeField(
          service, /* fieldName= */ "bigQuery", bigQueryMock, /* forceAccess= */ true);
    } catch (IllegalAccessException ignored) {
    }
  }

  /** Mock the response of dry run that returns the schema of a table. */
  private void setupBigQueryMock_returnSchema() {
    Schema schema = Schema.of(Field.of(FIELD, StandardSQLTypeName.STRING));

    when(queryStatisticsMock.getSchema()).thenReturn(schema);
    when(jobMock.getStatistics()).thenReturn(queryStatisticsMock);
    when(bigQueryMock.create(any(JobInfo.class))).thenReturn(jobMock);
  }

  /** Mock the response of dry run that throws a "Table Not Found" error. */
  private void setupBigQueryMock_throwException() {
    String errorMessage =
        "Not found: Table bigquery-public-data:samples.shakespearex was not found in location US";
    BigQueryError bigQueryError = new BigQueryError("", "", errorMessage);
    BigQueryException exception = new BigQueryException(400, errorMessage, bigQueryError);

    when(bigQueryMock.create(any(JobInfo.class))).thenThrow(exception);
  }

  /** Mock the listTables method of the BigQuery server. */
  private void setupBigQueryMock_returnTablePageMock() {
    when(tableMock1.getTableId()).thenReturn(TableId.of("fake", TABLE_1));
    when(tableMock2.getTableId()).thenReturn(TableId.of("fake", TABLE_2));
    List<Table> tables = ImmutableList.of(tableMock1, tableMock2);

    when(tablePageMock.iterateAll()).thenReturn(tables);
    when(bigQueryMock.listTables(any(DatasetId.class), any())).thenReturn(tablePageMock);
  }
}
