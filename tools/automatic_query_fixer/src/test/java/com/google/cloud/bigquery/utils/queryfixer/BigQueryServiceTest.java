package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.commons.lang3.reflect.FieldUtils;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryServiceTest {

  private BigQueryService service;

  @Before
  public void getService() {
    service = new BigQueryService("");
    fakeBigQuery();
  }

  @Test
  public void dryRun_success() {
    String query =
        "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus limit 1000";
    Job job = service.dryRun(query);
    JobStatistics.QueryStatistics statistics = job.getStatistics();
    FieldList fields = statistics.getSchema().getFields();
    assertEquals("corpus", fields.get(0).getName());
  }

  @Test
  public void dryRun_tableNotFound() {
    String query = "SELECT corpus FROM `bigquery-public-data.samples.shakespearex`";
    BigQueryException exception = service.catchExceptionFromDryRun(query);
    assertNotNull(exception);
    assertEquals("Not found: Table", exception.getMessage().substring(0, 16));
  }

  @Test
  public void listTables() {
    String project = "bigquery-public-data";
    String dataset = "austin_311";
    List<String> tables = service.listTableNames(project, dataset);
    assertEquals(2, tables.size());
    assertThat(tables, contains("311_request", "311_service_requests"));
  }

  /**
   * Mock the connection to the BigQuery server. Use reflection to inject the fake connection into
   * the {@link BigQueryService}.
   */
  private void fakeBigQuery() {
    BigQuery bigQuery = mock(BigQuery.class);
    correctShakespeare(bigQuery);
    incorrectShakespeare(bigQuery);
    fakeTables(bigQuery);
    try {
      FieldUtils.writeField(service, "bigQuery", bigQuery, true);
    } catch (IllegalAccessException ignored) {
    }
  }

  /**
   * Mock the response of dry run if "SELECT corpus FROM `bigquery-public-data.samples.shakespeare`
   * GROUP BY corpus limit 1000" is given.
   */
  private void correctShakespeare(BigQuery bigQuery) {
    String query =
        "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus limit 1000";
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query).setDryRun(true).build();

    Job job = mock(Job.class);
    JobStatistics.QueryStatistics queryStatistics = mock(JobStatistics.QueryStatistics.class);
    Schema schema = Schema.of(Field.of("corpus", StandardSQLTypeName.STRING));

    when(queryStatistics.getSchema()).thenReturn(schema);
    when(job.getStatistics()).thenReturn(queryStatistics);
    when(bigQuery.create(JobInfo.of(queryConfig))).thenReturn(job);
  }

  /**
   * Mock the response of dry run if SELECT corpus FROM `bigquery-public-data.samples.shakespearex`"
   * is given.
   */
  private void incorrectShakespeare(BigQuery bigQuery) {
    String query = "SELECT corpus FROM `bigquery-public-data.samples.shakespearex`";
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query).setDryRun(true).build();

    String errorMessage =
        "Not found: Table bigquery-public-data:samples.shakespearex was not found in location US";
    BigQueryError bigQueryError = new BigQueryError("", "", errorMessage);
    BigQueryException exception = new BigQueryException(400, errorMessage, bigQueryError);

    when(bigQuery.create(JobInfo.of(queryConfig))).thenThrow(exception);
  }

  /** Mock the listTables method of the BigQuery server. */
  private void fakeTables(BigQuery bigQuery) {
    List<Table> tables =
        ImmutableList.of(fakeTable("311_request"), fakeTable("311_service_requests"));
    Page<Table> page = (Page<Table>) mock(Page.class);
    when(page.iterateAll()).thenReturn(tables);
    when(bigQuery.listTables((DatasetId) any(), any())).thenReturn(page);
  }

  private Table fakeTable(String tableName) {
    Table table = mock(Table.class);
    when(table.getTableId()).thenReturn(TableId.of("fake", tableName));
    return table;
  }
}
