package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BigQueryServiceTest {

  private BigQueryService service;

  /**
   * Please modify this method if your BigQuery credential is not at default path or you would like
   * to use other options.
   *
   * <p>To create a credential in the default path, users can execute `gcloud auth
   * application-default login` in command line. Also, please modify the project ID to the one
   * belonging to your account.
   *
   * @return a default BigQuery options
   */
  private BigQueryOptions getOptions() {
    // please modify this projectId to the one belonging to you.
    String projectId = "sql-gravity-internship";
    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder().setProjectId(projectId);
    return builder.build();
  }

  @Before
  public void getService() {
    service = new BigQueryService(getOptions());
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
}
