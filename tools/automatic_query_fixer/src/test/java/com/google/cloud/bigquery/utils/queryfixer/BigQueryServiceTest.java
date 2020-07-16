package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BigQueryServiceTest {

  /**
   * Please modify this method if your BigQuery credential is not at default path or you would like
   * to use other options.
   *
   * @return a default BigQuery options
   */
  private BigQueryOptions getOptions() {
    String projectId = "sql-gravity-internship";
    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder().setProjectId(projectId);
    return builder.build();
  }

  private BigQueryService getService() {
    return new BigQueryService(getOptions());
  }

  @Test
  public void dryRun1() {
    BigQueryService service = getService();
    String query =
        "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus limit 1000";
    Job job = service.dryRun(query);
    JobStatistics.QueryStatistics statistics = job.getStatistics();
    FieldList fields = statistics.getSchema().getFields();
    assertEquals("corpus", fields.get(0).getName());
  }

  @Test
  public void dryRun2() {
    BigQueryService service = getService();
    String query = "SELECT corpus FROM `bigquery-public-data.samples.shakespearex`";
    BigQueryException exception = service.catchExceptionFromDryRun(query);
    assertNotNull(exception);
    assertEquals("Not found: Table", exception.getMessage().substring(0, 16));
  }
}
