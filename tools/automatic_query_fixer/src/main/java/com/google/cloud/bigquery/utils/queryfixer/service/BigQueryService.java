package com.google.cloud.bigquery.utils.queryfixer.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A service to connect with the BigQuery server. It is used to communicate with the server like
 * sending queries and receiving data.
 * */
public class BigQueryService {

  private final BigQuery bigquery;

  /**
   * Initialize a connection to BigQuery server with the customized options.
   * @param options customized options
   * */
  public BigQueryService(BigQueryOptions options) {
    this.bigquery = new BigQueryOptions.DefaultBigQueryFactory().create(options);
  }

  /**
   * Initialize a connection to BigQuery server with the default setting. The projectID should be specified.
   * @param projectId project ID
   * */
  public BigQueryService(String projectId) {
    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
        .setProjectId(projectId);
    this.bigquery = builder.build().getService();
  }

  /**
   * Dry run a BigQuery query and return the job instance. If a BigQuery Exception is generated, it will be thrown.
   * @param query the dry-run query
   * @return the job representing this dry run
   * @throws BigQueryException the error from the BigQuery Server.
   */
  public Job dryRun(String query) throws BigQueryException {
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setDryRun(true).build();
    return bigquery.create(JobInfo.of(queryConfig));
  }

  /**
   * return the BigQuery Exception if the dry run of a query generated the exception. If the query is correct and no
   * errors exist in the server, null will be returned.
   * @param query the dry-run query
   * @return the BigQueryException related with this query
   */
  public BigQueryException catchExceptionFromDryRun(String query) {
    try {
      dryRun(query);
    } catch (BigQueryException exception) {
      return exception;
    }

    return null;
  }

  /**
   * Fetch the names of all table from the BigQuery given the project and dataset. If the project and/or dataset do
   * not exist or not visible to the provided service account, {@link com.google.cloud.bigquery.BigQueryException} will
   * be thrown.
   * @param projectId project id
   * @param datasetId dataset id
   * @return list of table names belonging to the given project and dataset
   */
  public List<String> listTableNames(String projectId, String datasetId) throws BigQueryException {
    DatasetId projectDatasetId = DatasetId.of(projectId, datasetId);
    Page<Table> tables = bigquery.listTables(projectDatasetId);
    return StreamSupport.stream(tables.iterateAll().spliterator(), /* parallel= */false)
        .map(table -> table.getTableId().getTable())
        .collect(Collectors.toList());
  }

  public BigQueryOptions getBigQueryOptions() {
    return bigquery.getOptions();
  }
}
