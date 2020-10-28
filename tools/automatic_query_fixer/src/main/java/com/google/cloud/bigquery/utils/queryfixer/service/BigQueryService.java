package com.google.cloud.bigquery.utils.queryfixer.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import lombok.NonNull;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A service to connect with the BigQuery server. It is used to communicate with the server like
 * sending queries and receiving data.
 */
public class BigQueryService {

  private static final int TABLE_FETCH_SIZE = 1000;

  private final BigQuery bigQuery;

  /**
   * Initialize a connection to BigQuery server with the customized options.
   *
   * @param options customized options
   */
  public BigQueryService(@NonNull BigQueryOptions options) {
    this.bigQuery = new BigQueryOptions.DefaultBigQueryFactory().create(options);
  }

  /**
   * Initialize a connection to BigQuery server with the default setting. The projectID should be
   * specified.
   *
   * @param projectId project ID
   */
  public BigQueryService(@NonNull String projectId) {
    BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId(projectId).build();
    this.bigQuery = options.getService();
  }

  /**
   * Dry run a BigQuery query and return the job instance. If a BigQuery Exception is generated, it
   * will be thrown.
   *
   * @param query the dry-run query
   * @return the job representing this dry run
   * @throws BigQueryException the error from the BigQuery Server.
   */
  public Job dryRun(String query) throws BigQueryException {
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query).setDryRun(true).build();
    return bigQuery.create(JobInfo.of(queryConfig));
  }

  /**
   * Return the BigQuery Exception if the dry run of a query generated the exception. If the query
   * is correct and no errors exist in the server, null will be returned.
   *
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
   * Fetch the names of all table from the BigQuery given the project and dataset. If the project
   * and/or dataset do not exist or not visible to the provided service account, {@link
   * com.google.cloud.bigquery.BigQueryException} will be thrown.
   *
   * <p>It is possible that a dataset contains a large amount of tables, so this method will only
   * fetch maximum 1000 of them.
   *
   * @param projectId project to fetch tables
   * @param datasetId dataset to fetch tables
   * @return list of table names belonging to the given project and dataset
   */
  public List<String> listTableNames(String projectId, String datasetId) throws BigQueryException {
    DatasetId projectDatasetId = DatasetId.of(projectId, datasetId);
    Page<Table> tables =
        bigQuery.listTables(projectDatasetId, BigQuery.TableListOption.pageSize(TABLE_FETCH_SIZE));

    // The reason to turn off parallel is to avoid this program using too many thread resources. Stream does not allow
    // users to specify the max threads to use. It may cause the program to occupy too much CPU resource.
    return StreamSupport.stream(tables.iterateAll().spliterator(), /* parallel= */ false)
        .map(table -> table.getTableId().getTable())
        .collect(Collectors.toList());
  }

  public BigQueryOptions getBigQueryOptions() {
    return bigQuery.getOptions();
  }

  //TODO: Add a validate function to check if the BigQueryOption is valid.
  // It can be verified by sending a "select 1" to server.
}
