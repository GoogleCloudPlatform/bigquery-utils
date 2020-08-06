package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.utils.queryfixer.QueryPositionConverter;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.errors.TableNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.util.PatternMatcher;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;

import java.util.List;
import java.util.stream.Collectors;

/**
 * The fixer class responsible for "table not found" error. It fixes the error by (1) find the
 * similar table under the same project and dataset as the incorrect one, and (2) replace the
 * incorrect one with the most similar table.
 *
 * <p>If no similar tables are found in the dataset, the fixer will directly return the error
 * without providing any fix options.
 */
public class TableNotFoundFixer implements IFixer {

  /** This regex is able to parse both project:dataset.table and dataset.table. */
  private static final String tableIdRegex = "^((.*?):)?(.*?)\\.(.*?)$";

  private final String query;
  private final TableNotFoundError err;
  private final BigQueryService bigQueryService;
  private final QueryPositionConverter queryPositionConverter;

  public TableNotFoundFixer(String query, TableNotFoundError err, BigQueryService bigQueryService) {
    this.query = query;
    this.err = err;
    this.bigQueryService = bigQueryService;
    this.queryPositionConverter = new QueryPositionConverter(query);
  }

  @Override
  public FixResult fix() {
    TableId fullTableId = constructTableId(err.getTableName());
    List<String> tableNames =
        bigQueryService.listTableNames(fullTableId.getProject(), fullTableId.getDataset());

    StringUtil.SimilarStrings similarTables =
        StringUtil.findSimilarWords(tableNames, fullTableId.getTable());

    // This is an arbitrary standard. It requires the candidate table should share at least 50%
    // similarity as the incorrect table typo.
    // TODO: this could be user configurable in future.
    int editDistanceThreshold = (fullTableId.getTable().length() + 1) / 2;

    if (similarTables.getStrings().isEmpty()
        || similarTables.getDistance() > editDistanceThreshold) {
      return FixResult.failure(err);
    }

    // This method only finds the first occurrence of the incorrect table. It is possible that this
    // table exists in multiple positions of this query. What is worse, it is possible that the
    // table name is also part of a literal, then this auto fixing may have problem. The ultimate
    // solution for this issue is to use Parser to find the correct position of this table.
    int tableStartIndex = findTheIndexOfIncorrectTable();

    List<FixOption> fixOptions =
        similarTables.getStrings().stream()
            .map(
                table -> {
                  String fullTableName =
                      constructFullTableName(
                          fullTableId.getProject(), fullTableId.getDataset(), table);
                  String fixedQuery = replaceTable(fullTableName, tableStartIndex);
                  return FixOption.of(fullTableName, fixedQuery);
                })
            .collect(Collectors.toList());

    return FixResult.success(/*approach= */ "Replace the table name.", fixOptions, err);
  }

  private TableId constructTableId(String fullTableName) {
    List<String> contents = PatternMatcher.extract(fullTableName, tableIdRegex);
    String projectId;
    // Assume the table name from dry-run message is always correct, so no check is performed for
    // matching.
    // If contents[0] == null, it means the table looks like dataset.table, so the project ID should
    // be fetched from the BigQuery Client.
    if (contents.get(0) == null) {
      projectId = bigQueryService.getBigQueryOptions().getProjectId();
    } else {
      projectId = contents.get(1);
    }
    String datasetId = contents.get(2);
    String tableName = contents.get(3);
    return TableId.of(projectId, datasetId, tableName);
  }

  private String constructFullTableName(String projectId, String datasetId, String tableName) {
    return String.format("%s.%s.%s", projectId, datasetId, tableName);
  }

  private int findTheIndexOfIncorrectTable() {
    // The table in the error message is presented in the legacySQL mode, but this fixer is used to
    // fix the
    // standardSQL. Thus, the table name needs to be converted to the one consistent with
    // standardSQL.
    // The change is from project:dataset.table to project.dataset.table.
    String tableName = err.getTableName().replace(':', '.');
    int index = query.indexOf(tableName);

    // Since the TableNotFound error has no position info, this method will convert the index to the
    // position and assign to the `err`.
    Position position = queryPositionConverter.indexToPos(index);
    this.err.setErrorPosition(position);
    return index;
  }

  private String replaceTable(String newTable, int startIndex) {
    return StringUtil.replaceStringBetweenIndex(
        query, startIndex, startIndex + err.getTableName().length(), newTable);
  }
}
