package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.utils.queryfixer.QueryPositionConverter;
import com.google.cloud.bigquery.utils.queryfixer.entity.*;
import com.google.cloud.bigquery.utils.queryfixer.errors.TableNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
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
  private final QueryTokenProcessor queryTokenProcessor;
  private final QueryPositionConverter queryPositionConverter;

  public TableNotFoundFixer(
      String query,
      TableNotFoundError err,
      BigQueryService bigQueryService,
      QueryTokenProcessor queryTokenProcessor) {
    this.query = query;
    this.err = err;
    this.bigQueryService = bigQueryService;
    this.queryTokenProcessor = queryTokenProcessor;
    this.queryPositionConverter = new QueryPositionConverter(query);
  }

  @Override
  public FixResult fix() {
    TableId incorrectTableId = constructTableId(err.getTableName());
    List<String> tableNames =
        bigQueryService.listTableNames(
            incorrectTableId.getProject(), incorrectTableId.getDataset());

    StringUtil.SimilarStrings similarTables =
        StringUtil.findSimilarWords(tableNames, fullTableId.getTable(), /*caseSensitive=*/false);

    // This is an arbitrary standard. It requires the candidate table should share at least 50%
    // similarity as the incorrect table typo.
    // TODO: this could be user configurable in future.
    int editDistanceThreshold = (incorrectTableId.getTable().length() + 1) / 2;

    if (similarTables.getStrings().isEmpty()
        || similarTables.getDistance() > editDistanceThreshold) {
      return FixResult.failure(query, err, "No similar table was found.");
    }

    // This method only finds the first occurrence of the incorrect table. It is possible that this
    // table exists in multiple positions of this query.
    SubstringView incorrectTableView = findSubstringViewOfIncorrectTable(incorrectTableId);
    if (incorrectTableView == null) {
      return FixResult.failure(query, err, "Cannot locate the incorrect table position.");
    }

    List<FixOption> fixOptions =
        similarTables.getStrings().stream()
            .map(
                table -> {
                  String fullTableName =
                      constructFullTableName(
                          incorrectTableId.getProject(), incorrectTableId.getDataset(), table);
                  String fixedQuery = replaceTable(fullTableName, incorrectTableView);
                  return FixOption.of(String.format("Change to `%s`", fullTableName), fixedQuery);
                })
            .collect(Collectors.toList());

    String approach = String.format("Replace the table name `%s`", err.getTableName());
    return FixResult.success(query, approach, fixOptions, err, /*isConfident=*/ true);
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

  private SubstringView findSubstringViewOfIncorrectTable(TableId fullTableId) {
    String regex;
    // If the project ID is the default one, then the actual table may not include project ID,
    // which looks like "dataset.table".
    // TODO: default dataset may be supported if we allow users to specify it in the query fixer's
    // input flags.
    if (fullTableId.getProject().equals(bigQueryService.getBigQueryOptions().getProjectId())) {
      regex =
          String.format(
              "(`?%s`?\\.)?`?%s`?\\.`?%s`?",
              fullTableId.getProject(), fullTableId.getDataset(), fullTableId.getTable());
    } else {
      regex =
          String.format(
              "`?%s`?\\.`?%s`?\\.`?%s`?",
              fullTableId.getProject(), fullTableId.getDataset(), fullTableId.getTable());
    }

    List<SubstringView> substringViews = PatternMatcher.findAllSubstrings(query, regex);

    // Iterate the substring and check if this substring is an identifier. If yes, this substring
    // should be the incorrect table we are looking for.
    for (SubstringView view : substringViews) {
      Position position = queryPositionConverter.indexToPos(view.getStart());
      IToken token = queryTokenProcessor.getTokenAt(query, position.getRow(), position.getColumn());

      if (token.getKind() != IToken.Kind.IDENTIFIER) {
        continue;
      }

      this.err.setErrorPosition(position);
      return view;
    }

    return null;
  }

  private String replaceTable(String newTable, SubstringView replacedTable) {
    return StringUtil.replaceStringBetweenIndex(
        query, replacedTable.getStart(), replacedTable.getEnd(), newTable);
  }
}
