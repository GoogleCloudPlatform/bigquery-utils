package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.bigquery.utils.zetasqlhelper.ZetaSqlHelper;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.errors.ColumnNotGroupedError;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;

/**
 * A class to fix {@link ColumnNotGroupedError}. It will call the fixing function in ZetaSQL Helper
 * server. The server will locate the ungrouped column, find the select node having this ungrouped
 * column, then find its group-by node (create one if it does not exist), then add the ungrouped
 * column to the group-by clause.
 *
 * <p>Here is a BigQuery query with this error:
 *
 * <pre>
 *     SELECT status, count(*) FROM `bigquery-public-data.austin_311.311_request`
 * </pre>
 *
 * The error message is "SELECT list expression references column status which is neither grouped
 * nor aggregated at [1:8]". Thus, the fixer create a group by clause with the column "status":
 *
 * <pre>
 *     SELECT status, count(*) FROM `bigquery-public-data.austin_311.311_request` GROUP BY status
 * </pre>
 */
@AllArgsConstructor
@Getter
public class ColumnNotGroupedFixer implements IFixer {

  private final String query;
  private final ColumnNotGroupedError error;

  @Override
  public FixResult fix() {
    String missingColumn = error.getMissingColumn();
    int rowNumber = error.getErrorPosition().getRow();
    int columnNumber = error.getErrorPosition().getColumn();
    String fixedQuery =
        ZetaSqlHelper.fixColumnNotGrouped(query, missingColumn, rowNumber, columnNumber);

    FixOption option = FixOption.of("Add " + missingColumn, fixedQuery);
    return FixResult.success(
        query,
        /*approach=*/ "Add the missing column into its group-by clause (create one if not exists). Caution: this fix will reformat the query.",
        Collections.singletonList(option),
        error,
        /*isConfident=*/ true);
  }
}
