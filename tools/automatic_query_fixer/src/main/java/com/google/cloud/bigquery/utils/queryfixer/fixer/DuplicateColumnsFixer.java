package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.bigquery.utils.zetasqlhelper.ZetaSqlHelper;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.errors.DuplicateColumnsError;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;

/**
 * A class to fix {@link DuplicateColumnsError}. It will call the fixing function in ZetaSQL Helper
 * server. The server will first locate the Select_List Node having the duplicate columns and update
 * the aliases of the duplicate columns by adding a numeric suffix (e.g. _1, _2, ...) at the end of
 * the duplicate names.
 *
 * <p>Here is a BigQuery query with this error:
 *
 * <pre>
 *     SELECT status, status FROM `bigquery-public-data.austin_311.311_request`
 * </pre>
 *
 * The error message is "Duplicate column names in the result are not supported. Found duplicate(s):
 * status". Thus, the fixer will update the aliases of "status" columns.
 *
 * <pre>
 *     SELECT status AS status_1, status AS status_2 FROM `bigquery-public-data.austin_311.311_request`
 * </pre>
 */
@AllArgsConstructor
@Getter
public class DuplicateColumnsFixer implements IFixer {

  private final String query;
  private final DuplicateColumnsError error;

  @Override
  public FixResult fix() {
    String duplicate = error.getDuplicate();
    String fixedQuery = ZetaSqlHelper.fixDuplicateColumns(query, duplicate);

    FixOption option = FixOption.of("Add \"_[0-9]+\" after " + duplicate, fixedQuery);
    return FixResult.success(
        query,
        /*approach=*/ "Add/update alias. Caution: this fix will reformat the query.",
        Collections.singletonList(option),
        error,
        /*isConfident=*/ true);
  }
}
