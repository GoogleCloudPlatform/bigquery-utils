package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.QueryPositionConverter;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.errors.IllegalInputCharacterError;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;

import java.util.Collections;

/**
 * A class to fix {@link IllegalInputCharacterError}. The fixer just locates the illegal character
 * and deletes it from the query.
 *
 * <p>For example, the following query
 *
 * <pre>
 * SELECT unique_key FROM `bigquery-public-data.austin_incidents`.incidents_2008$ LIMIT 10
 * </pre>
 *
 * has an error "Syntax error: Illegal input character "$" at [1:78]", and the fixer will fix the
 * query as
 *
 * <pre>
 * SELECT unique_key FROM `bigquery-public-data.austin_incidents`.incidents_2008 LIMIT 10
 * </pre>
 */
public class IllegalInputCharacterFixer implements IFixer {

  private final String query;
  private final IllegalInputCharacterError err;
  private final QueryPositionConverter positionConverter;

  public IllegalInputCharacterFixer(String query, IllegalInputCharacterError err) {
    this.query = query;
    this.err = err;
    positionConverter = new QueryPositionConverter(query);
  }

  @Override
  public FixResult fix() {
    Position errorPosition = err.getErrorPosition();
    int index = positionConverter.posToIndex(errorPosition.getRow(), errorPosition.getColumn());
    if (index == -1) {
      FixResult.failure(query, err, "Cannot locate the illegal input character.");
    }

    // delete the character from the query.
    String fixedQuery = StringUtil.replaceStringBetweenIndex(query, index, index + 1, "");

    FixOption option = FixOption.of("delete " + err.getIllegalCharacter(), fixedQuery);
    return FixResult.success(
            query,
        /*approach=*/ "Delete illegal input character",
        Collections.singletonList(option),
        err,
        /*isConfident=*/ false);
  }
}
