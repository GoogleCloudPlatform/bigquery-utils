package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.errors.UnrecognizedColumnError;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;

/**
 * The fixer class responsible for unrecognized columns. It fixes the error by (1) reading the
 * similar column suggestion from the error message, and (2) replace the unrecognized column with
 * the suggested one.
 *
 * <p>If the error message does not provide similar column suggestions, the fixer will directly
 * return the error without providing any fix options.
 */
@AllArgsConstructor
@Getter
public class UnrecognizedColumnFixer implements IFixer {

  private final String query;
  private final UnrecognizedColumnError err;
  private final QueryTokenProcessor queryTokenProcessor;

  @Override
  public FixResult fix() {
    // If the failure does not include a suggestion, directly inform users that it cannot be auto
    // fixed.
    if (!err.hasSuggestion()) {
      return FixResult.failure(query, err, "No similar column was found.");
    }

    IToken token =
        queryTokenProcessor.getTokenAt(
            query, err.getErrorPosition().getRow(), err.getErrorPosition().getColumn());
    String fixedQuery = queryTokenProcessor.replaceToken(query, token, err.getSuggestion());

    String approach = String.format("Replace the column `%s`", err.getColumnName());
    String action = String.format("Change to `%s`", err.getSuggestion());

    FixOption fixOption = FixOption.of(action, fixedQuery);
    return FixResult.success(
        query, approach, Collections.singletonList(fixOption), err, /*isConfident=*/ true);
  }
}
