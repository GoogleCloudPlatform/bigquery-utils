package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.errors.FunctionNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;

/**
 * The fixer class responsible for unrecognized functions. It fixes the error by (1) reading the
 * similar function suggestion from the error message, and (2) replace the unrecognized function
 * with the suggested one.
 *
 * <p>If the error message does not provide similar function suggestions, the fixer will directly
 * return the error without providing any fix options.
 */
@AllArgsConstructor
@Getter
public class FunctionNotFoundFixer implements IFixer {

  private final String query;
  private final FunctionNotFoundError err;
  private final QueryTokenProcessor queryTokenProcessor;

  @Override
  public FixResult fix() {
    // If the failure does not include a suggestion, directly inform users that it cannot be auto
    // fixed.
    if (!err.hasSuggestion()) {
      return FixResult.failure(query, err, "No similar function was found.");
    }

    // TODO: If the unrecognized function looks like a UDF (i.e. proj.dataset.function), then
    // request BigQuery for a list of functions and find the most similar one.

    IToken token =
        queryTokenProcessor.getTokenAt(
            query, err.getErrorPosition().getRow(), err.getErrorPosition().getColumn());
    String fixedQuery = queryTokenProcessor.replaceToken(query, token, err.getSuggestion());

    String approach = String.format("Replace the function `%s`", err.getFunctionName());
    String action = String.format("Change to `%s`", err.getSuggestion());

    FixOption fixOption = FixOption.of(action, fixedQuery);
    return FixResult.success(
        query, approach, Collections.singletonList(fixOption), err, /*isConfident=*/ true);
  }
}
