package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.errors.ExpectKeywordButGotOthersError;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to fix {@link ExpectKeywordButGotOthersError}. If the expected token is a keyword, then
 * the fixer will either (1) replace a token near the error position that is similar to the expected
 * keyword to the expected one, or (2) insert the expected keyword at the error location.
 *
 * <p>Here is a BigQuery query with this error:
 *
 * <pre>
 *     Select max(col1) from table group col2
 * </pre>
 *
 * The error message is "Syntax error: Expected keyword BY but got identifier "col2" at [1:34]".
 * Thus, the fixer will insert a BY keyword at this error position, which looks like
 *
 * <pre>
 *     Select max(col1) from table group BY col2
 * </pre>
 */
public class ExpectKeywordButGotOthersFixer implements IFixer {

  private final String query;
  private final ExpectKeywordButGotOthersError err;
  private final QueryTokenProcessor queryTokenProcessor;

  // TODO: it could be configured by users in future.
  private static final double SIMILARITY_THRESHOLD = 0.5;

  public ExpectKeywordButGotOthersFixer(
          String query, ExpectKeywordButGotOthersError err, QueryTokenProcessor queryTokenProcessor) {
    this.query = query;
    this.err = err;
    this.queryTokenProcessor = queryTokenProcessor;
  }

  @Override
  public FixResult fix() {
    Position errorPosition = err.getErrorPosition();
    IToken token =
        queryTokenProcessor.getTokenAt(query, errorPosition.getRow(), errorPosition.getColumn());

    List<FixOption> fixOptions = new ArrayList<>();
    if (isTokenSimilarAsExpectedKeyword(token)) {
      fixOptions.add(replaceToken(token));
    }
    fixOptions.add(insertKeyword(token));

    String approach =
        String.format("Insert keyword %s at the error position", err.getExpectedKeyword());
    return FixResult.success(query, approach, fixOptions, err, /*isConfident=*/ true);
  }

  private boolean isTokenSimilarAsExpectedKeyword(IToken token) {
    int editDistance =
        StringUtil.editDistance(
            token.getImage(), err.getExpectedKeyword(), /*caseSensitive=*/ false);
    return 1.0 * editDistance / token.getImage().length() <= SIMILARITY_THRESHOLD;
  }

  private FixOption replaceToken(IToken token) {
    String fixedQuery = queryTokenProcessor.replaceToken(query, token, err.getExpectedKeyword());
    String action = String.format("Replace %s as %s", token.getImage(), err.getExpectedKeyword());
    return FixOption.of(action, fixedQuery);
  }

  private FixOption insertKeyword(IToken token) {
    String fixedQuery =
        queryTokenProcessor.insertBeforeToken(query, token, err.getExpectedKeyword());
    String action = String.format("Insert %s", err.getExpectedKeyword());
    return FixOption.of(action, fixedQuery);
  }
}
