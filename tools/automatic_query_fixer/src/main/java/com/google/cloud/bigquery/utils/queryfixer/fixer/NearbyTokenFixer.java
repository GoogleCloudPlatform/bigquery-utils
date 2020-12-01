package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.bigquery.utils.zetasqlhelper.ZetaSqlHelper;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.errors.ExpectKeywordButGotOthersError;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import com.google.cloud.bigquery.utils.queryfixer.util.PatternMatcher;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A class to fix general syntax errors. Usually, a general syntax error looks like {@link
 * ExpectKeywordButGotOthersError} but expects "end of input". It basically means there may be
 * multiple expected tokens but the error message cannot recognize them, so the fixer will try to
 * see if the tokens near the error position are similar to any keywords. If yes, then replace the
 * token to the similar keyword and check if the error is eliminated from the query.
 *
 * <p>Here is an example with an input query:
 *
 * <pre>
 *     SELECT status FORM `bigquery-public-data.austin_311.311_request` LIMIT 10
 * </pre>
 *
 * It causes an error "Syntax error: Expected end of input but got identifier `...` at [1:20]". The
 * fixer will look around this position and find FORM looks like a keyword FROM and convert it to
 * FROM, leading to a new query
 *
 * <pre>
 *     SELECT status FROM `bigquery-public-data.austin_311.311_request` LIMIT 10
 * </pre>
 */
public class NearbyTokenFixer implements IFixer {

  private final String query;
  private final ExpectKeywordButGotOthersError err;
  private final QueryTokenProcessor queryTokenProcessor;
  private final BigQueryService bigQueryService;

  private static List<String> KEYWORDS;

  // TODO: it could be configured by users in future.
  private static final double SIMILARITY_THRESHOLD = 0.5;

  public NearbyTokenFixer(
      String query,
      ExpectKeywordButGotOthersError err,
      QueryTokenProcessor queryTokenProcessor,
      BigQueryService bigQueryService) {
    this.query = query;
    this.err = err;
    this.queryTokenProcessor = queryTokenProcessor;
    this.bigQueryService = bigQueryService;

    // lazy initialization of KEYWORDS
    if (KEYWORDS == null) {
      KEYWORDS = getAllKeywords();
    }
  }

  @Override
  public FixResult fix() {

    Position errorPosition = err.getErrorPosition();
    Pair<IToken, IToken> tokens =
        queryTokenProcessor.getNearbyTokens(
            query, errorPosition.getRow(), errorPosition.getColumn());

    // Find the similar keywords that can replace the right token.
    List<String> rightTokenKeywords = findSimilarKeywords(tokens.getRight());
    List<FixOption> options =
        filterKeywordsAndToFixOptions(
            tokens.getRight(), rightTokenKeywords, /*offsetErrorPosition=*/ false);

    // Find the similar keywords that can replace the left token.
    List<String> leftTokenKeywords = findSimilarKeywords(tokens.getLeft());
    List<FixOption> leftTokenOptions =
        filterKeywordsAndToFixOptions(
            tokens.getLeft(), leftTokenKeywords, /*offsetErrorPosition=*/ true);

    options.addAll(leftTokenOptions);

    if (options.isEmpty()) {
      return FixResult.failure(query, err);
    }

    String approach = String.format("Replace `%s` or `%s`", tokens.getLeft(), tokens.getRight());
    return FixResult.success(query, approach, options, err, /*isConfident=*/ false);
  }

  private List<String> getAllKeywords() {
    return ZetaSqlHelper.getAllKeywords();
  }

  private List<String> findSimilarKeywords(IToken token) {
    if (token == null) {
      return Collections.emptyList();
    }

    int tokenSize = token.getImage().length();
    int maxEditDistance = (int) Math.ceil(SIMILARITY_THRESHOLD * tokenSize);
    return StringUtil.findSimilarWords(
        KEYWORDS, token.getImage(), maxEditDistance, /*caseSensitive=*/ false);
  }

  private FixOption toFixOption(IToken token, String keyword, String fixedQuery) {
    String action = String.format("%s => %s", token.getImage(), keyword);
    return FixOption.of(action, fixedQuery);
  }

  private Position dryRun_getErrorPosition(String query) {
    BigQueryException exception = bigQueryService.catchExceptionFromDryRun(query);
    if (exception == null) {
      return null;
    }

    // Syntax error must have error position. If an error does not contain position, it must be
    // semantic error.
    List<String> contents =
        PatternMatcher.extract(exception.getMessage(), " (\\[[0-9]+\\:[0-9]+\\])$");
    if (contents == null) {
      return null;
    }

    return PatternMatcher.extractPosition(contents.get(0));
  }

  private boolean isErrorMovedForward(String query, int columnOffset) {
    Position position = dryRun_getErrorPosition(query);
    if (position == null) {
      return true;
    }

    if (position.getRow() > err.getErrorPosition().getRow()) {
      return true;
    }
    if (position.getRow() < err.getErrorPosition().getRow()) {
      return false;
    }

    return position.getColumn() > err.getErrorPosition().getColumn() + columnOffset;
  }

  /**
   * Filter similar keywords by checking if replacing to a similar keyword can eliminate the error
   * or at least make the error position move forward.
   */
  private List<FixOption> filterKeywordsAndToFixOptions(
      IToken token, List<String> keywords, boolean offsetErrorPosition) {
    List<FixOption> fixOptions = new ArrayList<>();
    for (String keyword : keywords) {
      String modifiedQuery = queryTokenProcessor.replaceToken(query, token, keyword);
      int offset = offsetErrorPosition ? keyword.length() - token.getImage().length() : 0;

      if (isErrorMovedForward(modifiedQuery, offset)) {
        fixOptions.add(toFixOption(token, keyword, modifiedQuery));
      }
    }

    return fixOptions;
  }
}
