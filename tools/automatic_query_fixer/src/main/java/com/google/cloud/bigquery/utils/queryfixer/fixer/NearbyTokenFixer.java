package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.errors.ExpectKeywordButGotOthersError;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.stream.Collectors;

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
  private final List<String> keywords;

  // TODO: it could be configured by users in future.
  private static final double SIMILARITY_THRESHOLD = 0.5;

  public NearbyTokenFixer(
      String query, ExpectKeywordButGotOthersError err, QueryTokenProcessor queryTokenProcessor) {
    this.query = query;
    this.err = err;
    this.queryTokenProcessor = queryTokenProcessor;
    this.keywords = getAllKeywords();
  }

  @Override
  public FixResult fix() {

    Position errorPosition = err.getErrorPosition();
    Pair<IToken, IToken> tokens =
        queryTokenProcessor.getNearbyTokens(
            query, errorPosition.getRow(), errorPosition.getColumn());

    StringUtil.SimilarStrings rightTokenSimilarStrings = findSimilarKeywords(tokens.getRight());
    StringUtil.SimilarStrings leftTokenSimilarStrings = findSimilarKeywords(tokens.getLeft());

    // The overall logic below is that if left token has a more similar (measured by
    // edit distance) keyword, then only choose the fixes on left tokens. If right token
    // has more similar keywords, choose the fixes on right tokens. If both of them have
    // the similar keywords with the same edit distance, then choose both of them.
    // Otherwise, if neither of them have any similar keywords, then return FAILURE FixResult.
    if (leftTokenSimilarStrings.isEmpty() && rightTokenSimilarStrings.isEmpty()) {
      return FixResult.failure(query, err);
    }

    if (rightTokenSimilarStrings.isEmpty()) {
      List<FixOption> options = toFixOptions(tokens.getLeft(), leftTokenSimilarStrings);
      String approach = String.format("Replace `%s`.", tokens.getLeft());
      return FixResult.success(query, approach, options, err, /*isConfident=*/ false);
    }

    if (leftTokenSimilarStrings.isEmpty()) {
      List<FixOption> options = toFixOptions(tokens.getRight(), rightTokenSimilarStrings);
      String approach = String.format("Replace `%s`.", tokens.getRight());
      return FixResult.success(query, approach, options, err, /*isConfident=*/ false);
    }

    if (rightTokenSimilarStrings.getDistance() > leftTokenSimilarStrings.getDistance()) {
      List<FixOption> options = toFixOptions(tokens.getLeft(), leftTokenSimilarStrings);
      String approach = String.format("Replace `%s`.", tokens.getLeft());
      return FixResult.success(query, approach, options, err, /*isConfident=*/ false);
    }

    if (rightTokenSimilarStrings.getDistance() < leftTokenSimilarStrings.getDistance()) {
      List<FixOption> options = toFixOptions(tokens.getRight(), rightTokenSimilarStrings);
      String approach = String.format("Replace `%s`.", tokens.getRight());
      return FixResult.success(query, approach, options, err, /*isConfident=*/ false);
    }

    List<FixOption> options = toFixOptions(tokens.getRight(), rightTokenSimilarStrings);
    options.addAll(toFixOptions(tokens.getLeft(), leftTokenSimilarStrings));
    String approach = String.format("Replace `%s` or `%s`.", tokens.getLeft(), tokens.getRight());
    return FixResult.success(query, approach, options, err, /*isConfident=*/ false);
  }

  private List<String> getAllKeywords() {
    // TODO: needs to implement actual log when the ZetaSQL Helper is updated.
    return ImmutableList.of("SELECT", "FROM");
  }

  private StringUtil.SimilarStrings findSimilarKeywords(IToken token) {
    if (token == null) {
      return StringUtil.SimilarStrings.empty();
    }
    // TODO: only consider the identifier token. This logic will be implemented when #155 is merged.

    StringUtil.SimilarStrings similarStrings =
        StringUtil.findSimilarWords(keywords, token.getImage(), /*caseSensitive=*/ false);

    int tokenSize = token.getImage().length();
    if (1.0 * similarStrings.getDistance() / tokenSize > SIMILARITY_THRESHOLD) {
      return StringUtil.SimilarStrings.empty();
    }
    return similarStrings;
  }

  private List<FixOption> toFixOptions(IToken token, StringUtil.SimilarStrings similarStrings) {
    return similarStrings.getStrings().stream()
        .map(
            word -> {
              String action = String.format("%s => %s", token.getImage(), word);
              String modifiedQuery = queryTokenProcessor.replaceToken(query, token, word);
              return FixOption.of(action, modifiedQuery);
            })
        .collect(Collectors.toList());
  }
}
