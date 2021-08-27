package com.google.cloud.bigquery.utils.queryfixer.tokenizer;

import com.google.cloud.bigquery.utils.queryfixer.QueryPositionConverter;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/** A class used to convert between index and position (row and column) of a query. */
@AllArgsConstructor
public class QueryTokenProcessor {

  private final Tokenizer tokenizer;

  /**
   * Return a token which is closed to a specific position at a query.
   *
   * @param query the input query
   * @param row the row of the position.
   * @param column the column of the position
   * @return the closed token.
   */
  public IToken getTokenAt(String query, int row, int column) {
    for (IToken token : getAllTokens(query)) {
      if (token.getEndRow() >= row && token.getEndColumn() >= column) {
        return token;
      }
    }

    return null;
  }

  /**
   * return a pair of tokens which is closed to a specific position at a query. The left returned
   * token is left to the position, and the right returned token is at or right to the position.
   *
   * @param query the input query
   * @param line the line of the position.
   * @param column the column of the position
   * @return a pair of tokens.
   */
  public Pair<IToken, IToken> getNearbyTokens(String query, int line, int column) {
    IToken previous = null;

    for (IToken token : getAllTokens(query)) {
      if (token.getEndRow() >= line && token.getEndColumn() >= column) {
        return Pair.of(previous, token);
      }
      previous = token;
    }

    return Pair.of(previous, previous);
  }

  /**
   * Tokenize a query and return all its tokens.
   *
   * @param query the query to be tokenized
   * @return a list of tokens of the query
   */
  public List<IToken> getAllTokens(String query) {
    return tokenizer.tokenize(query);
  }

  /**
   * Replace a token of a query and return the new query.
   *
   * @param query the query whose token is to be replaced
   * @param token the token to be replaced
   * @param identifier the identifier the token is placed to.
   * @return the replaced query
   */
  public String replaceToken(String query, IToken token, String identifier) {
    QueryPositionConverter converter = new QueryPositionConverter(query);
    // The token's row and column number are 1-index, but the array and string index start with 0.
    int startIndex = converter.posToIndex(token.getBeginRow(), token.getBeginColumn());
    int endIndex = converter.posToIndex(token.getEndRow(), token.getEndColumn());
    if (startIndex == -1 || endIndex == -1) {
      throw new IllegalArgumentException("Token position does not fit in the input query");
    }
    return StringUtil.replaceStringBetweenIndex(query, startIndex, endIndex + 1, identifier);
  }

  /**
   * Insert an identifier before a token and return the new query.
   *
   * @param query the query to be inserted
   * @param token the token to be inserted in front
   * @param identifier the inserted identifier
   * @return the inserted query
   */
  public String insertBeforeToken(String query, IToken token, String identifier) {
    return replaceToken(query, token, String.format(" %s %s", identifier, token.getImage()));
  }

  /**
   * Delete a token from a query and return the new query.
   *
   * @param query the query whose token is to be deleted
   * @param token the token to be deleted
   * @return the modified query
   */
  public String deleteToken(String query, IToken token) {
    return replaceToken(query, token, "");
  }
}
