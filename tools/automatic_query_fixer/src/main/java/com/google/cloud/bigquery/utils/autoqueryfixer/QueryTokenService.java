package com.google.cloud.bigquery.utils.autoqueryfixer;

import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.parser.babel.Token;
import org.apache.calcite.util.SourceStringReader;
import org.apache.commons.lang3.tuple.Pair;

import com.google.cloud.bigquery.utils.autoqueryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.autoqueryfixer.entity.TokenImpl;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class QueryTokenService {

  private final static String BackTickQuotingMode = "BTID";
  private final BigQueryParserFactory parserFactory;

  /**
   * return a pair of tokens which is closed to a specific position at a query. The left returned token
   * is left to the position, and the right returned token is at or right to the position.
   * @param sql the input query
   * @param line the line of the position.
   * @param column the column of the position
   * @return a pair of tokens.
   */
  public Pair<IToken, IToken> getNearbyTokens(String sql, int line, int column) {
    IToken previous = null;

    for (IToken token : getAllTokens(sql)) {
      if (token.getBeginLine() >= line && token.getBeginCol() >= column) {
        return Pair.of(previous, token);
      }
      previous = token;
    }

    return Pair.of(previous, previous);
  }

  /**
   * tokenize a query and return all its tokens.
   * @param sql the query to be tokenized
   * @return a list of tokens of the query
   */
  public List<IToken> getAllTokens(String sql) {
    List<IToken> tokens = new ArrayList<>();

    // SqlBabelParserImpl has a token manager to tokenize the input query.
    SqlBabelParserImpl parserImpl = (SqlBabelParserImpl) parserFactory.getParserConfig()
        .parserFactory().getParser(new SourceStringReader(sql));
    parserImpl.switchTo(BackTickQuotingMode);

    Token token;
    final int EndKind = 0;
    while ((token = parserImpl.getNextToken()).kind != EndKind) {
      tokens.add(new TokenImpl(token));
    }

    return tokens;
  }

  /**
   * replace a token of a query and return the new query.
   * @param sql the query whose token is to be replaced
   * @param token the token to be replaced
   * @param identifier the identifier the token is placed to.
   * @return the replaced query
   */
  public String replaceToken(String sql, IToken token, String identifier) {
    String[] lines = sql.split("\n");
    validateToken(lines, token);

    // The token's line and column number are 1-index,
    // but the array and string index start with 0.
    String line = lines[token.getBeginLine() - 1];
    line = replaceStringBetweenIndex(line, token.getBeginCol() - 1, token.getEndCol(), identifier);
    lines[token.getBeginLine() - 1] = line;

    return String.join("\n", lines);
  }

  /**
   * insert an identifier before a token and return the new query.
   * @param sql the query to be inserted
   * @param token the token to be inserted in front
   * @param identifier the inserted identifier
   * @return the inserted query
   */
  public String insertBeforeToken(String sql, IToken token, String identifier) {
    return replaceToken(sql, token, identifier + " " + token.getImage());
  }

  /**
   * delete a token from a query and return the new query.
   * @param sql the query whose token is to be deleted
   * @param token the token to be deleted
   * @return the modified query
   */
  public String deleteToken(String sql, IToken token) {
    return replaceToken(sql, token, "");
  }

  private void validateToken(String[] lines, IToken token) {
    if (token.getBeginLine() != token.getEndLine()) {
      throw new IllegalArgumentException("Illegal Token");
    }

    if (token.getEndLine() > lines.length) {
      throw new IllegalArgumentException("the end line of token exceeds the total length of query");
    }
  }

  /**
   * replace a substring of a string to a new one. the replacing range is replaced as [startIndex, endIndex),
   * i.e. the endIndex is excluded.
   */
  private String replaceStringBetweenIndex(String old, int startIndex, int endIndex, String replacingPart) {
    StringBuilder builder = new StringBuilder(old);
    builder.replace(startIndex, endIndex, replacingPart);
    return builder.toString();
  }

}
