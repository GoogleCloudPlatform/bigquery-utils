package com.google.cloud.bigquery.utils.auto_query_fixer;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.parser.babel.Token;
import org.apache.calcite.util.SourceStringReader;
import org.apache.commons.lang3.tuple.Pair;

import com.google.cloud.bigquery.utils.auto_query_fixer.entity.IToken;
import com.google.cloud.bigquery.utils.auto_query_fixer.entity.TokenImpl;

import java.util.ArrayList;
import java.util.List;

public class QueryTokenService {

  public Pair<IToken, IToken> getNearbyTokens(String sql, int beginLine, int beginColumn) {
    IToken previous = null;

    for (IToken token : getAllTokens(sql)) {
      if (token.getBeginLine() >= beginLine && token.getBeginCol() >= beginColumn) {
        return Pair.of(previous, token);
      }
      previous = token;
    }

    return Pair.of(previous, previous);
  }

  public List<IToken> getAllTokens(String sql) {
    List<IToken> tokens = new ArrayList<>();

    // a parser contains token manager, which will be used to tokenize sql.
    final SqlParser.ConfigBuilder configBuilder =
        SqlParser.configBuilder()
            .setParserFactory(SqlBabelParserImpl.FACTORY);
    SqlBabelParserImpl parserImpl = (SqlBabelParserImpl) configBuilder.build().parserFactory().getParser(new SourceStringReader(sql));

    Token token;
    while ((token = parserImpl.getNextToken()).kind != 0) {
      tokens.add(new TokenImpl(token));
    }

    return tokens;
  }

  public String replaceToken(String sql, IToken token, String identifier) {
    String[] lines = sql.split("\n");
    validateToken(lines, token);

    // the token's line and column number are 1-index,
    // but the array and string index start with 0.
    String line = lines[token.getBeginLine() - 1];
    line = replaceStringBetweenIndex(line, token.getBeginCol() - 1, token.getEndCol(), identifier);
    lines[token.getBeginLine() - 1] = line;

    return String.join("\n", lines);
  }

  public String insertBeforeToken(String sql, IToken token, String identifier) {
    return replaceToken(sql, token, identifier + " " + token.getImage());
  }

  public String deleteToken(String sql, IToken token) {
    return replaceToken(sql, token,  "");
  }

  private void validateToken(String[] lines, IToken token) {
    if (token.getBeginLine() != token.getEndLine()) {
      throw new IllegalArgumentException("Illegal Token");
    }

    if (token.getEndLine() > lines.length) {
      throw new IllegalArgumentException("the end line of token exceeds the total length of query");
    }
  }

  // end index is excluded!
  private String replaceStringBetweenIndex(String old, int startIndex, int endIndex, String replacingPart) {
    StringBuilder builder = new StringBuilder(old);
    builder.replace(startIndex, endIndex, replacingPart);
    return builder.toString();
  }

}
