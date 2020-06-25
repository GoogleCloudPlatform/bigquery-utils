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
    QueryPositionConverter converter = new QueryPositionConverter(sql);
    int startIndex = converter.posToIndex(token.getBeginLine(), token.getBeginCol());
    int endIndex = converter.posToIndex(token.getEndLine(), token.getEndCol());
    if (startIndex == -1 || endIndex == -1) {
      throw new IllegalArgumentException("token position does not fit in the input query");
    }
    return replaceStringBetweenIndex(sql, startIndex, endIndex + 1, identifier);
  }

  public String insertBeforeToken(String sql, IToken token, String identifier) {
    return replaceToken(sql, token, identifier + " " + token.getImage());
  }

  public String deleteToken(String sql, IToken token) {
    return replaceToken(sql, token,  "");
  }

  // end index is excluded!
  private String replaceStringBetweenIndex(String old, int startIndex, int endIndex, String replacingPart) {
    StringBuilder builder = new StringBuilder(old);
    builder.replace(startIndex, endIndex, replacingPart);
    return builder.toString();
  }

}
