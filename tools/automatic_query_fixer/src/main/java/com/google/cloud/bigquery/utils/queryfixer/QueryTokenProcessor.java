package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.parser.babel.Token;

import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.TokenImpl;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A processor provides methods for query and token. It can be used to tokenize queries, find tokens based on
 * positions, and modify a query in token-level.
 * */
@AllArgsConstructor
public class QueryTokenProcessor {

  private final BigQueryParserFactory parserFactory;

  /**
   * tokenize a query and return all its tokens.
   * @param sql the query to be tokenized
   * @return a list of tokens of the query
   */
  public List<IToken> getAllTokens(String sql) {
    Objects.requireNonNull(sql, "input query should not be null");

    List<IToken> tokens = new ArrayList<>();

    // SqlBabelParserImpl has a token manager to tokenize the input query.
    SqlBabelParserImpl parserImpl = parserFactory.getBabelParserImpl(sql);

    Token token;
    final int EndKind = 0;
    // Token kind means the category: it could be some keywords, identifier, literals, and etc.
    // The end of a token stream is end token, whose value is zero.
    while ((token = parserImpl.getNextToken()).kind != EndKind) {
      tokens.add(new TokenImpl(token));
    }

    return tokens;
  }

}
