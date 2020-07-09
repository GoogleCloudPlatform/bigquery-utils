package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.parser.babel.Token;

import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.TokenImpl;
import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * A processor provides methods for queries and tokens. It can be used to tokenize queries, find tokens based on
 * positions, and modify a query in token-level.
 * */
@AllArgsConstructor
public class QueryTokenProcessor {

  private final BigQueryParserFactory parserFactory;

  /**
   * Tokenize a query and return all its tokens.
   * @param query the query to be tokenized
   * @return a list of tokens of the query
   */
  public List<IToken> getAllTokens(String query) {
    Preconditions.checkNotNull(query, "Input query should not be null.");

    List<IToken> tokens = new ArrayList<>();

    // SqlBabelParserImpl has a token manager to tokenize the input query.
    SqlBabelParserImpl parserImpl = parserFactory.getBabelParserImpl(query);

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
