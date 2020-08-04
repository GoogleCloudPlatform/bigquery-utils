package com.google.cloud.bigquery.utils.queryfixer.tokenizer;

import com.google.cloud.bigquery.utils.queryfixer.BigQueryParserFactory;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.TokenImpl;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.parser.babel.Token;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
public class CalciteTokenizer implements Tokenizer {

  private final BigQueryParserFactory parserFactory;

  @Override
  public List<IToken> tokenize(@NonNull final String query) {
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
