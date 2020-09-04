package com.google.cloud.bigquery.utils.queryfixer.tokenizer;

import com.google.bigquery.utils.zetasqlhelper.Token;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.ZetaSqlToken;

import java.util.List;
import java.util.stream.Collectors;

/** A class to use ZetaSQL Helper to tokenize queries */
public class ZetaSqlTokenizer implements Tokenizer {

  @Override
  public List<IToken> tokenize(final String query) {

    ZetaSqlToken.Factory factory = new ZetaSqlToken.Factory(query);
    // rawTokens mean the tokens directly from ZetaSQL
    List<Token> rawTokens = com.google.bigquery.utils.zetasqlhelper.ZetaSqlHelper.tokenize(query);

    return rawTokens.stream()
        // We don't need the last End_Of_Input Token
        .limit(rawTokens.size() - 1)
        .map(factory::create)
        .collect(Collectors.toList());
  }
}
