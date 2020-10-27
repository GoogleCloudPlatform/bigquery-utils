package com.google.cloud.bigquery.utils.queryfixer.fixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.errors.UnexpectedKeywordError;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import lombok.AllArgsConstructor;

import java.util.Collections;

/**
 * A class to fix {@link UnexpectedKeywordError}. Since the error usually is occurred by an
 * identifier having the same name as a keyword, the fixer would try to convert the unexpected
 * keyword to an identifier by adding backticks around it. This fixing can also cover some scenarios
 * leading to this error, so the fixing is considered not confident.
 *
 * <p>Here is an example of fixing. The input query
 *
 * <pre>
 *  SELECT hash FROM `bigquery-public-data.crypto_bitcoin.blocks`
 * </pre>
 *
 * has an error "Syntax error: Unexpected keyword HASH at [1:8]". The fixer will convert the HASH as
 * an identifier, making it look like
 *
 * <pre>
 *     SELECT `HASH` FROM `bigquery-public-data.crypto_bitcoin.blocks`
 * </pre>
 */
@AllArgsConstructor
public class UnexpectedKeywordFixer implements IFixer {

  private final String query;
  private final UnexpectedKeywordError err;
  private final QueryTokenProcessor queryTokenProcessor;

  @Override
  public FixResult fix() {
    Position errorPosition = err.getErrorPosition();
    IToken token =
        queryTokenProcessor.getTokenAt(query, errorPosition.getRow(), errorPosition.getColumn());
    String convertedImage = String.format("`%s`", token.getImage());
    String fixedQuery = queryTokenProcessor.replaceToken(query, token, convertedImage);

    String action = String.format("Change to %s", convertedImage);
    FixOption option = FixOption.of(action, fixedQuery);

    return FixResult.success(
        query,
        /*approach=*/ String.format("Convert keyword %s to an identifier", err.getKeyword()),
        Collections.singletonList(option),
        err,
        /*isConfident=*/ false);
  }
}
