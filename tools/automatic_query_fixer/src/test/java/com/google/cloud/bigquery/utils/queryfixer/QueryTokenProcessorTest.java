package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.CalciteTokenizer;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class QueryTokenProcessorTest {

  private QueryTokenProcessor tokenService;

  @Before
  public void createService() {
    tokenService = new QueryTokenProcessor(new CalciteTokenizer(new BigQueryParserFactory()));
  }

  @Test
  public void convertQueryToTokens() {
    String query = "Select col from `d1.t1`\n" + "where t1.col>'val'";

    List<IToken> tokens = tokenService.getAllTokens(query);
    QueryPositionConverter converter = new QueryPositionConverter(query);

    for (IToken token : tokens) {
      int startIndex = converter.posToIndex(token.getBeginRow(), token.getBeginColumn());
      int endIndex = converter.posToIndex(token.getEndRow(), token.getEndColumn() + 1);
      assertEquals(
          token.getImage().toUpperCase(), query.substring(startIndex, endIndex).toUpperCase());
    }
  }

  @Test
  public void verifyModifiedQuery() {
    String origin = "Select col from t1 Join\nt2 on t1.id = t2.id\nwhere t1.col > 'val'";
    String target;

    target = "Select Distinct col from t1 Join\nt2 on t1.id = t2.id\nwhere t1.col > 'val'";
    String identifier = "Select Distinct";
    IToken token = tokenService.getTokenAt(origin, 1, 1);
    // Replace Select With Select Distinct
    String modifiedQuery = tokenService.replaceToken(origin, token, identifier);
    assertEquals(target, modifiedQuery);

    target = "Select  Distinct col from t1 Join\nt2 on t1.id = t2.id\nwhere t1.col > 'val'";
    identifier = "Distinct";
    token = tokenService.getTokenAt(origin, 1, 8);
    // Insert Distinct before FROM
    modifiedQuery = tokenService.insertBeforeToken(origin, token, identifier);
    assertEquals(target, modifiedQuery);

    target = "Select  from t1 Join\nt2 on t1.id = t2.id\nwhere t1.col > 'val'";
    token = tokenService.getTokenAt(origin, 1, 9);
    // Delete FROM token
    modifiedQuery = tokenService.deleteToken(origin, token);
    assertEquals(target, modifiedQuery);
  }
}
