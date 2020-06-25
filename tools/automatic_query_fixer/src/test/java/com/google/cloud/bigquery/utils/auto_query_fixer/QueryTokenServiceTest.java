package com.google.cloud.bigquery.utils.auto_query_fixer;

import org.apache.commons.lang3.tuple.Pair;

import com.google.cloud.bigquery.utils.auto_query_fixer.entity.IToken;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class QueryTokenServiceTest {

  @Test
  public void convertSqlToTokens() {
    QueryTokenService tokenService = new QueryTokenService();
    String sql = "Select col from t1 Join\n" +
        "t2 on t1.id = t2.id\n" +
        "where t1.col > 'val'";

    List<IToken> tokens =  tokenService.getAllTokens(sql);
    for (IToken token : tokens) {
      System.out.println(token);
    }
  }

  @Test
  public void testNearbyTokens() {
    QueryTokenService tokenService = new QueryTokenService();
    String sql = "Select col from t1 Join\n" +
        "t2 on t1.id = t2.id\n" +
        "where t1.col > 'val'";

    Pair<IToken, IToken> tokenPair =  tokenService.getNearbyTokens(sql, 1, 12);
    assertEquals("col", tokenPair.getLeft().getImage());
    assertEquals("from", tokenPair.getRight().getImage());

    tokenPair =  tokenService.getNearbyTokens(sql, 3, 1);
    assertEquals("id", tokenPair.getLeft().getImage());
    assertEquals("where", tokenPair.getRight().getImage());
  }

  @Test
  public void modifyQuery() {
    QueryTokenService tokenService = new QueryTokenService();
    String sql = "Select col from t1 Join\n" +
        "t2 on t1.id = t2.id\n" +
        "where t1.col > 'val'";

    String identifier = "Select Distinct";

    IToken token =  tokenService.getNearbyTokens(sql, 1, 1).getRight();
    String modifiedQuery =  tokenService.replaceToken(sql, token, identifier);
    // modified: Select Distinct col
    assertEquals(identifier, modifiedQuery.substring(0, identifier.length()));

    identifier = "Distinct";
    token =  tokenService.getNearbyTokens(sql, 1, 8).getRight();
    modifiedQuery =  tokenService.insertBeforeToken(sql, token, identifier);
    // modified: Select Distinct col
    assertEquals(identifier, modifiedQuery.substring(7, 7 + identifier.length()));

    token =  tokenService.getNearbyTokens(sql, 1, 8).getRight();
    modifiedQuery =  tokenService.deleteToken(sql, token);
    // modified: Select  from ...
    assertEquals("from", modifiedQuery.substring(8, 12));
  }

}
