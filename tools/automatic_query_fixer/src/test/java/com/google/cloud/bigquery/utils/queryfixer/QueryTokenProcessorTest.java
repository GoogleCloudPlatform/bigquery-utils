package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class QueryTokenProcessorTest {

  private QueryTokenProcessor tokenService;

  @Before
  public void createService() {
    tokenService = new QueryTokenProcessor(new BigQueryParserFactory());
  }

  @Test
  public void convertQueryToTokens() {
    String sql = "Select col from `d1.t1`\n" + "where t1.col>'val'";

    List<IToken> tokens = tokenService.getAllTokens(sql);
    assertEquals(10, tokens.size());
    assertEquals("Select", tokens.get(0).getImage());
    assertEquals("'val'", tokens.get(tokens.size() - 1).getImage());
  }
}
