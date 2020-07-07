package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import com.google.cloud.bigquery.utils.queryfixer.exception.ParserCreationException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParseFactoryTest {

  @Test
  public void getParser() throws SqlParseException {
    BigQueryParserFactory factory = new BigQueryParserFactory();
    SqlParser parser = factory.getParser("select a from b");
    SqlNode node = parser.parseQuery();
    assertEquals(SqlKind.SELECT, node.getKind());
  }

  @Test
  public void queryNotNull() {
    BigQueryParserFactory factory = new BigQueryParserFactory();

    try {
      factory.getBabelParserImpl(null);
      fail();
    } catch (NullPointerException e) {
      assertEquals("the input query should not be null", e.getMessage());
    }

  }

  @Test
  public void getParserImplFromIncorrectFactory() {
    BigQueryParserFactory factory = new BigQueryParserFactory(SqlParserImpl.FACTORY);
    try {
      factory.getBabelParserImpl("select 1");
      fail();
    } catch (ParserCreationException e) {
      assertEquals("the factory does not produce Babel Parser", e.getMessage());
    }
  }
}
