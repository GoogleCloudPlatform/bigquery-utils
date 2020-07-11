package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import com.google.cloud.bigquery.utils.queryfixer.exception.ParserCreationException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ParserFactoryTest {

  @Test
  public void getParser_selectQuery() throws SqlParseException {
    BigQueryParserFactory factory = new BigQueryParserFactory();
    SqlParser parser = factory.getParser("select a from b");
    SqlNode node = parser.parseQuery();
    assertEquals(SqlKind.SELECT, node.getKind());
  }

  @Test
  public void getParser_selectWithSubQuery() throws SqlParseException {
    BigQueryParserFactory factory = new BigQueryParserFactory();
    SqlParser parser =
        factory.getParser(
            "select * from (select a, b, c from x cross join y where d > 10) inner join z");
    SqlNode node = parser.parseQuery();
    assertEquals(SqlKind.SELECT, node.getKind());
  }

  @Test
  public void getParser_nullQuery() {
    BigQueryParserFactory factory = new BigQueryParserFactory();

    try {
      factory.getBabelParserImpl(null);
      fail();
    } catch (NullPointerException e) {
      assertEquals("The input query should not be null.", e.getMessage());
    }
  }

  @Test
  public void getParserImpl_returnsIncorrectParser() {
    BigQueryParserFactory factory = new BigQueryParserFactory(SqlParserImpl.FACTORY);
    try {
      factory.getBabelParserImpl("select 1");
      fail();
    } catch (ParserCreationException e) {
      String prefix = "This factory does not produce Babel Parser.";
      assertTrue(e.getMessage().contains(prefix));
    }
  }
}
