package com.google.bigquery;

import static org.junit.Assert.*;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CalciteParserTest {
  CalciteParser parser;

  @Before
  public void initParser() {
    parser = new CalciteParser();
  }

  @Test(expected= SqlParseException.class)
  public void parseQueryFail() throws SqlParseException {
    parser.parseQuery("BLAH SELECT");
  }

  @Test
  public void parseQuerySuccess() throws SqlParseException {
    parser.parseQuery("SELECT a FROM A");
  }

  @Test
  public void parseQueryMultipleLineSuccess() throws SqlParseException {
    parser.parseQuery("SELECT a FROM A; SELECT b FROM B");
  }

  @Test(expected= SqlParseException.class)
  public void parseQueryMultipleLineFail() throws SqlParseException {
    parser.parseQuery("SELECT a FROM A; BLAH SELECT");
  }

  // the Calcite parser can handle trailing/leading spaces as well as spaces in the middle
  @Test
  public void parseQuerySpaceSuccess() throws SqlParseException {
    Assert.assertEquals(parser.parseQuery("SELECT a FROM A"),
        parser.parseQuery("  SELECT a                      FROM A  "));
  }
}