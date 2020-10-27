package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    System.out.println(parser.parseQuery("SELECT a FROM A"));
    assertEquals("(SELECT \"a\"\n" + "FROM \"A\")\n", outContent.toString());
  }

  @Test
  public void parseQueryMultipleLineSuccess() throws SqlParseException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    System.out.println(parser.parseQuery("SELECT a FROM A; SELECT b FROM B"));
    assertEquals("(SELECT \"a\"\n" + "FROM \"A\"), (SELECT \"b\"\n" + "FROM \"B\")\n",
        outContent.toString());
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

  // tests for exception position retrieval
  @Test
  public void parseQueryExceptionCatchSingleLine() {
    try {
      parser.parseQuery("BLAH SELECT a FROM A");
    }
    catch (SqlParseException e){
      assertEquals(1, e.getPos().getLineNum());
      assertEquals(1, e.getPos().getEndLineNum());
      assertEquals(1, e.getPos().getColumnNum());
      assertEquals(4, e.getPos().getEndColumnNum());
    }
  }

  @Test
  public void parseQueryExceptionCatchMultipleLine() {
    try {
      parser.parseQuery("SELECT a FROM A; BLAH SELECT b FROM B");
    }
    catch (SqlParseException e){
      assertEquals(1, e.getPos().getLineNum());
      assertEquals(1, e.getPos().getEndLineNum());
      assertEquals(18, e.getPos().getColumnNum());
      assertEquals(21, e.getPos().getEndColumnNum());
    }
  }

  @Test
  public void parseQueryExceptionCatchNewLine() {
    try {
      parser.parseQuery("SELECT a FROM A; "
          + '\n' + "BLAH SELECT b FROM B");
    }
    catch (SqlParseException e){
      assertEquals(2, e.getPos().getLineNum());
      assertEquals(2, e.getPos().getEndLineNum());
      assertEquals(1, e.getPos().getColumnNum());
      assertEquals(4, e.getPos().getEndColumnNum());
    }
  }
}