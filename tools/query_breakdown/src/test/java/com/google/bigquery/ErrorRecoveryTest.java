package com.google.bigquery;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import javax.management.Query;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

public class ErrorRecoveryTest {
  @Test
  public void deletionSingleLine() {
    String query = "SELECT GROUP a FROM A";
    assertEquals("SELECT a FROM A",
        QueryBreakdown.deletion(query, 1, 8, 12));
  }

  @Test
  public void deletionSingleLineNoSpace() {
    String query = "SELECT a FROM A WHERE a>GROUP";
    assertEquals("SELECT a FROM A WHERE a>",
        QueryBreakdown.deletion(query, 1, 25, 29));
  }

  @Test
  public void deletionMultipleLines() {
    String query = "SELECT a FROM A;" + '\n' + "SELECT GROUP b FROM B";
    assertEquals("SELECT a FROM A;" + '\n' + "SELECT b FROM B",
        QueryBreakdown.deletion(query, 2, 8, 12));
  }

  @Test
  public void deletionMultipleLinesNoSpace() {
    String query = "SELECT a FROM A;" + '\n' + "SELECT b FROM B WHERE b>GROUP";
    assertEquals("SELECT a FROM A;" + '\n' + "SELECT b FROM B WHERE b>",
        QueryBreakdown.deletion(query, 2, 25, 29));
  }

  @Test
  public void replacementExpectedSingle() {
    String query = "SELECT a FROM A GROUP WITH a";
    Parser parser = new CalciteParser();
    try {
      parser.parseQuery(query);
    } catch (Exception e) {
      Collection<String> test = ((SqlParseException) e).getExpectedTokenNames();
      Collection<String> actual = new HashSet<>();
      for (String s : test) {
        s = s.replace("\"", "");
        actual.add(s);
      }
      HashSet<String> expected = new HashSet<>();
      expected.add("BY");
      assertEquals(expected, actual);
    }
  }

  @Test
  public void expectedTokenFilterSingleLine() {
    String query = "SELECT a WHERE b";
    Parser parser = new CalciteParser();
    try {
      parser.parseQuery(query);
    } catch (Exception e) {
      Collection<String> test = ((SqlParseException) e).getExpectedTokenNames();
      HashSet<String> expected = new HashSet<>();
      expected.add("!");
      expected.add("%");
      expected.add("!=");
      assertEquals(expected,
          ReplacementLogic.replace("", QueryBreakdown.expectedTokensFilter(test)));
    }
  }

  @Test
  public void replacementSingleLine() {
    String query = "SELECT a WHERE b";
    Parser parser = new CalciteParser();
    try {
      parser.parseQuery(query);
    } catch (Exception e) {
      Collection<String> test = ((SqlParseException) e).getExpectedTokenNames();
      HashSet<String> expected = new HashSet<>();
      expected.add("SELECT a ! b");
      expected.add("SELECT a % b");
      expected.add("SELECT a != b");
      assertEquals(expected,
          QueryBreakdown.replacement(query, ((SqlParseException) e).getPos().getLineNum(),
              ((SqlParseException) e).getPos().getColumnNum(),
              ((SqlParseException) e).getPos().getEndColumnNum(),
              test));
    }
  }
}