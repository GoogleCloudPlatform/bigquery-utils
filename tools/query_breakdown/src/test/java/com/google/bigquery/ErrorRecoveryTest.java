package com.google.bigquery;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

public class ErrorRecoveryTest {
  @Test
  public void deletionSingleLine() {
    String query = "SELECT GROUP a FROM A";
    assertEquals("SELECT  a FROM A",
        QueryBreakdown.deletion(query, 1, 8, 1,12));
  }

  @Test
  public void deletionSingleLineNoSpace() {
    String query = "SELECT a FROM A WHERE a>GROUP";
    assertEquals("SELECT a FROM A WHERE a>",
        QueryBreakdown.deletion(query, 1, 25, 1,29));
  }

  @Test
  public void deletionMultipleLines() {
    String query = "SELECT a FROM A;" + '\n' + "SELECT GROUP b FROM B";
    assertEquals("SELECT a FROM A;" + '\n' + "SELECT  b FROM B",
        QueryBreakdown.deletion(query, 2, 8, 2,12));
  }

  @Test
  public void deletionMultipleLinesNoSpace() {
    String query = "SELECT a FROM A;" + '\n' + "SELECT b FROM B WHERE b>GROUP";
    assertEquals("SELECT a FROM A;" + '\n' + "SELECT b FROM B WHERE b>",
        QueryBreakdown.deletion(query, 2, 25, 2,29));
  }

  @Test
  public void deletionMultipleLinesNoSpaceAcrossLines() {
    String query = "SELECT a FROM A" + '\n' + "SELECT b FROM B WHERE b > 3";
    assertEquals("SELECT a " + "\nb FROM B WHERE b > 3",
        QueryBreakdown.deletion(query, 1, 10, 2,7));
  }

  @Test
  public void deletionLineGreaterThanOneMultiple() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    assertEquals("SELE\n" + " A = 3; SELECT b FROM B",
        QueryBreakdown.deletion(query, 1, 5, 3,5));
  }

  @Test
  public void deletionLineGreaterThanOneMultipleNewLine() {
    String query = "\n SELECT b FROM B";
    assertEquals("\nCT b FROM B",
        QueryBreakdown.deletion(query, 1, 1, 2,5));
  }

  @Test
  public void replacementExpectedSingle() {
    String query = "SELECT a FROM A GROUP WITH a";
    Parser parser = new CalciteParser();
    try {
      parser.parseQuery(query);
    } catch (Exception e) {
      Collection<String> test = ((SqlParseException) e).getExpectedTokenNames();
      Collection<String> actual = new ArrayList<>();
      for (String s : test) {
        s = s.replace("\"", "");
        actual.add(s);
      }
      ArrayList<String> expected = new ArrayList<>();
      expected.add("BY");
      assertEquals(expected, actual);
    }
  }

  @Test
  public void expectedTokenLimit() {
    String query = "SELECT a WHERE b";
    Parser parser = new CalciteParser();
    try {
      parser.parseQuery(query);
    } catch (Exception e) {
      Collection<String> test = ((SqlParseException) e).getExpectedTokenNames();
      ArrayList<ReplacedComponent> actual =  QueryBreakdown.replacement(query, 3,
          ((SqlParseException) e).getPos().getLineNum(),
          ((SqlParseException) e).getPos().getColumnNum(),
          ((SqlParseException) e).getPos().getEndLineNum(),
          ((SqlParseException) e).getPos().getEndColumnNum(),
          test);
      assertEquals(3, actual.size());
    }
  }

  @Test
  public void replacementMultipleLinesNoSpaceAcrossLines() {
    String query = "SELECT a FROM A" + '\n' + "SELECT b FROM B WHERE b > 3";
    Collection<String> test = new ArrayList<>();
    test.add("TEST");
    ArrayList<ReplacedComponent> expected = new ArrayList<>();
    expected.add(new ReplacedComponent("SELECT a TEST\nb FROM B WHERE b > 3",
        "FROM A\nSELECT ", "TEST"));
    ArrayList<ReplacedComponent> actual =  QueryBreakdown.replacement(query,
        3, 1, 10, 2, 7, test);
    assertEquals(expected, actual);
  }

  @Test
  public void replacementMultipleGreaterThanOneMultiple() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    Collection<String> test = new ArrayList<>();
    test.add("TEST");
    ArrayList<ReplacedComponent> expected = new ArrayList<>();
    expected.add(new ReplacedComponent("SELETEST\n A = 3; SELECT b FROM B",
        "CT a\nFROM A\nWHERE", "TEST"));
    ArrayList<ReplacedComponent> actual =  QueryBreakdown.replacement(query,
        3, 1, 5, 3, 5, test);
    assertEquals(expected, actual);
  }

  @Test
  public void replacementMultipleGreaterThanOneMultipleNewLine() {
    String query = "\n SELECT b FROM B";
    Collection<String> test = new ArrayList<>();
    test.add("TEST");
    ArrayList<ReplacedComponent> expected = new ArrayList<>();
    expected.add(new ReplacedComponent("TEST\nCT b FROM B",
        "\n" + " SELE", "TEST"));
    ArrayList<ReplacedComponent> actual =  QueryBreakdown.replacement(query,
        3, 1, 1, 2, 5, test);
    assertEquals(expected, actual);
  }
}