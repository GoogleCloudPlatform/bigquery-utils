package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.junit.Test;

public class QueryBreakdownTest {
  @Test
  public void findNthIndexOfDNE() {
    String test = "abcde";
    assertEquals(-1, QueryBreakdown.findNthIndexOf(test, 'f', 1));
  }

  @Test
  public void findNthIndexOfFirst() {
    String test = "abcde";
    assertEquals(2, QueryBreakdown.findNthIndexOf(test, 'c', 1));
  }

  @Test
  public void findNthIndexOfMultiple() {
    String test = "abcddde";
    assertEquals(5, QueryBreakdown.findNthIndexOf(test, 'd', 3));
  }

  @Test
  public void findNthIndexOfMultipleDNE() {
    String test = "abcddde";
    assertEquals(-1, QueryBreakdown.findNthIndexOf(test, 'd', 4));
  }

  @Test
  public void findNthIndexOfMultipleNewLines() {
    String test = "abc\nd\ne";
    assertEquals(3, QueryBreakdown.findNthIndexOf(test, '\n', 1));
    assertEquals(5, QueryBreakdown.findNthIndexOf(test, '\n', 2));
    assertEquals(-1, QueryBreakdown.findNthIndexOf(test, '\n', 3));
  }

  @Test
  public void QueryBreakdownRunSimpleDeletion() throws IOException {
    // code to test println
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    String query = ir.readInput(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/simpleDeletion.txt");
    qb.run(query, "", 0, ir.getLocationTracker());
    assertEquals("Unparseable portion: Start Line 1, End Line 1, "
        + "Start Column 1, End Column 4, DELETION\n", outContent.toString());
  }

  @Test
  public void QueryBreakdownRunMultiDeletion() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    String query = ir.readInput(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/multipleDeletion.txt");
    qb.run(query, "", 0, ir.getLocationTracker());
    assertEquals(
        "Unparseable portion: Start Line 2, End Line 2, "
            + "Start Column 28, End Column 31, DELETION\n"
            + "Unparseable portion: Start Line 2, End Line 2, "
            + "Start Column 1, End Column 4, DELETION\n"
            + "Unparseable portion: Start Line 1, End Line 1, "
            + "Start Column 1, End Column 4, DELETION\n",
        outContent.toString());
  }
}