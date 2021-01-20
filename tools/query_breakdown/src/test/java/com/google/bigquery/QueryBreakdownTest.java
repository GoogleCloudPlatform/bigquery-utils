package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.junit.Test;

public class QueryBreakdownTest {

  private void printNodes(List<Node> list) {
    for (Node n: list) {
      System.out.println(n.toString());
    }
  }

  @Test
  public void QueryBreakdownRunSimpleDeletion() throws IOException {
    // code to test println
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/simpleDeletion.txt");
    List<Node> result = qb.run(ir.getQueries().get(0), 100, 3,
        ir.getLocationTrackers().get(0));
    printNodes(result);
    assertEquals("Unparseable portion: Start Line 1, End Line 1, "
        + "Start Column 1, End Column 4, DELETION\n", outContent.toString());
  }

  @Test
  public void QueryBreakdownRunMultiDeletion() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
    QueryBreakdown qb2 = new QueryBreakdown(new CalciteParser());
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/multipleDeletion.txt");
    List<Node> result = qb.run(ir.getQueries().get(0), 100, 3,
        ir.getLocationTrackers().get(0));
    List<Node> result2 =
        qb2.run(ir.getQueries().get(1), 100, 3,
            ir.getLocationTrackers().get(1));
    printNodes(result);
    printNodes(result2);
    assertEquals(
        "Unparseable portion: Start Line 1, End Line 1, "
            + "Start Column 1, End Column 4, DELETION\n"
            + "Unparseable portion: Start Line 2, End Line 2, "
            + "Start Column 1, End Column 4, DELETION\n"
            + "Unparseable portion: Start Line 2, End Line 2, "
            + "Start Column 28, End Column 31, DELETION\n",
        outContent.toString());
  }

  @Test
  public void QueryBreakdownRunSingleDeletionReplacement() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
    QueryBreakdown qb2 = new QueryBreakdown(new CalciteParser());
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/singleDeletionReplacement.txt");
    List<Node> result = qb.run(ir.getQueries().get(0), 10000, 3,
        ir.getLocationTrackers().get(0));
    List<Node> result2 =
        qb2.run(ir.getQueries().get(1), 10000, 3,
            ir.getLocationTrackers().get(1));
    printNodes(result);
    printNodes(result2);
    assertEquals(
        "Unparseable portion: Start Line 1, End Line 1, "
            + "Start Column 1, End Column 4, DELETION\n"
            + "Unparseable portion: Start Line 2, End Line 2, "
            + "Start Column 1, End Column 4, DELETION\n"
            + "Unparseable portion: Start Line 2, End Line 2, "
            + "Start Column 28, End Column 31, REPLACEMENT: replaced BLAH with BY\n",
        outContent.toString());
  }

  @Test
  public void QueryBreakdownRunSingleton() throws IOException {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    QueryBreakdown qb = new QueryBreakdown(new CalciteParser());
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/singleton.txt");
    List<Node> result = qb.run(ir.getQueries().get(0), 100, 3,
        ir.getLocationTrackers().get(0));
    assertEquals(0, result.size());
  }
}