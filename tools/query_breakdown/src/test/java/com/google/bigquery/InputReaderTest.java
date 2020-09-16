package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class InputReaderTest {
  @Test
  public void inputReaderTestSingleLine() throws IOException {
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath +
        "/src/test/java/com/google/bigquery/InputTestFiles/singleLine.txt");
    assertEquals("SELECT a FROM A", ir.getQueries().get(0));
    assertEquals(1, ir.getQueries().size());
    assertEquals(1, ir.getLocationTrackers().size());
  }

  @Test
  public void inputReaderTestMultipleQuery() throws IOException {
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath +
        "/src/test/java/com/google/bigquery/InputTestFiles/multipleLines.txt");
    assertEquals("SELECT a FROM A", ir.getQueries().get(0));
    assertEquals("\n\nSELECT b FROM B", ir.getQueries().get(1));
    assertEquals(2, ir.getQueries().size());
    assertEquals(2, ir.getLocationTrackers().size());
  }

  @Test
  public void inputReaderTestSpaceWithoutSemicolon() throws IOException {
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath +
        "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/multipleLinesNoSemicolon.txt");
    assertEquals("SELECT a FROM A", ir.getQueries().get(0));
    assertEquals("\n\nSELECT b FROM B", ir.getQueries().get(1));
    assertEquals(2, ir.getQueries().size());
    assertEquals(2, ir.getLocationTrackers().size());
  }

  @Test
  public void inputReaderTestMultiLineQuery() throws IOException {
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath +
        "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/multipleLineQuery.txt");
    assertEquals("SELECT a\nFROM A\nWHERE A = 3", ir.getQueries().get(0));
    assertEquals(" SELECT b FROM B", ir.getQueries().get(1));
    assertEquals(2, ir.getQueries().size());
    assertEquals(2, ir.getLocationTrackers().size());
  }

  @Test
  public void inputReaderEmpty() throws IOException {
    String absPath = new File("").getAbsolutePath();
    InputReader ir = new InputReader(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/empty.txt");
    assertEquals(0, ir.getQueries().size());
    assertEquals(0, ir.getLocationTrackers().size());
  }
}