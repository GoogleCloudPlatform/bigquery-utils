package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class InputReaderTest {
  @Test
  public void inputReaderTestSingleLine() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A", ir.readInput(absPath +
        "/src/test/java/com/google/bigquery/InputTestFiles/singleLine.txt"));
  }

  @Test
  public void inputReaderTestMultipleQuery() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A;\n\nSELECT b FROM B;",
        ir.readInput(absPath +
        "/src/test/java/com/google/bigquery/InputTestFiles/multipleLines.txt"));
  }

  @Test
  public void inputReaderTestSpaceWithoutSemicolon() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A;\n\nSELECT b FROM B",
            ir.readInput(absPath +
                    "/src/test/java/com/google/bigquery"
                + "/InputTestFiles/multipleLinesNoSemicolon.txt"));
  }

  @Test
  public void inputReaderTestMultiLineQuery() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a\nFROM A\nWHERE A = 3; SELECT b FROM B",
        ir.readInput(absPath +
            "/src/test/java/com/google/bigquery"
            + "/InputTestFiles/multipleLineQuery.txt"));
  }
}