package com.google.bigquery;

import static org.junit.Assert.*;

import com.google.bigquery.InputReader;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class InputReaderTest {
  @Test
  public void inputReaderTest() throws IOException {
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A", InputReader.readInput(absPath +
        "/src/test/java/com/google/bigquery/InputTestFiles/singleLine.txt"));
  }

  @Test
  public void inputReaderTestMultipleQuery() throws IOException {
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A;\nSELECT b FROM B;",
        InputReader.readInput(absPath +
        "/src/test/java/com/google/bigquery/InputTestFiles/multipleLines.txt"));
  }

  @Test
  public void inputReaderTestSpaceWithoutSemicolon() throws IOException {
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A;\nSELECT b FROM B",
            InputReader.readInput(absPath +
                    "/src/test/java/com/google/bigquery"
                + "/InputTestFiles/multipleLinesNoSemicolon.txt"));
  }
}