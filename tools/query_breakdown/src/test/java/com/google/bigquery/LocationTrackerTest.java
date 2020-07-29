package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class LocationTrackerTest {

  @Test
  public void locationTrackerTestInitializedCorrectly() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    ir.readInput(absPath + "/src/test/java/com/google/bigquery"
            + "/InputTestFiles/multipleLineQuery.txt");
    assertEquals(4, ir.getLocationTracker().getOriginalPosition(3, 4));
  }

  @Test
  public void locationTrackerTestInitializedCorrectlyClone() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    ir.readInput(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/multipleLineQuery.txt");
    assertEquals(4, ir.getLocationTracker().cloneTracker().getOriginalPosition(3, 4));
  }

  @Test
  public void locationTrackerDeletionTest() throws IOException {
    InputReader ir = new InputReader();
    String absPath = new File("").getAbsolutePath();
    ir.readInput(absPath + "/src/test/java/com/google/bigquery"
        + "/InputTestFiles/multipleLineQuery.txt");
    LocationTracker lt = ir.getLocationTracker().delete(3, 1, 5);
    assertEquals(6, lt.getOriginalPosition(3, 1));
  }
}