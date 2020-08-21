package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class LocationTrackerTest {

  /**
  @Test
  public void locationTrackerTestInitializedCorrectly() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    InputReader.readFromString(query);
    Pair expected = new Pair(3, 4);
    assertEquals(expected, ir.getLocationTracker().getOriginalPosition(3, 4));
  }

  @Test
  public void locationTrackerTestInitializedCorrectlyClone() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    InputReader.readFromString(query);
    Pair expected = new Pair(3, 4);
    assertEquals(expected,
        ir.getLocationTracker().cloneTracker().getOriginalPosition(3, 4));
  }

  @Test
  public void locationTrackerDeletionTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    InputReader.readFromString(query);
    LocationTracker locationTracker =
        ir.getLocationTracker().delete(3, 1, 3,5);
    Pair expected = new Pair(3, 6);
    assertEquals(expected, locationTracker.getOriginalPosition(3, 1));
  }

  @Test
  public void locationTrackerDeletionMultipleLinesTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    InputReader.readFromString(query);
    LocationTracker locationTracker =
        ir.getLocationTracker().delete(1, 5, 3,5);
    Pair expected = new Pair(1, 4);
    Pair expected2 = new Pair(3, 10);
    assertEquals(expected, locationTracker.getOriginalPosition(1, 4));
    assertEquals(expected2, locationTracker.getOriginalPosition(2, 5));
  } **/
}