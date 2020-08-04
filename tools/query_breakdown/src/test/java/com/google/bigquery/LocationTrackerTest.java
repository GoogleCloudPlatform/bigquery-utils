package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class LocationTrackerTest {

  @Test
  public void locationTrackerTestInitializedCorrectly() {
    InputReader ir = new InputReader();
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    ir.readFromString(query);
    assertEquals(4, ir.getLocationTracker().getOriginalPosition(3, 4));
  }

  @Test
  public void locationTrackerTestInitializedCorrectlyClone() {
    InputReader ir = new InputReader();
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    ir.readFromString(query);
    assertEquals(4, ir.getLocationTracker().cloneTracker().getOriginalPosition(3, 4));
  }

  @Test
  public void locationTrackerDeletionTest() {
    InputReader ir = new InputReader();
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    ir.readFromString(query);
    LocationTracker locationTracker =
        ir.getLocationTracker().delete(3, 1, 5);
    assertEquals(6, locationTracker.getOriginalPosition(3, 1));
  }
}