package com.google.bigquery;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.xml.stream.Location;
import org.junit.Test;

public class LocationTrackerTest {
  @Test
  public void locationTrackerTestInitializedCorrectly() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3;\nSELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    Pair expected = new Pair(3, 4);
    Pair expected2 = new Pair(2, 5);
    Pair expected3 = new Pair(4, 5);
    Pair expected4 = new Pair(3, 13);
    assertEquals(expected, trackers.get(0).getOriginalPosition(3, 4));
    assertEquals(expected2, trackers.get(0).getOriginalPosition(2, 5));
    assertEquals(expected3, trackers.get(1).getOriginalPosition(2, 5));
    assertEquals(expected4, trackers.get(1).getOriginalPosition(1, 1));
  }

  @Test
  public void locationTrackerTestInitializedCorrectlyClone() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3;\nSELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    Pair expected = new Pair(3, 4);
    Pair expected2 = new Pair(2, 5);
    Pair expected3 = new Pair(4, 5);
    Pair expected4 = new Pair(3, 13);
    assertEquals(expected, trackers.get(0).cloneTracker().getOriginalPosition(3, 4));
    assertEquals(expected2, trackers.get(0).cloneTracker().getOriginalPosition(2, 5));
    assertEquals(expected3, trackers.get(1).cloneTracker().getOriginalPosition(2, 5));
    assertEquals(expected4, trackers.get(1).cloneTracker().getOriginalPosition(1, 1));
  }

  @Test
  public void locationTrackerDeletionTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    LocationTracker locationTracker =
        trackers.get(0).delete(3, 1, 3,5);
    LocationTracker locationTracker2 =
        trackers.get(1).delete(1, 2, 1,6);
    Pair expected = new Pair(3, 6);
    Pair expected2 = new Pair(3, 19);
    assertEquals(expected, locationTracker.getOriginalPosition(3, 1));
    assertEquals(expected2, locationTracker2.getOriginalPosition(1, 2));
  }

  @Test
  public void locationTrackerDeletionMultipleLinesTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3;\n SELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    LocationTracker locationTracker =
        trackers.get(0).delete(1, 5, 3,5);
    LocationTracker locationTracker2 =
        trackers.get(1).delete(1, 1, 2,5);
    Pair expected = new Pair(1, 4);
    Pair expected2 = new Pair(3, 10);
    Pair expected3 = new Pair(4, 7);
    assertEquals(expected, locationTracker.getOriginalPosition(1, 4));
    assertEquals(expected2, locationTracker.getOriginalPosition(2, 5));
    assertEquals(expected3, locationTracker2.getOriginalPosition(2, 2));
  }

  @Test
  public void locationTrackerDeletionMultipleLinesEndTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3;\n SELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    LocationTracker locationTracker =
        trackers.get(0).delete(1, 5, 3,12);
    Pair expected = new Pair(1, 4);
    assertEquals(expected, locationTracker.getOriginalPosition(1, 4));
    assertEquals(1, locationTracker.getLocation().size());
  }

  @Test
  public void locationTrackerReplacementTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3; SELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    LocationTracker locationTracker =
        trackers.get(0).replace(3, 1, 3, 5,
            "WHERE", "TEST");
    LocationTracker locationTracker2 =
        trackers.get(1).replace(1, 2, 1, 6,
            "SELEC", "TEST");
    Pair expected = new Pair(3, 1);
    Pair expected2 = new Pair(3, 6);
    Pair expected3 = new Pair(3, 13);
    Pair expected4 = new Pair(3, 23);
    assertEquals(expected, locationTracker.getOriginalPosition(3, 1));
    assertEquals(expected2, locationTracker.getOriginalPosition(3, 5));
    assertEquals(expected3, locationTracker2.getOriginalPosition(1, 1));
    assertEquals(expected4, locationTracker2.getOriginalPosition(1, 10));
  }

  @Test
  public void locationTrackerReplacementMultipleLinesTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3;\n SELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    LocationTracker locationTracker =
        trackers.get(0).replace(1, 5, 3, 5,
            "CT a\nFROM A\nWHERE;", "TEST");
    LocationTracker locationTracker2 =
        trackers.get(1).replace(1, 1, 2, 5,
        "\n SELE;", "TEST");
    Pair expected = new Pair(1, 4);
    Pair expected2 = new Pair(3, 10);
    Pair expected3 = new Pair(4, 7);
    Pair expected4 = new Pair(1, 9);
    assertEquals(expected, locationTracker.getOriginalPosition(1, 4));
    assertEquals(expected2, locationTracker.getOriginalPosition(2, 5));
    assertEquals(expected3, locationTracker2.getOriginalPosition(2, 2));
    assertEquals(expected4, locationTracker.getOriginalPosition(1, 9));
  }

  @Test
  public void locationTrackerReplacementMultipleLinesEndTest() {
    String query = "SELECT a\n" + "FROM A\n" + "WHERE A = 3;\n SELECT b FROM B";
    List<LocationTracker> trackers = InputReader.readFromString(query);
    LocationTracker locationTracker =
        trackers.get(0).replace(1, 5, 3, 12,
            "CT a\nFROM A\nWHERE A = 3;", "TEST");
    Pair expected = new Pair(1, 4);
    assertEquals(expected, locationTracker.getOriginalPosition(1, 4));
    assertEquals(1, locationTracker.getLocation().size());
  }
}