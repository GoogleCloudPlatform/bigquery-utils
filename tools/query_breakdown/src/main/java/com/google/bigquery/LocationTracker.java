package com.google.bigquery;

import java.util.ArrayList;
import javax.xml.stream.Location;

/**
 * This class tracks the original location of components in the query, thereby making sure that
 * the error locations are correctly represented.
 */
public class LocationTracker {
  /* we keep a double arraylist to represent the position of each character (line and column).
     We can do this as the line number of the component will not change
     (deletion and replacement won't change the line numbers)
   */
  private ArrayList<ArrayList<Integer>> location;

  /**
   * Constructor for the class
   */
  public LocationTracker() {
    location = new ArrayList<>();
  }

  /**
   * This method interacts with the InputReader and adds a pair to the location field that
   * represents the position (x, y) in the original query. x and y are 1-indexed, so we
   * adjust accordingly.
   */
  public void add(int x, int y) {
    location.get(x - 1).add(y);
  }

  /**
   * This method adds an empty line to the location field
   */
  public void addLine() {
    location.add(new ArrayList<>());
  }

  /**
   * This method removes an entry from location specified by (x, y) in the original query
   */
  public void remove(int x, int y) {
    if (x > 1 && x <= location.size()) {
      if (y > 1 && y <= location.get(x).size()) {
        location.get(x - 1).remove(y - 1);
      }
    }
  }

  /**
   * This method gets the original position of the component in (x,y) of the intermediate query
   */
  public int getOriginalPosition(int x, int y) {
    return location.get(x - 1).get(y - 1);
  }

  /**
   * This method ensures that the location field is kept correctly despite the deletion. It
   * returns a new LocationTracker object
   */
  public LocationTracker delete(int line, int startColumn, int endColumn) {
    LocationTracker lt = cloneTracker();
    for (int i = startColumn; i < endColumn + 1; i++) {
      lt.remove(line, i);
    }
    return lt;
  }

  /**
   * To be implemented
   */
  public LocationTracker replace() {
    return null;
  }

  public LocationTracker cloneTracker() {
    LocationTracker lt = new LocationTracker();
    for (int i = 0; i < location.size(); i++) {
      lt.addLine();
      ArrayList<Integer> lineOriginal = location.get(i);
      for (int j = 0; j < lineOriginal.size(); j++) {
        lt.add(i + 1, lineOriginal.get(j));
      }
    }
    return lt;
  }


}
