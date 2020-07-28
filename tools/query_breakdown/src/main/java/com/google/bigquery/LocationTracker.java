package com.google.bigquery;

import java.util.ArrayList;
import org.apache.calcite.util.mapping.IntPair;

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
   * represents the position (x, y) in the original query
   */
  public void add(int x, int y) {
    location.get(x).add(y);
  }

  /**
   * This method gets the original position of the component in (x,y) of the intermediate query
   */
  public int getOriginalPosition(int x, int y) {
    return location.get(x).get(y);
  }

  public void delete(int line, int startColumn, int endColumn) {

  }

  public void replace() {

  }




}
