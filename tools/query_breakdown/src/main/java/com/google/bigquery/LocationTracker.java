package com.google.bigquery;

import java.util.ArrayList;

/**
 * This class tracks the original location of components in the query, thereby making sure that
 * the error locations are correctly represented. For each pair of (line, column) in the original
 * query, it is initialized in position at the (line - 1)th arraylist and
 * (column - 1)th element of the arraylist as the integer column - 1.
 * Since the line number won't ever change (but the column number will change constantly),
 * we simply keep track of the original column number as the integer in the double arraylist
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
   * This method inserts an element in the middle of the location ArrayList to replicate
   * replacement
   */
  private void add(int x, int y, int numberToAdd) {
    location.get(x - 1).add(y, numberToAdd);
  }

  /**
   * This method removes an entry from location specified by (x, y) in the original query.
   */
  public void remove(int x, int y) {
    if (x > 0 && x <= location.size()) {
      if (y > 0 && y <= location.get(x - 1).size()) {
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
   * This method ensures that the location field is kept correctly despite the deletion. We do
   * this by removing the entry in location that corresponds to the deleted characters from
   * position. We also make sure that we return a new location tracker instance (by making
   * a deep copy) such that a new copy is passed to further runs of the tool.
   */
  public LocationTracker delete(int line, int startColumn, int endColumn) {
    LocationTracker locationTracker = cloneTracker();
    for (int i = startColumn; i < endColumn + 1; i++) {
      locationTracker.remove(line, startColumn);
    }
    return locationTracker;
  }

  /**
   * This method ensures that the location field is kept correctly despite the replacement. We
   * case by the length of the string we replaceFrom to the string that we replaceTo. Depending
   * on the length, we either leave location as is, add to it, or delete from it.
   */
  public LocationTracker replace(int line, int startColumn, int endColumn, String replaceFrom,
      String replaceTo) {
    if (replaceFrom.length() == replaceTo.length()) {
      return this;
    }
    // if we replace the token with a longer token and need to add to the locationTracker
    else if (replaceFrom.length() < replaceTo.length()) {
      LocationTracker locationTracker = cloneTracker();
      for (int i = endColumn; i < endColumn + replaceTo.length() - replaceFrom.length(); i++) {
        locationTracker.add(line, i, -1);
      }
      return locationTracker;
    }
    // if we replace the token with a shorter token and need to subtract from the locationTracker
    else {
      LocationTracker locationTracker = cloneTracker();
      for (int j = endColumn - replaceFrom.length() + replaceTo.length(); j < endColumn; j++) {
        locationTracker.remove(line, startColumn + replaceTo.length());
      }
      return locationTracker;
    }
  }

  /**
   * This method produces a deep copy of the LocationTracker instance, thereby allowing a new
   * instance to be passed during the traversal of the tree
   */
  public LocationTracker cloneTracker() {
    LocationTracker locationTracker = new LocationTracker();
    for (int i = 0; i < location.size(); i++) {
      locationTracker.addLine();
      ArrayList<Integer> lineOriginal = location.get(i);
      for (int j = 0; j < lineOriginal.size(); j++) {
        locationTracker.add(i + 1, lineOriginal.get(j));
      }
    }
    return locationTracker;
  }


}
