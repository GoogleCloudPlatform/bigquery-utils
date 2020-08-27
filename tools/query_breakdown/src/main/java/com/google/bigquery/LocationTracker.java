package com.google.bigquery;

import java.util.ArrayList;

/**
 * This class tracks the original location of components in the query, thereby making sure that
 * the error locations are correctly represented. For each pair of (line, column) in the original
 * query, it is initialized in position at the (line - 1)th arraylist and
 * (column - 1)th element of the arraylist as the integer column - 1. The line and column number
 * will change throughout the course of the tool, so we store the original pair location object
 * in the arraylist.
 */
public class LocationTracker {
  /* we keep a double arraylist to represent the position of each character (line and column).
     We can do this as the line number of the component will not change
     (deletion and replacement won't change the line numbers)
   */
  private ArrayList<ArrayList<Pair>> location;

  /**
   * Constructor for the class
   */
  public LocationTracker() {
    location = new ArrayList<>();
  }

  /**
   * Getter method for location field
   */
  public ArrayList<ArrayList<Pair>> getLocation() {
    return location;
  }

  /**
   * This method interacts with the InputReader and adds a pair to the line-1th list in the
   * location field. The pair represents the position (x, y) in the original query.
   * x and y are 1-indexed, so we adjust accordingly.
   */
  public void add(int line, int x, int y) {
    location.get(line - 1).add(new Pair(x, y));
  }

  /**
   * This method adds an empty line to the location field
   */
  public void addLine() {
    location.add(new ArrayList<>());
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
   * This method removes the specified line from location
   */
  public void removeLine(int lineNumber) {
    if (lineNumber < 0 ||  lineNumber > location.size()) {
      return;
    }
    location.remove(lineNumber - 1);
  }

  /**
   * This method gets the original position of the component in (x,y) of the intermediate query
   */
  public Pair getOriginalPosition(int x, int y) {
    return location.get(x - 1).get(y - 1);
  }

  /**
   * This method ensures that the location field is kept correctly despite the deletion. We do
   * this by removing the entry in location that corresponds to the deleted characters from
   * position. We also make sure that we return a new location tracker instance (by making
   * a deep copy) such that a new copy is passed to further runs of the tool.
   */
  public LocationTracker delete(int startLine, int startColumn, int endLine,
      int endColumn) {
    LocationTracker locationTracker = cloneTracker();
    if (startLine == endLine) {
      for (int i = startColumn; i < endColumn + 1; i++) {
        locationTracker.remove(startLine, startColumn);
      }
    }
    else {
      /* different line case */
      // startLine
      for (int x = startColumn; x < location.get(startLine - 1).size(); x++) {
        locationTracker.remove(startLine, startColumn);
      }

      // endLine
      if (endColumn == locationTracker.getLocation().get(endLine - 1).size()) {
        locationTracker.removeLine(endLine);
      }
      else {
        for (int y = 1; y < endColumn + 1; y++) {
          locationTracker.remove(endLine, 1);
        }
      }

      // lines in the middle
      if (endLine - startLine > 1) {
        for (int z = startLine + 1; z < endLine; z++) {
          locationTracker.removeLine(z);
        }
      }
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
        locationTracker.add(line, -1, i);
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
      ArrayList<Pair> lineOriginal = location.get(i);
      for (int j = 0; j < lineOriginal.size(); j++) {
        locationTracker.add(i + 1, lineOriginal.get(j).getX(), lineOriginal.get(j).getY());
      }
    }
    return locationTracker;
  }
}
