package com.google.bigquery;

import java.util.ArrayList;

/**
 * This class tracks the original location of components in the query, thereby making sure that
 * the error locations are correctly represented. For each pair of (line, column) in the original
 * query, it is initialized in position at the (line - 1)th arraylist and
 * (column - 1)th element of the arraylist as the pair (line, column). The line and column number
 * will change throughout the course of the tool, so we store the original pair location object
 * in the arraylist.
 */
public class LocationTracker {
  /* we keep a double arraylist to represent the original position of each character
  (line and column) in the queries (intermediate and final).
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
   */
  public void add(int line, int x, int y) {
    location.get(line - 1).add(new Pair(x, y));
  }

  /**
   * This method interacts with the InputReader and adds a pair to the column - 1th element of the
   * line - 1th list in the location field. The pair represents the position (x, y) in the original
   * query. line and column are 1-indexed, so we adjust accordingly.
   */
  public void add(int line, int column, int x, int y) {
    location.get(line - 1).add(column - 1, new Pair(x, y));
  }

  /**
   * This method adds an empty line to the location field
   */
  public void addLine() {
    location.add(new ArrayList<>());
  }

  /**
   * This method removes an entry from location specified by (x, y) in the original query. x and
   * y are 1-indexed, so we adjust accordingly.
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
   * This method gets the original position of the component in (x,y) of the query. x and y are
   * 1-indexed, so we adjust accordingly.
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
    // same line case, just remove the corresponding locations
    if (startLine == endLine) {
      for (int i = startColumn; i < endColumn + 1; i++) {
        locationTracker.remove(startLine, startColumn);
      }
    }
    else {
      /* different line case */
      // startLine: note that we add a new line character to the startline in multi-line deletion
      for (int x = startColumn; x < location.get(startLine - 1).size(); x++) {
        locationTracker.remove(startLine, startColumn);
      }

      // endLine: if entire endline is removed vs part of it is removed
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
  public LocationTracker replace(int startLine, int startColumn, int endLine,
      int endColumn, String replaceFrom, String replaceTo) {
    // same line, same length replacement
    if (replaceFrom.length() == replaceTo.length() && startLine == endLine) {
      return this;

    }
    LocationTracker locationTracker = cloneTracker();

    // position of last character of replaceFrom
    int end = (startLine == endLine) ? endColumn : location.get(startLine - 1).size();

    // how much the replaceTo is longer by
    int longer = (startLine == endLine) ? replaceTo.length() - replaceFrom.length() :
        replaceTo.length() - (end - startColumn);

    // how much the replaceTo is shorter by
    int shorter = (startLine == endLine) ? replaceFrom.length() - replaceTo.length() :
        end - startColumn - replaceTo.length();

    // if we replace the token with a longer token and need to add to the locationTracker
    if (longer > 0) {
      for (int i = end + 1; i <= end + longer; i++) {
        /* adding letters that are not in the original document, but still need to have them
           appear in the original document in the frontend as well as cli*/
        locationTracker.add(startLine, i, location.get(startLine - 1).get(end - 1).getX(), i);
      }
    }
    // if we replace the token with a shorter token and need to subtract from the locationTracker
    else {
      for (int j = end - shorter; j < end; j++) {
        locationTracker.remove(startLine, startColumn + replaceTo.length());
      }
    }

    /* multi-line replacement considerations (end line and middle line). Note that replace
       the component only in the startLine for simplicity.
     */
    if (startLine != endLine) {
      if (endColumn == locationTracker.getLocation().get(endLine - 1).size()) {
        locationTracker.removeLine(endLine);
      }
      else {
        for (int k = 1; k < endColumn + 1; k++) {
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
   * This method produces a deep copy of the LocationTracker instance, thereby allowing a new
   * instance to be passed around during the traversal of the tree
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
