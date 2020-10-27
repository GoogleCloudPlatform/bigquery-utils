package com.google.bigquery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * This class will take care of the input handling logic, essentially parsing the input document
 * into multiple queries via semicolon and data-cleaning if needed.
 */
public class InputReader {

  // separates the queries and instantiates a new location tracker per each query
  private final List<String> queries;
  private final List<LocationTracker> locationTrackers;
  private int docLength;

  /**
   * Constructor for the class. The constructor will take in a txt file name, use BufferedReader to
   * parse the input, and return all the queries split into a string array format.
   * We also initialize a separate LocationTracker instance per query to keep track of the
   * original location of the components. Finally, we keep the length of the document for 
   * performance calculation. 
   */
  public InputReader(String filename) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    StringBuilder sb = new StringBuilder();
    queries = new ArrayList<>();
    locationTrackers = new ArrayList<>();
    LocationTracker locationTracker = new LocationTracker();
    docLength = 0;

    // local state for input reading
    int current = reader.read();

    // absolute position in the original document
    int line = 1;
    int column = 1;

    // local position within the query
    int localLine = 1;

    // for the first line
    if (current != -1) {
      locationTracker.addLine();
    }

    // loop for input reading
    while (current != -1) {
      docLength++;
      sb.append((char) current);

      // break down query using semicolon
      if ((char) current == ';') {
        queries.add(sb.substring(0, sb.length() - 1));
        sb = new StringBuilder();
        locationTrackers.add(locationTracker);
        locationTracker = new LocationTracker();
        locationTracker.addLine();
        column++;
        localLine = 1;
      }

      // line changes
      else if ((char) current == '\n') {
        locationTracker.add(localLine, line, column);
        column = 1;
        line++;
        localLine++;
        locationTracker.addLine();
      }
      else {
        locationTracker.add(localLine, line, column);
        column++;
      }

      // advance current pointer
      current = reader.read();
    }

    // deals with case where a single query or the last query don't have semicolons
    if (sb.length() != 0) {
      queries.add(sb.toString());
      locationTrackers.add(locationTracker);
    }

    reader.close();
  }

  /**
   * Method created to initialize a LocationTracker instance from an input string for testing
   */
  public static List<LocationTracker> readFromString(String input) {
    List<LocationTracker> trackers = new ArrayList<>();
    LocationTracker locationTracker = new LocationTracker();

    // local state for input reading
    int current = 0;
    int line = 1;
    int column = 1;
    int localLine = 1;
    boolean lastline = false;

    // empty string
    if (input == null || input.length() == 0) {
      return null;
    }

    // for the first line
    locationTracker.addLine();

    while (current < input.length()) {
      lastline = false;
      // break down query using semicolon
      if (input.charAt(current) == ';') {
        locationTracker.add(localLine, line, column);
        trackers.add(locationTracker);
        locationTracker = new LocationTracker();
        locationTracker.addLine();
        column++;
        localLine = 1;
        lastline = true;
      }

      // line changes
      else if (input.charAt(current) == '\n') {
        locationTracker.add(localLine, line, column);
        column = 1;
        line++;
        localLine++;
        locationTracker.addLine();
      }
      else {
        locationTracker.add(localLine, line, column);
        column++;
      }

      // advance current pointer
      current++;
    }

    // deals with case where a single query or the last query don't have semicolons
    if (!lastline) {
      trackers.add(locationTracker);
    }

    return trackers;
  }

  /**
   * Getter method for the array of queries
   */
  public List<String> getQueries() {
    return queries;
  }

  /**
   * Getter method for the array of location trackers
   */
  public List<LocationTracker> getLocationTrackers() {
    return locationTrackers;
  }

  /**
   * Getter method for the length of the input document
   */
  public int getDocLength() {
    return docLength;
  }
}
