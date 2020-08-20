package com.google.bigquery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import javax.xml.stream.Location;


/**
 * This class will take care of the input handling logic, essentially parsing the input document
 * into queries and data-cleaning if needed.
 */
public class InputReader {

  private List<String> queries;
  private List<LocationTracker> locationTrackers;

  /**
   * Constructor for the class. The constructor will take in a txt file name, use BufferedReader to
   * parse the input, and return all the queries split into a string array format.
   * We also initialize LocationTracker instances since this is where we are processing the input.
   * Finally, we keep track of the starting position of each individual query to keep track of the
   * original location.
   */
  public InputReader(String filename) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    StringBuilder sb = new StringBuilder();
    LocationTracker locationTracker = new LocationTracker();

    // local state for input reading
    int current = reader.read();
    int line = 1;
    int column = 1;

    // for the first line
    if (current != -1) {
      locationTracker.addLine();
    }

    // loop for input reading
    while (current != -1) {
      sb.append((char) current);

      // break down query using semicolon
      if ((char) current == ';') {
        queries.add(sb.toString());
        sb = new StringBuilder();
        locationTrackers.add(locationTracker);
        locationTracker = new LocationTracker();
        locationTracker.addLine();
      }
      
      // line changes
      if ((char) current == '\n') {
        locationTracker.add(line, column);
        column = 1;
        line++;
        locationTracker.addLine();
      }
      else {
        locationTracker.add(line, column);
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
  public void readFromString(String input, LocationTracker locationTracker) {
    // local state for input reading
    int current = 0;
    int line = 1;
    int column = 1;

    // empty string
    if (input == null || input.length() == 0) {
      return;
    }

    // for the first line
    locationTracker.addLine();

    // loop for input reading
    while (current < input.length()) {
      // line changes
      if (input.charAt(current) == '\n') {
        locationTracker.add(line, column);
        column = 1;
        line++;
        locationTracker.addLine();
      }
      else {
        locationTracker.add(line, column);
        column++;
      }

      // advance current pointer
      current++;
    }
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
}
