package com.google.bigquery;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


/**
 * This class will take care of the input handling logic, essentially parsing the input document
 * into queries and data-cleaning if needed.
 */
public class InputReader {

  private LocationTracker locationTracker;

  /**
   * Constructor for the class
   */
  public InputReader() {
    locationTracker = new LocationTracker();
  }

  /**
   * This method will take in a txt file name, use BufferedReader to parse the input, and return
   * all the queries in a string format. We also initialize a LocationTracker instance since
   * this is where we are processing the input.
   *
   * TODO: more robust method for input parsing needed (ex: semicolons in strings, comments)
   */
  public String readInput(String filename) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    StringBuilder sb = new StringBuilder();

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

      // line changes
      if ((char) current == '\n') {
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

    reader.close();
    return sb.toString();
  }

  /**
   * Method created to initialize a LocationTracker instance from an input string for testing
   */
  public void readFromString(String input) {
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
   * Getter method for LocationTracker of the input
   */
  public LocationTracker getLocationTracker() {
    return locationTracker;
  }
}
