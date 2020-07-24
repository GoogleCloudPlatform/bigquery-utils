package com.google.bigquery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


/**
 * This class will take care of the input handling logic, essentially parsing the input document
 * into queries and data-cleaning if needed.
 */
public class InputReader {

  /**
   * This method will take in a txt file name, use BufferedReader to parse the input, and return
   * all the queries in a string format
   *
   * TODO: more robust method for input parsing needed (ex: semicolons in strings, comments)
   */
  public static String readInput(String filename) throws IOException {
    List<String> lines = Files.readAllLines(Paths.get(filename));

    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line);
    }

    String parsedInput = sb.toString();

    parsedInput = parsedInput.replaceAll(";", ";\n");

    int len = parsedInput.length();

    // deals with case where last query ends with a semicolon
    if (parsedInput.charAt(len - 1) == '\n') {
      parsedInput = parsedInput.substring(0, len - 1);
    }

    return parsedInput;
  }
}
