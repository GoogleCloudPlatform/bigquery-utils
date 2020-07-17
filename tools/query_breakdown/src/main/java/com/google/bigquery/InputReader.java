package com.google.bigquery;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * This class will take care of the input handling logic, essentially parsing the input document
 * into queries and data-cleaning if needed.
 */
public class InputReader {

  /**
   * This method will take in a txt file name, use BufferedReader to parse the input, and return
   * all the queries in a string format
   */
  public static String readInput(String filename) throws IOException {
    BufferedReader reader = null;
    String currentLine = "";
    try {
      reader = new BufferedReader(new FileReader((filename)));
    } catch (FileNotFoundException e) {
      System.out.println("A file of this name is not found");
    }

    StringBuilder sb = new StringBuilder();

    while (currentLine != null) {
      sb.append(currentLine);
      currentLine = reader.readLine();
    }

    reader.close();

    String parsedInput = sb.toString();
    int len = parsedInput.length();
    boolean lastSemicolon = false;

    // deals with case where last query ends with a semicolon
    if (parsedInput.charAt(len - 1) == ';') {
      parsedInput = parsedInput.substring(0, len - 1);
      lastSemicolon = true;
    }

    parsedInput = parsedInput.replaceAll(";", ";\n");
    if (lastSemicolon) {
      parsedInput = parsedInput + ';';
    }

    return parsedInput;
  }
}
