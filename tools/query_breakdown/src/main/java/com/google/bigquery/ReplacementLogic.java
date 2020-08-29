package com.google.bigquery;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * This class contains the logic the tool will use to determine replacements
 */
public class ReplacementLogic {

  /**
   * Given a component, provides a recommendation as to which component the input should be
   * replaced with. Returns a list of components that it recommends. The default method of
   * choosing recommendations is through randomization.
   *
   * We choose ArrayLists as the data structure because we want to potentially impose a
   * certain ordering with the recommendations: certain recommendations could be "better" than
   * others
   *
   * replacementLimit is an integer that controls the number of replacement options
   */
  public static ArrayList<String> replace (String component, int replacementLimit,
      ArrayList<String> options) {
    ArrayList<String> result = new ArrayList<>();

    /*
     * The component field in the input arguments of this function denotes the component that the
     * tool replaces from. Therefore, we use that information to determine good replacements if any.
     * For instance, in the example code below, we deem that A AS is a good replacement for AS in
     * cases where we have a AS just immediately following the previous WITH:
     *
     * if (component.equals("AS")) { result.add("A AS"); return result; }
     */

    // returns the filtered list if the number of possible replacements is smaller than the limit
    if (options.size() <= replacementLimit) {
      for (int i = 0; i < options.size(); i++) {
        // gets rid of cases such as <QUOTED_STRING>
        if (options.get(i).charAt(0) != '<' || options.get(i).length() <= 1) {
          result.add(options.get(i));
        }
      }
      return result;
    }

    // field to make sure we don't have duplicates in the result
    HashSet<Integer> seen = new HashSet<>();

    // randomly populate result until full
    while (result.size() < replacementLimit && seen.size() < options.size()) {
      int random = randomNumber(options.size() - 1);
      if (seen.contains(random)) {
        continue;
      }
      else if (options.get(random).charAt(0) == '<' && options.get(random).length() > 1) {
        seen.add(random);
      }
      else {
        result.add(options.get(random));
        seen.add(random);
      }
    }
    return result;
  }

  // helper function that returns a random integer between 0 - n
  private static int randomNumber(int n) {
    return (int) (Math.random() * (n + 1));
  }
}
