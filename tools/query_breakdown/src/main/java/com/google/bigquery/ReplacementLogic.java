package com.google.bigquery;

import java.util.ArrayList;

/**
 * This class implements all the replacement logic.
 */
public class ReplacementLogic {

  /**
   * Given a component, provides a recommendation as to which component the input should be
   * replaced with. Returns a list of components that it recommends.
   *
   * We choose ArrayLists as the data structure because we want to impose a certain ordering
   * with the recommendations: certain recommendations should be "better" than others
   */
  public static ArrayList<String> replace (String component, ArrayList<String> options) {
    // simply returns the first n options
    int n = 3;

    if (options.size() <= n) {
      return new ArrayList<String>(options);
    }

    ArrayList<String> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      result.add(options.get(i));
    }
    return result;
  }
}
