package com.google.bigquery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/**
 * This class implements all the replacement logic.
 */
public class ReplacementLogic {

  /**
   * Given a component, provides a recommendation as to which component the input should be
   * replaced with. Returns a list of components that it recommends. The default method of
   * choosing recommendations is through randomization.
   *
   * We choose ArrayLists as the data structure because we want to impose a certain ordering
   * with the recommendations: certain recommendations should be "better" than others
   *
   * n is an integer that controls the number of replacement options
   */
  public static ArrayList<String> replace (String component, ArrayList<String> options) {
    int n = 3;

    ArrayList<String> result = new ArrayList<>();

    if (options.size() <= n) {
      for (int i = 0; i < options.size(); i++) {
        // gets rid of cases such as <QUOTED_STRING>
        if (options.get(i).charAt(0) != '<' || options.get(i).length() <= 1) {
          result.add(options.get(i));
        }
      }
      return result;
    }

    HashSet<Integer> seen = new HashSet<>();
    // randomly populate result until full
    while (result.size() < n && seen.size() < options.size()) {
      Random randomGenerator = new Random();
      int random = randomGenerator.nextInt(n);
      if (seen.contains(random)) {
        continue;
      }
      else if (options.get(random).charAt(0) == '<' && options.get(random).length() > 1) {
        seen.add(random);
        continue;
      }
      else {
        result.add(options.get(random));
        seen.add(random);
      }
    }
    return result;
  }
}
