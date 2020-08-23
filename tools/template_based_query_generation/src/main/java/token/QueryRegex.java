package token;

import parser.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryRegex {

  private List<String> flattenedQueries = new ArrayList<>();

  // for every feature in the input list of features, map each feature to its raw query
  public QueryRegex(List<String> featureRegexList, int maxOptionalExpressions) {
    if (maxOptionalExpressions < 1) {
      // throw error
    }

    for (String featureRegex : featureRegexList) {
      String currentString = featureRegex;
      StringBuilder sb = new StringBuilder();
      while (currentString.contains("[") || currentString.contains("{") || currentString.contains("|")) {
        int i = 0;
        while (i < currentString.length()) {
          if (currentString.charAt(i) == '[') {
            int start = i;
            int matchingBracketCount = 1;

            // find the index of the matching closing bracket
            while (matchingBracketCount > 0) {
              i++;
              if (currentString.charAt(i) == '[') {
                matchingBracketCount++;
              } else if (currentString.charAt(i) == ']') {
                matchingBracketCount--;
              }
            }
            String optionalExpression = currentString.substring(start + 1, i);
            int randomExpressionCount = Utils.getRandomInteger(maxOptionalExpressions);
            for (int j = 0; j < randomExpressionCount; j++) {
              sb.append(optionalExpression);
            }
          } else if (currentString.charAt(i) == '{') {
            int start = i;
            int matchingBraceCount = 1;

            // find the index of the matching closing brace
            while (matchingBraceCount > 0) {
              i++;
              if (currentString.charAt(i) == '{') {
                matchingBraceCount++;
              } else if (currentString.charAt(i) == '}') {
                matchingBraceCount--;
              }
            }
            String choiceExpression = currentString.substring(start + 1, i);
            List<String> choices;
            if (choiceExpression.contains("|")) {
              choices = new ArrayList<>(Arrays.asList(choiceExpression.split("\\|", -1)));
            } else {
              choices = new ArrayList<>();
              choices.add(choiceExpression);
            }
            int choiceIndex = Utils.getRandomInteger(choices.size() - 1);
            String choice = choices.get(choiceIndex);
            sb.append(choice);
          } else {
            sb.append(currentString.charAt(i));
          }

          i++;
        }

        currentString = sb.toString();
        sb = new StringBuilder();
      }

      flattenedQueries.add(currentString);
    }
  }

  public List<String> getFlattenedQueries() {
    return flattenedQueries;
  }
}
