package token;

import parser.Utils;
import query.SkeletonPiece;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class QueryRegex {

  private List<List<SkeletonPiece>> querySkeletons = new ArrayList<>();
  private int maxOptionalExpressions;
  private Random r = new Random();
  private TokenProvider tokenProvider = new TokenProvider(r);

  // for every feature in the input list of features, map each feature to its raw query
  public QueryRegex(List<String> featureRegexList, int maxOptionalExpressions) throws IOException {
    if (maxOptionalExpressions < 1) {
      // throw error
    }
    this.maxOptionalExpressions = maxOptionalExpressions - 1;

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
            int randomExpressionCount = Utils.getRandomInteger(this.maxOptionalExpressions);
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
      List<String> rawQuery = new ArrayList<>(Arrays.asList(currentString.split(" ")));
      List<SkeletonPiece> querySkeleton = new ArrayList<>();
      for (String querySlice : rawQuery) {
        if (querySlice.charAt(0) == '<') {
          // slice is an expression
          if (querySlice.contains(",")) {
            List<String> optionals = new ArrayList<>(Arrays.asList(querySlice.split(",")));
            for (String optional : optionals) {
              String tokenExpression = optional.substring(1, optional.length() - 1);
              querySkeleton.add(tokenProvider.tokenize(tokenExpression));
            }
          } else {
            String tokenExpression = querySlice.substring(1, querySlice.length() - 1);
            querySkeleton.add(tokenProvider.tokenize(tokenExpression));
          }
        } else {
          // otherwise the slice is a feature
          SkeletonPiece sp = new SkeletonPiece();
          sp.setKeyword(querySlice);
          querySkeleton.add(sp);
        }
      }
      querySkeletons.add(querySkeleton);
    }
  }

  /**
   *
   * @return the list of skeleton pieces to be translated by the query generator
   */
  public List<List<SkeletonPiece>> getQuerySkeletons() {
    return querySkeletons;
  }

  /**
   *
   * @return the token provider used to tokenize the skeleton pieces
   */
  public TokenProvider getTokenProvider() {
    return tokenProvider;
  }
}
