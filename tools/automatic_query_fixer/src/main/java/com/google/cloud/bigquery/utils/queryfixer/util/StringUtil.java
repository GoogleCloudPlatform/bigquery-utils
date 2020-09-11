package com.google.cloud.bigquery.utils.queryfixer.util;

import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

/** A utility class to provide static helper methods regarding String. */
public class StringUtil {

  /**
   * Find word(s) from a dictionary that are most similar to a target word. The similarity is
   * measured by edit distance between two words.
   *
   * @param dict dictionary of words
   * @param target target word
   * @param caseSensitive whether considering case sensitive.
   * @return a list of Strings and their edit distance to the target.
   */
  public static SimilarStrings findMostSimilarWords(
      Collection<String> dict, String target, boolean caseSensitive) {
    List<Pair<Integer, String>> distanceWordPairs =
        dict.stream()
            .map(word -> Pair.of(editDistance(word, target, caseSensitive), word))
            .collect(Collectors.toList());

    if (distanceWordPairs.isEmpty()) {
      return SimilarStrings.empty();
    }

    Integer minDistance =
        distanceWordPairs.stream().min(Comparator.comparingInt(Pair::getLeft)).get().getLeft();

    List<String> words =
        distanceWordPairs.stream()
            .filter(pair -> (pair.getLeft().equals(minDistance)))
            .map(Pair::getRight)
            .collect(Collectors.toList());
    return new SimilarStrings(words, minDistance);
  }

  /**
   * Find all the similar words that are within a certain edit distance from the target string.
   *
   * @param dict a set of candidate words
   * @param target target string to compare with
   * @param maxEditDistance max edit distance to consider similarity
   * @param caseSensitive whether considering case sensitive.
   * @return a list of similar strings
   */
  public static List<String> findSimilarWords(
      @NonNull Collection<String> dict,
      @NonNull String target,
      int maxEditDistance,
      boolean caseSensitive) {
    List<Pair<Integer, String>> distanceWordPairs =
        dict.stream()
            .map(word -> Pair.of(editDistance(word, target, caseSensitive), word))
            .filter(pair -> pair.getLeft() <= maxEditDistance)
            .collect(Collectors.toList());

    if (distanceWordPairs.isEmpty()) {
      return Collections.emptyList();
    }

    return distanceWordPairs.stream().map(Pair::getRight).collect(Collectors.toList());
  }

  /**
   * Compute the edit distance between two strings.
   *
   * @param word1 a string.
   * @param word2 another string.
   * @param caseSensitive whether considering case sensitive.
   * @return the edit distance between word1 and word2.
   */
  public static int editDistance(String word1, String word2, boolean caseSensitive) {
    if (!caseSensitive) {
      word1 = word1.toLowerCase();
      word2 = word2.toLowerCase();
    }

    int len1 = word1.length();
    int len2 = word2.length();

    // len1+1, len2+1, because finally return dp[len1][len2]
    int[][] dp = new int[len1 + 1][len2 + 1];

    for (int i = 0; i <= len1; i++) {
      dp[i][0] = i;
    }

    for (int j = 0; j <= len2; j++) {
      dp[0][j] = j;
    }

    // iterate though, and check last char
    for (int i = 0; i < len1; i++) {
      char c1 = word1.charAt(i);
      for (int j = 0; j < len2; j++) {
        char c2 = word2.charAt(j);

        // if last two chars equal
        if (c1 == c2) {
          // update dp value for +1 length
          dp[i + 1][j + 1] = dp[i][j];
        } else {
          int replace = dp[i][j] + 1;
          int insert = Math.min(dp[i][j + 1], dp[i + 1][j]) + 1;
          dp[i + 1][j + 1] = Math.min(replace, insert);
        }
      }
    }

    return dp[len1][len2];
  }

  /**
   * Replace a substring of a string to a new one. the replacing range is replaced as [startIndex,
   * endIndex), * i.e. the endIndex is excluded.
   *
   * @param old string to be replaced
   * @param startIndex start index of replacement
   * @param endIndex exclusively end index of replacement
   * @param replacingPart the substring to replace
   * @return replaced string
   */
  public static String replaceStringBetweenIndex(
      String old, int startIndex, int endIndex, String replacingPart) {
    StringBuilder builder = new StringBuilder(old);
    builder.replace(startIndex, endIndex, replacingPart);
    return builder.toString();
  }

  @Value
  public static class SimilarStrings {
    List<String> strings;
    int distance;

    public boolean isEmpty() {
      return strings.isEmpty();
    }

    public static SimilarStrings empty() {
      return new SimilarStrings(new ArrayList<>(), -1);
    }
  }
}
