package com.google.cloud.bigquery.utils.queryfixer.util;

import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.entity.StringView;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A helper class that provides static methods to extract substrings from a string based on regular
 * expression.
 */
@AllArgsConstructor
public class PatternMatcher {

  private static final String POSITION_REGEX = "\\[(.*?):(.*?)\\]";

  /**
   * Check if a string matches a regular expression
   *
   * @param source the string to match
   * @param regex regular expression
   * @return true if it is matched else false.
   */
  public static boolean isMatched(@NonNull String source, @NonNull String regex) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(source);
    return matcher.find();
  }

  /**
   * Extract substrings from a string based on regular expression. Only the grouped items will be
   * extracted. A group item is enclosed by a pair of parentheses `()`. The sequence of the
   * extracted substrings are determined by the position of their left parenthesis. For more
   * details, please read {@link Pattern}. If a pattern is not matched in the input string, a null
   * pointer will be returned.
   *
   * @param source the string to match
   * @param regex regular expression
   * @return a list of extracted substrings or null pointer.
   */
  public static List<String> extract(@NonNull String source, @NonNull String regex) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(source);

    if (!matcher.find()) {
      return null;
    }

    List<String> contents = new ArrayList<>();
    for (int i = 1; i <= matcher.groupCount(); i++) {
      contents.add(matcher.group(i));
    }

    return contents;
  }

  /**
   * Find all the substrings that matches a regex in a string.
   *
   * @param source string to be matched.
   * @param regex regex to match substring
   * @return A list of {@link StringView}
   */
  public static List<StringView> findAllSubstrings(
      @NonNull String source, @NonNull String regex) {

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(source);

    List<StringView> stringViews = new ArrayList<>();
    while (matcher.find()) {
      stringViews.add(StringView.of(source, matcher.start(), matcher.end()));
    }

    return stringViews;
  }

  /**
   * Extract the position information from a string like [x:y], where x and y are row and column
   * numbers.
   *
   * @param posStr a string like [x:y]
   * @return Position represented by the string
   */
  public static Position extractPosition(String posStr) {
    List<String> contents = PatternMatcher.extract(posStr, POSITION_REGEX);
    if (contents == null) {
      return null;
    }
    int rowNum = Integer.parseInt(contents.get(0));
    int colNum = Integer.parseInt(contents.get(1));
    return new Position(rowNum, colNum);
  }
}
