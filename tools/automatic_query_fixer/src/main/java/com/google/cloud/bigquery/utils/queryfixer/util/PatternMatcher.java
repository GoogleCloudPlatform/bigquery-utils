package com.google.cloud.bigquery.utils.queryfixer.util;

import com.google.cloud.bigquery.utils.queryfixer.entity.SubstringView;
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
   * @return A list of {@link SubstringView}
   */
  public static List<SubstringView> findAllSubstrings(
      @NonNull String source, @NonNull String regex) {

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(source);

    List<SubstringView> substringViews = new ArrayList<>();
    while (matcher.find()) {
      substringViews.add(SubstringView.of(source, matcher.start(), matcher.end()));
    }

    return substringViews;
  }
}
