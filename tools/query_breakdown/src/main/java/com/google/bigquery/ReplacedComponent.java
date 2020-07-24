package com.google.bigquery;

/**
 * This class captures the replaced query, original component, and what it is replaced with
 */
public class ReplacedComponent {
  private String query;
  private String original;
  private String replacement;

  public ReplacedComponent(String query, String original, String replacement) {
    this.query = query;
    this.original = original;
    this.replacement = replacement;
  }

  public String getQuery() {
    return query;
  }

  public String getOriginal() {
    return original;
  }

  public String getReplacement() {
    return replacement;
  }
}
