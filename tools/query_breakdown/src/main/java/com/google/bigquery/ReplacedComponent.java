package com.google.bigquery;

/**
 * This class captures the replaced query, original component, and what it is replaced with
 */
public class ReplacedComponent {
  private final String query;
  private final String original;
  private final String replacement;

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

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!ReplacedComponent.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    ReplacedComponent rc = (ReplacedComponent) obj;
    if (rc.getQuery().equals(query) && rc.getOriginal().equals(original)
        && rc.getReplacement().equals(replacement)) {
      return true;
    }
    else {
      return false;
    }
  }
}
