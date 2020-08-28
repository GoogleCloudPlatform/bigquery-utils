package com.google.bigquery;

/**
 * This class captures the replaced query, original component, and what it is replaced with to
 * pass information across different functions in QueryBreakdown
 */
public class ReplacedComponent {
  private final String query;
  private final String original;
  private final String replacement;

  // constructor
  public ReplacedComponent(String query, String original, String replacement) {
    this.query = query;
    this.original = original;
    this.replacement = replacement;
  }

  // getter methods
  public String getQuery() {
    return query;
  }

  public String getOriginal() {
    return original;
  }

  public String getReplacement() {
    return replacement;
  }

  /* override equals method for better equality check between ReplacedComponent
     objects and testing/debugging
   */
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
