package com.google.cloud.bigquery.utils.queryfixer.entity;

import lombok.Value;

/** A view that represents a part of a string. */
@Value(staticConstructor = "of")
public class StringView {
  String source;
  int start;
  // End of a substring (excluded).
  int end;

  @Override
  public String toString() {
    return source.substring(start, end);
  }
}
