package com.google.cloud.bigquery.utils.queryfixer.entity;

import lombok.Value;

/** A view that represents a part of a string. */
@Value(staticConstructor = "of")
public class StringView {
  String source;
  int start;
  // End of a substring (excluded).
  int end;

  /**
   * Create a StringView from the start and end byte offsets of a query.
   * @param source query the view belongs to
   * @param startByteOffset start byte offset (included)
   * @param endByteOffset end byte offset (excluded)
   * @return a string view
   */
  public static StringView fromByteOffsets(String source, int startByteOffset, int endByteOffset) {
    byte[] bytes = source.getBytes();
    int startIndex = new String(bytes, 0, startByteOffset).length();
    int length = new String(bytes, startByteOffset, endByteOffset - startByteOffset).length();
    return StringView.of(source, startIndex, startIndex + length);
  }

  @Override
  public String toString() {
    return source.substring(start, end);
  }
}
