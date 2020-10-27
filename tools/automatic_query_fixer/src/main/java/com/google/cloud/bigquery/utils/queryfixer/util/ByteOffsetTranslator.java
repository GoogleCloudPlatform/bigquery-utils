package com.google.cloud.bigquery.utils.queryfixer.util;

import com.google.bigquery.utils.zetasqlhelper.QueryLocationRange;
import com.google.cloud.bigquery.utils.queryfixer.entity.StringView;
import lombok.Getter;

import java.util.Arrays;

/**
 * A utility class that can convert a String index to the corresponding Byte Index, and vice versa.
 *
 * <p>For example, We have a String "a哈bc" encoded in UTF-8. The second character (a unicode
 * character) will take 3 bytes, and rest of the alphabets take one byte each. Therefore, the offset
 * array looks like [0, 1, 4, 5, 6]. The last offset represents the byte length of this String.
 */
@Getter
public class ByteOffsetTranslator {
  private final String query;
  private final int[] offsets;

  private ByteOffsetTranslator(String query) {
    this.query = query;
    offsets = new int[query.length() + 1];
    for (int i = 0; i < query.length(); i++) {
      int byteLength = query.substring(i, i + 1).getBytes().length;
      offsets[i + 1] = offsets[i] + byteLength;
    }
  }

  /**
   * Static constructor
   *
   * @param query a query where the conversions are operated on
   * @return new instance of ByteOffsetTranslator
   */
  public static ByteOffsetTranslator of(String query) {
    return new ByteOffsetTranslator(query);
  }

  public int offsetToIndex(int offset) {
    if (offset > offsets[offsets.length - 1]) {
      return -1;
    }

    int index = Arrays.binarySearch(offsets, offset);
    if (index >= 0) {
      return index;
    }
    // if index is negative, it equals (-insertion_point - 1).
    // Thus, insertion_point = -index - 1
    return -index - 1;
  }

  /**
   * Convert a String index to the corresponding byte offset
   *
   * @param index String index to convert
   * @return corresponding byte offset
   */
  public int indexToOffset(int index) {
    if (index >= offsets.length) {
      return -1;
    }

    return offsets[index];
  }

  /**
   * Convert a String byte offset to the corresponding index
   *
   * @param range byte offset to convert
   * @return String index
   */
  public StringView toStringView(QueryLocationRange range) {
    int start = offsetToIndex(range.getStartByteOffset());
    int end = offsetToIndex(range.getEndByteOffset());
    return StringView.of(query, start, end);
  }
}
