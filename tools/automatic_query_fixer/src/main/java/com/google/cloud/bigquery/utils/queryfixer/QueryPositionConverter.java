package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/** A class used to convert between index and position (row and column) of a query. */
@Getter
public class QueryPositionConverter {

  /** The query used to convert indices and positions. */
  private final String query;

  /** The index in the query corresponding to the starting position of each line. */
  private List<Integer> startIndices;

  public QueryPositionConverter(String query) {
    this.query = query;
    fillInStartIndices(query);
  }

  private void fillInStartIndices(String query) {
    startIndices = new ArrayList<>();
    startIndices.add(0);
    for (int i = 0; i < query.length() - 1; i++) {
      if (query.charAt(i) == '\n') {
        startIndices.add(i + 1);
      }
    }
  }

  /**
   * Convert row and column number to an index.
   *
   * @param row row number
   * @param col col number
   * @return index
   */
  public int posToIndex(int row, int col) {
    if (row > startIndices.size()) {
      return -1;
    }

    // row and col are 1-index, but the String is 0-index
    // therefore -1 is needed for conversion.
    int index = startIndices.get(row - 1) + col - 1;

    int nextLineStartIndex;
    if (row == startIndices.size()) {
      nextLineStartIndex = query.length();
    } else {
      nextLineStartIndex = startIndices.get(row);
    }
    if (index > nextLineStartIndex) {
      return -1;
    }
    return index;
  }

  /**
   * Convert index to position.
   *
   * @param index index to be converted
   * @return position
   */
  public Position indexToPos(int index) {
    if (index < 0 || index >= query.length()) {
      return Position.invalid();
    }
    int line = findStartLine(index);
    return new Position(line, index - startIndices.get(line - 1) + 1);
  }

  // Use the binary search on startIndices to find the greatest start index
  // that is less than or equals to index. The return line should be 1-index.
  private int findStartLine(int index) {
    int left = 0, right = startIndices.size() - 1;
    while (left < right) {
      // in case left + right overflow.
      int mid = left + (right - left + 1) / 2;
      if (startIndices.get(mid) == index) {
        // +1 is to convert the 0-based index to 1-based.
        return mid + 1;
      } else if (startIndices.get(mid) < index) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }
    // +1 is to convert the 0-based index to 1-based.
    return left + 1;
  }
}
