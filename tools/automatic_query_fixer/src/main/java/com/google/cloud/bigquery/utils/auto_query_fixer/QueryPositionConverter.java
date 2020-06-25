package com.google.cloud.bigquery.utils.auto_query_fixer;

import com.google.cloud.bigquery.utils.auto_query_fixer.entity.Position;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class QueryPositionConverter {

  private final String query;

  // the index in the query corresponding to the starting position of each line
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

  public int posToIndex(int line, int col) {
    if (line > startIndices.size()) {
      return -1;
    }

    // line and col are 1-index, but the String is 0-index
    // therefore -1 is needed for conversion.
    int index = startIndices.get(line - 1) + col - 1;

    int nextLineStartIndex;
    if (line == startIndices.size()) {
      nextLineStartIndex = query.length();
    } else {
      nextLineStartIndex = startIndices.get(line);
    }
    if (index > nextLineStartIndex) {
      return -1;
    }
    return index;
  }

  public Position indexToPos(int index) {
    if (index >= query.length()) {
      return Position.invalid();
    }
    int line = findStartLine(index);
    return new Position(line, index - startIndices.get(line - 1) + 1);
  }

  //use the binary search on startIndices to find the greatest start index
  // that is less or equal index.
  // the return line should be 1-index
  private int findStartLine(int index) {
    int left = 0, right = startIndices.size() - 1;
    while (left < right) {
      // in case left + right overflow.
      int mid = left + (right - left + 1) / 2;
      if (startIndices.get(mid) == index) {
        return mid + 1;
      } else if (startIndices.get(mid) < index) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }
    return left + 1;
  }


}
