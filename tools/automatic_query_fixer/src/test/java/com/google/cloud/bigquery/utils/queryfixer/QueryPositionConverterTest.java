package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryPositionConverterTest {

  @Test
  public void verifyConversionBetweenIndexAndPosition() {
    String query = "Select col FROM\ntable WHERE a = 10";
    verifyIndexAndPosition(query);

    query = "Select * from\n\ntable WHERE a > \n023 GROUP\nBY col1, col2, LIMIT\n10\n\n\n\n";
    verifyIndexAndPosition(query);
  }

  private void verifyIndexAndPosition(String query) {
    QueryPositionConverter converter = new QueryPositionConverter(query);

    int row = 1, col = 1;
    for (int index = 0; index < query.length(); index++) {
      if (query.charAt(index) == '\n') {
        row++;
        col = 1;
        continue;
      }
      assertEquals(index, converter.posToIndex(row, col));
      assertEquals(new Position(row, col), converter.indexToPos(index));
      col++;
    }
  }
}
