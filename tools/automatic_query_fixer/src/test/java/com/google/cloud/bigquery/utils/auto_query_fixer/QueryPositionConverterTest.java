package com.google.cloud.bigquery.utils.auto_query_fixer;

import com.google.cloud.bigquery.utils.auto_query_fixer.entity.Position;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryPositionConverterTest {

  @Test
  public void posToIndexTest() {
    String sql = "Select col FROM\n" +
        "table WHERE a = 10";
    QueryPositionConverter converter = new QueryPositionConverter(sql);
    assertEquals(0, converter.posToIndex(1, 1));
    assertEquals(6, converter.posToIndex(1, 7));
    assertEquals(16, converter.posToIndex(2, 1));
    assertEquals(22, converter.posToIndex(2, 7));
  }

  @Test
  public void indexToPosTest() {
    String sql = "Select col FROM\n" +
        "table WHERE a = 10";
    QueryPositionConverter converter = new QueryPositionConverter(sql);
    assertEquals(new Position(1, 1), converter.indexToPos(0));
    assertEquals(new Position(1, 7), converter.indexToPos(6));
    assertEquals(new Position(2, 1), converter.indexToPos(16));
    assertEquals(new Position(2, 7), converter.indexToPos(22));
  }
}
