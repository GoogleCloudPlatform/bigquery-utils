package data;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableTest {

  @Test
  public void test_getName() {
    Table t = new Table("table 1");
    Table t2 = new Table("a second table");
    assertEquals("table 1", t.getName());
    assertEquals("a second table", t2.getName());
  }

  @Test
  public void test_setName() {
    Table t = new Table("t");
    t.setName("a new name");
    assertEquals("a new name", t.getName());
  }

  @Test
  public void test_addColumn() {
    Table t = new Table("t");
    t.addColumn("column 1", DataType.BOOL);
    t.addColumn("column 2", DataType.REAL);
    t.addColumn("times", DataType.TIME);
    t.addColumn("hello world", DataType.STR);
    HashMap<String, DataType> schema = t.getSchema();
    assertEquals(4, schema.size());
    assertEquals(DataType.BOOL, schema.get("column 1"));
    assertEquals(DataType.REAL, schema.get("column 2"));
    assertEquals(DataType.TIME, schema.get("times"));
    assertEquals(DataType.STR, schema.get("hello world"));
    assertEquals(null, schema.get("hello"));
  }

}
