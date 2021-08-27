package data;

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableTest {

  @Test
  public void test_addColumn() {
    Table t = new Table("t", 1);
    t.addColumn("Test", DataType.STR);
    assertEquals(new MutablePair("Test", DataType.STR), t.getSchema().get(0));
  }

  @Test
  public void test_getRandomColumn() {
    Table t = new Table("t", 1);
    assertEquals("Test", t.getRandomColumn("Test", DataType.STR));
  }

  @Test
  public void test_generateColumn() {
    Table t = new Table("t", 1);
    List<?> integerData = t.generateColumn(1, DataType.INTEGER);
    assertEquals(integerData.get(0).getClass(), Integer.class);
  }

  @Test
  public void test_generateData() {
    Table t = new Table("t", 1);
    t.addColumn("Test", DataType.INTEGER);
    List<List<?>> data = t.generateData(1);
    assertEquals(data.get(0).get(0).getClass(), Integer.class);
  }

}
