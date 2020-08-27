package data;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableTest {

  @Test
  public void test_getName() {
    Table t = new Table("table 1", 10);
    Table t2 = new Table("a second table", 10);
    assertEquals("table 1", t.getName());
    assertEquals("a second table", t2.getName());
  }

  @Test
  public void test_setName() {
    Table t = new Table("t", 10);
    t.setName("a new name");
    assertEquals("a new name", t.getName());
  }

  @Test
  public void test_addColumn() {

  }

}
