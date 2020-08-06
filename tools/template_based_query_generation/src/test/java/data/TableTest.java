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

  }

}
