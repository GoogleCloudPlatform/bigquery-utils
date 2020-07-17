package parser;

import org.junit.jupiter.api.Test;
import parser.KeywordsMapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeywordsMappingTest {

  // TODO (spoiledhua): fix tests here to correspond with new changes to parser.KeywordsMapping

  @Test
  public void test_getMappingPostgreDDL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("CREATE", km.getMappingDDL("DDL_CREATE"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingDDL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingBigQueryDDL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("CREATE", km.getMappingDDL("DDL_CREATE"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingDDL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingPostgreDML() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("INSERT INTO", km.getMappingDML("DML_INSERT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingDML("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingBigQueryDML() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("INSERT", km.getMappingDML("DML_INSERT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingDML("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingPostgreDQL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("SELECT", km.getMappingDQL("DQL_SELECT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingDQL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingBigQueryDQL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("SELECT", km.getMappingDQL("DQL_SELECT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingDQL("NON KEYWORD");
    });
  }
}
