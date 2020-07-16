import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeywordsMappingTest {

  // TODO (spoiledhua): fix tests here to correspond with new changes to KeywordsMapping

  @Test
  public void test_getMappingPostgreDDL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("CREATE", km.getMappingPostgreDDL("DDL_CREATE"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingPostgreDDL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingBigQueryDDL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("CREATE", km.getMappingBigQueryDDL("DDL_CREATE"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingBigQueryDDL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingPostgreDML() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("INSERT INTO", km.getMappingPostgreDML("DML_INSERT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingPostgreDML("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingBigQueryDML() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("INSERT", km.getMappingBigQueryDML("DML_INSERT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingBigQueryDML("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingPostgreDQL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("SELECT", km.getMappingPostgreDQL("DQL_SELECT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingPostgreDQL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingBigQueryDQL() {
    KeywordsMapping km = new KeywordsMapping();
    assertEquals("SELECT", km.getMappingBigQueryDQL("DQL_SELECT"));
    assertThrows(IllegalArgumentException.class, () -> {
      km.getMappingBigQueryDQL("NON KEYWORD");
    });
  }
}
