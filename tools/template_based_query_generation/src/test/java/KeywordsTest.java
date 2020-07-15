import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeywordsTest {

  Keywords keywords;

  @BeforeEach
  public void initialize() {
    keywords = new Keywords();
  }

  @Test
  public void test_getKeywordsDDL() {
    ImmutableSet<String> setDDL = keywords.getKeywordsDDL();
    assertTrue(setDDL.contains("DDL_CREATE"));
    assertFalse(setDDL.contains("DQL_SELECT"));
  }

  @Test
  public void test_getKeywordsDML() {
    ImmutableSet<String> setDML = keywords.getKeywordsDML();
    assertTrue(setDML.contains("DML_INSERT"));
    assertFalse(setDML.contains("DDL_CREATE"));
  }

  @Test
  public void test_getKeywordsDQL() {
    ImmutableSet<String> setDQL = keywords.getKeywordsDQL();
    assertTrue(setDQL.contains("DQL_SELECT"));
    assertFalse(setDQL.contains("DML_INSERT"));
  }

  @Test
  public void test_inKeywordsDDL() {
    assertTrue(keywords.inKeywordsDDL("DDL_CREATE"));
    assertFalse(keywords.inKeywordsDDL("DQL_SELECT"));
  }

  @Test
  public void test_inKeywordsDML() {
    assertTrue(keywords.inKeywordsDML("DML_INSERT"));
    assertFalse(keywords.inKeywordsDML("DQL_CREATE"));
  }

  @Test
  public void test_keywordsDQL() {
    assertTrue(keywords.inKeywordsDQL("DQL_SELECT"));
    assertFalse(keywords.inKeywordsDQL("DML_INSERT"));
  }
}
