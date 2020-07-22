package parser;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import parser.Keywords;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeywordsTest {

  @Test
  public void test_getKeywordsDDL() {
    Keywords keywords = new Keywords();
    ImmutableSet<String> setDDL = keywords.getKeywordsDDL();
    assertTrue(setDDL.contains("DDL_CREATE"));
    assertFalse(setDDL.contains("DQL_SELECT"));
  }

  @Test
  public void test_getKeywordsDML() {
    Keywords keywords = new Keywords();
    ImmutableSet<String> setDML = keywords.getKeywordsDML();
    assertTrue(setDML.contains("DML_INSERT"));
    assertFalse(setDML.contains("DDL_CREATE"));
  }

  @Test
  public void test_getKeywordsDQL() {
    Keywords keywords = new Keywords();
    ImmutableSet<String> setDQL = keywords.getKeywordsDQL();
    assertTrue(setDQL.contains("DQL_SELECT"));
    assertFalse(setDQL.contains("DML_INSERT"));
  }

  @Test
  public void test_inKeywordsDDL() {
    Keywords keywords = new Keywords();
    assertTrue(keywords.inKeywordsDDL("DDL_CREATE"));
    assertFalse(keywords.inKeywordsDDL("DQL_SELECT"));
  }

  @Test
  public void test_inKeywordsDML() {
    Keywords keywords = new Keywords();
    assertTrue(keywords.inKeywordsDML("DML_INSERT"));
    assertFalse(keywords.inKeywordsDML("DQL_CREATE"));
  }

  @Test
  public void test_keywordsDQL() {
    Keywords keywords = new Keywords();
    assertTrue(keywords.inKeywordsDQL("DQL_SELECT"));
    assertFalse(keywords.inKeywordsDQL("DML_INSERT"));
  }
}
