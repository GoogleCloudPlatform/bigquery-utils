import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeywordsTest {

	@Test
	public void test_keywordsDML() {
		KeywordsDML dml = new KeywordsDML();
		ImmutableSet<String> setDML = dml.getKeywordsSet();
		assertTrue(setDML.contains("DML_INSERT"));
		assertFalse(setDML.contains("DDL_CREATE"));
		assertTrue(dml.inKeywordsSet("DML_INSERT"));
		assertFalse(dml.inKeywordsSet("DQL_CREATE"));
	}

	@Test
	public void test_keywordsDQL() {
		KeywordsDQL dql = new KeywordsDQL();
		ImmutableSet<String> setDQL = dql.getKeywordsSet();
		assertTrue(setDQL.contains("DQL_SELECT"));
		assertFalse(setDQL.contains("DML_INSERT"));
		assertTrue(dql.inKeywordsSet("DQL_SELECT"));
		assertFalse(dql.inKeywordsSet("DML_INSERT"));
	}

	@Test
	public void test_keywordsDDL() {
		KeywordsDDL ddl = new KeywordsDDL();
		ImmutableSet<String> setDDL = ddl.getKeywordsSet();
		assertTrue(setDDL.contains("DDL_CREATE"));
		assertFalse(setDDL.contains("DQL_SELECT"));
		assertTrue(ddl.inKeywordsSet("DDL_CREATE"));
		assertFalse(ddl.inKeywordsSet("DQL_SELECT"));
	}
}
