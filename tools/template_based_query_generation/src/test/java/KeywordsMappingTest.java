import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeywordsMappingTest {

	@Test
	public void test_keywordsMappingDML() {
		KeywordsMappingDML dml = new KeywordsMappingDML();
		assertEquals("INSERT INTO", dml.getKeywordsMappingPostgre("DML_INSERT"));
		assertEquals("INSERT", dml.getKeywordsMappingBQ("DML_INSERT"));
		assertThrows(IllegalArgumentException.class, () -> {
			dml.getKeywordsMappingPostgre("NON KEYWORD");
		});
		assertThrows(IllegalArgumentException.class, () -> {
			dml.getKeywordsMappingBQ("NON KEYWORD");
		});
	}

	@Test
	public void test_keywordsMappingDQL() {
		KeywordsMappingDQL dql = new KeywordsMappingDQL();
		assertEquals("SELECT", dql.getKeywordsMappingPostgre("DQL_SELECT"));
		assertEquals("SELECT", dql.getKeywordsMappingBQ("DQL_SELECT"));
		assertThrows(IllegalArgumentException.class, () -> {
			dql.getKeywordsMappingPostgre("NON KEYWORD");
		});
		assertThrows(IllegalArgumentException.class, () -> {
			dql.getKeywordsMappingBQ("NON KEYWORD");
		});
	}

	@Test
	public void test_keywordsMappingDDL() {
		KeywordsMappingDDL ddl = new KeywordsMappingDDL();
		assertEquals(ddl.getKeywordsMappingPostgre("DDL_CREATE"), "CREATE");
		assertEquals(ddl.getKeywordsMappingBQ("DDL_CREATE"), "CREATE");
		assertThrows(IllegalArgumentException.class, () -> {
			ddl.getKeywordsMappingPostgre("NON KEYWORD");
		});
		assertThrows(IllegalArgumentException.class, () -> {
			ddl.getKeywordsMappingBQ("NON KEYWORD");
		});
	}
}
