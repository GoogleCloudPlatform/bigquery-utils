import com.google.common.collect.ImmutableSet;

/**
 * Set of user-defined DDL keywords
 */
public class KeywordsDDL implements Keywords {

	private final String fileNameDDL = "./src/main/resources/user_config/ddl.txt";

	private final ImmutableSet<String> keywordsSetDDL;

	/**
	 * Constructor of user-defined keywords, parsed from the config file
	 */
	public KeywordsDDL() {
		keywordsSetDDL = Utils.makeImmutableSet(fileNameDDL);
	}

	/**
	 * Returns the set of SQL DDL keywords
	 *
	 * @return the set of SQL DDL keywords
	 */
	public ImmutableSet<String> getKeywordsSet() {
		return keywordsSetDDL;
	}

	/**
	 * Checks whether a word is in the set of user-specified keywords
	 *
	 * @param word the word to check
	 * @return a boolean indicating if the word is in the DDL keywords set
	 */
	public boolean inKeywordsSet(String word) {
		return keywordsSetDDL.contains(word);
	}
}
