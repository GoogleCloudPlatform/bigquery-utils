import com.google.common.collect.ImmutableSet;

/**
 * Set of user-defined DML keywords
 */
public class KeywordsDML implements Keywords {

	private final String fileNameDML = "../resources/user_config.txt/dml.txt";

	private final ImmutableSet<String> keywordsSetDML;

	/**
	 * Constructor of user-defined keywords, parsed from the config file
	 */
	public KeywordsDML() {
		keywordsSetDML = Utils.makeImmutableSet(fileNameDML);
	}

	/**
	 * Returns the set of SQL DML keywords
	 *
	 * @return the set of SQL DML keywords
	 */
	public ImmutableSet<String> getKeywordsSet() {
		return keywordsSetDML;
	}

	/**
	 * Checks whether a word is in the set of user-specified keywords
	 *
	 * @param word the word to check
	 * @return a boolean indicating if the word is in the DML keywords set
	 */
	public boolean inKeywordsSet(String word) {
		return keywordsSetDML.contains(word);
	}
}
