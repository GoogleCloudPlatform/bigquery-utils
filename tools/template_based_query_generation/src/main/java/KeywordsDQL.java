import com.google.common.collect.ImmutableSet;

/**
 * Set of user-defined DQL keywords
 */
public class KeywordsDQL implements Keywords {

	private final String fileNameDQL = "../resources/user_config.txt/dql.txt";

	private final ImmutableSet<String> keywordsSetDQL;

	/**
	 * Constructor of user-defined keywords, parsed from the config file
	 */
	public KeywordsDQL() {
		keywordsSetDQL = Utils.makeImmutableSet(fileNameDQL);
	}

	/**
	 * Returns the set of SQL DQL keywords
	 *
	 * @return the set of SQL DQL keywords
	 */
	public ImmutableSet<String> getKeywordsSet() {
		return keywordsSetDQL;
	}

	/**
	 * Checks whether a word is in the set of user-specified keywords
	 *
	 * @param word the word to check
	 * @return a boolean indicating if the word is in the DQL keywords set
	 */
	public boolean inKeywordsSet(String word) {
		return keywordsSetDQL.contains(word);
	}
}
