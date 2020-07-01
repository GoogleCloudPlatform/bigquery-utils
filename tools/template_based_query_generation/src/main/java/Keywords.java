import com.google.common.collect.ImmutableSet;

/**
 * Keywords set interface that all keyword set classes implement
 */

public interface Keywords {

	/**
	 * Returns the set of keywords
	 *
	 * @return the set of keywords
	 */
	public ImmutableSet<String> getKeywordsSet();

	/**
	 * Checks whether a word is in the set of user-specified keywords
	 *
	 * @param word the word to check
	 * @return a boolean indicating if the word is in the keywords set
	 */
	public boolean inKeywordsSet(String word);
}
