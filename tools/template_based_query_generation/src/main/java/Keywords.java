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
}
