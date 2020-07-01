/**
 * Keywords mapping interface that all keyword mapping classes implement
 */

public interface KeywordsMapping {

	/**
	 * Returns the PostgreSQL mapping to a keyword
	 *
	 * @param keyword a keyword to be translated
	 * @return the PostgreSQL mapping to the keyword
	 */
	public String getKeywordsMappingPostgre(String keyword);

	/**
	 * Returns the BigQuery mapping to a keyword
	 *
	 * @param keyword a keyword to be translated
	 * @return the BigQuery mapping to the keyword
	 */
	public String getKeywordsMappingBQ(String keyword);
}
