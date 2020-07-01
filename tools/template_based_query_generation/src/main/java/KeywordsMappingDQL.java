import com.google.common.collect.ImmutableMap;

public class KeywordsMappingDQL implements KeywordsMapping {

	private final String fileNamePostgre = "./src/main/resources/dialect_config/dql_mapping_postgre.txt";

	private final String fileNameBQ = "./src/main/resources/dialect_config/dql_mapping_BQ.txt";

	private final KeywordsDQL keywordsSetDQL = new KeywordsDQL();

	private final ImmutableMap<String, String> keywordsMapPostgre;

	private final ImmutableMap<String, String> keywordsMapBQ;

	/**
	 * Constructor of DQL keywords mapping, parsed from the config file
	 */
	public KeywordsMappingDQL() {
		keywordsMapPostgre = Utils.makeImmutableMap(fileNamePostgre, keywordsSetDQL);

		keywordsMapBQ = Utils.makeImmutableMap(fileNameBQ, keywordsSetDQL);
	}

	/**
	 * Returns the PostgreSQL mapping to a word
	 *
	 * @param word the word to be translated
	 * @return the PostgreSQL mapping to the word
	 * @throws IllegalArgumentException if the PostgreSQL mapping does not contain the word
	 */
	public String getKeywordsMappingPostgre(String word) throws IllegalArgumentException {
		if (!keywordsSetDQL.inKeywordsSet(word)) {
			throw new IllegalArgumentException("The word is not in the DQL set");
		}

		return keywordsMapPostgre.get(word);
	}

	/**
	 * Returns the BigQuery mapping to a keyword
	 *
	 * @param word a keyword to be translated
	 * @return the BigQuery mapping to the keyword
	 * @throws IllegalArgumentException if the BigQuery mapping does not contain the word
	 */
	public String getKeywordsMappingBQ(String word) throws IllegalArgumentException {
		if (!keywordsSetDQL.inKeywordsSet(word)) {
			throw new IllegalArgumentException("The word is not in the DQL set");
		}

		return keywordsMapBQ.get(word);
	}
}
