import com.google.common.collect.ImmutableMap;

public class KeywordsMappingDML implements KeywordsMapping {

	private final String fileNamePostgre = "./src/main/resources/dialect_config/dml_mapping_postgre.txt";

	private final String fileNameBQ = "./src/main/resources/dialect_config/dml_mapping_BQ.txt";

	private final KeywordsDML keywordsSetDML = new KeywordsDML();

	private final ImmutableMap<String, String> keywordsMapPostgre;

	private final ImmutableMap<String, String> keywordsMapBQ;

	/**
	 * Constructor of DML keywords mapping, parsed from the config file
	 */
	public KeywordsMappingDML() {
		keywordsMapPostgre = Utils.makeImmutableMap(fileNamePostgre, keywordsSetDML);

		keywordsMapBQ = Utils.makeImmutableMap(fileNameBQ, keywordsSetDML);
	}

	/**
	 * Returns the PostgreSQL mapping to a word
	 *
	 * @param word the word to be translated
	 * @return the PostgreSQL mapping to the word
	 * @throws IllegalArgumentException if the PostgreSQL mapping does not contain the word
	 */
	public String getKeywordsMappingPostgre(String word) throws IllegalArgumentException {
		if (!keywordsSetDML.inKeywordsSet(word)) {
			throw new IllegalArgumentException("The word is not in the DML set");
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
		if (!keywordsSetDML.inKeywordsSet(word)) {
			throw new IllegalArgumentException("The word is not in the DML set");
		}

		return keywordsMapBQ.get(word);
	}
}
