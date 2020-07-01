import com.google.common.collect.ImmutableMap;

public class KeywordsMappingDDL implements KeywordsMapping {

	private final String fileNamePostgre = "./src/main/resources/dialect_config/ddl_mapping_postgre.txt";

	private final String fileNameBQ = "./src/main/resources/dialect_config/ddl_mapping_BQ.txt";

	private final KeywordsDDL keywordsSetDDL = new KeywordsDDL();

	private final ImmutableMap<String, String> keywordsMapPostgre;

	private final ImmutableMap<String, String> keywordsMapBQ;

	/**
	 * Constructor of DDL keywords mapping, parsed from the config file
	 */
	public KeywordsMappingDDL() {
		keywordsMapPostgre = Utils.makeImmutableMap(fileNamePostgre, keywordsSetDDL);

		keywordsMapBQ = Utils.makeImmutableMap(fileNameBQ, keywordsSetDDL);

		System.out.println(System.getProperty("user.dir"));
	}

	/**
	 * Returns the PostgreSQL mapping to a word
	 *
	 * @param word the word to be translated
	 * @return the PostgreSQL mapping to the word
	 * @throws IllegalArgumentException if the PostgreSQL mapping does not contain the word
	 */
	public String getKeywordsMappingPostgre(String word) throws IllegalArgumentException {
		if (!keywordsSetDDL.inKeywordsSet(word)) {
			throw new IllegalArgumentException("The word is not in the DDL set");
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
		if (!keywordsSetDDL.inKeywordsSet(word)) {
			throw new IllegalArgumentException("The word is not in the DDL set");
		}

		return keywordsMapBQ.get(word);
	}
}
