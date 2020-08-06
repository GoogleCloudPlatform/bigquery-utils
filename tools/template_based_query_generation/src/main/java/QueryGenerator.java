
/**
 * Class that parses config file and creates queries from markov chain
 */
public class QueryGenerator {

	/**
	 *
	 * @param dialectConfigPaths
	 * @param userConfigPaths
	 * @param mainUserConfig
	 * @throws Exception
	 */
	public QueryGenerator(String[] dialectConfigPaths, String[] userConfigPaths, String mainUserConfig) throws Exception {
		// TODO (Victor):
		//  1. Use parser.Utils to parse user json and create graph.MarkovChain and nodes
		//  2. Generate number of queries given in config
		//  3. pass to them to Keyword or Skeleton

	}

	/**
	 * generates queries from markov chain starting from root
	 * @param targetDirectory
	 */
	public void generateQueries(String targetDirectory) {
		// TODO (Victor): generate output text files containing the number of queries specified
		//  by the user in config files. Output is put in targetDirectory
	}

}
