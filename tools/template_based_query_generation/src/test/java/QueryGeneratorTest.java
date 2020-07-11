import org.junit.jupiter.api.Test;

<<<<<<< HEAD
class QueryGeneratorTest {

    @Test
    public void test_generateQueries_1() throws Exception {
        // TODO (Allen): tests for generating queries generate an array of queries from
        //  MarkovChain. Tests will manually whether all dependencies are satisfied from
        //  test config files
    }
=======
import java.util.ArrayList;

class QueryGeneratorTest {

	@Test
	public void test_generateQueries_1() throws Exception {
		String[] dialectConfigPaths = new String[]{"dialect_config/ddl_dependencies.txt",
				"dialect_config/dml_dependencies.txt",
				"dialect_config/dql_dependencies.txt",
				"dialect_config/root_dependencies.txt"};
		String[] userConfigPaths = new String[]{"user_config/ddl.json",
				"user_config/dml.json",
				"user_config/dql.json",
				"user_config/root.txt"};
		String mainUserConfig = "user_config/config.txt";
		QueryGenerator qg = new QueryGenerator(dialectConfigPaths, userConfigPaths, mainUserConfig);
		ArrayList<ArrayList<Query>> queries = qg.generateQueries(50);
		for (ArrayList<Query> query : queries) {
			System.out.println(query);
		}
	}
>>>>>>> 33835babc5a545ddf0f7fbbd442e277e56124165

}
