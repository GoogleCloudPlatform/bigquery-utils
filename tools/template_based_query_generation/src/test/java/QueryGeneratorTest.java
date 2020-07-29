import org.junit.jupiter.api.Test;

class QueryGeneratorTest {

	@Test
	public void test_generateQueries() throws Exception {
		// TODO (Victor): tests for generating queries generate an array of queries from
		//  graph.MarkovChain. Tests will manually whether all dependencies are satisfied from
		//  test config files
		QueryGenerator queryGenerator = new QueryGenerator();
		for (int i = 0; i < 10; i++) {
			System.out.println(queryGenerator.generateQueries());
		}
	}

}
