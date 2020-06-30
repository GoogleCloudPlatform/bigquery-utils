import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class QueryGeneratorTest {

    @Test
    public void test_generateQueries_1() throws Exception {
        String[] dialectConfigPaths = new String[] {"dialect_config/ddl_dependencies.txt",
                "dialect_config/dml_dependencies.txt",
                "dialect_config/dql_dependencies.txt",
                "dialect_config/root_dependencies.txt"};
        String[] userConfigPaths = new String[] {"user_config/ddl.txt",
                "user_config/dml.txt",
                "user_config/dql.txt",
                "user_config/root.txt"};
        String mainUserConfig = "user_config/config.txt";
        QueryGenerator qg = new QueryGenerator(dialectConfigPaths, userConfigPaths, mainUserConfig);
        ArrayList<ArrayList<Query>> queries = qg.generateQueries(50);
        for (ArrayList<Query> query: queries) {
            System.out.println(query);
        }
    }

}