import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;


/**
 * Class that parses config file and creates queries
 */
public class QueryGenerator {

    private MarkovChain<Query> mcGenerator;
    private Node<Query> queryRoot;

    /**
     *
     * @param dialectConfigPaths
     * @param userConfigPaths
     * @param mainUserConfig
     * @throws Exception
     */
    public QueryGenerator(String[] dialectConfigPaths, String[] userConfigPaths, String mainUserConfig) throws Exception {
        // read in lines from userConfigPaths, ignoring lines that begin with ' ' or '/'
        // stores active queries in activatedQueries
        HashSet<String> activatedQueries = new HashSet<String>();
        String line;
        for (String path: userConfigPaths) {
            BufferedReader br = new BufferedReader(new FileReader(path));
            while ((line = br.readLine()) != null) {
                if (line.length() > 2 && line.charAt(0) != ' '
                        && !(line.charAt(0) == '/' && line.charAt(1) == '/')) {
                    String[] res = line.split(":");
                    if (res[1].charAt(0) == '1') {
                        activatedQueries.add(res[0]);
                    }
                }
            }
            br.close();
        }

        // read in lines from dialectConfigPaths, ignoring lines that begin with ' ' or '/'
        // stores directed edges in dependencies
        HashMap<String, HashSet<String>> dependencies = new HashMap<String, HashSet<String>>();
        for (String path: dialectConfigPaths) {
            BufferedReader br = new BufferedReader(new FileReader(path));
            while ((line = br.readLine()) != null) {
                if (line.length() > 2 && line.charAt(0) != ' '
                        && !(line.charAt(0) == '/' && line.charAt(1) == '/')) {
                    String[] res = line.split(":");
                    if (!dependencies.keySet().contains(res[0]) && activatedQueries.contains(res[0])) {
                        dependencies.put(res[0], new HashSet<String>());
                    }
                    if (!dependencies.keySet().contains(res[1]) && activatedQueries.contains(res[1])) {
                        dependencies.put(res[1], new HashSet<String>());
                    }
                    if (activatedQueries.contains(res[0]) && activatedQueries.contains(res[1])) {
                        dependencies.get(res[0]).add(res[1]);
                    }
                }
            }
            br.close();
        }

        // create a Node object for each query
        HashMap<String, Node<Query>> nodes = new HashMap<String, Node<Query>>();
        for (String query: activatedQueries) {
            nodes.put(query, new Node(new Query(QueryType.valueOf(query)),0));
        }

        // for each query, correctly set neighbors and set root
        for (String query: activatedQueries) {
            HashSet<Node<Query>> neighbors = new HashSet<Node<Query>>();
            for (String s2: dependencies.get(query)) {
                neighbors.add(nodes.get(s2));
            }
            nodes.get(query).setNeighbors(neighbors);
        }

        // parse lines from mainUserConfig, ignoring lines that begin with ' ' or '/'
        BufferedReader br = new BufferedReader(new FileReader(mainUserConfig));
        while ((line = br.readLine()) != null) {
            if (line.length() > 2 && line.charAt(0) != ' '
                    && !(line.charAt(0) == '/' && line.charAt(1) == '/')) {
                String[] res = line.split(":");
                if(res[0].equals("root")) {
                    this.queryRoot = nodes.get(res[1]);
                }
            }
        }
        br.close();
        this.mcGenerator = new MarkovChain<Query>((HashSet<Node<Query>>) nodes.values(), 0);
    }

    /**
     * generates queries from markov chain starting from root
     * @param numQueries
     * @return
     */
    public ArrayList<ArrayList<Query>> generateQueries(int numQueries) {
        ArrayList<ArrayList<Query>> queries = new ArrayList<ArrayList<Query>>();
        for (int i = 0; i < numQueries; i++) {
            queries.add(this.mcGenerator.randomWalk(this.queryRoot));
        }
        return queries;
    }

    /**
     * temporary main method to test progress thus far
     * @param args
     */
    public static void main(String[] args) throws Exception {
//        String[] dialectConfigPaths = new String[] {"dialect_config/ddl_dependencies.txt", "dialect_config/dml_dependencies.txt", "dialect_config/dql_dependencies.txt", "dialect_config/root_dependencies.txt"};
//        String[] userConfigPaths = new String[] {"user_config/ddl.txt", "user_config/dml.txt", "user_config/dql.txt", "user_config/root.txt"};
//        String mainUserConfig = "user_config/config.txt";
//        QueryGenerator qg = new QueryGenerator(dialectConfigPaths, userConfigPaths, mainUserConfig);

    }

}
