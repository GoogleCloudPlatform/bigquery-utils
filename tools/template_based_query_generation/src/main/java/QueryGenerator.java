import data.Table;
import graph.MarkovChain;
import graph.Node;
import parser.FeatureType;
import parser.User;
import parser.Utils;
import query.Query;
import query.Skeleton;
import token.Tokenizer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

/**
 * Class that parses config file and creates queries from markov chain
 */
public class QueryGenerator {

  private final String filePathConfigDDL = "./src/main/resources/user_config/ddl.json";
  private final String filePathConfigDML = "./src/main/resources/user_config/dml.json";
  private final String filePathConfigDQL = "./src/main/resources/user_config/dql.json";
  private final String filePathDependenciesRoot = "./src/main/resources/dialect_config/root_dependencies.json";
  private final String filePathDependenciesDDL = "./src/main/resources/dialect_config/ddl_dependencies.json";
  private final String filePathDependenciesDML = "./src/main/resources/dialect_config/dml_dependencies.json";
  private final String filePathDependenciesDQL = "./src/main/resources/dialect_config/dql_dependencies.json";
  private final String filePathUser = "./src/main/resources/user_config/config.json";

  private final MarkovChain<Query> markovChain;
  private Random r = new Random();
  private final User user = Utils.getUser(Paths.get(filePathUser));
  private Node<Query> source = new Node<>(new Query(user.getStartFeature()), r);
  private Node<Query> sink = new Node<>(new Query(user.getEndFeature()), r);

  /**
   * Query generator that converts query skeletons to real query strings ready for output
   * @throws IOException if the IO for user parsing fails
   */
  public QueryGenerator() throws IOException {

    // create map of references to nodes
    Map<FeatureType, Node<Query>> nodeMap = new HashMap<>();
    Utils.addNodeMap(nodeMap, Paths.get(filePathConfigDDL), r);
    Utils.addNodeMap(nodeMap, Paths.get(filePathConfigDML), r);
    Utils.addNodeMap(nodeMap, Paths.get(filePathConfigDQL), r);
    nodeMap.put(user.getStartFeature(), source);
    nodeMap.put(user.getEndFeature(), sink);

    // create map of nodes to their neighbors
    Map<FeatureType, List<FeatureType>> neighborMap = new HashMap<>();
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDDL));
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDML));
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDQL));
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesRoot));

    // set neighbors for each node
    for (FeatureType nodeKey : nodeMap.keySet()) {
      HashSet<Node<Query>> nodeNeighbors = new HashSet<>();
      for (FeatureType neighbor : neighborMap.get(nodeKey)) {
        if (nodeMap.keySet().contains(neighbor)) {
          nodeNeighbors.add(nodeMap.get(neighbor));
        }
        nodeMap.get(nodeKey).setNeighbors(nodeNeighbors);
      }
    }

    markovChain = new MarkovChain(new HashSet<Node<Query>>(nodeMap.values()));
  }

  /**
   * @return real queries from markov chain starting from root
   */
  public void generateQueries() throws IOException {
    Map<String, List<String>> dialectQueries = new HashMap<>();

    for (String dialect : user.getDialectIndicators().keySet()) {
      if (user.getDialectIndicators().get(dialect)) {
        dialectQueries.put(dialect, new ArrayList<>());
      }
    }

    Tokenizer tokenizer = new Tokenizer(r);

    int i = 0;
    while (i < user.getNumQueries()) {
      List<Query> rawQueries = markovChain.randomWalk(source);
      if (rawQueries.get(rawQueries.size()-1).getType() == FeatureType.FEATURE_SINK) {
        List<Query> actualQueries = rawQueries.subList(2, rawQueries.size() - 1);
        Skeleton skeleton = new Skeleton(actualQueries, tokenizer);
        for (String dialect : user.getDialectIndicators().keySet()) {
          if (user.getDialectIndicators().get(dialect)) {
            dialectQueries.get(dialect).add(String.join(" ", skeleton.getDialectSkeletons().get(dialect)) + ";");
          }
        }
      }
      i++;
    }

    Table dataTable = tokenizer.getTable();

    try {
      Utils.writeDirectory(dialectQueries, dataTable);
    } catch (IOException exception){
      exception.printStackTrace();
    }
  }
}
