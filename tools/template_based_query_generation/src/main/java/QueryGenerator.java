import com.google.common.collect.ImmutableMap;
import data.DataType;
import graph.MarkovChain;
import graph.Node;
import org.apache.commons.lang3.tuple.MutablePair;
import parser.KeywordsMapping;
import parser.User;
import parser.Utils;
import query.SkeletonPiece;
import token.QueryRegex;

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
  private final String filePathDataTypeMap = "./src/main/resources/dialect_config/datatype_mapping.json";
  private final String filePathRegexMap = "./src/main/resources/dialect_config/regex_mapping.json";

  private final MarkovChain<String> markovChain;
  private final KeywordsMapping keywordsMapping = new KeywordsMapping();
  private final ImmutableMap<DataType, Map<String, String>> dataTypeMapping = Utils.makeImmutableDataTypeMap(Paths.get(filePathDataTypeMap));
  private final Map<String, String> regexMapping = Utils.makeRegexMap(Paths.get(filePathRegexMap));
  private Random r = new Random();
  private final User user = Utils.getUser(Paths.get(filePathUser));
  private Node<String> source = new Node<>(user.getStartFeature(), r);
  private Node<String> sink = new Node<>(user.getEndFeature(), r);

  /**
   * Query generator that converts query skeletons to real query strings ready for output
   * @throws IOException if the IO for user parsing fails
   */
  public QueryGenerator() throws IOException {

    // create map of references to nodes
    Map<String, Node<String>> nodeMap = new HashMap<>();
    Utils.addNodeMap(nodeMap, Paths.get(filePathConfigDDL), r);
    Utils.addNodeMap(nodeMap, Paths.get(filePathConfigDML), r);
    Utils.addNodeMap(nodeMap, Paths.get(filePathConfigDQL), r);
    nodeMap.put(user.getStartFeature(), source);
    nodeMap.put(user.getEndFeature(), sink);

    // create map of nodes to their neighbors
    Map<String, List<String>> neighborMap = new HashMap<>();
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDDL));
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDML));
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDQL));
    Utils.addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesRoot));

    // set neighbors for each node
    for (String nodeKey : nodeMap.keySet()) {
      HashSet<Node<String>> nodeNeighbors = new HashSet<>();
      for (String neighbor : neighborMap.get(nodeKey)) {
        if (nodeMap.keySet().contains(neighbor)) {
          nodeNeighbors.add(nodeMap.get(neighbor));
        }
        nodeMap.get(nodeKey).setNeighbors(nodeNeighbors);
      }
    }

    markovChain = new MarkovChain(new HashSet<Node<String>>(nodeMap.values()));
  }

  /**
   * @return real queries from markov chain starting from root
   */
  public void generateQueries() throws IOException {

    List<String> regexQueries = new ArrayList<>();
    int i = 0;
    while (i < user.getNumQueries()) {
      List<String> rawQueries = markovChain.randomWalk(source);
      if (rawQueries.get(rawQueries.size() - 1).equals("FEATURE_SINK")) {
        List<String> actualQueries = rawQueries.subList(2, rawQueries.size() - 1);
        StringBuilder sb = new StringBuilder();
        for (String actualQuery : actualQueries) {
          sb.append(regexMapping.get(actualQuery));
          sb.append(" ");
        }

        String regexQuery = sb.toString().trim();
        regexQueries.add(regexQuery);
        i++;
      }
    }

    QueryRegex qr = new QueryRegex(regexQueries, user.getNumColumns());
    List<List<SkeletonPiece>> querySkeletons = qr.getQuerySkeletons();

    Map<String, List<String>> dialectQueries = new HashMap<>();

    for (String dialect : user.getDialectIndicators().keySet()) {
      if (user.getDialectIndicators().get(dialect)) {
        dialectQueries.put(dialect, new ArrayList<>());
      }
    }

    for (List<SkeletonPiece> querySkeleton : querySkeletons) {
      for (String dialect: dialectQueries.keySet()) {
        StringBuilder realQuery = new StringBuilder();
        for (SkeletonPiece sp : querySkeleton) {
          if (sp.getKeyword() != null) {
            realQuery.append(keywordsMapping.getLanguageMap(sp.getKeyword()).get(dialect));
            realQuery.append(" ");
          } else if (sp.getToken() != null) {
            realQuery.append(sp.getToken());
            realQuery.append(" ");
          } else {
            realQuery.append(" (");
            for (MutablePair<String, DataType> pair : sp.getSchemaData()) {
              realQuery.append(pair.getLeft());
              realQuery.append(" ");
              realQuery.append(dataTypeMapping.get(pair.getRight()).get(dialect));
              realQuery.append(", ");
            }
            realQuery.append(" )");
          }
        }
        dialectQueries.get(dialect).add(realQuery.toString().trim());
      }
    }

    for (String dialect : dialectQueries.keySet()) {
      for (String query : dialectQueries.get(dialect)) {
        System.out.println(query);
      }
    }

    /*

    try {
      Utils.writeDirectory(dialectQueries, dataTable);
    } catch (IOException exception){
      exception.printStackTrace();
    }
     */
  }
}
