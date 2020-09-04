import com.google.common.collect.ImmutableMap;
import data.DataType;
import data.Table;
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

    Map<String, Map<String, List<String>>> dialectQueries = new HashMap<>();

    for (String dialect : user.getDialectIndicators().keySet()) {
      if (user.getDialectIndicators().get(dialect)) {
        Map<String, List<String>> queryType = new HashMap<>();
        queryType.put("DQL", new ArrayList<>());
        queryType.put("DDL", new ArrayList<>());
        queryType.put("DML", new ArrayList<>());
        dialectQueries.put(dialect, queryType);
      }
    }

    for (List<SkeletonPiece> querySkeleton : querySkeletons) {
      for (String dialect: dialectQueries.keySet()) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < querySkeleton.size(); j++) {
          SkeletonPiece sp = querySkeleton.get(j);
          if (sp.getKeyword() != null) {
            sb.append(keywordsMapping.getLanguageMap(sp.getKeyword()).get(dialect));
            sb.append(" ");
          } else if (sp.getToken() != null) {
            sb.append(sp.getToken());
            if (j != querySkeleton.size() - 1 && querySkeleton.get(j + 1).getToken() != null) {
              sb.append(", ");
            } else {
              sb.append(" ");
            }
          } else {
            sb.append("(");
            for (MutablePair<String, DataType> pair : sp.getSchemaData()) {
              sb.append(pair.getLeft());
              sb.append(" ");
              sb.append(dataTypeMapping.get(pair.getRight()).get(dialect));
              sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(") ");
          }
        }
        String realQuery = sb.toString();
        if (realQuery.substring(0, 6).equals("SELECT")) {
          dialectQueries.get(dialect).get("DQL").add(realQuery.trim() + ";");
        } else if (realQuery.substring(0, 6).equals("INSERT")) {
          dialectQueries.get(dialect).get("DML").add(realQuery.trim() + ";");
        } else {
          dialectQueries.get(dialect).get("DDL").add(realQuery.trim() + ";");
        }
      }
    }

    try {
      Utils.writeDirectory(dialectQueries, qr.getTokenProvider().getTables());
    } catch (IOException exception){
      exception.printStackTrace();
    }

    for (Table table : qr.getTokenProvider().getTables()) {
      System.out.println(table.getName() + ": " + table.getSchema());
    }
  }
}
