import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import graph.MarkovChain;
import graph.Node;
import parser.*;
import query.Query;
import query.Skeleton;
import token.Tokenizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

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

  private final MarkovChain<Query> markovChain;
  private Random r = new Random();
  private Node<Query> source = new Node<>(new Query(FeatureType.FEATURE_ROOT), r);

  /**
   *
   * @throws Exception
   */
  public QueryGenerator() throws Exception {
    // TODO (Victor):
    //  1. Use parser.Utils to parse user json and create graph.MarkovChain and nodes
    //  2. Generate number of queries given in config
    //  3. pass to them to Keyword or query.Skeleton

    // create nodes
    Map<String, Node<Query>> nodeMap = new HashMap<>();
    addNodeMap(nodeMap, Paths.get(filePathConfigDDL), r);
    addNodeMap(nodeMap, Paths.get(filePathConfigDML), r);
    addNodeMap(nodeMap, Paths.get(filePathConfigDQL), r);

    // TODO (Victor): Parse these two helper nodes from user config
    nodeMap.put("FEATURE_ROOT", source);
    nodeMap.put("FEATURE_SINK", new Node<>(new Query(FeatureType.FEATURE_SINK), r));

    Map<String, List<String>> neighborMap = new HashMap<>();
    addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDDL));
    addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDML));
    addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesDQL));
    addNeighborMap(neighborMap, nodeMap.keySet(), Paths.get(filePathDependenciesRoot));

    for (String nodeKey : nodeMap.keySet()) {
      HashSet<Node<Query>> nodeNeighbors = new HashSet<>();
      for (String neighbor : neighborMap.get(nodeKey)) {
        if (nodeMap.keySet().contains(neighbor)) {
          nodeNeighbors.add(nodeMap.get(neighbor));
        }
        nodeMap.get(nodeKey).setNeighbors(nodeNeighbors);
      }
    }

    markovChain = new MarkovChain(new HashSet<Node<Query>>(nodeMap.values()));
  }

  /**
   * generates queries from markov chain starting from root
   */
  public void generateQueries(int numberQueries) {
    ImmutableList.Builder<String> postgreBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> bigQueryBuilder = ImmutableList.builder();
    Tokenizer tokenizer = new Tokenizer(r);

    int i = 0;
    while (i < numberQueries) {
      List<Query> rawQueries = markovChain.randomWalk(source);

      if (rawQueries.get(rawQueries.size()-1).getType() == FeatureType.FEATURE_SINK) {
        List<Query> actualQueries = rawQueries.subList(2, rawQueries.size()-1);
        Skeleton skeleton = new Skeleton(actualQueries, tokenizer);
        postgreBuilder.add(String.join(" ", skeleton.getPostgreSkeleton()));
        bigQueryBuilder.add(String.join(" ", skeleton.getBigQuerySkeleton()));
        bigQueryBuilder.add(";");
        i++;
      }
    }

    ImmutableList<String> postgreSyntax = postgreBuilder.build();
    ImmutableList<String> bigQuerySyntax = bigQueryBuilder.build();

    ImmutableMap.Builder<String, ImmutableList<String>> builder = ImmutableMap.builder();
    builder.put("PostgreSQL", postgreSyntax);
    builder.put("BigQuery", bigQuerySyntax);
    ImmutableMap<String, ImmutableList<String>> outputs = builder.build();

    try {
      Utils.writeDirectory(outputs);
    } catch (IOException exception){
      exception.printStackTrace();
    }
  }

  private Map<String, Node<Query>> addNodeMap(Map<String, Node<Query>> nodeMap, Path input, Random r) {
    try {
      BufferedReader reader = Files.newBufferedReader(input, UTF_8);
      Gson gson = new Gson();
      FeatureIndicators featureIndicators = gson.fromJson(reader, FeatureIndicators.class);

      for (FeatureIndicator featureIndicator : featureIndicators.getFeatureIndicators()) {
        if (featureIndicator.getIsIncluded()) {
          nodeMap.put(featureIndicator.getFeature().name(), new Node<>(new Query(featureIndicator.getFeature()), r));
        }
      }
    } catch (IOException exception) {
      exception.printStackTrace();
    }

    return nodeMap;
  }

  private Map<String, List<String>> addNeighborMap(Map<String, List<String>> neighborMap, Set<String> nodes, Path input) {
    try {
      BufferedReader reader = Files.newBufferedReader(input, UTF_8);
      Gson gson = new Gson();
      Dependencies dependencies = gson.fromJson(reader, Dependencies.class);

      for (Dependency dependency : dependencies.getDependencies()) {
        if (nodes.contains(dependency.getNode())) {
          neighborMap.put(dependency.getNode(), dependency.getNeighbors());
        }
      }
    } catch (IOException exception) {
      exception.printStackTrace();
    }

    return neighborMap;
  }

}
