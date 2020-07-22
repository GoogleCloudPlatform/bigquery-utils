package parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import token.TokenInfo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

  @Test
  public void test_getRandomInteger() {
    int randomInt = Utils.getRandomInteger(10);
    assertTrue(randomInt > 0);
    assertTrue(randomInt <= 10);
    assertEquals(0, Utils.getRandomInteger(0));
    assertThrows(IllegalArgumentException.class, () -> {
      Utils.getRandomInteger(-1);
    });
  }

  @Test
  public void test_getRandomString() {
    String randomString = Utils.getRandomString(10);
    assertEquals(10, randomString.length());
    assertFalse(randomString.contains("!"));
    assertFalse(Character.isDigit(randomString.charAt(0)));
    assertThrows(IllegalArgumentException.class, () -> {
      Utils.getRandomString(0);
    });
  }

  @Test
  public void test_writeDirectory(@TempDir Path testDir) throws IOException {
    List<String> expected_bq_skeletons = new ArrayList<>();
    List<String> expected_bq_tokenized = new ArrayList<>();
    List<String> expected_postgre_skeletons = new ArrayList<>();
    List<String> expected_postgre_tokenized = new ArrayList<>();
    expected_bq_skeletons.add("BQ Skeletons!");
    expected_bq_tokenized.add("BQ Tokens!");
    expected_postgre_skeletons.add("PostgreSQL Skeletons!");
    expected_postgre_tokenized.add("PostgreSQL Tokens!");
    Map<String, ImmutableList<String>> expectedOutputs = new HashMap<>();
    expectedOutputs.put("BQ_skeletons", ImmutableList.copyOf(expected_bq_skeletons));
    expectedOutputs.put("BQ_tokenized", ImmutableList.copyOf(expected_bq_tokenized));
    expectedOutputs.put("Postgre_skeletons", ImmutableList.copyOf(expected_postgre_skeletons));
    expectedOutputs.put("Postgre_tokenized", ImmutableList.copyOf(expected_postgre_tokenized));

    Utils.writeDirectory(ImmutableMap.copyOf(expectedOutputs), testDir);

    List<String> actual_bq_skeletons = Files.readAllLines(Paths.get(testDir.toString() + "/bq_skeleton.txt"));
    List<String> actual_bq_tokenized = Files.readAllLines(Paths.get(testDir.toString() + "/bq_tokenized.txt"));
    List<String> actual_postgre_skeletons = Files.readAllLines(Paths.get(testDir.toString() + "/postgre_skeleton.txt"));
    List<String> actual_postgre_tokenized = Files.readAllLines(Paths.get(testDir.toString() + "/postgre_tokenized.txt"));
    Map<String, ImmutableList<String>> actualOutputs = new HashMap<>();
    actualOutputs.put("BQ_skeletons", ImmutableList.copyOf(actual_bq_skeletons));
    actualOutputs.put("BQ_tokenized", ImmutableList.copyOf(actual_bq_tokenized));
    actualOutputs.put("Postgre_skeletons", ImmutableList.copyOf(actual_postgre_skeletons));
    actualOutputs.put("Postgre_tokenized", ImmutableList.copyOf(actual_postgre_tokenized));

    assertEquals(ImmutableMap.copyOf(expectedOutputs), ImmutableMap.copyOf(actualOutputs));
  }

  @Test
  public void test_writeFile(@TempDir Path testDir) throws IOException {
    List<String> expected = new ArrayList<>();

    Utils.writeFile(ImmutableList.copyOf(expected), testDir.resolve("test.txt"));
    List<String> actual = Files.readAllLines(testDir.resolve("test.txt"));

    assertEquals(ImmutableList.copyOf(expected), ImmutableList.copyOf(actual));

    expected.add("Test 1");
    expected.add("Test 2");
    expected.add("Test 3");

    Utils.writeFile(ImmutableList.copyOf(expected), testDir.resolve("test.txt"));
    actual = Files.readAllLines(testDir.resolve("test.txt"));

    assertEquals(ImmutableList.copyOf(expected), ImmutableList.copyOf(actual));
  }

  // TODO (spoiledhua): add unit tests for makeImmutableMap and makeImmutableSet

  @Test
  public void test_makeImmutableSet(@TempDir Path testDir) throws IOException {
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    builder.add("Test 1");
    builder.add("Test 3");
    ImmutableSet<String> expected = builder.build();

    FeatureIndicator featureIndicator1 = new FeatureIndicator();
    FeatureIndicator featureIndicator2 = new FeatureIndicator();
    FeatureIndicator featureIndicator3 = new FeatureIndicator();
    featureIndicator1.setFeature("Test 1");
    featureIndicator2.setFeature("Test 2");
    featureIndicator3.setFeature("Test 3");
    featureIndicator1.setIsIncluded(true);
    featureIndicator2.setIsIncluded(false);
    featureIndicator3.setIsIncluded(true);
    List<FeatureIndicator> featureIndicatorList = new ArrayList<FeatureIndicator>();
    featureIndicatorList.add(featureIndicator1);
    featureIndicatorList.add(featureIndicator2);
    featureIndicatorList.add(featureIndicator3);
    FeatureIndicators featureIndicators = new FeatureIndicators();
    featureIndicators.setFeatureIndicators(featureIndicatorList);

    try (BufferedWriter writer = Files.newBufferedWriter(testDir.resolve("test.txt"), UTF_8)) {
      Gson gson = new Gson();
      gson.toJson(featureIndicators, writer);
    }

    ImmutableSet<String> actual = Utils.makeImmutableSet(testDir.resolve("test.txt"));

    assertEquals(expected, actual);
  }

  @Test
  public void test_makeImmutableMap(@TempDir Path testDir) throws IOException {
    TokenInfo tokenInfo = new TokenInfo();
    tokenInfo.setCount(1);
    tokenInfo.setRequired(true);
    tokenInfo.setTokenName("Test Token");
    ArrayList<TokenInfo> tokenInfos = new ArrayList<>();
    tokenInfos.add(tokenInfo);
    Mapping mapping = new Mapping();
    mapping.setPostgres("Test Postgre");
    mapping.setBigQuery("Test BigQuery");
    mapping.setTokenInfos(tokenInfos);
    ArrayList<Mapping> mappings = new ArrayList<>();
    mappings.add(mapping);

    ImmutableList<Mapping> expectedList = ImmutableList.copyOf(mappings);
    ImmutableMap.Builder<String, ImmutableList<Mapping>> builder = ImmutableMap.builder();
    builder.put("Test Feature", expectedList);
    ImmutableMap<String, ImmutableList<Mapping>> expected = builder.build();

    Feature feature = new Feature();
    feature.setFeature("Test Feature");
    feature.setAllMappings(mappings);
    ArrayList<Feature> featureList = new ArrayList<>();
    featureList.add(feature);
    Features features = new Features();
    features.setFeatures(featureList);

    try (BufferedWriter writer = Files.newBufferedWriter(testDir.resolve("test.txt"), UTF_8)) {
      Gson gson = new Gson();
      gson.toJson(features, writer);
    }

    ImmutableSet.Builder<String> keywordsBuilder = ImmutableSet.builder();
    keywordsBuilder.add("Test Feature");
    ImmutableSet<String> keywordsTest = keywordsBuilder.build();

    ImmutableMap<String, ImmutableList<Mapping>> actual = Utils.makeImmutableMap(testDir.resolve("test.txt"), keywordsTest);

    assertEquals(expected.get("Test Feature").get(0).getTokenInfos().get(0).getCount(), actual.get("Test Feature").get(0).getTokenInfos().get(0).getCount());
    assertEquals(expected.get("Test Feature").get(0).getTokenInfos().get(0).getRequired(), actual.get("Test Feature").get(0).getTokenInfos().get(0).getRequired());
    assertEquals(expected.get("Test Feature").get(0).getTokenInfos().get(0).getTokenName(), actual.get("Test Feature").get(0).getTokenInfos().get(0).getTokenName());
    assertEquals(expected.get("Test Feature").get(0).getPostgres(), actual.get("Test Feature").get(0).getPostgres());
    assertEquals(expected.get("Test Feature").get(0).getBigQuery(), actual.get("Test Feature").get(0).getBigQuery());
  }
}
