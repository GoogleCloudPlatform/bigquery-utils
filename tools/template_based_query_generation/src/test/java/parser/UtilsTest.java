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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    List<String> expected_bigQuery = new ArrayList<>();
    List<String> expected_postgreSQL = new ArrayList<>();
    expected_bigQuery.add("BigQuery Tokens!");
    expected_postgreSQL.add("PostgreSQL Tokens!");
    Map<String, ImmutableList<String>> expectedOutputs = new HashMap<>();
    expectedOutputs.put("BigQuery", ImmutableList.copyOf(expected_bigQuery));
    expectedOutputs.put("PostgreSQL", ImmutableList.copyOf(expected_postgreSQL));

    Utils.writeDirectory(ImmutableMap.copyOf(expectedOutputs), testDir);

    List<String> actual_bigQuery = Files.readAllLines(Paths.get(testDir.toString() + "/bigQuery.txt"));
    List<String> actual_postgreSQL = Files.readAllLines(Paths.get(testDir.toString() + "/postgreSQL.txt"));
    Map<String, ImmutableList<String>> actualOutputs = new HashMap<>();
    actualOutputs.put("BigQuery", ImmutableList.copyOf(actual_bigQuery));
    actualOutputs.put("PostgreSQL", ImmutableList.copyOf(actual_postgreSQL));

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
    builder.add("DDL_CREATE");
    ImmutableSet<String> expected = builder.build();

    FeatureIndicator featureIndicator1 = new FeatureIndicator();
    featureIndicator1.setFeature("DDL_CREATE");
    featureIndicator1.setIsIncluded(true);
    List<FeatureIndicator> featureIndicatorList = new ArrayList<>();
    featureIndicatorList.add(featureIndicator1);
    FeatureIndicators featureIndicators = new FeatureIndicators();
    featureIndicators.setFeatureIndicators(featureIndicatorList);

    try (BufferedWriter writer = Files.newBufferedWriter(testDir.resolve("test.txt"), UTF_8)) {
      Gson gson = new Gson();
      gson.toJson(featureIndicators, writer);
    }

    ImmutableSet<String> actual = Utils.makeImmutableKeywordSet(testDir.resolve("test.txt"));

    assertEquals(expected, actual);
  }

  @Test
  public void test_makeImmutableMap(@TempDir Path testDir) throws IOException {
    TokenInfo tokenInfo = new TokenInfo();
    tokenInfo.setCount(1);
    tokenInfo.setRequired(true);
    tokenInfo.setTokenType("table_name");
    ArrayList<TokenInfo> tokenInfos = new ArrayList<>();
    tokenInfos.add(tokenInfo);
    Mapping mapping = new Mapping();
    Map<String, String> dialectMap = new HashMap<>();
    dialectMap.put("postgres", "Test Postgre");
    dialectMap.put("bigQuery", "Test BigQuery");
    mapping.setTokenInfos(tokenInfos);
    mapping.setDialectMap(dialectMap);
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

    ImmutableMap<String, ImmutableList<Mapping>> actual = Utils.makeImmutableKeywordMap(testDir.resolve("test.txt"), keywordsTest);

    assertEquals(expected.get("Test Feature").get(0).getTokenInfos().get(0).getCount(), actual.get("Test Feature").get(0).getTokenInfos().get(0).getCount());
    assertEquals(expected.get("Test Feature").get(0).getTokenInfos().get(0).getRequired(), actual.get("Test Feature").get(0).getTokenInfos().get(0).getRequired());
    assertEquals(expected.get("Test Feature").get(0).getTokenInfos().get(0).getTokenType(), actual.get("Test Feature").get(0).getTokenInfos().get(0).getTokenType());
    assertEquals(expected.get("Test Feature").get(0).getDialectMap().get("postgres"), actual.get("Test Feature").get(0).getDialectMap().get("postgres"));
    assertEquals(expected.get("Test Feature").get(0).getDialectMap().get("bigQuery"), actual.get("Test Feature").get(0).getDialectMap().get("bigQuery"));
  }
}
