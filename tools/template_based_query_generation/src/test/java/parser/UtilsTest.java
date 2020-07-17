package parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parser.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

  @Test
  public void test_getRandomInteger() {
    int randomInt = Utils.getRandomInteger(10);
    assertTrue(randomInt > 0);
    assertTrue(randomInt <= 10);
    assertEquals(1, Utils.getRandomInteger(1));
    assertThrows(IllegalArgumentException.class, () -> {
      Utils.getRandomInteger(0);
    });
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
    List<String> actual = Files.readAllLines(Paths.get(testDir.toString() + "/test.txt"));

    assertEquals(ImmutableList.copyOf(expected), ImmutableList.copyOf(actual));

    expected.add("Test 1");
    expected.add("Test 2");
    expected.add("Test 3");

    Utils.writeFile(ImmutableList.copyOf(expected), testDir.resolve("test.txt"));
    actual = Files.readAllLines(Paths.get(testDir.toString() + "/test.txt"));

    assertEquals(ImmutableList.copyOf(expected), ImmutableList.copyOf(actual));
  }

  // TODO (spoiledhua): add unit tests for makeImmutableMap and makeImmutableSet
}
