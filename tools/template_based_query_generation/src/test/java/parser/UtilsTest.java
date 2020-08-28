package parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

  public final String testDir = "./src/test/resources/";

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
    // TODO: refactor with new write functions
  }

  @Test
  public void test_writeFile(@TempDir Path testDir) throws IOException {
    List<String> expected = new ArrayList<>();

    Utils.writeFile(expected, testDir.resolve("test.txt"));
    List<String> actual = Files.readAllLines(testDir.resolve("test.txt"));

    assertEquals(expected, actual);

    expected.add("Test 1");
    expected.add("Test 2");
    expected.add("Test 3");

    Utils.writeFile(expected, testDir.resolve("test.txt"));
    actual = Files.readAllLines(testDir.resolve("test.txt"));

    assertEquals(expected, actual);
  }

  // TODO (spoiledhua): add unit tests for makeImmutableMap and makeImmutableSet

  //@Test
  public void test_makeImmutableSet(@TempDir Path testDir) throws IOException {
  }

  //@Test
  public void test_makeImmutableMap(@TempDir Path testDir) throws IOException {

  }

}
