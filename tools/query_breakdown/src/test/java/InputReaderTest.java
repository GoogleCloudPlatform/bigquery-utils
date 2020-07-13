import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class InputReaderTest {
  @Test
  public void inputReaderTest() throws IOException {
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A", InputReader.readInput(absPath +
        "/src/test/java/InputTestFiles/singleLine.txt"));
  }

  @Test
  public void inputReaderTestMultipleQuery() throws IOException {
    String absPath = new File("").getAbsolutePath();
    assertEquals("SELECT a FROM A;SELECT b FROM B;",
        InputReader.readInput(absPath +
        "/src/test/java/InputTestFiles/multipleLines.txt"));
  }
}