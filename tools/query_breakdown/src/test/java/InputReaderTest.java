import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class InputReaderTest {
  @Test
  public void inputReaderTest() throws IOException {
    String absPath = new File("").getAbsolutePath();
    System.out.println(InputReader.readInput(absPath +
        "/src/test/java/InputTestFiles/singleLine.txt"));
  }
}