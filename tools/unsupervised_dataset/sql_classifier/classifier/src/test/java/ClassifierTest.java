import org.junit.Test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Optional;

public class ClassifierTest {

    final String resourcesPath = "src/test/resources/";

    @Test
    public void testFileNotFound() {
        String invalidFile = "invalidFile.csv";
        assertTrue(Classifier.readCSV(invalidFile).isEmpty());
    }

    @Test
    public void testReadCSV() {
        String filepath = "queries_large.csv";
        Optional<List<String[]>> data = Classifier.readCSV(resourcesPath + filepath);
        assertTrue(data.isPresent());
        assertEquals(data.get().size(), 7637);
    }

    @Test
    public void testClassificationSmall() {
        String filepath = "queries_small.csv";
        String[] args = {resourcesPath + filepath};
        Classifier.main(args);
        File directory = new File("queries/");
        assertTrue(directory.exists());
    }

    @Test
    public void testClassificationLarge() {
        String filepath = "queries_large.csv";
        String[] args = {resourcesPath + filepath};
        Classifier.main(args);
        File directory = new File("queries/");
        assertTrue(directory.exists());
    }

    @Test
    public void testCleanQueries() {
        String query1 = "SELECT 1 + 1;";
        assertEquals("SELECT 1 + 1", Classifier.cleanQuery(query1));

        String query2 = "SELECT 2 +\u00a02;";
        assertEquals("SELECT 2 + 2", Classifier.cleanQuery(query2));
    }

}
