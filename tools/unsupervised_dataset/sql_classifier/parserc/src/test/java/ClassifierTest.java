import org.junit.Test;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

public class ClassifierTest {

    final String resourcesPath = "src/test/resources/";

    @Test
    public void testFileNotFound() {
        String invalidFile = "invalidFile.csv";
        assertNull(Parserc.readCSV(invalidFile));
    }

    @Test
    public void testReadCSV() {
        String filepath = "queries_large.csv";
        List<String[]> data = Parserc.readCSV(resourcesPath + filepath);
        assertNotNull(data);
        assertEquals(data.size(), 7637);
    }

    @Test
    public void testClassificationSmall() {
        String filepath = "queries_small.csv";
        String[] args = {resourcesPath + filepath};
        Parserc.main(args);
        File directory = new File("queries/");
        assertTrue(directory.exists());
    }

    @Test
    public void testClassificationLarge() {
        String filepath = "queries_large.csv";
        String[] args = {resourcesPath + filepath};
        Parserc.main(args);
        File directory = new File("queries/");
        assertTrue(directory.exists());
    }

    @Test
    public void testCleanQueries() {
        String query1 = "SELECT 1 + 1;";
        assertEquals("SELECT 1 + 1", Parserc.cleanQuery(query1));

        String query2 = "SELECT 2 +\u00a02;";
        assertEquals("SELECT 2 + 2", Parserc.cleanQuery(query2));
    }

}
