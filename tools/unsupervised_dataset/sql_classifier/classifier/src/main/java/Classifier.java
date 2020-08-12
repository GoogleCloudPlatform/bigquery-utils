import com.opencsv.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import org.apache.calcite.sql.parser.dialect1.Dialect1ParserImpl;
import org.apache.calcite.sql.parser.bigquery.BigQueryParserImpl;
import org.apache.calcite.sql.parser.defaultdialect.DefaultDialectParserImpl;
import org.apache.calcite.sql.parser.postgresql.PostgreSQLParserImpl;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Classifier {

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    /*
     * Runs the classification tool. Takes a CSV file of queries, classifies queries based on dialect, and creates
     * several subdirectories to store the queries by dialect.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            LOGGER.log(Level.SEVERE, "Please provide a CSV file.");
            return;
        }
        Optional<List<String[]>> allData = readCSV(args[0]);
        if (allData.isEmpty()) {
            return;
        }
        SqlParser.Config[] parserConfigs = new SqlParser.Config[4];
        parserConfigs[0] = SqlParser.configBuilder().setParserFactory(DefaultDialectParserImpl.FACTORY).build();
        parserConfigs[1] = SqlParser.configBuilder().setParserFactory(Dialect1ParserImpl.FACTORY).build();
        parserConfigs[2] = SqlParser.configBuilder().setParserFactory(BigQueryParserImpl.FACTORY).build();
        parserConfigs[3] = SqlParser.configBuilder().setParserFactory(PostgreSQLParserImpl.FACTORY).build();

        CSVWriter[] writers = setupOutput();

        for (String[] data : allData.get()) {
            Map<SqlParser.Config, Boolean> results = classifyQuery(cleanQuery(data[0]), parserConfigs);
            boolean unclassified = true;
            String[] nextLine = {data[0], data[1]};
            for (int i = 0; i < parserConfigs.length; i++) {
                if (results.get(parserConfigs[i])) {
                    unclassified = false;
                    writers[i].writeNext(nextLine);
                }
            }
            if (unclassified) {
                writers[writers.length-1].writeNext(nextLine);
            }
        }
        try {
            for (CSVWriter writer : writers) {
                writer.close();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
        }

    }

    /*
     * Reads a CSV file and returns the data as a list of String arrays.
     *
     * @param filename Path to a CSV file
     * @return Optional containing contents of the CSV file
     */
    static Optional<List<String[]>> readCSV(String filename) {
        try {
            FileReader filereader = new FileReader(filename);
            CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(1).build();
            return Optional.of(csvReader.readAll());
        }
        catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return Optional.empty();
        }
    }

    /*
     * Classifies a single query using different SQL parsers.
     *
     * @param query The query to be classified
     * @param parserConfigs The parsers for each of the different dialects
     * @return A map where parsers are mapped to booleans, with true values if the query can be classified in a dialect
     * and false otherwise
     */
    static Map<SqlParser.Config, Boolean> classifyQuery(String query, SqlParser.Config[] parserConfigs) {

        Map<SqlParser.Config, Boolean> results = new HashMap<>();
        for (int i = 0; i < parserConfigs.length; i++) {
            try {
                SqlParser.create(query, parserConfigs[i]).parseStmt();
                results.put(parserConfigs[i], true);
            } catch (SqlParseException e) {
                results.put(parserConfigs[i], false);
            }
        }
        return results;
    }

    /*
     * Cleans a query by removing unicode characters and semicolons.
     *
     * @param query The query to be cleaned. This should only be one query, and if there are multiple, it will only
     * clean one by splitting at semicolons.
     * @return The cleaned query
     */
    static String cleanQuery(String query) {
        return query.split(";")[0].replaceAll("\\P{Print}"," ");
    }

    /*
     * Creates output directories if they do not exists and file descriptors for output CSV files. Number of directories
     * and files depends on the number of dialects being classified.
     *
     * @return An array of CSVWriters to write the final CSV output files
     */
    static CSVWriter[] setupOutput() {
        String path = "queries/";
        String[] dialects = {"default", "dialect1", "bigquery", "postgresql", "unclassified"};
        String[] header = {"Queries", "URL"};
        String timeString = getTimeString();
        CSVWriter[] writers = new CSVWriter[5];
        for (int i = 0; i < dialects.length; i++) {
            File directory = new File(path + dialects[i]);
            if (!directory.exists()) {
                directory.mkdirs();
            }
            try {
                File file = new File(path + dialects[i] + "/" + dialects[i] + timeString);
                writers[i] = new CSVWriter(new FileWriter(file));
                writers[i].writeNext(header);

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage());
                writers[i] = null;
            }
        }
        return writers;
    }

    /*
     * Gets the time-stamped name for a CSV output file.
     *
     * @return Name of output file with current timestamp
     */
    static String getTimeString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("_yyyy_MM_dd_hh_mm");
        return "_queries" + dateFormat.format(new Date()) + ".csv";
    }


}