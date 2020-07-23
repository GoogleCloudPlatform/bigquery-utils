package com.google.bigquery;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.cli.*;

public class Main {

    /**
     * usage: query_verification -q <PATH> <PATH> [-d <PATHS>] [-s <PATH> <PATH>]
     *        [-h]
     *  -q,--query <PATH> <PATH>    First argument is the path to the migrated
     *                              query file. Second argument is the path to
     *                              the original query file and only required
     *                              when data is provided.
     *  -d,--data <PATHS>           Paths for table data in CSV format. File
     *                              names should be formatted as
     *                              "[dataset].[table].csv".
     *  -s,--schema <PATH> <PATH>   First argument is the path to the migrated
     *                              schema path. Second argument is the path to
     *                              the original schema query and is optional.
     *                              Referenced files should be in a JSON format.
     *  -h,--help                   Print this help screen.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        CommandLine command = buildCommand(args);
        if (command == null) {
            return;
        }

        QueryVerificationQuery migratedQuery = null;
        QueryVerificationSchema migratedSchema = null;

        QueryVerificationQuery originalQuery = null;
        QueryVerificationSchema originalSchema = null;

        List<QueryVerificationData> data = new ArrayList<QueryVerificationData>();

        // Query input handling
        if (command.hasOption("q")) {
            String[] queryOptionValues = command.getOptionValues("q");

            if (queryOptionValues.length >= 1) {
                String migratedQueryPath = queryOptionValues[0];
                String migratedQueryContents = getContentsOfFile(migratedQueryPath);
                if (migratedQueryContents != null) {
                    migratedQuery = QueryVerificationQuery.create(migratedQueryContents, migratedQueryPath);
                }
            }

            if (queryOptionValues.length >= 2) {
                String originalQueryPath = queryOptionValues[1];
                String originalQueryContents = getContentsOfFile(originalQueryPath);
                if (originalQueryContents != null) {
                    originalQuery = QueryVerificationQuery.create(originalQueryContents, originalQueryPath);
                }
            }
        }

        // Schema input handling
        if (command.hasOption("s")) {
            String[] schemaOptionValues = command.getOptionValues("s");

            if (schemaOptionValues.length >= 1) {
                String migratedSchemaPath = schemaOptionValues[0];
                String migratedSchemaContents = getContentsOfFile(migratedSchemaPath);
                if (migratedSchemaContents != null) {
                    migratedSchema = QueryVerificationSchema.create(migratedSchemaContents, migratedSchemaPath);
                }
            }

            if (schemaOptionValues.length >= 2) {
                String originalSchemaPath = schemaOptionValues[1];
                String originalSchemaContents = getContentsOfFile(originalSchemaPath);
                if (originalSchemaContents != null) {
                    originalSchema = QueryVerificationSchema.create(originalSchemaContents, originalSchemaPath);
                }
            }
        }

        // Data input handling
        if (command.hasOption("d")) {
            String[] dataOptionValues = command.getOptionValues("d");

            for (String dataFilePath : dataOptionValues) {
                String dataFileName = new File(dataFilePath).getName();
                String dataContents = getContentsOfFile(dataFilePath);
                String[] dataTableId = dataFileName.split("\\.");

                data.add(QueryVerificationData.create(dataTableId[0], dataTableId[1], dataFilePath, dataContents));
            }
        }

        QueryVerifier queryVerifier = new QueryVerifier(migratedQuery, migratedSchema, originalQuery, originalSchema, data);
        queryVerifier.verify();

        System.exit(0);
    }

    /**
     * @param args Command Line Arguments
     * @return Command parsed from arguments
     */
    public static CommandLine buildCommand(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = buildOptions();

        HelpFormatter formatter = new HelpFormatter();
        formatter.setOptionComparator(new Comparator<Option>() {
            public int compare(Option o1, Option o2) {
                if (o1.isRequired() != o2.isRequired())
                    return o1.isRequired() ? -1 : 1;
                else if (o1.hasArg() != o2.hasArg())
                    return o1.hasArg() ? -1 : 1;
                else
                    return o1.getLongOpt().compareTo(o2.getLongOpt());
            }
        });

        CommandLine command;
        try {
            command = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("query_verification", options, true);
            return null;
        }

        // Help
        if (command.hasOption("h")) {
            formatter.printHelp("query_verification", options, true);
        }

        return command;
    }

    /**
     * @return CLI options
     */
    public static Options buildOptions() {
        Options options = new Options();

        options.addOption(Option.builder("q")
                .required(true)
                .longOpt("query")
                .numberOfArgs(Option.UNLIMITED_VALUES) // Allows for 2 arguments without both being required
                .valueSeparator(' ')
                .argName("PATH> <PATH") // Appears as "<PATH> <PATH>"
                .desc("First argument is the path to the migrated query file. Second argument is the path to the original query file and only required when data is provided.")
                .build());
        options.addOption(Option.builder("s")
                .longOpt("schema")
                .numberOfArgs(Option.UNLIMITED_VALUES) // Allows for 2 arguments without both being required
                .valueSeparator(' ')
                .argName("PATH> <PATH") // Appears as "<PATH> <PATH>"
                .desc("First argument is the path to the migrated schema path. Second argument is the path to the original schema query and is optional. Referenced files should be DDL statements or in JSON format.")
                .build());
        options.addOption(Option.builder("d")
                .longOpt("data")
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .valueSeparator(' ')
                .argName("PATHS")
                .desc("Paths for table data in CSV format. File names should be formatted as \"[dataset].[table].csv\".")
                .build());
        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Print this help screen.")
                .build());

        return options;
    }

    /**
     * Retrieves the text contents from file
     *
     * @param path The path to the file to be read
     * @return Text contents in file
     */
    public static String getContentsOfFile(String path) {
        String contents = null;
        try {
            contents = new String(Files.readAllBytes(Paths.get(path)));
        } catch (NoSuchFileException e) {
            System.out.println("File Not Found: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("I/O Exception: " + e.getMessage());
        } finally {
            return contents;
        }
    }

}
