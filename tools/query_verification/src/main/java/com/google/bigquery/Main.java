package com.google.bigquery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;

import org.apache.commons.cli.*;

public class Main {

    /**
     * usage: query_verification -q <PATH> <PATH> [-d <PATH>] [-s <PATH> <PATH>]
     *        [-h]
     *  -q,--query <PATH> <PATH>    First argument is the path to the migrated
     *                              query file. Second argument is the path to
     *                              the original query file and only required
     *                              when data is provided.
     *  -d,--data <PATH>            Path for table data in CSV format.
     *  -s,--schema <PATH> <PATH>   First argument is the path to the migrated
     *                              schema path. Second argument is the path to
     *                              the original schema query and is optional.
     *                              Referenced files should be in a JSON format.
     *  -h,--help                   Print this help screen.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        Main main = new Main();
        main.run(args);

        System.exit(0);
    }

    public void run(String[] args) {
        CommandLine command = buildCommand(args);
        if (command == null) {
            return;
        }

        QueryVerification queryVerification = new QueryVerification();

        // Query
        if (command.hasOption("q")) {
            String[] queryOptionValues = command.getOptionValues("q");

            if (queryOptionValues.length >= 1) {
                String migratedQueryPath = queryOptionValues[0];
                QVQuery migratedQuery = new QVQuery(getContentsOfFile(migratedQueryPath), migratedQueryPath);
                queryVerification.setMigratedQuery(migratedQuery);
            }

            if (queryOptionValues.length >= 2) {
                String originalQueryPath = queryOptionValues[1];
                QVQuery originalQuery = new QVQuery(getContentsOfFile(originalQueryPath), originalQueryPath);
                queryVerification.setMigratedQuery(originalQuery);
            }
        }

        // Schema
        if (command.hasOption("s")) {
            String[] schemaOptionValues = command.getOptionValues("s");

            if (schemaOptionValues.length >= 1) {
                String migratedSchemaPath = schemaOptionValues[0];
                QVSchema migratedSchema = new QVSchema(getContentsOfFile(migratedSchemaPath), migratedSchemaPath);
                queryVerification.setMigratedSchema(migratedSchema);
            }

            if (schemaOptionValues.length >= 2) {
                String originalSchemaPath = schemaOptionValues[1];
                QVSchema originalSchema = new QVSchema(getContentsOfFile(originalSchemaPath), originalSchemaPath);
                queryVerification.setOriginalSchema(originalSchema);
            }
        }

        // Data
        if (command.hasOption("d")) {
            // TODO Data input for data aware verification
        }

        queryVerification.verify();
    }

    /**
     * @param args Command Line Arguments
     * @return Command parsed from arguments
     */
    public CommandLine buildCommand(String[] args) {
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
    public Options buildOptions() {
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
                .desc("First argument is the path to the migrated schema path. Second argument is the path to the original schema query and is optional. Referenced files should be in a JSON format.")
                .build());
        options.addOption(Option.builder("d")
                .longOpt("data")
                .hasArg(true)
                .argName("PATH")
                .desc("Path for table data in CSV format.")
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
    public String getContentsOfFile(String path) {
        String contents = null;
        try {
            contents = new String(Files.readAllBytes(Paths.get(path)));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            return contents;
        }
    }

}

