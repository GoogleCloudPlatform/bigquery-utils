package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.cmd.CommandLineInteraction;
import com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.*;

public class QueryFixerMain {

  public static void main(String[] args) {
    QueryFixerOptions queryFixerOptions = QueryFixerOptions.readFlags(args);
    if (queryFixerOptions == null) {
      QueryFixerOptions.printHelpAndExit();
    }

    String credentialPath = queryFixerOptions.getOptionValue(CREDENTIAL);
    String projectId = queryFixerOptions.getOptionValue(PROJECT_ID);
    BigQueryOptions bigQueryOptions = buildBigQueryOptions(credentialPath, projectId);

    String query = queryFixerOptions.getQuery();
    if (query == null) {
      // In CLI mode, all the instructions are output by print functions, because logger outputs
      // extract info (time, code position) that distracts users.
      System.out.println(
          "Please provide the query as an argument, enclosed by double quote. Use --help for instruction.");
    }
    String mode = queryFixerOptions.getOptionValue(MODE);
    String outputFormat = queryFixerOptions.getOptionValue(OUTPUT);
    CommandLineInteraction interaction =
        CommandLineInteraction.create(mode, outputFormat, bigQueryOptions);
    interaction.interact(query);
  }

  private static BigQueryOptions buildBigQueryOptions(String credentialPath, String projectId) {
    if (credentialPath == null) {
      return BigQueryOptions.newBuilder().setProjectId(projectId).build();
    } else {
      // TODO: should support this in near future.
      System.out.println("Customized credential path is not supported");
      System.exit(1);
      return null;
    }
  }
}
