package com.google.cloud.bigquery.utils.queryfixer;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.cmd.CommandLineInteraction;
import com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static com.google.cloud.bigquery.utils.queryfixer.cmd.QueryFixerOptions.*;
import static java.lang.System.exit;

public class QueryFixerMain {

  public static void main(String[] args) {
    QueryFixerOptions queryFixerOptions = QueryFixerOptions.readUserInput(args);
    if (queryFixerOptions == null) {
      QueryFixerOptions.printHelpAndExit();
    }

    String credentialsPath = queryFixerOptions.getOptionValue(CREDENTIALS);
    String projectId = queryFixerOptions.getOptionValue(PROJECT_ID);
    BigQueryOptions bigQueryOptions = buildBigQueryOptions(credentialsPath, projectId);

    String query = queryFixerOptions.getQuery();
    if (query == null) {
      // In CLI mode, all the instructions are output by print functions, because logger outputs
      // extract info (time, code position) that distracts users.
      System.out.println(
          "Please provide the query as an argument, enclosed by double quote. Use --help for instruction.");
      exit(1);
    }
    String mode = queryFixerOptions.getOptionValue(MODE);
    String outputFormat = queryFixerOptions.getOptionValue(OUTPUT);
    CommandLineInteraction interaction =
        CommandLineInteraction.create(mode, outputFormat, bigQueryOptions);
    interaction.interact(query);
  }

  /** Create the BigQueryOption based on user-input credentials path and project ID. */
  private static BigQueryOptions buildBigQueryOptions(String credentialsPath, String projectId) {
    if (projectId == null) {
      System.out.println("Project ID should not be null. Please provide it through the flag -p.");
      printHelpAndExit();
    }

    // If no credentials is provided, the program uses:
    // <ol>
    // <li> the default path in the Env variable: GOOGLE_APPLICATION_CREDENTIALS.
    // <li> Default credentials location (OS dependent). For example,
    // "~/.config/gcloud/application_default_credentials.json" in Linux. Usually, it should be same
    // path of the credentials you create through calling the command "gcloud auth
    // application-default login".
    // </ol>
    if (credentialsPath == null) {
      return BigQueryOptions.newBuilder().setProjectId(projectId).build();
    }

    File credentialsFile = new File(credentialsPath);
    GoogleCredentials credentials;
    try (FileInputStream serviceAccountStream = new FileInputStream(credentialsFile)) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
    } catch (IOException e) {
      System.out.println(
          "Cannot read the credentials from the flag -c. Please provide a correct credentials path with read permission.");
      exit(1);
      // The program will never reach to this point, but the compiler needs an extra RETURN to avoid compilation error.
      // The reason is that compiler does not recognize `exit` function as a breaking point.
      return null;
    }
    return BigQueryOptions.newBuilder().setProjectId(projectId).setCredentials(credentials).build();
  }
}
