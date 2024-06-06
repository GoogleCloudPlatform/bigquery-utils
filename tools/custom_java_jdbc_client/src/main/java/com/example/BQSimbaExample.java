/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class BQSimbaExample {
    private static HashMap<String, String> commandLineArgParser(String[] args) {
        HashMap<String, String> argMap = new HashMap<>();
        for (String arg : args){
            String[] kvPair = arg.split("=");
            argMap.put(kvPair[0], kvPair[1]);
        }
        return argMap;
    }

    private static String generateConnectionUrl(OAuthUserConfig oAuthConfig, HashMap<String, String> configOptions) {
        // Build the provided authentication and query options into the JDBC connection URL.
        StringBuilder connUrlBuilder = new StringBuilder();
        connUrlBuilder.append("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;");

        for (Map.Entry<String, String> userConfig : oAuthConfig.getOAuthUserConfigMap().entrySet()) {
            connUrlBuilder.append(String.format("%s=%s;", userConfig.getKey(), userConfig.getValue()));
        }
        for (Map.Entry<String, String> option : configOptions.entrySet()) {
            connUrlBuilder.append(String.format("%s=%s;", option.getKey(), option.getValue()));
        }
        return connUrlBuilder.toString();
    }

    public static void main(String[] args) {
//        HashMap<String, String> argMap = commandLineArgParser(args);  // If args input via command line flags is preferred.

        // Set default values.
        String projectId = "my-billing-project";  // Used as billing project
        String additionalDataProjects = "my-data-project";
        String datasetId = "my-dataset";
        String tableId = "my-table";

        // Set authentication configuration.
        String serviceAccountEmail = "my-service-account@my-project.iam.gserviceaccount.com";
        String serviceAccountFile = "/Path/to/service-account-keyfile.json";
                // argMap.get("serviceAccountFilePath");
                // "$HOME/.config/gcloud/application_default_credentials.json";

        OAuthUserConfig oAuthUserConfig = new OAuthUserConfig
                .Builder(OAuthUserType.SERVICE_ACCOUNT, projectId)
                .setServiceAccountEmail(serviceAccountEmail)
                .setKeyFilePath(serviceAccountFile)
                .build();

        // Set configuration options for the query. See the JDBC driver documentation for details (link in README.md).
        HashMap<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("AdditionalProjects", additionalDataProjects);
        configOptions.put("QueryProperties",String.format("dataset_project_id=%s", additionalDataProjects));
        configOptions.put("DefaultDataset", datasetId);
        configOptions.put("FilterTablesOnDefaultDataset", "1");

        // Create connection URL containing auth and options to use for the query.
        String connectionUrl = generateConnectionUrl(oAuthUserConfig, configOptions);
        System.out.println("connectionUrl: " + connectionUrl);

        // Create the connection, execute the query, and retrieve the results.
        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement stmt = conn.createStatement()) {

            String query = String.format("SELECT * FROM `%s.%s` LIMIT 5", datasetId, tableId);
            System.out.println("Executing query: " + query);
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to BigQuery: " + e.getMessage());
        }
    }
}
