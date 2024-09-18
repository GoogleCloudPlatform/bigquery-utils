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
import java.util.Objects;

public class BQSimbaExample {
    private static HashMap<String, String> commandLineArgParser(String[] args) {
        HashMap<String, String> argMap = new HashMap<>();
        for (String arg : args){
            String[] kvPair = arg.split("=", 2);
            if (kvPair.length != 2) {
                throw new IllegalArgumentException(
                        String.format("Invalid CLI argument: %s. CLI args should be key-value pairs with the format key=value.", arg)
                );
            }
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
        // Parse CLI args
        HashMap<String, String> argMap = commandLineArgParser(args);
        String query = argMap.get("query");  // eg. "SELECT column1, column2 FROM mydataset.mytable;"
        if (query == null) {
            throw new IllegalArgumentException("The 'query' command line argument must be provided, eg. `query=SELECT id FROM mydataset.mytable;`");
        }

        // Get project and dataset values from CLI args, else use defaults configured
        String projectId = Objects.requireNonNullElse(argMap.get("projectId"), "my-billing-project");
        String additionalDataProjects = Objects.requireNonNullElse(argMap.get("additionalDataProjects"),"my-data-project");
        String defaultDataset = Objects.requireNonNullElse(argMap.get("defaultDataset"),"my-dataset");

        // Create OAuth config for Application Default Credentials.
        // For details on setting up ADC, refer to https://cloud.google.com/docs/authentication/provide-credentials-adc.
        OAuthUserConfig oAuthUserConfig = new OAuthUserConfig
                .Builder(OAuthUserType.APPLICATION_DEFAULT_CREDENTIALS, projectId)
                .build();

        // Set configuration options for the query. See the JDBC driver documentation for details (link in README.md).
        HashMap<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("AdditionalProjects", additionalDataProjects);
        configOptions.put("QueryProperties",String.format("dataset_project_id=%s", additionalDataProjects));
        configOptions.put("DefaultDataset", defaultDataset);
        configOptions.put("FilterTablesOnDefaultDataset", "1");

        // Create connection URL containing auth and options to use for the query.
        String connectionUrl = generateConnectionUrl(oAuthUserConfig, configOptions);
        System.out.println("JDBC connection URL: " + connectionUrl);

        // Create the connection, execute the query, and retrieve the results.
        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement stmt = conn.createStatement()) {
            System.out.println("Executing query: " + query);
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                // Print the first column of each row.
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to BigQuery: " + e.getMessage());
        }
    }
}
