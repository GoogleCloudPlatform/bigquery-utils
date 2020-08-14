package com.google.bigquery;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to communicate with TD to create tables and run queries using the JDBC driver.
 */
public class TeradataManager implements DataWarehouseManager {

    private final QueryVerificationQuery query;
    private final QueryVerificationSchema schema;
    private final List<QueryVerificationData> data;

    private Statement statementConnection;
    private Connection csvConnection;

    public TeradataManager(QueryVerificationQuery query, QueryVerificationSchema schema, List<QueryVerificationData> data) {
        this.query = query;
        this.schema = schema;
        this.data = data;
    }

    @Override
    public String getName() {
        return "Teradata";
    }

    /**
     * Sends query jobs to TD
     * @return List of query results
     * @throws Exception
     */
    @Override
    public List<QueryJobResults> runQueries() throws Exception {
        setupConnection();

        List<String> tables = createTablesFromSchema();
        populateTablesFromData();

        // Create query jobs
        List<String> statements = getStatementsFromQuery();

        // Store results from every job
        List<QueryJobResults> jobResults = new ArrayList<QueryJobResults>();

        for (int i = 0; i < statements.size(); i++) {
            String statement = statements.get(i);

            QueryJobResults jobResult;
            try {
                // Run query job
                ResultSet resultSet = statementConnection.executeQuery(statement);

                // Parse and store query results
                List<List<String>> rawResults = new ArrayList<List<String>>();
                Set<List<Object>> results = new HashSet<List<Object>>();
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                while (resultSet.next()) {
                    List<String> rowRawResults = new ArrayList<String>();

                    // Result set columns start at 1 instead of 0
                    for (int j = 1; j <= resultSetMetaData.getColumnCount(); j++) {
                        rowRawResults.add(resultSet.getString(j));
                    }
                    rawResults.add(rowRawResults);
                    results.add(parseResults(resultSet, resultSetMetaData));
                }

                jobResult = QueryJobResults.create(statement, query, null, results, rawResults);
            } catch (SQLException e) {
                // Print out errors returned from TD
                jobResult = QueryJobResults.create(statement, query, e.getMessage(), null, null);
            }

            // Store results
            jobResults.add(jobResult);
        }

        // Clear tables created
        tables.forEach(this::deleteTable);

        closeConnection();

        return jobResults;
    }

    /**
     * Reads connection properties to TD database from config file
     * @return databaseServerName, username, password
     */
    private String[] getConnectionPropertiesFromConfigFile() throws IOException {
        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("config.json");
        String configContents = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
        inputStream.close();

        JsonObject configJson = JsonParser.parseString(configContents).getAsJsonObject();
        JsonObject tdJsonObject = configJson.getAsJsonObject(getName().toLowerCase());

        String[] connectionProperties = new String[3];
        connectionProperties[0] = tdJsonObject.get("databaseServerName").getAsString();
        connectionProperties[1] = tdJsonObject.get("username").getAsString();
        connectionProperties[2] = tdJsonObject.get("password").getAsString();

        if (connectionProperties[0].isEmpty()) {
            throw new IllegalArgumentException("Please enter the " + getName() + " database server name and credentials in config.json");
        }

        return connectionProperties;
    }

    /**
     * Establishes a connection to the TD database
     */
    private void setupConnection() throws IOException {
        try {
            String[] connectionProperties = getConnectionPropertiesFromConfigFile();
            String url = String.format("jdbc:teradata://%s/TMODE=ANSI,CHARSET=UTF8", connectionProperties[0]);
            String user = connectionProperties[1];
            String password = connectionProperties[2];

            // Create connection for running queries
            Connection connection = DriverManager.getConnection(url, user, password);
            statementConnection = connection.createStatement();

            // Create connection for uploading CSV files
            csvConnection = DriverManager.getConnection(url + ",TYPE=FASTLOADCSV", user, password);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Closes the connection to the TD database
     */
    private void closeConnection() {
        try {
            statementConnection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public List<String> getStatementsFromQuery() {
        // TODO Account for edge case where semicolon could be inside statement
        return Arrays.stream(query.query().split(";")).map(String::trim).filter(statement -> !statement.isEmpty()).collect(Collectors.toList());
    }

    /**
     * Creates TD tables based on the provided schema
     * @return List of newly created tables
     */
    public List<String> createTablesFromSchema() throws IllegalArgumentException {
        List<String> tables = new ArrayList<String>();

        if (schema != null) {
            String ddlSchema;
            if (schema.isInJsonFormat()) {
                // Schema is JSON
                ddlSchema = String.join(";\n", generateDdlStatementsFromJsonSchema());
            } else {
                // Schema is DDL
                ddlSchema = schema.schema();
            }
            try {
                statementConnection.executeUpdate(ddlSchema);
            } catch (SQLException e) {
                throw new IllegalArgumentException(schema.path() + " is not correctly formatted. " + e.getMessage());
            }

            tables = getTablesFromDdlSchema(ddlSchema);

            if (tables.isEmpty()) {
                throw new IllegalArgumentException(schema.path() + " is not correctly formatted.");
            }
        }

        return tables;
    }

    /**
     * Reads JSON schema to generate equivalent DDL statements
     * @return List of DDL statements
     */
    public List<String> generateDdlStatementsFromJsonSchema() {
        List<String> ddlStatements = new ArrayList<String>();
        for (JsonElement schemaElement : schema.getJsonArray()) {
            if (schema.getJsonArray().size() == 0) {
                return null;
            }
            JsonObject schemaObject = schemaElement.getAsJsonObject();

            if (schemaObject.has("tableReference")) {
                JsonObject tableReference = schemaObject.get("tableReference").getAsJsonObject();

                if (tableReference.has("datasetId") && tableReference.has("tableId")) {
                    StringBuilder statement = new StringBuilder("CREATE TABLE ");

                    statement.append(tableReference.get("datasetId").getAsString());
                    statement.append(".");
                    statement.append(tableReference.get("tableId").getAsString());
                    statement.append(" (");

                    // Generate column syntax for each field provided in JSON
                    List<String> columns = new ArrayList<>();
                    for (JsonElement fieldElement : schemaObject.getAsJsonArray("fields")) {
                        JsonObject field = fieldElement.getAsJsonObject();

                        // Assemble column syntax
                        StringBuilder column = new StringBuilder();
                        column.append(field.get("name").getAsString());
                        column.append(" ");
                        column.append(field.get("type").getAsString());
                        if (field.has("mode")) {
                            column.append(" ");
                            column.append(field.get("mode").getAsString());
                        }

                        columns.add(column.toString());
                    }
                    statement.append(String.join(", ", columns));
                    statement.append(")");

                    ddlStatements.add(statement.toString());
                }
            }
        }
        return ddlStatements;
    }

    /**
     * Read DDL schema to identify tables being created
     * @return List of new table ids
     */
    public List<String> getTablesFromDdlSchema(String schemaContents) {
        List<String> tables = new ArrayList<String>();

        // Separate DDL schema into statements
        // TODO Account for edge case where semicolon could be inside statement
        String[] statements = schemaContents.split(";");

        for (String statement : statements) {
            statement = statement.trim().replaceAll("\\s+", " ");

            // Obtain table name
            if (statement.toUpperCase().startsWith("CREATE TABLE")) {
                String[] schema = statement.split(" ");
                if (schema.length >= 3) {
                    tables.add(schema[2]);
                }
            }
        }

        return tables;
    }

    /**
     * Converts each value of query results to Java objects based on the field's type
     * @param values from query results
     * @param metadata from the schema of the results
     * @return List of objects parsed from query results
     */
    private List<Object> parseResults(ResultSet values, ResultSetMetaData metadata) throws SQLException {
        List<Object> results = new ArrayList<Object>();

        // Result set columns start at 1 instead of 0
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            Object result;
            int type = metadata.getColumnType(i);

            switch (type) {
                case Types.BOOLEAN:
                    result = values.getBoolean(i);
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    result = values.getBigDecimal(i).setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR);
                    break;
                case Types.FLOAT:
                    result = BigDecimal.valueOf(values.getFloat(i)).setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR);
                    break;
                case Types.DOUBLE:
                    result = BigDecimal.valueOf(values.getDouble(i)).setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR);
                    break;
                case Types.SMALLINT:
                case Types.BIGINT:
                case Types.INTEGER:
                    result = values.getLong(i);
                    break;
                // TODO Add support for Types.STRUCT
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    result = values.getDate(i);
                    break;
                case Types.VARCHAR:
                    result = values.getString(i);
                    break;
                default:
                    // Handle unknown/unsupported types as String
                    System.err.println("Warning: Unsupported type: " + metadata.getColumnTypeName(i));
                    result = values.getString(i);
            }

            results.add(result);
        }

        return results;
    }

    /**
     * Populates TD tables based on the provided table data.
     */
    private void populateTablesFromData() {
        for (QueryVerificationData queryVerificationData : data) {
            String tableId = queryVerificationData.datasetName() + "." + queryVerificationData.tableName();

            try {
                // Clear out error tables
                statementConnection.executeUpdate("DROP TABLE " + tableId + "_ERR_1;");
                statementConnection.executeUpdate("DROP TABLE " + tableId + "_ERR_2;");
            } catch (SQLException e) {
                e.printStackTrace();
            }

            String csvContents = queryVerificationData.contents();

            // Identifies how many columns of data are provideed
            String firstRow = csvContents.split("\n")[0];
            int columns = firstRow.length() - firstRow.replace(",", "").length() + 1;

            // Assemble placeholders (question marks) in the INSERT INTO statement for each column
            String[] columnPlaceholders = new String[columns];
            Arrays.fill(columnPlaceholders, "?");
            String columnValues = String.join(",", columnPlaceholders);

            String insertIntoStatement = "INSERT INTO " + tableId + " VALUES(" + columnValues + ");";

            // Workaround since Fastload CSV utility doesn't include first and last lines of CSV files
            csvContents = columnValues + "\n" + csvContents;
            if (!csvContents.endsWith("\n")) {
                csvContents += "\n";
            }

            // Run INSERT INTO statement
            try {
                PreparedStatement insertCsv = csvConnection.prepareStatement(insertIntoStatement);
                InputStream inputStream = new ByteArrayInputStream(csvContents.getBytes());
                insertCsv.setAsciiStream(1, inputStream, -1);
                insertCsv.executeUpdate();
                insertCsv.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Deletes tables after running queries
     * @param table
     */
    private void deleteTable(String table) {
        String dropStatement = "DROP TABLE " + table + ";";
        try {
            statementConnection.executeUpdate(dropStatement);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
