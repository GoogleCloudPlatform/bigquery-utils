package com.google.bigquery;

public class BigQueryInstance {

    private String query;
    private String schema;

    public BigQueryInstance(String query) {
        this.query = query;
    }

    public BigQueryInstance(String query, String schema) {
        this.query = query;
        this.schema = schema;
    }

    public boolean dryRun() {
        // TODO Send query to BQ as dry run
        return false;
    }

}