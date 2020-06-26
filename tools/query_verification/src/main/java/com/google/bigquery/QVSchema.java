package com.google.bigquery;

public class QVSchema {

    private String schema;
    private String path;

    public QVSchema(String schema, String path) {
        this.schema = schema;
        this.path = path;
    }

    public String getSchema() {
        return schema;
    }

    public String getPath() {
        return path;
    }
}
