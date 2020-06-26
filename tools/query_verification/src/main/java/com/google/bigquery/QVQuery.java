package com.google.bigquery;

public class QVQuery {

    private String query;
    private String path;

    public QVQuery(String query, String path) {
        this.query = query;
        this.path = path;
    }

    public String getQuery() {
        return query;
    }

    public String getPath() {
        return path;
    }

}