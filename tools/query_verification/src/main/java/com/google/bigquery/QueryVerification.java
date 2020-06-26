package com.google.bigquery;

public class QueryVerification {

    private QVQuery migratedQuery;
    private QVSchema migratedSchema;

    private QVQuery originalQuery;
    private QVSchema originalSchema;

    public QueryVerification() {
    }

    public QueryVerification(QVQuery migratedQuery, QVSchema migratedSchema) {
        this.migratedQuery = migratedQuery;
        this.migratedSchema = migratedSchema;
    }

    public QueryVerification(QVQuery migratedQuery, QVSchema migratedSchema, QVQuery originalQuery, QVSchema originalSchema) {
        this.migratedQuery = migratedQuery;
        this.migratedSchema = migratedSchema;

        this.originalQuery = originalQuery;
        this.originalSchema = originalSchema;
    }

    public QVQuery getMigratedQuery() {
        return migratedQuery;
    }

    public void setMigratedQuery(QVQuery migratedQuery) {
        this.migratedQuery = migratedQuery;
    }

    public QVSchema getMigratedSchema() {
        return migratedSchema;
    }

    public void setMigratedSchema(QVSchema migratedSchema) {
        this.migratedSchema = migratedSchema;
    }

    public QVQuery getOriginalQuery() {
        return originalQuery;
    }

    public void setOriginalQuery(QVQuery originalQuery) {
        this.originalQuery = originalQuery;
    }

    public QVSchema getOriginalSchema() {
        return originalSchema;
    }

    public void setOriginalSchema(QVSchema originalSchema) {
        this.originalSchema = originalSchema;
    }

    /**
     * Determines which verification method to use based on provided inputs and prints the result of the query verification.
     */
    public void verify() {
        boolean useDataAwareVerification = originalQuery != null && originalSchema != null;
        boolean verificationResult;

        if (useDataAwareVerification) {
            verificationResult = dataAwareVerification();
        } else {
            verificationResult = dataFreeVerification();
        }

        System.out.printf("Data-%s Verification %s\n", useDataAwareVerification ? "Aware" : "Free", verificationResult ? "Succeeded" : "Failed");
    }

    public boolean dataFreeVerification() {
        // TODO Implement data free verification
        return false;
    }

    public boolean dataAwareVerification() {
        // TODO Implement data aware verification
        return false;
    }

}
