package com.google.bigquery;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TeradataManagerTest {

    final String resourcesPath = "src/test/resources/";

    final String sampleDdl = "CREATE TABLE dataset.table (stringField VARCHAR(255), integerField BIGINT)";

    @Test
    public void testGenerateDdlStatementsFromJsonSchema() {
        String schemaContents = Main.getContentsOfFile(resourcesPath + "schema4.json");
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        List<String> ddlStatements = new TeradataManager(null, schema, null).generateDdlStatementsFromJsonSchema();

        assertEquals(ddlStatements.size(), 1);
        assertEquals(ddlStatements.get(0), sampleDdl);
    }

    @Test
    public void testGetTablesFromDdlSchema() {
        QueryVerificationSchema schema = QueryVerificationSchema.create(sampleDdl, "");
        List<String> ddlStatements = new TeradataManager(null, schema, null).getTablesFromDdlSchema(schema.schema());

        assertEquals(ddlStatements.size(), 1);
        assertEquals(ddlStatements.get(0), "dataset.table");
    }

}
