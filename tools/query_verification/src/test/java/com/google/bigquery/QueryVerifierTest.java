package com.google.bigquery;

import com.google.cloud.bigquery.*;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueryVerifierTest {

    final String resourcesPath = "src/test/resources/";

    @Test
    public void testGetTableIdFromJsonSchema() {
        String schemaContents = Main.getContentsOfFile(resourcesPath + "schema1.json");
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        TableInfo tableInfo = QueryVerifier.getTableInfoFromJsonSchema(schema);
        TableId tableId = tableInfo.getTableId();

        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "table");
    }

    @Test
    public void testGetFieldsFromJsonSchema() {
        String schemaContents = Main.getContentsOfFile(resourcesPath + "schema2.json");
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        TableInfo tableInfo = QueryVerifier.getTableInfoFromJsonSchema(schema);
        FieldList fieldList = tableInfo.getDefinition().getSchema().getFields();

        assertEquals(fieldList.size(), 3);

        Field.Builder builder;

        builder = Field.newBuilder("stringField", StandardSQLTypeName.STRING);
        builder.setMode(Field.Mode.NULLABLE);
        assertEquals(fieldList.get(0), builder.build());

        builder = Field.newBuilder("integerField", StandardSQLTypeName.INT64);
        builder.setMode(Field.Mode.REQUIRED);
        builder.setDescription("This field stores integers.");
        assertEquals(fieldList.get(1), builder.build());

        Field subField = Field.of("integerField", StandardSQLTypeName.INT64);
        builder = Field.newBuilder("structField", StandardSQLTypeName.STRUCT, FieldList.of(subField));
        builder.setMode(Field.Mode.REPEATED);
        assertEquals(fieldList.get(2), builder.build());
    }

}
