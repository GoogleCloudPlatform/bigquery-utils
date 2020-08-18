package com.google.bigquery;

import com.google.cloud.bigquery.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class BigQueryManagerTest {

    final String resourcesPath = "src/test/resources/";

    @Test
    public void testGetTableIdFromJsonSchema() {
        String schemaContents = Main.getContentsOfFile(resourcesPath + "schema1.json");
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        List<TableInfo> tableInfos = new BigQueryManager(null, schema, null, null).getTableInfoFromJsonSchema();

        assertEquals(tableInfos.size(), 1);
        TableId tableId = tableInfos.get(0).getTableId();

        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "table");
    }

    @Test
    public void testGetMultipleTableIdFromJsonSchema() {
        String schemaContents = Main.getContentsOfFile(resourcesPath + "schema3.json");
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        List<TableInfo> tableInfos = new BigQueryManager(null, schema, null, null).getTableInfoFromJsonSchema();

        assertEquals(tableInfos.size(), 2);
        TableId tableId;

        tableId = tableInfos.get(0).getTableId();
        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "firstTable");

        tableId = tableInfos.get(1).getTableId();
        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "secondTable");
    }

    @Test
    public void testGetFieldsFromJsonSchema() {
        String schemaContents = Main.getContentsOfFile(resourcesPath + "schema2.json");
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        List<TableInfo> tableInfos = new BigQueryManager(null, schema, null, null).getTableInfoFromJsonSchema();

        assertEquals(tableInfos.size(), 1);
        FieldList fieldList = tableInfos.get(0).getDefinition().getSchema().getFields();

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

    @Test
    public void testGetTableIdFromDdlSchema() {
        String schemaContents = "CREATE TABLE dataset.table (stringField STRING, integerField INT64);";
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        List<TableId> tableIds = new BigQueryManager(null, schema, null, null).getTableIdsFromDdlSchema();

        assertEquals(tableIds.size(), 1);
        TableId tableId = tableIds.get(0);

        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "table");
    }

    @Test
    public void testGetMultipleTableIdFromDdlSchema() {
        String schemaContents = "CREATE TABLE dataset.firstTable (stringField STRING, integerField INT64);\n" +
                "CREATE TABLE dataset.secondTable (stringField STRING, integerField INT64);";
        QueryVerificationSchema schema = QueryVerificationSchema.create(schemaContents, "");
        List<TableId> tableIds = new BigQueryManager(null, schema, null, null).getTableIdsFromDdlSchema();

        assertEquals(tableIds.size(), 2);
        TableId tableId;

        tableId = tableIds.get(0);
        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "firstTable");

        tableId = tableIds.get(1);
        assertEquals(tableId.getDataset(), "dataset");
        assertEquals(tableId.getTable(), "secondTable");
    }

    @Test
    public void testGetJobInfoFromQuery() {
        String queryContents = "SELECT * FROM table";
        QueryVerificationQuery query = QueryVerificationQuery.create(queryContents, "");
        List<JobInfo> jobInfos = new BigQueryManager(query, null, null, null).getJobInfosFromQuery(true);

        assertEquals(jobInfos.size(), 1);

        QueryJobConfiguration queryJobConfiguration = jobInfos.get(0).getConfiguration();
        assertEquals(queryJobConfiguration.getQuery(), "SELECT * FROM table");
    }

    @Test
    public void testGetMultipleJobInfosFromQuery() {
        String queryContents = "SELECT * FROM table1;\nSELECT column1 FROM table2; SELECT column2 FROM table2;";
        QueryVerificationQuery query = QueryVerificationQuery.create(queryContents, "");
        List<JobInfo> jobInfos = new BigQueryManager(query, null, null, null).getJobInfosFromQuery(true);

        assertEquals(jobInfos.size(), 3);

        List<String> queries = jobInfos.stream().map(jobInfo -> {
            QueryJobConfiguration queryJobConfiguration = jobInfo.getConfiguration();
            return queryJobConfiguration.getQuery();
        }).collect(Collectors.toList());

        assertEquals(queries.get(0), "SELECT * FROM table1");
        assertEquals(queries.get(1), "SELECT column1 FROM table2");
        assertEquals(queries.get(2), "SELECT column2 FROM table2");
    }

    @Test
    public void testParseResults() throws ParseException {
        BigQueryManager bigQueryManager = new BigQueryManager(null, null, null, null);

        Map<StandardSQLTypeName, String> types = new LinkedHashMap<StandardSQLTypeName, String>();
        types.put(StandardSQLTypeName.BOOL, "true");
        types.put(StandardSQLTypeName.FLOAT64, "2.25");
        types.put(StandardSQLTypeName.INT64, "10");
        types.put(StandardSQLTypeName.NUMERIC, "3.333333333");
        types.put(StandardSQLTypeName.DATE, "2020-01-01");
        types.put(StandardSQLTypeName.DATETIME, "2020-01-01T12:00:00.000000");
        types.put(StandardSQLTypeName.TIME, "12:00:00.000000");
        types.put(StandardSQLTypeName.TIMESTAMP, "2020-01-01 12:00:00.000000 UTC");
        types.put(StandardSQLTypeName.STRING, "value");

        Date date = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSSSS zzz").parse("2020-01-01 12:00:00.000000 UTC");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeFormat = new SimpleDateFormat("hh:mm:ss.SSSSSS");

        // Test individual data types

        FieldValueList values = FieldValueList.of(types.values().stream().map(value -> FieldValue.of(FieldValue.Attribute.PRIMITIVE, value)).collect(Collectors.toList()));
        FieldList fields = FieldList.of(types.keySet().stream().map(type -> Field.newBuilder(type.name(), type).build()).collect(Collectors.toList()));

        List<Object> results = bigQueryManager.parseResults(values, fields);

        assertEquals(results.size(), 9);
        assertEquals(results.get(0), true);
        assertEquals(results.get(1), BigDecimal.valueOf(2.25f).setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR));
        assertEquals(results.get(2), 10L);
        assertEquals(results.get(3), new BigDecimal("3.333333333").setScale(QueryVerifier.DECIMAL_PRECISION, RoundingMode.FLOOR));
        assertEquals(dateFormat.format(results.get(4)), dateFormat.format(date));
        assertEquals(results.get(5), date);
        assertEquals(timeFormat.format(results.get(6)), timeFormat.format(date));
        assertEquals(results.get(7), date);
        assertEquals(results.get(8), "value");

        // Test struct data type

        FieldValueList structValues = FieldValueList.of(Arrays.asList(FieldValue.of(FieldValue.Attribute.REPEATED, values)));
        FieldList structFields = FieldList.of(Arrays.asList(Field.newBuilder("STRUCT", StandardSQLTypeName.STRUCT, fields).build()));

        List<Object> structResults = bigQueryManager.parseResults(structValues, structFields);
        assertEquals(structResults.size(), 1);
        assertEquals(structResults.get(0), results);
    }

}
