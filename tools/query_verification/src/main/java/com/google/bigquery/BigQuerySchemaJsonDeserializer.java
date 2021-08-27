package com.google.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.gson.*;

import java.lang.reflect.Type;

/**
 * Class to deserialize the JSON schema to BigQuery fields
 */
public class BigQuerySchemaJsonDeserializer implements JsonDeserializer<FieldList> {

    @Override
    public FieldList deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonArray fieldsArray = jsonElement.getAsJsonArray();

        for (JsonElement fieldElement : fieldsArray) {
            JsonObject fieldObject = fieldElement.getAsJsonObject();

            // Field class stores types as LegacySQLTypeName instead of StandardSQLTypeName
            if (fieldObject.has("type")) {
                // Convert standard type to legacy type
                StandardSQLTypeName standardType = StandardSQLTypeName.valueOf(fieldObject.get("type").getAsString());
                LegacySQLTypeName legacyType = LegacySQLTypeName.legacySQLTypeName(standardType);

                // Insert LegacySQLTypeName object so it can be used for type
                JsonObject typeObject = new JsonObject();
                typeObject.addProperty("constant", legacyType.name());
                fieldObject.add("type", typeObject);
            }

            // Field class uses subFields instead of fields for STRUCT/RECORD type
            if (fieldObject.has("fields")) {
                fieldObject.add("subFields", fieldObject.get("fields"));
                fieldObject.remove("fields");
            }
        }

        Field[] fields = jsonDeserializationContext.deserialize(fieldsArray, Field[].class);

        return FieldList.of(fields);
    }

}
