package com.google.bigquery;

import com.google.auto.value.AutoValue;
import com.google.gson.JsonArray;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import javax.annotation.Nullable;

/**
 * Value class for schema. Contains the contents and path of the schema file.
 */
@AutoValue
public abstract class QueryVerificationSchema {

    public abstract String schema();
    public abstract String path();

    public static QueryVerificationSchema create(String schema, String path) {
        return new AutoValue_QueryVerificationSchema(schema, path);
    }

    public boolean isInJsonFormat() {
        return getJsonArray() != null;
    }

    /**
     * Attempts to parse schema as a JSON object
     * @return JSON object of schema if possible
     */
    @Nullable
    public JsonArray getJsonArray() {
        try {
            JsonArray jsonArray = JsonParser.parseString(schema()).getAsJsonArray();
            return jsonArray;
        } catch (JsonParseException e) {
            return null;
        }
    }

}