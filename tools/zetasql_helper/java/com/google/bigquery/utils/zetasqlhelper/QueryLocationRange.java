package com.google.bigquery.utils.zetasqlhelper;

import com.google.zetasql.ParseLocationRangeProto;
import lombok.Value;

import java.util.Arrays;

/**
 * Range of a substring at a query. It represents the range using byte offsets, and the end byte
 * offset is excluded.
 */
@Value
public class QueryLocationRange {
    String query;
    int startByteOffset;
    int endByteOffset;

    public QueryLocationRange(String query, int startByteOffset, int endByteOffset) {
        this.query = query;
        this.startByteOffset = startByteOffset;
        this.endByteOffset = endByteOffset;
    }

    public QueryLocationRange(String query, ParseLocationRangeProto proto) {
        this(query, proto.getStart(), proto.getEnd());
    }

    @Override
    public String toString() {
        byte[] bytes = query.getBytes();
        byte[] subBytes = Arrays.copyOfRange(bytes, startByteOffset, endByteOffset);
        return new String(subBytes);
    }
}
