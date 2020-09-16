package com.google.bigquery.utils.zetasqlhelper;

import com.google.zetasql.ParseLocationRangeProto;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Range of q function. It includes the ranges of function body, name, and its arguments.
 */
@Getter
public class QueryFunctionRange {
    private final QueryLocationRange function;
    private final QueryLocationRange name;
    private final List<QueryLocationRange> arguments;

    public QueryFunctionRange(String query, ParseLocationRangeProto functionRangeProto,
                              ParseLocationRangeProto nameRangeProto,
                              List<ParseLocationRangeProto> argumentRangeProtos) {
        function = new QueryLocationRange(query, functionRangeProto);
        name = new QueryLocationRange(query, nameRangeProto);
        arguments = argumentRangeProtos.stream()
                .map(proto -> new QueryLocationRange(query, proto))
                .collect(Collectors.toList());
    }
}
