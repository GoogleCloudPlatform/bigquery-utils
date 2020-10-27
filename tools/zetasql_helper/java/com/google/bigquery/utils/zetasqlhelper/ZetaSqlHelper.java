package com.google.bigquery.utils.zetasqlhelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A client to call functions from ZetaSQL Helper server through RPC.
 */
public class ZetaSqlHelper {

    /**
     * Call the "tokenize" method from ZetaSQL Helper server.
     *
     * @param query query to be tokenized
     * @return a list of ZetaSQL tokens
     */
    public static List<Token> tokenize(String query) {
        LocalService.TokenizeRequest request = LocalService.TokenizeRequest.newBuilder()
                .setQuery(query)
                .build();

        LocalService.TokenizeResponse response = Client.getStub().tokenize(request);
        return response.getParseTokensList().stream()
                .map(Token::fromPb)
                .collect(Collectors.toList());
    }

    /**
     * Get all the ZetaSQL keywords from ZetaSQL Helper server.
     *
     * @return all ZetaSQL keywords
     */
    public static List<String> getAllKeywords() {
        LocalService.GetAllKeywordsRequest request = LocalService.GetAllKeywordsRequest.newBuilder()
                .build();

        LocalService.GetAllKeywordsResponse response = Client.getStub().getAllKeywords(request);
        return response.getKeywordsList();
    }

    /**
     * Locate the ranges of all tables meeting the input regular expression.
     * The ranges are presented by the start and end byte offsets (UTF-8 based) of the input query, and end
     * offset is excluded (i.e [start, end)).
     *
     * @param query      the query where the tables will be matched
     * @param tableRegex regex to match tables
     * @return a list of table ranges.
     */
    public static List<QueryLocationRange> locateTableRanges(String query, String tableRegex) {
        LocalService.LocateTableRangesRequest request = LocalService.LocateTableRangesRequest.newBuilder()
                .setQuery(query)
                .setTableRegex(tableRegex)
                .build();

        LocalService.LocateTableRangesResponse response = Client.getStub().locateTableRanges(request);
        return response.getTableRangesList().stream()
                .map(proto -> new QueryLocationRange(query, proto))
                .collect(Collectors.toList());
    }

    /**
     * Extract the range of a function given the starting position of the function. The range is presented by the
     * start and end byte offsets (UTF-8 based) of the input query, and end offset is excluded (i.e [start, end)).
     *
     * @param query  query to look for the function.
     * @param row    row number of the starting position of a function
     * @param column column number of the starting position of a function
     * @return function range
     */
    public static QueryFunctionRange extractFunctionRange(String query, int row, int column) {
        LocalService.ExtractFunctionRangeRequest request = LocalService.ExtractFunctionRangeRequest.newBuilder()
                .setQuery(query)
                .setLineNumber(row)
                .setColumnNumber(column)
                .build();

        LocalService.ExtractFunctionRangeResponse response = Client.getStub().extractFunctionRange(request);
        return new QueryFunctionRange(query,
                response.getFunctionRange().getFunction(),
                response.getFunctionRange().getName(),
                response.getFunctionRange().getArgumentsList());
    }

    /**
     * Fix a query with "Column not Grouped" error given the name of ungrouped column and its position.
     *
     * @param query         query with "Column not Grouped" error
     * @param missingColumn name of the ungrouped column
     * @param row           row number of the ungrouped column
     * @param column        column number of the ungrouped column
     * @return fixed query
     */
    public static String fixColumnNotGrouped(String query,
                                             String missingColumn,
                                             int row,
                                             int column) {
        LocalService.FixColumnNotGroupedRequest request = LocalService.FixColumnNotGroupedRequest.newBuilder()
                .setQuery(query)
                .setLineNumber(row)
                .setColumnNumber(column)
                .setMissingColumn(missingColumn)
                .build();

        LocalService.FixColumnNotGroupedResponse response = Client.getStub().fixColumnNotGrouped(request);
        return response.getFixedQuery();
    }

    /**
     * Fix a query with "Duplicate Columns" error given the name of duplicate columns.
     *
     * @param query           query with "Duplicate Columns" error
     * @param duplicateColumn name of duplicate columns
     * @return fixed query
     */
    public static String fixDuplicateColumns(String query, String duplicateColumn) {
        LocalService.FixDuplicateColumnsRequest request = LocalService.FixDuplicateColumnsRequest.newBuilder()
                .setQuery(query)
                .setDuplicateColumn(duplicateColumn)
                .build();

        LocalService.FixDuplicateColumnsResponse response = Client.getStub().fixDuplicateColumns(request);
        return response.getFixedQuery();
    }
}
