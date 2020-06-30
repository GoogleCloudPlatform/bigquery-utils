/**
 * Types of queries in SQL language encoding
 */
public enum QueryType {
    QUERY_ROOT,
    DDL,
    DML,
    DQL,
    DDL_CREATE,
    DDL_PARTITION,
    DDL_CLUSTER,
    DDL_OPTIONS,
    DDL_AS,
    SINK;
    // TODO: hardcode all or import from google spreadsheet
}
