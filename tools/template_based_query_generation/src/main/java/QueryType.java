/**
 * Types of queries in SQL language encoding
 * ROOT, DDL, DML, DQL are special and will be indicated in the Query class
 */
public enum QueryType {
	ROOT,
	DDL,
	DML,
	DQL,
	DDL_CREATE,
	DDL_PARTITION,
	DDL_CLUSTER,
	DDL_AS,
	DML_INSERT,
	DML_DELETE,
	DML_WHERE,
	DML_UPDATE,
	DML_SET,
	DQL_SELECT,
	DQL_FROM,
	DQL_WHERE,
	DQL_GROUP,
	DQL_HAVING,
	DQL_ORDER,
	DQL_LIMIT,
	DQL_OFFSET,
}
