/**
 * Types of queries in SQL language encoding
 */
public enum QueryType {
	// TODO (Allen) : finish filling in all types with mapping from Google Sheet
	//  'SPECIAL' indicates node is one of ROOT, SINK, DDL, DML, DQL which will be added to another Enum type
	//  The Query class is empty at the moment, but it's possible that it will be refactored into something more general
	//  so that it can contain both information about QueryType and whether or not its a SPECIAL node
	SPECIAL,
	DDL_CREATE,
	DDL_PARTITION,
	DDL_CLUSTER,
	DDL_OPTIONS,
	DDL_AS,
	DML_INSERT,
	DML_DELETE,
	DML_WHERE,
	DML_UPDATE,
	DML_INTO,
	DML_VALUES,
	DML_SET,
	DML_FROM,
	DQL_WITH,
	DQL_SELECT,
	DQL_REPLACE,
	DQL_EXCEPT,
	DQL_AS,
	DQL_FROM,
	DQL_WHERE,
	DQL_LIMIT,
	DQL_ORDER,
}
