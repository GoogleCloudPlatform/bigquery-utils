/**
 * Types of queries in SQL language encoding
 * feature_root, ddl_feature_root, dml_feature_root, dql_feature_root, feature_sink are types to help create reference nodes
 */
public enum FeatureType {
	FEATURE_ROOT, // type for the root node
	DDL_FEATURE_ROOT, // type for the root node of all ddl features
	DML_FEATURE_ROOT, // type for the root node of all dml features
	DQL_FEATURE_ROOT, // type for the root node of all dql features
	FEATURE_SINK, // sink for all features, signals the end of a query
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
