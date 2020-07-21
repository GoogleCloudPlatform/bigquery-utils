/**
 * Types of queries in SQL language encoding
 * feature_root, ddl_feature_root, dml_feature_root, dql_feature_root, feature_sink are types to help create reference nodes
 */
public enum FeatureType {
	feature_root, // type for the root node
	ddl_feature_root, // type for the root node of all ddl features
	dml_feature_root, // type for the root node of all dml features
	dql_feature_root, // type for the root node of all dql features
	feature_sink, // sink for all features, signals the end of a query
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
