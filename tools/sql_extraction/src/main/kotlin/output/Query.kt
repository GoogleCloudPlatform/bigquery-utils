package com.google.cloud.sqlecosystem.sqlextraction.output

data class Query(
    val file: String,
    val confidence: Double,
    val query: QueryFragment,
    val usages: List<Location>,
    val notes: String? = null
)

data class QueryUsages(
    val query: QueryFragment,
    val usages: List<Location>
)