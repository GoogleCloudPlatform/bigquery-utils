package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment

private const val WITH_DELIMITER = "((?<=%1\$s)|(?=%1\$s))"

/**
 * Rates a [QueryFragment] a confidence value to indicate how likely it is to be a query.
 * @see rate
 */
class ConfidenceRater {
    private val keywords: Map<String, Int> = mapOf(
        "Add" to 1,
        "All" to 1,
        "Alter" to 1,
        "And" to 1,
        "Any" to 1,
        "As" to 1,
        "Asc" to 1,
        "Backup" to 1,
        "Between" to 1,
        "Case" to 1,
        "Check" to 1,
        "Column" to 1,
        "Constraint" to 1,
        "Create" to 1,
        "Database" to 1,
        "Default" to 1,
        "Delete" to 1,
        "Desc" to 1,
        "Distinct" to 1,
        "Drop" to 1,
        "Exec" to 1,
        "Exists" to 1,
        "Foreign" to 1,
        "From" to 1,
        "Full" to 1,
        "Group" to 1,
        "Having" to 1,
        "In" to 1,
        "Index" to 1,
        "Inner" to 1,
        "Insert" to 1,
        "Is" to 1,
        "Join" to 1,
        "Left" to 1,
        "Like" to 1,
        "Limit" to 1,
        "Not" to 1,
        "Null" to 1,
        "Or" to 1,
        "Order" to 1,
        "Outer" to 1,
        "Primary" to 1,
        "Procedure" to 1,
        "Right" to 1,
        "RowNum" to 1,
        "Select" to 1,
        "Set" to 1,
        "Table" to 1,
        "Top" to 1,
        "Truncate" to 1,
        "Union" to 1,
        "Unique" to 1,
        "Update" to 1,
        "Values" to 1,
        "View" to 1,
        "Where" to 1
    ).mapKeys { it.key.toUpperCase() }

    /**
     * Given [query], returns a confidence value within range [0.0, 1.0],
     * where 1.0 means it is most likely a SQL query
     */
    fun rate(query: QueryFragment): Double {
        val count = countSqlLikeTokens(query)
        if (count == 0) return 0.0
        return (count) / (count + 1.0)
    }

    /**
     * Count the number of SQL keywords in [query].
     * Keywords are weighted, and the return value can be higher than the exact count.
     */
    private fun countSqlLikeTokens(query: QueryFragment): Int {
        if (query.complex != null) {
            return query.complex.sumBy(this::countSqlLikeTokens)
        } else if (query.literal.isNullOrBlank()) {
            return 0
        }

        var count = 0
        for ((index, token) in tokenize(query.literal).withIndex()) {
            var localCount = keywords.getOrDefault(token.toUpperCase(), 0)
            if (index == 0 && localCount > 0) {
                localCount++
            }

            count += localCount
        }

        return count
    }

    /**
     * Splits [query] into non-empty tokens.
     * Whitespaces are removed, but symbols are returned as individual tokens.
     */
    private fun tokenize(query: String): Sequence<String> {
        return query.split(WITH_DELIMITER.format("[^\\w\\d]").toRegex()).asSequence()
            .filter { it.isNotBlank() }
    }
}