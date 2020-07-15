package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment

private const val WITH_DELIMITER = "((?<=%1\$s)|(?=%1\$s))"

/**
 * Rates a [QueryFragment] a confidence value to indicate how likely it is to be a query.
 * @see rate
 */
class ConfidenceRater {
    private val keywords: Set<String> = sequenceOf(
        "Add",
        "All",
        "Alter",
        "And",
        "Any",
        "As",
        "Asc",
        "Backup",
        "Between",
        "Case",
        "Check",
        "Column",
        "Constraint",
        "Create",
        "Database",
        "Default",
        "Delete",
        "Desc",
        "Distinct",
        "Drop",
        "Exec",
        "Exists",
        "Foreign",
        "From",
        "Full",
        "Group",
        "Having",
        "In",
        "Index",
        "Inner",
        "Insert",
        "Is",
        "Join",
        "Left",
        "Like",
        "Limit",
        "Not",
        "Null",
        "Or",
        "Order",
        "Outer",
        "Primary",
        "Procedure",
        "Right",
        "RowNum",
        "Select",
        "Set",
        "Table",
        "Top",
        "Truncate",
        "Union",
        "Unique",
        "Update",
        "Values",
        "View",
        "Where"
    ).map { it.toUpperCase() }.toSet()

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
            var localCount = if (token.toUpperCase() in keywords) 1 else 0
            
            // give more points for keywords found at the start of the query
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