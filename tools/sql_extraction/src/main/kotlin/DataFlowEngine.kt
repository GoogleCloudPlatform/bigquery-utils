package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.Location
import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment
import com.google.cloud.sqlecosystem.sqlextraction.output.QueryUsages

/**
 * Backend engine for running data-flow analysis
 * @see FrontEnd
 */
class DataFlowEngine {
    private val queryUsages: HashMap<QueryFragment, HashSet<Location>> = HashMap()

    /**
     * Returns all query fragments and usages found by the engine thus far
     */
    fun getAllQueries(): Sequence<QueryUsages> {
        return queryUsages.asSequence()
            .filter { it.value.isNotEmpty() }
            .map { QueryUsages(it.key, it.value.sorted()) }
    }

    private fun registerUsage(usage: Location, query: QueryFragment) {
        queryUsages.computeIfAbsent(query) { HashSet() }.add(usage)
    }
}