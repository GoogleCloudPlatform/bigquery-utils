package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.Location
import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment
import com.google.cloud.sqlecosystem.sqlextraction.output.QueryUsages
import mu.KotlinLogging

private val LOGGER = KotlinLogging.logger { }

/**
 * Backend engine for running data-flow analysis.
 *
 * @see FrontEnd
 */
class DataFlowEngine(private val environment: Environment = Environment()) {
    private val queryUsages: MutableMap<QueryFragment, HashSet<Location>> = HashMap()

    /**
     * Returns all query fragments and usages found by the engine thus far.
     */
    fun getAllQueries(): Sequence<QueryUsages> {
        return queryUsages.asSequence()
            .filter { it.value.isNotEmpty() }
            .map { QueryUsages(it.key, it.value.sorted()) }
    }

    /**
     * Visits method scope from an Antlr Visitor.
     *
     * @param[visitChildren] function to visit the rest of the method.
     */
    fun visitMethod(visitChildren: () -> Unit) {
        visitScope(visitChildren)
    }

    /**
     * Associates [paramName] as a method argument for the most recently called [visitMethod].
     */
    fun addMethodParameter(paramName: String) {
        LOGGER.debug("Add method parameter $paramName.")
        environment.declareVariable(paramName)
    }

    /**
     * Visits a variable scope or a block from an Antlr visitor.
     *
     * @param[visitChildren] function to visit the rest of the scope block.
     */
    fun visitScope(visitChildren: () -> Unit) {
        environment.pushScope()
        visitChildren()
        environment.popScope()
    }

    /**
     * Visits a variable declaration with the namee [variableName].
     */
    fun declareVariable(variableName: String) {
        LOGGER.debug("Declare variable $variableName.")
        environment.declareVariable(variableName)
    }

    /**
     * Marks [usage] as one of the part of code where [query] is used.
     *
     * Usage examples: method call argument, return, added to non-local data structure, etc.
     */
    private fun addUsage(query: QueryFragment, usage: Location) {
        queryUsages.computeIfAbsent(query) { HashSet() }.add(usage)
    }
}