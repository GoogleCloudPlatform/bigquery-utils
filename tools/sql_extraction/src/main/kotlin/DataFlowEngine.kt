package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.ComplexType
import com.google.cloud.sqlecosystem.sqlextraction.output.FragmentCount
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
    /**
     * The query related to the expression currently being computed.
     */
    private var currentQuery: QueryFragment? = null
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
     * Visits an annotation located at [location].
     * Strings passed as arguments can be considered a usage.
     *
     * @param[visitArguments] Potential visit for all annotation arguments.
     */
    fun visitAnnotation(location: Location, visitArguments: () -> Unit) {
        val prevQuery = currentQuery

        currentQuery = null
        visitArguments()

        if (currentQuery != null) {
            addUsage(currentQuery!!, location)
        }

        currentQuery = prevQuery
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
     * Visits a statement from an Antlr visitor.
     */
    fun visitStatement(visitChildren: () -> Unit) {
        visitChildren()
        currentQuery = null
    }

    /**
     * Visits a variable declaration with the name [variableName].
     */
    fun declareVariable(variableName: String) {
        LOGGER.debug("Declare variable $variableName.")
        environment.declareVariable(variableName)
    }

    /**
     * Visits a string literal found at [location] and spelling out [literal].
     * This can potentially contain an embedded SQL query fragment.
     */
    fun visitStringLiteral(literal: String, location: Location) {
        LOGGER.debug { "Literal \"$literal\" found at $location" }

        // todo: count and complex type as defined by current control flow
        val visitedLiteral = QueryFragment.createLiteral(FragmentCount.SINGLE, location, literal)
        currentQuery = if (currentQuery == null) {
            visitedLiteral
        } else {
            QueryFragment.combine(
                FragmentCount.SINGLE,
                currentQuery!!,
                visitedLiteral,
                ComplexType.UNKNOWN
            )
        }
    }

    /**
     * Visits an assignment operator (=) to an optionally undeclared variable.
     *
     * @param[variableName] Left-hand-side variable that is getting mutated.
     *     This will be declared in current scope if it does not exist.
     * @param[visitRhs] Expression to set [variableName].
     */
    fun visitAssignmentWithOptionalDeclaration(variableName: String, visitRhs: () -> Unit) {
        if (!environment.isVariableDeclaredInAnyScope(variableName)) {
            environment.declareVariable(variableName)
        }

        visitAssignment(variableName, visitRhs)
    }

    /**
     * Visits an assignment operator (=) or an concatenation assignment operator (+=).
     *
     * @param[variableName] Left-hand-side variable that is getting mutated.
     * @param[visitRhs] Expression to set [variableName].
     * @param[concatenate] Whether to replace the variable (if false)
     *     or to append to the variable (if true).
     */
    fun visitAssignment(variableName: String, visitRhs: () -> Unit, concatenate: Boolean = false) {
        visitRhs()

        if (concatenate) {
            environment.setVariableReference(
                variableName, QueryFragment.combine(
                    FragmentCount.SINGLE, // todo: count as defined by current control flow
                    environment.getVariableReference(variableName),
                    currentQuery,
                    ComplexType.AND
                )
            )
        } else {
            environment.setVariableReference(variableName, currentQuery)
        }

        LOGGER.debug {
            "$variableName ${if (concatenate) "+=" else "="} " +
                    environment.getVariableReference(variableName)?.toCombinedString()
        }
    }

    /**
     * Visits a concatenation operator (+), which operates on the current expression.
     *
     * @param[visitLhs] Left side of the operator.
     * @param[visitRhs] Right side of the operator.
     */
    fun visitConcatenation(visitLhs: () -> Unit, visitRhs: () -> Unit) {
        visitLhs()
        val leftFragment = currentQuery
        currentQuery = null

        visitRhs()
        val rightFragment = currentQuery

        currentQuery = QueryFragment.combine(
            FragmentCount.SINGLE, // todo: count as defined by current control flow
            leftFragment,
            rightFragment,
            ComplexType.AND
        )
    }

    /**
     * Visits a mass concatenation/summation operator, which operates on the current expression.
     *
     * @param[visitChildren] Sequence of expressions that evaluate to a String Literal
     *     such that all literals are appended together.
     */
    fun visitConcatenation(visitChildren: Sequence<() -> Unit>) {
        currentQuery =
            QueryFragment.createComplex(
                FragmentCount.SINGLE,// todo: count as defined by current control flow
                visitChildren.mapNotNull {
                    it()
                    val literal = currentQuery
                    currentQuery = null
                    literal
                }.asIterable(),
                ComplexType.AND
            )
    }

    /**
     * Visits a return statement located at [location].
     * This can be counted as a query usage.
     *
     * @param[visitChildren] the expression that is getting returned.
     */
    fun visitReturn(location: Location, visitChildren: () -> Unit) {
        visitChildren()
        if (currentQuery != null) {
            addUsage(usage = location)
            LOGGER.debug { "Return at $location: ${currentQuery?.toCombinedString()}" }
        }
    }

    /**
     * Visits a variable named [name] within an expression.
     * This variable can potentially be related to a query fragment.
     */
    fun visitVariable(name: String) {
        val value = environment.getVariableReferenceOrDefault(name)
        if (value != null) {
            currentQuery = if (currentQuery == null) {
                value
            } else {
                QueryFragment.combine(
                    FragmentCount.SINGLE, // todo: count as defined by current control flow
                    currentQuery!!,
                    value,
                    ComplexType.UNKNOWN
                )
            }
        }
    }

    /**
     * Visits a method call located at [location].
     * The combined argument list can be considered a usage.
     *
     * @param[visitChildren] Sequence of expressions for each method parameter.
     */
    fun visitMethodArguments(location: Location, visitChildren: Sequence<() -> Unit>) {
        val prevQuery = currentQuery
        var concatenated: QueryFragment? = null

        for (arg in visitChildren) {
            currentQuery = null
            arg()

            if (currentQuery != null) {
                concatenated = QueryFragment.combine(
                    FragmentCount.SINGLE, // todo: count as defined by current control flow
                    concatenated,
                    currentQuery
                )
            }
        }

        // who called the method? (who is `this`)
        concatenated = QueryFragment.combine(FragmentCount.SINGLE, prevQuery, concatenated)
        if (concatenated != null) {
            addUsage(concatenated, location)
        }
        currentQuery = concatenated
    }

    /**
     * Marks [usage] as one of the part of code where [query] is used.
     *
     * Usage examples: method call argument, return, added to non-local data structure, etc.
     */
    private fun addUsage(query: QueryFragment = currentQuery!!, usage: Location) {
        queryUsages.computeIfAbsent(query) { HashSet() }.add(usage)
    }
}