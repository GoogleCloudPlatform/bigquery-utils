package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment

/**
 * A mapping from variable name to all possible query fragment values
 */
class Environment {
    private var parentScope: Environment?
    private var reachingDefs: HashMap<String, QueryFragment?>

    constructor() {
        this.parentScope = null
        this.reachingDefs = HashMap()
    }

    private constructor(copy: Environment) {
        this.parentScope = copy.parentScope
        this.reachingDefs = copy.reachingDefs
    }

    /**
     * Declare [varName] as a newly declared variable. Its initial reaching query is null.
     * [setVariable] only runs successfully for declared variables.
     */
    fun declareVariable(varName: String) {
        reachingDefs[varName] = null
    }

    /**
     * Checks whether a variable of name [varName] was declared in this current scope.
     *
     * @return true if variable was declared in this scope
     */
    fun hasVariableInScope(varName: String): Boolean {
        return varName in reachingDefs
    }

    /**
     * Gets all possible queries for the variable [varName].
     * Variable can be declared in any reachable scope.
     *
     * @throws[NullPointerException] if variable does not exist
     */
    fun getVariable(varName: String): QueryFragment? {
        return if (hasVariableInScope(varName)) {
            reachingDefs[varName]
        } else {
            parentScope!!.getVariable(varName)
        }
    }

    /**
     * Gets all possible queries for the variable [varName].
     * Variable can be declared in any reachable scope.
     * [default] is returned if variable doesn't exist in any reachable scope.
     */
    fun getVariableOrDefault(varName: String, default: QueryFragment? = null): QueryFragment? {
        return when {
            hasVariableInScope(varName) -> reachingDefs[varName]
            parentScope != null -> parentScope!!.getVariableOrDefault(varName)
            else -> default
        }
    }

    /**
     * Overwrites the possible queries for the variable [varName] existing in the most recent scope.
     * Variable needs to be defined first to be set.
     *
     * @throws[NullPointerException] if variable does not exist
     */
    fun setVariable(varName: String, query: QueryFragment?) {
        if (hasVariableInScope(varName)) {
            reachingDefs[varName] = query
        } else {
            parentScope!!.setVariable(varName, query)
        }
    }

    /**
     * Enter a new variable scope and set it as the most recent scope
     */
    fun pushScope() {
        parentScope = Environment(this)
        reachingDefs = HashMap()
    }

    /**
     * Exit the most recent variable scope
     */
    fun popScope() {
        val prev = parentScope
        parentScope = prev!!.parentScope
        reachingDefs = prev.reachingDefs
    }
}
