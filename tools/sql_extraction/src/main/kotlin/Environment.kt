package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment

/**
 * A mapping from variable name to all possible query fragment values.
 */
class Environment {
    private var parentScope: Environment?
    private var variableReference: MutableMap<String, QueryFragment?>

    constructor() {
        this.parentScope = null
        this.variableReference = HashMap()
    }

    private constructor(copy: Environment) {
        this.parentScope = copy.parentScope
        this.variableReference = copy.variableReference
    }

    /**
     * Declare [varName] as a newly declared variable. Its initial reaching query is null.
     * [setVariableReference] only runs successfully for declared variables.
     */
    fun declareVariable(varName: String) {
        variableReference[varName] = null
    }

    /**
     * Checks whether a variable of name [varName] was declared in this current scope.
     *
     * @return true if variable was declared in this scope.
     */
    fun isVariableDeclaredInScope(varName: String): Boolean {
        return varName in variableReference
    }

    /**
     * Checks whether a variable of name [varName] was declared in any existing scope.
     *
     * @return true if variable was declared in any scope.
     */
    fun isVariableDeclaredInAnyScope(varName: String): Boolean {
        return when {
            isVariableDeclaredInScope(varName) -> true
            parentScope != null -> parentScope!!.isVariableDeclaredInAnyScope(varName)
            else -> false
        }
    }

    /**
     * Gets all possible queries for the variable [varName].
     * Variable can be declared in any reachable scope.
     *
     * @throws[NullPointerException] if variable does not exist.
     */
    fun getVariableReference(varName: String): QueryFragment? {
        return if (isVariableDeclaredInScope(varName)) {
            variableReference[varName]
        } else {
            parentScope!!.getVariableReference(varName)
        }
    }

    /**
     * Gets all possible queries for the variable [varName].
     * Variable can be declared in any reachable scope.
     * [default] is returned if variable doesn't exist in any reachable scope.
     */
    fun getVariableReferenceOrDefault(
        varName: String,
        default: QueryFragment? = null
    ): QueryFragment? {
        return when {
            isVariableDeclaredInScope(varName) -> variableReference[varName]
            parentScope != null -> parentScope!!.getVariableReferenceOrDefault(varName, default)
            else -> default
        }
    }

    /**
     * Overwrites the possible queries for the variable [varName] existing in the most recent scope,
     * or declares a variable in the global scope if it does not exist.
     */
    fun setVariableReference(varName: String, query: QueryFragment?) {
        if (isVariableDeclaredInScope(varName)) {
            variableReference[varName] = query
        } else if (parentScope == null) {
            variableReference[varName] = query
        } else {
            parentScope!!.setVariableReference(varName, query)
        }
    }

    /**
     * Enter a new variable scope and set it as the most recent scope.
     */
    fun pushScope() {
        parentScope = Environment(this)
        variableReference = HashMap()
    }

    /**
     * Exit the most recent variable scope.
     */
    fun popScope() {
        val prev = parentScope
        parentScope = prev!!.parentScope
        variableReference = prev.variableReference
    }
}
