package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryUsages
import mu.KotlinLogging
import java.nio.file.Path

private val LOGGER = KotlinLogging.logger { }

/**
 * Detects SQL query construction and usages
 *
 * @param[frontends] List of language-specific frontends to support during data-flow analysis
 */
class DataFlowSolver(private val frontends: List<FrontEnd>) {
    /**
     * Finds all SQL query construction and usages in the given [filePath]
     *
     * @param[engine] Engine to analyze the file with
     * @return Detected queries
     * @throws[IllegalArgumentException] If the given file cannot be
     *     analyzed by any registered frontend
     */
    fun solveDataFlow(engine: DataFlowEngine, filePath: Path): Sequence<QueryUsages> {
        for (frontEnd in frontends) {
            if (frontEnd.canSolve(filePath)) {
                LOGGER.debug { "Using ${frontEnd.javaClass.simpleName} for $filePath" }
                frontEnd.solveDataFlow(engine, frontEnd.openFile(filePath))
                return engine.getAllQueries()
            }
        }

        throw IllegalArgumentException("Cannot analyze $filePath. No frontend available.")
    }
}