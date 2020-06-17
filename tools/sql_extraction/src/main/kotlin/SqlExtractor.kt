package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.Output
import com.google.cloud.sqlecosystem.sqlextraction.output.Query
import mu.KotlinLogging
import java.nio.file.Path

private val LOGGER = KotlinLogging.logger { }

/**
 * Extracts SQL queries using the given solver
 *
 * @param[dataFlowSolver] Solver responsible for detecting query construction and usages
 */
class SqlExtractor(private val dataFlowSolver: DataFlowSolver) {
    /**
     * @return Detected SQL queries sorted by confidence
     */
    fun process(filePaths: Collection<Path>): Output {
        val queries = ArrayList<Query>()
        for (filePath in filePaths) {
            LOGGER.debug { "Scanning $filePath" }
            queries.addAll(dataFlowSolver.solveDataFlow(DataFlowEngine(), filePath))
        }

        return Output(queries)
    }
}
