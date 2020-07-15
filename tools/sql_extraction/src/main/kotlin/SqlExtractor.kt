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
class SqlExtractor(
    private val dataFlowSolver: DataFlowSolver,
    private val confidenceRater: ConfidenceRater
) {
    /**
     * @return Detected SQL queries sorted by confidence
     */
    fun process(filePaths: Sequence<Path>, showProgress: Boolean = false): Output {
        val queries = ArrayList<Query>()

        var numCompleted = 0
        val (length, processedFilePaths) = if (showProgress) {
            // iterate paths to count up the length
            val list = filePaths.toList()
            Pair(list.size, list.iterator().asSequence())
        } else {
            Pair(1, filePaths)
        }

        for (filePath in processedFilePaths) {
            LOGGER.debug { "Scanning $filePath" }
            if (showProgress) {
                System.err.printf("%.1f%% Analyzing %s...", numCompleted * 100.0 / length, filePath)
                System.err.println()
            }

            queries.addAll(
                dataFlowSolver.solveDataFlow(DataFlowEngine(), filePath)
                    .map {
                        Query(
                            filePath.toString(),
                            confidenceRater.rate(it.query),
                            it.query,
                            it.usages
                        )
                    })
            numCompleted++

            if (showProgress) {
                System.err.printf("%.1f%% Analyzed %s.", numCompleted * 100.0 / length, filePath)
                System.err.println()
            }
        }

        return Output(queries.sortedByDescending { it.confidence })
    }
}
