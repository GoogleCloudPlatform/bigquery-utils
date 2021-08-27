package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.Output
import com.google.cloud.sqlecosystem.sqlextraction.output.Query
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

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
    fun process(
        filePaths: Sequence<Path>,
        confidenceThreshold: Double = 0.0,
        showProgress: Boolean = false,
        parallelize: Boolean = false
    ): Output {
        val queries = ConcurrentLinkedQueue<Query>()

        val numCompleted = AtomicInteger(0)
        val (length, processedFilePaths) = if (showProgress) {
            // iterate paths to count up the length
            val list = filePaths.toList()
            Pair(list.size, list.iterator().asSequence())
        } else {
            Pair(1, filePaths)
        }

        val coroutineDispatcher = Executors.newWorkStealingPool().asCoroutineDispatcher()
        runBlocking {
            for (filePath in processedFilePaths) {
                if (parallelize) {
                    launch(coroutineDispatcher) {
                        analyzeFile(
                            filePath,
                            showProgress,
                            numCompleted,
                            length,
                            confidenceThreshold,
                            queries
                        )
                    }
                } else {
                    analyzeFile(
                        filePath,
                        showProgress,
                        numCompleted,
                        length,
                        confidenceThreshold,
                        queries
                    )
                }
            }
        }

        return Output(queries.sortedByDescending { it.confidence })
    }

    private fun analyzeFile(
        filePath: Path,
        showProgress: Boolean,
        numCompleted: AtomicInteger,
        numTotal: Int,
        confidenceThreshold: Double,
        queries: ConcurrentLinkedQueue<Query>
    ) {
        LOGGER.debug { "Scanning $filePath" }
        if (showProgress) {
            System.err.printf(
                "%.1f%% Analyzing %s...%n",
                numCompleted.get() * 100.0 / numTotal,
                filePath
            )
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
                }.filter { it.confidence >= confidenceThreshold }
        )
        val incremented = numCompleted.incrementAndGet()

        if (showProgress) {
            System.err.printf(
                "%.1f%% Analyzed %s.%n",
                incremented * 100.0 / numTotal,
                filePath
            )
        }
    }
}
