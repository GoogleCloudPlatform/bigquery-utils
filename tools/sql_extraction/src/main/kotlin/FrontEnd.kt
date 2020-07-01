package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.Query
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CharStreams
import java.nio.file.Path

/**
 * Language-specific module to run data-flow analysis in the AST level
 *
 * @see DataFlowEngine
 */
interface FrontEnd {
    /**
     * Checks whether the given [filePath] can be analyzed by this frontend
     */
    fun canSolve(filePath: Path): Boolean

    /**
     * Opens the given [filePath] as a CharStream
     */
    fun openFile(filePath: Path): CharStream {
        return CharStreams.fromPath(filePath)
    }

    /**
     * Solves data-flow analysis using the given [engine] for the given [fileStream]
     */
    fun solveDataFlow(engine: DataFlowEngine, fileStream: CharStream)
}