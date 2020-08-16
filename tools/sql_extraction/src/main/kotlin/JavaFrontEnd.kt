package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.antlr.Java9Lexer
import com.google.cloud.sqlecosystem.sqlextraction.antlr.Java9Parser
import com.google.cloud.sqlecosystem.sqlextraction.output.Query
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.TokenSource
import org.antlr.v4.runtime.TokenStream
import java.nio.file.Path

/**
 * Data-flow analysis frontend for Java (.java files)
 */
class JavaFrontEnd : FrontEnd {
    override fun canSolve(filePath: Path): Boolean {
        return filePath.toString().endsWith(".java", ignoreCase = true)
    }

    override fun solveDataFlow(engine: DataFlowEngine, fileStream: CharStream) {
        val lexer = Java9Lexer(fileStream)
        val tokens = CommonTokenStream(lexer as TokenSource)
        val parser = Java9Parser(tokens as TokenStream)
        val tree = parser.compilationUnit()

        JavaAnalyzer(engine).visit(tree)
    }
}