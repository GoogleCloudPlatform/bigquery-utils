package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.antlr.Python3Lexer
import com.google.cloud.sqlecosystem.sqlextraction.antlr.Python3Parser
import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.TokenSource
import org.antlr.v4.runtime.TokenStream
import java.nio.file.Path

/**
 * Data-flow analysis frontend for Java (.java files)
 */
class PythonFrontEnd : FrontEnd {
    override fun canSolve(filePath: Path): Boolean {
        return filePath.toString().endsWith(".py", ignoreCase = true)
                || filePath.toString().endsWith(".py3", ignoreCase = true)
    }

    override fun solveDataFlow(engine: DataFlowEngine, fileStream: CharStream) {
        val lexer = Python3Lexer(fileStream)
        val tokens = CommonTokenStream(lexer as TokenSource)
        val parser = Python3Parser(tokens as TokenStream)
        val tree = parser.file_input()

        PythonAnalyzer(engine).visit(tree)
    }
}