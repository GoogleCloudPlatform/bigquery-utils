package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryUsages
import org.antlr.v4.runtime.CharStreams
import kotlin.test.Test
import kotlin.test.assertEquals

class PythonAnalysisTest {
    @Test
    fun `simple string return`() {
        val program = """
def test():
    return "test"
"""

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    @Test
    fun `simple method argument`() {
        val program = """
import something
something.some_method("test")
"""

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    @Test
    fun `literal concatenation`() {
        val program = """
import something
something.some_method("hello" + "world")
"""

        val result = analyze(program)

        assertCorrect(result, "(helloworld)")
    }

    @Test
    fun `variable assignment and usage`() {
        val program = """
import something
a = "test"
something.some_method(a)
"""

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    private fun analyze(program: String): Sequence<QueryUsages> {
        val engine = DataFlowEngine()
        PythonFrontEnd().solveDataFlow(engine, CharStreams.fromString(program))
        return engine.getAllQueries()
    }

    private fun assertCorrect(actual: Sequence<QueryUsages>, vararg expected: String) =
        assertEquals(expected.toSet(), actual.map { it.query.toCombinedString() }.toSet())
}