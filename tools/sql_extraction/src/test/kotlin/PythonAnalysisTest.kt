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
    fun `triple quote multiline string return`() {
        val program = """
def test():
    return '''test
a
b
c'''
"""

        val result = analyze(program)

        assertCorrect(result, "test\na\nb\nc")
    }

    @Test
    fun `parenthesized multiline string return`() {
        val program = """
def test():
    return ("test"
        "a"
        "b"
        "c")
"""

        val result = analyze(program)

        assertCorrect(result, "(testabc)")
    }

    @Test
    fun `backslash multiline string return`() {
        val program = """
def test():
    return "test"\
        "a"\
        "b"\
        "c"
"""

        val result = analyze(program)

        assertCorrect(result, "(testabc)")
    }

    @Test
    fun `escaped multiline string return`() {
        val program = """
def test():
    return "test\
a\
b\
c"
"""

        val result = analyze(program).toList()

        assertEquals(result.count(), 1)
        assertEquals(result[0].query.type, null)
        // the grammar does not remove the backslash
        assertEquals(result[0].query.literal!!.replace("\\", ""), "test\na\nb\nc")
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

    @Test
    fun `literal in decorator`() {
        val program = """
import something
@something("test")
def test(i):
    return i
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