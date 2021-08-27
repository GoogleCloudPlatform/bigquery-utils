package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryUsages
import org.antlr.v4.runtime.CharStreams
import kotlin.test.Test
import kotlin.test.assertEquals

class JavaAnalysisTest {
    @Test
    fun `strings without usages are not returned`() {
        val program = classHeaders(
            voidMethodHeaders(
                """
String a = "test";
"""
            )
        )

        val result = analyze(program)

        assertEquals(0, result.count())
    }

    @Test
    fun `simple string return`() {
        val program = classHeaders(
            stringMethodHeaders(
                """
return "test";
"""
            )
        )

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    @Test
    fun `return usage location`() {
        val program = """
public class Test { public void test() { // line 1
return "test"; // line 2
}} // line 3
""".trim()

        val result = analyze(program).first()

        assertEquals(1, result.usages.size)
        assertEquals(2, result.usages[0].startLine)
        assertEquals(2, result.usages[0].endLine)
    }

    @Test
    fun `string variable return`() {
        val program = classHeaders(
            stringMethodHeaders(
                """
String query = "test";
return query;
"""
            )
        )

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    @Test
    fun `reaching definition return`() {
        val program = classHeaders(
            stringMethodHeaders(
                """
            String query = "test";
            query = "test2";
            return query;
            """
            )
        )

        val result = analyze(program)

        assertCorrect(result, "test2")
    }

    @Test
    fun `string concatenation return`() {
        val program = classHeaders(
            stringMethodHeaders(
                """
return "test1" + "test2";
"""
            )
        )

        val result = analyze(program)

        assertCorrect(result, "(test1test2)")
    }

    @Test
    fun `variable concatenation return`() {
        val program = classHeaders(
            stringMethodHeaders(
                """
String a = "test1";
String b = "test2";
return a + b;
"""
            )
        )

        val result = analyze(program)

        assertCorrect(result, "(test1test2)")
    }

    @Test
    fun `literal in method argument`() {
        val program = classHeaders(
            voidMethodHeaders(
                """
String a = "test";
External.sink(a);
"""
            )
        )

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    @Test
    fun `literal in annotation`() {
        val program = classHeaders(
            "@Annotation(\"test\")\n" + voidMethodHeaders("")
        )

        val result = analyze(program)

        assertCorrect(result, "test")
    }

    private fun analyze(program: String): Sequence<QueryUsages> {
        val engine = DataFlowEngine()
        JavaFrontEnd().solveDataFlow(engine, CharStreams.fromString(program))
        return engine.getAllQueries()
    }

    private fun assertCorrect(actual: Sequence<QueryUsages>, vararg expected: String) =
        assertEquals(expected.toSet(), actual.map { it.query.toCombinedString() }.toSet())

    private fun classHeaders(program: String): String = "public class Test {$program}"

    private fun voidMethodHeaders(program: String): String =
        "public void test() {$program}"

    private fun stringMethodHeaders(program: String): String =
        "public String test() {$program}"
}