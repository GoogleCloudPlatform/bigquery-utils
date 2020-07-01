package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment
import io.mockk.mockk
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class EnvironmentTest {
    @Test
    fun `declareVariable causes hasVariableInScope`() {
        val env = Environment()

        assertFalse(env.hasVariableInScope("test"))

        env.declareVariable("test")

        assertTrue(env.hasVariableInScope("test"))
    }

    @Test
    fun `getVariable fails for undefined variables`() {
        val env = Environment()

        assertThrows<RuntimeException> { env.getVariable("test") }

        env.declareVariable("test")

        assertEquals(null, env.getVariable("test"))
    }

    @Test
    fun `getVariableOrDefault returns default for undefined variables`() {
        val env = Environment()
        val default = mockk<QueryFragment>(relaxed = true)

        assertEquals(default, env.getVariableOrDefault("test", default))
        assertEquals(null, env.getVariableOrDefault("test2"))

        env.declareVariable("test")

        assertEquals(null, env.getVariable("test"))
    }

    @Test
    fun `setVariable updates getVariable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        assertEquals(null, env.getVariable("test"))

        env.setVariable("test", stub)

        assertEquals(stub, env.getVariable("test"))
    }

    @Test
    fun `hasVariableInScope does not work for parent environment`() {
        val env = Environment()
        env.declareVariable("test")

        env.pushScope()
        assertFalse(env.hasVariableInScope("test"))

        env.popScope()
        assertTrue(env.hasVariableInScope("test"))
    }

    @Test
    fun `getVariable gets parent's variable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        env.setVariable("test", stub)

        env.pushScope()
        assertEquals(stub, env.getVariable("test"))

        env.popScope()
        assertEquals(stub, env.getVariable("test"))
    }

    @Test
    fun `getVariableOrDefault gets parent's variable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        env.setVariable("test", stub)

        env.pushScope()
        assertEquals(stub, env.getVariableOrDefault("test"))

        env.popScope()
        assertEquals(stub, env.getVariableOrDefault("test"))
    }

    @Test
    fun `setVariable updates parent's getVariable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        env.pushScope()
        assertEquals(null, env.getVariable("test"))

        env.setVariable("test", stub)
        assertEquals(stub, env.getVariable("test"))

        env.popScope()
        assertEquals(stub, env.getVariable("test"))
    }
}