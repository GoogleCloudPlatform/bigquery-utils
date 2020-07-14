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

        assertFalse(env.isVariableDeclaredInScope("test"))

        env.declareVariable("test")

        assertTrue(env.isVariableDeclaredInScope("test"))
    }

    @Test
    fun `getVariable fails for undefined variables`() {
        val env = Environment()

        assertThrows<RuntimeException> { env.getVariableReference("test") }

        env.declareVariable("test")

        assertEquals(null, env.getVariableReference("test"))
    }

    @Test
    fun `getVariableOrDefault returns default for undefined variables`() {
        val env = Environment()
        val default = mockk<QueryFragment>(relaxed = true)

        assertEquals(default, env.getVariableReferenceOrDefault("test", default))
        assertEquals(null, env.getVariableReferenceOrDefault("test2"))

        env.declareVariable("test")

        assertEquals(null, env.getVariableReference("test"))
    }

    @Test
    fun `setVariable updates getVariable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        assertEquals(null, env.getVariableReference("test"))

        env.setVariableReference("test", stub)

        assertEquals(stub, env.getVariableReference("test"))
    }

    @Test
    fun `hasVariableInScope does not work for parent environment`() {
        val env = Environment()
        env.declareVariable("test")

        env.pushScope()
        assertFalse(env.isVariableDeclaredInScope("test"))

        env.popScope()
        assertTrue(env.isVariableDeclaredInScope("test"))
    }

    @Test
    fun `getVariable gets parent's variable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        env.setVariableReference("test", stub)

        env.pushScope()
        assertEquals(stub, env.getVariableReference("test"))

        env.popScope()
        assertEquals(stub, env.getVariableReference("test"))
    }

    @Test
    fun `getVariableOrDefault gets parent's variable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        env.setVariableReference("test", stub)

        env.pushScope()
        assertEquals(stub, env.getVariableReferenceOrDefault("test"))

        env.popScope()
        assertEquals(stub, env.getVariableReferenceOrDefault("test"))
    }

    @Test
    fun `setVariable updates parent's getVariable`() {
        val stub = mockk<QueryFragment>(relaxed = true)
        val env = Environment()
        env.declareVariable("test")
        env.pushScope()
        assertEquals(null, env.getVariableReference("test"))

        env.setVariableReference("test", stub)
        assertEquals(stub, env.getVariableReference("test"))

        env.popScope()
        assertEquals(stub, env.getVariableReference("test"))
    }
}