package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryFragment
import io.mockk.MockKAnnotations
import io.mockk.impl.annotations.RelaxedMockK
import org.junit.Before
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class EnvironmentTest {
    @RelaxedMockK
    lateinit var stubQueryFragment: QueryFragment

    @Before
    fun setUp() = MockKAnnotations.init(this)

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

        assertEquals(
            stubQueryFragment,
            env.getVariableReferenceOrDefault("test", stubQueryFragment)
        )
        assertEquals(null, env.getVariableReferenceOrDefault("test2"))

        env.declareVariable("test")

        assertEquals(null, env.getVariableReference("test"))
    }

    @Test
    fun `setVariable updates getVariable`() {
        val env = Environment()
        env.declareVariable("test")
        assertEquals(null, env.getVariableReference("test"))

        env.setVariableReference("test", stubQueryFragment)

        assertEquals(stubQueryFragment, env.getVariableReference("test"))
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
        val env = Environment()
        env.declareVariable("test")
        env.setVariableReference("test", stubQueryFragment)

        env.pushScope()
        assertEquals(stubQueryFragment, env.getVariableReference("test"))

        env.popScope()
        assertEquals(stubQueryFragment, env.getVariableReference("test"))
    }

    @Test
    fun `getVariableOrDefault gets parent's variable`() {
        val env = Environment()
        env.declareVariable("test")
        env.setVariableReference("test", stubQueryFragment)

        env.pushScope()
        assertEquals(stubQueryFragment, env.getVariableReferenceOrDefault("test"))

        env.popScope()
        assertEquals(stubQueryFragment, env.getVariableReferenceOrDefault("test"))
    }

    @Test
    fun `setVariable updates parent's getVariable`() {
        val env = Environment()
        env.declareVariable("test")
        env.pushScope()
        assertEquals(null, env.getVariableReference("test"))

        env.setVariableReference("test", stubQueryFragment)
        assertEquals(stubQueryFragment, env.getVariableReference("test"))

        env.popScope()
        assertEquals(stubQueryFragment, env.getVariableReference("test"))
    }

    @Test
    fun `setVariable creates global variable if variable does not exist`() {
        val env = Environment()
        env.pushScope()

        env.setVariableReference("test", stubQueryFragment)
        assertEquals(stubQueryFragment, env.getVariableReference("test"))

        env.popScope()
        assertEquals(stubQueryFragment, env.getVariableReference("test"))
    }
}