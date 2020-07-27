package com.google.cloud.sqlecosystem.sqlextraction

import io.mockk.MockKAnnotations
import io.mockk.confirmVerified
import io.mockk.impl.annotations.MockK
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.Before
import kotlin.test.Test

class DataFlowEngineTest {
    @MockK(relaxUnitFun = true)
    lateinit var environment: Environment

    @Before
    fun setUp() = MockKAnnotations.init(this)

    @Test
    fun `visitScope pushes and pops env scope`() {
        val engine = DataFlowEngine(environment)
        engine.visitScope { environment.declareVariable("<>_stub") }

        verifyOrder {
            environment.pushScope()
            environment.declareVariable("<>_stub")
            environment.popScope()
        }
        confirmVerified()
    }

    @Test
    fun `visitMethod pushes and pops env scope`() {
        val engine = DataFlowEngine(environment)
        engine.visitMethod { environment.declareVariable("<>_stub") }

        verifyOrder {
            environment.pushScope()
            environment.declareVariable("<>_stub")
            environment.popScope()
        }
        confirmVerified()
    }

    @Test
    fun `declareVariable declares variable`() {
        val varName = "<>_stub"

        val engine = DataFlowEngine(environment)
        engine.declareVariable(varName)

        verify { environment.declareVariable(varName) }
        confirmVerified()
    }

    @Test
    fun `addMethodParameter declares variable`() {
        val varName = "<>_stub"

        val engine = DataFlowEngine(environment)
        engine.addMethodParameter(varName)

        verify { environment.declareVariable(varName) }
        confirmVerified()
    }
}
