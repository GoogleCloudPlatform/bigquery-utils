package com.google.cloud.sqlecosystem.sqlextraction

import io.mockk.MockKAnnotations
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.verify
import io.mockk.verifySequence
import org.antlr.v4.runtime.CharStream
import org.junit.Before
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals

class DataFlowSolverTest {
    @RelaxedMockK
    lateinit var engine: DataFlowEngine

    @RelaxedMockK
    lateinit var stubFilePath: Path

    @RelaxedMockK
    lateinit var stubStream: CharStream

    @MockK(relaxUnitFun = true)
    lateinit var frontEnd: FrontEnd

    @MockK
    lateinit var invalidFrontEnd: FrontEnd

    @Before
    fun setUp() = MockKAnnotations.init(this)

    @Test
    fun `solver returns empty sequence without any frontends`() {
        val solver = DataFlowSolver(emptyList())
        val queries = solver.solveDataFlow(engine, stubFilePath)

        assertEquals(emptySequence(), queries)
    }

    @Test
    fun `solver calls canSolve, openFile, and solveDataFlow for a valid frontend`() {
        every { frontEnd.canSolve(any()) } returns true
        every { frontEnd.openFile(any()) } returns stubStream

        val solver = DataFlowSolver(listOf(frontEnd))
        solver.solveDataFlow(engine, stubFilePath)

        verifySequence {
            frontEnd.canSolve(stubFilePath)
            frontEnd.openFile(stubFilePath)
            frontEnd.solveDataFlow(any(), stubStream)
        }
        confirmVerified()
    }

    @Test
    fun `solver returns empty sequence without any valid frontends`() {
        every { frontEnd.canSolve(any()) } returns false

        val solver = DataFlowSolver(listOf(frontEnd))
        val queries = solver.solveDataFlow(engine, stubFilePath)

        assertEquals(emptySequence(), queries)
    }

    @Test
    fun `solver only uses a valid frontend`() {
        every { invalidFrontEnd.canSolve(any()) } returns false

        every { frontEnd.canSolve(any()) } returns true
        every { frontEnd.openFile(any()) } returns stubStream

        val solver = DataFlowSolver(listOf(invalidFrontEnd, frontEnd))
        solver.solveDataFlow(engine, stubFilePath)

        verify(inverse = true) { invalidFrontEnd.solveDataFlow(any(), any()) }
        verifySequence {
            frontEnd.canSolve(stubFilePath)
            frontEnd.openFile(stubFilePath)
            frontEnd.solveDataFlow(any(), stubStream)
        }
        confirmVerified()
    }
}