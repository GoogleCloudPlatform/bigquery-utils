package com.google.cloud.sqlecosystem.sqlextraction

import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifySequence
import org.antlr.v4.runtime.CharStream
import org.junit.jupiter.api.assertThrows
import java.nio.file.Path
import kotlin.test.Test

class DataFlowSolverTest {
    @Test
    fun `solver throws without any frontends`() {
        val solver = DataFlowSolver(emptyList())

        assertThrows<IllegalArgumentException> { solver.solveDataFlow(mockk(relaxed = true), mockk(relaxed = true)) }
    }

    @Test
    fun `solver calls canSolve, openFile, and solveDataFlow for a valid frontend`() {
        val filePath = mockk<Path>(relaxed = true)
        val stream = mockk<CharStream>(relaxed = true)

        val frontEnd = mockk<FrontEnd>(relaxUnitFun = true)
        every { frontEnd.canSolve(any()) } returns true
        every { frontEnd.openFile(any()) } returns stream

        val solver = DataFlowSolver(listOf(frontEnd))
        solver.solveDataFlow(mockk(relaxed = true), filePath)

        verifySequence {
            frontEnd.canSolve(filePath)
            frontEnd.openFile(filePath)
            frontEnd.solveDataFlow(any(), stream)
        }
        confirmVerified()
    }

    @Test
    fun `solver throws without any valid frontends`() {
        val frontEnd = mockk<FrontEnd>()
        every { frontEnd.canSolve(any()) } returns false

        val solver = DataFlowSolver(listOf(frontEnd))

        assertThrows<IllegalArgumentException> { solver.solveDataFlow(mockk(relaxed = true), mockk(relaxed = true)) }
    }

    @Test
    fun `solver only uses a valid frontend`() {
        val filePath = mockk<Path>(relaxed = true)
        val stream = mockk<CharStream>(relaxed = true)

        val invalidFrontEnd = mockk<FrontEnd>()
        every { invalidFrontEnd.canSolve(any()) } returns false

        val frontEnd = mockk<FrontEnd>(relaxUnitFun = true)
        every { frontEnd.canSolve(any()) } returns true
        every { frontEnd.openFile(any()) } returns stream

        val solver = DataFlowSolver(listOf(invalidFrontEnd, frontEnd))
        solver.solveDataFlow(mockk(relaxed = true), filePath)

        verify(inverse = true) { invalidFrontEnd.solveDataFlow(any(), any()) }
        verifySequence {
            frontEnd.canSolve(filePath)
            frontEnd.openFile(filePath)
            frontEnd.solveDataFlow(any(), stream)
        }
        confirmVerified()
    }
}