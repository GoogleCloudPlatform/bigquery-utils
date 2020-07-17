package com.google.cloud.sqlecosystem.sqlextraction

import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import java.nio.file.Path
import kotlin.test.Test

class SqlExtractorTest {
    @Test
    fun `given data-flow solver is used`() {
        val solver = mockk<DataFlowSolver>()
        every { solver.solveDataFlow(any(), any()) } returns emptySequence()

        SqlExtractor(solver, mockk(relaxed = true)).process(sequenceOf(mockk(relaxed = true)))
        verify { solver.solveDataFlow(any(), any()) }

        confirmVerified()
    }

    @Test
    fun `given data-flow solver is used for each path`() {
        val solver = mockk<DataFlowSolver>()
        every { solver.solveDataFlow(any(), any()) } returns emptySequence()

        val filePaths = listOf<Path>(
            mockk(relaxed = true),
            mockk(relaxed = true),
            mockk(relaxed = true)
        )

        SqlExtractor(solver, mockk(relaxed = true)).process(filePaths.asSequence())
        verifyAll { filePaths.map { solver.solveDataFlow(any(), it) } }

        confirmVerified()
    }

    @Test
    fun `detected queries are rated by the given confidence rater`() {
        val solver = mockk<DataFlowSolver>()
        every { solver.solveDataFlow(any(), any()) } returns sequenceOf(mockk(relaxed = true))

        val rater = mockk<ConfidenceRater>()
        every { rater.rate(any()) } returns 1.0

        SqlExtractor(solver, rater).process(sequenceOf(mockk(relaxed = true)))
        verifyAll { rater.rate(any()) }

        confirmVerified()
    }
}