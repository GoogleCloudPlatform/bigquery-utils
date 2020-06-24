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

        SqlExtractor(solver).process(sequenceOf(mockk(relaxed = true)))
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

        SqlExtractor(solver).process(filePaths.asSequence())
        verifyAll { filePaths.map { solver.solveDataFlow(any(), it) } }

        confirmVerified()
    }
}