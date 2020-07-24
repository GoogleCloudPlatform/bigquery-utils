package com.google.cloud.sqlecosystem.sqlextraction

import io.mockk.MockKAnnotations
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.Before
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.nio.file.Path
import kotlin.test.Test

@RunWith(Parameterized::class)
class SqlExtractorTest(private val showProgress: Boolean) {
    companion object {
        @JvmStatic
        @Parameterized.Parameters
        fun data() = listOf(arrayOf(false), arrayOf(true))
    }

    @MockK
    lateinit var solver: DataFlowSolver

    @MockK
    lateinit var rater: ConfidenceRater

    @InjectMockKs
    lateinit var sqlExtractor: SqlExtractor

    @Before
    fun setUp() = MockKAnnotations.init(this)

    @Test
    fun `given data-flow solver is used`() {
        every { solver.solveDataFlow(any(), any()) } returns emptySequence()

        sqlExtractor.process(sequenceOf(mockk(relaxed = true)), showProgress)
        verify { solver.solveDataFlow(any(), any()) }

        confirmVerified()
    }

    @Test
    fun `given data-flow solver is used for each path`() {
        every { solver.solveDataFlow(any(), any()) } returns emptySequence()

        val filePaths = listOf<Path>(
            mockk(relaxed = true),
            mockk(relaxed = true),
            mockk(relaxed = true)
        )

        sqlExtractor.process(filePaths.asSequence(), showProgress)
        verifyAll { filePaths.map { solver.solveDataFlow(any(), it) } }

        confirmVerified()
    }

    @Test
    fun `detected queries are rated by the given confidence rater`() {
        every { solver.solveDataFlow(any(), any()) } returns sequenceOf(mockk(relaxed = true))

        every { rater.rate(any()) } returns 1.0

        sqlExtractor.process(sequenceOf(mockk(relaxed = true)), showProgress)
        verifyAll { rater.rate(any()) }

        confirmVerified()
    }
}