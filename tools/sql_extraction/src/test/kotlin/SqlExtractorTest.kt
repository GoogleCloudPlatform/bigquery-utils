package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.output.QueryUsages
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
import kotlin.test.assertEquals

@RunWith(Parameterized::class)
class SqlExtractorTest(private val showProgress: Boolean, private val parallelize: Boolean) {
    companion object {
        @JvmStatic
        @Parameterized.Parameters
        fun data() = listOf(
            arrayOf(false, false),
            arrayOf(false, true),
            arrayOf(true, false),
            arrayOf(true, true)
        )
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

        sqlExtractor.process(
            sequenceOf(mockk(relaxed = true)),
            showProgress = showProgress,
            parallelize = parallelize
        )
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

        sqlExtractor.process(
            filePaths.asSequence(),
            showProgress = showProgress,
            parallelize = parallelize
        )
        verifyAll { filePaths.map { solver.solveDataFlow(any(), it) } }

        confirmVerified()
    }

    @Test
    fun `detected queries are rated by the given confidence rater`() {
        every { solver.solveDataFlow(any(), any()) } returns sequenceOf(mockk(relaxed = true))

        every { rater.rate(any()) } returns 1.0

        sqlExtractor.process(
            sequenceOf(mockk(relaxed = true)),
            showProgress = showProgress,
            parallelize = parallelize
        )
        verifyAll { rater.rate(any()) }

        confirmVerified()
    }

    @Test
    fun `detected queries are filtered by confidence threshold`() {
        val queries = listOf<QueryUsages>(
            mockk(relaxed = true),
            mockk(relaxed = true),
            mockk(relaxed = true),
            mockk(relaxed = true),
            mockk(relaxed = true)
        )
        every { solver.solveDataFlow(any(), any()) } returns queries.asSequence()

        every { rater.rate(queries[0].query) } returns 0.0
        every { rater.rate(queries[1].query) } returns 0.25
        every { rater.rate(queries[2].query) } returns 0.5
        every { rater.rate(queries[3].query) } returns 0.75
        every { rater.rate(queries[4].query) } returns 1.0

        val output = sqlExtractor.process(
            sequenceOf(mockk(relaxed = true)),
            0.5,
            showProgress,
            parallelize
        )

        assertEquals(3, output.queries.size)
    }
}