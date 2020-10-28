package com.google.cloud.sqlecosystem.sqlextraction.output

import io.mockk.MockKAnnotations
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import org.junit.Before
import kotlin.test.Test
import kotlin.test.assertEquals

class QueryFragmentTest {
    @RelaxedMockK
    lateinit var location: Location

    @RelaxedMockK
    lateinit var literalA: QueryFragment

    @RelaxedMockK
    lateinit var literalB: QueryFragment

    @Before
    fun setUp() = MockKAnnotations.init(this)

    @Test
    fun `toCombinedString for literal`() {
        val fragment = QueryFragment.createLiteral(
            FragmentCount.SINGLE,
            location,
            "test"
        )

        val result = fragment.toCombinedString()

        assertEquals("test", result)
    }

    @Test
    fun `toCombinedString for optional literal`() {
        val fragment = QueryFragment.createLiteral(
            FragmentCount.OPTIONAL,
            location,
            "test"
        )

        val result = fragment.toCombinedString()

        assertEquals("test?", result)
    }

    @Test
    fun `toCombinedString for multiple literal`() {
        val fragment = QueryFragment.createLiteral(
            FragmentCount.MULTIPLE,
            location,
            "test"
        )

        val result = fragment.toCombinedString()

        assertEquals("test*", result)
    }

    @Test
    fun `toCombinedString for complex and`() {
        every { literalA.toCombinedString() } returns "testA"
        every { literalB.toCombinedString() } returns "testB"

        val fragment = QueryFragment.createComplex(
            FragmentCount.SINGLE,
            listOf(literalA, literalB),
            ComplexType.AND
        )

        val result = fragment.toCombinedString()

        assertEquals("(testAtestB)", result)
    }

    @Test
    fun `toCombinedString for complex or`() {
        every { literalA.toCombinedString() } returns "testA"
        every { literalB.toCombinedString() } returns "testB"

        val fragment = QueryFragment.createComplex(
            FragmentCount.SINGLE,
            listOf(literalA, literalB),
            ComplexType.OR
        )

        val result = fragment.toCombinedString()

        assertEquals("(testA|testB)", result)
    }

    @Test
    fun `toCombinedString for complex and optional`() {
        every { literalA.toCombinedString() } returns "testA"
        every { literalB.toCombinedString() } returns "testB"

        val fragment = QueryFragment.createComplex(
            FragmentCount.OPTIONAL,
            listOf(literalA, literalB),
            ComplexType.AND
        )

        val result = fragment.toCombinedString()

        assertEquals("(testAtestB)?", result)
    }

    @Test
    fun `toCombinedString for complex and multiple`() {
        every { literalA.toCombinedString() } returns "testA"
        every { literalB.toCombinedString() } returns "testB"

        val fragment = QueryFragment.createComplex(
            FragmentCount.MULTIPLE,
            listOf(literalA, literalB),
            ComplexType.OR
        )

        val result = fragment.toCombinedString()

        assertEquals("(testA|testB)*", result)
    }
}