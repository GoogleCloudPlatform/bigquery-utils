package com.google.cloud.sqlecosystem.sqlextraction.output

import io.mockk.every
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

internal class QueryFragmentTest {
    @Test
    fun `toCombinedString for literal`() {
        val fragment = QueryFragment.createLiteral(
            FragmentCount.SINGLE,
            mockk(relaxed = true),
            "test"
        )

        val result = fragment.toCombinedString()

        assertEquals("test", result)
    }

    @Test
    fun `toCombinedString for optional literal`() {
        val fragment = QueryFragment.createLiteral(
            FragmentCount.OPTIONAL,
            mockk(relaxed = true),
            "test"
        )

        val result = fragment.toCombinedString()

        assertEquals("test?", result)
    }

    @Test
    fun `toCombinedString for multiple literal`() {
        val fragment = QueryFragment.createLiteral(
            FragmentCount.MULTIPLE,
            mockk(relaxed = true),
            "test"
        )

        val result = fragment.toCombinedString()

        assertEquals("test*", result)
    }

    @Test
    fun `toCombinedString for complex and`() {
        val literalA = mockk<QueryFragment>(relaxed = true)
        every { literalA.toCombinedString() } returns "testA"
        val literalB = mockk<QueryFragment>(relaxed = true)
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
        val literalA = mockk<QueryFragment>(relaxed = true)
        every { literalA.toCombinedString() } returns "testA"
        val literalB = mockk<QueryFragment>(relaxed = true)
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
        val literalA = mockk<QueryFragment>(relaxed = true)
        every { literalA.toCombinedString() } returns "testA"
        val literalB = mockk<QueryFragment>(relaxed = true)
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
        val literalA = mockk<QueryFragment>(relaxed = true)
        every { literalA.toCombinedString() } returns "testA"
        val literalB = mockk<QueryFragment>(relaxed = true)
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