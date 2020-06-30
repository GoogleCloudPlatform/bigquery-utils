package com.google.cloud.sqlecosystem.sqlextraction

import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FilesExpanderTest {
    val expander = FilesExpander()

    @Test
    fun `all files are included`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/a.java")
        val fileB = fs.getPath("/b.java")
        Files.createFile(fileA)
        Files.createFile(fileB)

        val result = expander.expandAndFilter(listOf(fileA, fileB), false)

        assertEquals(setOf(fileA, fileB), result.toSet())
    }

    @Test
    fun `files within directories included`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        Files.createDirectories(fs.getPath("/dir"))
        val fileA = fs.getPath("/dir/a.java")
        val fileB = fs.getPath("/dir/b.java")
        Files.createFile(fileA)
        Files.createFile(fileB)

        val result = expander.expandAndFilter(listOf(fs.getPath("/dir")), false)

        assertEquals(setOf(fileA, fileB), result.toSet())
    }

    @Test
    fun `files within subdirectories are not included without recursive`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        Files.createDirectories(fs.getPath("/dir"))
        Files.createDirectories(fs.getPath("/dir/subdir"))
        val fileA = fs.getPath("/dir/a.java")
        val fileB = fs.getPath("/dir/subdir/b.java")
        Files.createFile(fileA)
        Files.createFile(fileB)

        val result = expander.expandAndFilter(listOf(fs.getPath("/dir")), false)

        assertEquals(setOf(fileA), result.toSet())
    }

    @Test
    fun `files within subdirectories are included with recursive`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        Files.createDirectories(fs.getPath("/dir"))
        Files.createDirectories(fs.getPath("/dir/subdir"))
        val fileA = fs.getPath("/dir/a.java")
        val fileB = fs.getPath("/dir/subdir/b.java")
        Files.createFile(fileA)
        Files.createFile(fileB)

        val result = expander.expandAndFilter(listOf(fs.getPath("/dir")), true)

        assertEquals(setOf(fileA, fileB), result.toSet())
    }

    @Test
    fun `returned collection doesn't have duplicates`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/a.java")
        Files.createFile(fileA)

        val result = expander.expandAndFilter(listOf(fileA, fileA), false)

        assertTrue(result.count() == 1)
    }

    @Test
    fun `include filters out by GLOB`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/a.java")
        val fileB = fs.getPath("/a.notjava")
        Files.createFile(fileA)
        Files.createFile(fileB)

        val result = expander.expandAndFilter(
            listOf(fs.getPath("/")), false,
            includes = listOf("*.java")
        )

        assertEquals(setOf(fileA), result.toSet())
    }

    @Test
    fun `multiple includes take the union`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/a.java")
        val fileB = fs.getPath("/a.maybejava")
        val fileC = fs.getPath("/a.notjava")
        Files.createFile(fileA)
        Files.createFile(fileB)
        Files.createFile(fileC)

        val result =
            expander.expandAndFilter(
                listOf(fs.getPath("/")), false,
                includes = listOf("*.java", "*.maybejava")
            )

        assertEquals(setOf(fileA, fileB), result.toSet())
    }

    @Test
    fun `exclude filters out by GLOB`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/a.java")
        val fileB = fs.getPath("/a.notjava")
        Files.createFile(fileA)
        Files.createFile(fileB)

        val result =
            expander.expandAndFilter(listOf(fs.getPath("/")), false, excludes = listOf("*.notjava"))

        assertEquals(setOf(fileA), result.toSet())
    }

    @Test
    fun `multiple excludes subtract the union`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/a.java")
        val fileB = fs.getPath("/a.maybejava")
        val fileC = fs.getPath("/a.notjava")
        Files.createFile(fileA)
        Files.createFile(fileB)
        Files.createFile(fileC)

        val result =
            expander.expandAndFilter(
                listOf(fs.getPath("/")), false,
                excludes = listOf("*.notjava", "*.maybejava")
            )

        assertEquals(setOf(fileA), result.toSet())
    }

    @Test
    fun `includes and excludes work together`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        val fileA = fs.getPath("/1.java")
        val fileB = fs.getPath("/2.java")
        val fileC = fs.getPath("/3.notjava")
        Files.createFile(fileA)
        Files.createFile(fileB)
        Files.createFile(fileC)

        val result = expander.expandAndFilter(
            listOf(fs.getPath("/")),
            false,
            includes = listOf("*.java"),
            excludes = listOf("2.*")
        )

        assertEquals(setOf(fileA), result.toSet())
    }

    @Test
    fun `include GLOB works in any directory`() {
        val fs = Jimfs.newFileSystem(Configuration.unix())
        Files.createDirectories(fs.getPath("/1"))
        Files.createDirectories(fs.getPath("/2"))
        val fileA = fs.getPath("/1/a.java")
        val fileB = fs.getPath("/2/a.java")
        val fileC = fs.getPath("/1/a.notjava")
        val fileD = fs.getPath("/2/a.notjava")
        Files.createFile(fileA)
        Files.createFile(fileB)
        Files.createFile(fileC)
        Files.createFile(fileD)

        val result = expander.expandAndFilter(
            listOf(fs.getPath("/")), true,
            includes = listOf("*.java")
        )

        assertEquals(setOf(fileA, fileB), result.toSet())
    }
}