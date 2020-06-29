package com.google.cloud.sqlecosystem.sqlextraction

import java.nio.file.Files
import java.nio.file.Path
import kotlin.streams.asSequence

/**
 * Expands a collection of file and directory paths to a list of all applicable file paths
 */
class FilesExpander {
    /**
     * Converts a collection of file and directory paths to all applicable file paths
     *
     * @param[dirs] File and directory paths to consider
     * @param[recursive] True of visit subdirectories recursively,
     *     false to only visit the top-level directories
     * @param[includes] List of GLOB patterns to limit added output file paths
     * @param[excludes] List of GLOB patterns to filter out output file paths
     * @return Expanded and filtered collection of file paths to analyze
     */
    fun expandAndFilter(
        dirs: Collection<Path>,
        recursive: Boolean = false,
        includes: List<String> = emptyList(),
        excludes: List<String> = emptyList()
    ): Sequence<Path> {
        assert(!dirs.isEmpty()) { "dirs cannot be empty" }

        val fileSystem = dirs.first().fileSystem
        val includeMatchers = includes.map { fileSystem.getPathMatcher("glob:$it") }
        val excludeMatchers = excludes.map { fileSystem.getPathMatcher("glob:$it") }

        return dirs.asSequence().flatMap { basePath ->
            Files.walk(basePath, if (recursive) Int.MAX_VALUE else 1)
                .asSequence().filter { Files.isRegularFile(it) }
                .filter { path ->
                    includeMatchers.isEmpty() || includeMatchers.any { matcher ->
                        matcher.matches(path) || matcher.matches(path.fileName)
                    }
                }
                .filter { path ->
                    excludeMatchers.isEmpty() || !excludeMatchers.any { matcher ->
                        matcher.matches(path) || matcher.matches(path.fileName)
                    }
                }
        }.distinct()
    }
}