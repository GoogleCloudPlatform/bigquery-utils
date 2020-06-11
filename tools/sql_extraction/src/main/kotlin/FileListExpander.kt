package com.google.cloud.sqlecosystem.sqlextraction

import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

class FileListExpander {
    fun expandAndFilter(
        dirs: Collection<Path>,
        recursive: Boolean = false,
        includes: List<String> = emptyList(),
        excludes: List<String> = emptyList()
    ): Collection<Path> {
        assert(!dirs.isEmpty()) { "dirs cannot be empty" }

        val fileSystem = dirs.first().fileSystem
        val includeMatchers = includes.map { fileSystem.getPathMatcher("glob:$it") }
        val excludeMatchers = excludes.map { fileSystem.getPathMatcher("glob:$it") }

        return dirs.stream().flatMap { basePath ->
            Files.walk(basePath, if (recursive) Int.MAX_VALUE else 1)
                .filter { Files.isRegularFile(it) }
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
        }.collect(Collectors.toSet())
    }
}