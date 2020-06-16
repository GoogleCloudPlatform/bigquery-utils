package com.google.cloud.sqlecosystem.sqlextraction

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.path
import mu.KotlinLogging
import java.nio.file.Path

private val logger = KotlinLogging.logger { }

fun main(args: Array<String>) = Cli().main(args)

class Cli : CliktCommand(
    name = "sql_extraction",
    printHelpOnEmptyArgs = true,
    help = """
    Command line application to extract raw SQL query strings and their usage within code.
    
    Sample Usages:
    ```
    > sql_extraction path/to/file.java
    > sql_extraction -r path/to/directory path/to/another/directory path/to/file.java
    > sql_extraction --include="*.java" path/to/directory
    > sql_extraction -r --include="*.java" --include="*.cs" .
    > sql_extraction -r --exclude="*.cs" /
    ```
    """
) {
    val recursive: Boolean by option(
        "-R", "-r", "--recursive",
        help = "scan files in subdirectories recursively"
    ).flag()
    val filePaths: List<Path> by argument(help = "file and directory paths to analyze").path(
        mustExist = true,
        canBeDir = true,
        canBeFile = true
    ).multiple()
    val includes: List<String> by option(
        "--include",
        metavar = "GLOB",
        help = "Search only files whose base name matches GLOB"
    ).multiple()
    val excludes: List<String> by option(
        "--exclude",
        metavar = "GLOB",
        help = "Skip files whose base name matches GLOB"
    ).multiple()

    override fun run() {
        logger.debug("Starting SQL Extraction from command line")

        val files = FileListExpander().expandAndFilter(filePaths, recursive, includes, excludes)
        logger.debug { "Files to analyze: $files" }
    }
}
