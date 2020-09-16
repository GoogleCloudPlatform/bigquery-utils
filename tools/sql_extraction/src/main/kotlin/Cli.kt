package com.google.cloud.sqlecosystem.sqlextraction

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.double
import com.github.ajalt.clikt.parameters.types.path
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import mu.KotlinLogging
import org.slf4j.impl.SimpleLogger
import java.nio.file.Path

fun main(args: Array<String>) = Cli(
    FilesExpander(),
    SqlExtractor(
        DataFlowSolver(
            listOf(
                JavaFrontEnd(),
                PythonFrontEnd()
            )
        ), ConfidenceRater()
    )
).main(args)

private class Cli(
    private val filesExpander: FilesExpander,
    private val sqlExtractor: SqlExtractor
) :
    CliktCommand(
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
    private val filePaths: List<Path> by argument(
        help = "File and directory paths to analyze"
    ).path(
        mustExist = true,
        canBeDir = true,
        canBeFile = true
    ).multiple()

    private val recursive: Boolean by option(
        "-R", "-r", "--recursive",
        help = "Scan files in subdirectories recursively"
    ).flag()

    private val includes: List<String> by option(
        "--include",
        metavar = "GLOB",
        help = "Search only files whose base name matches GLOB"
    ).multiple()

    private val excludes: List<String> by option(
        "--exclude",
        metavar = "GLOB",
        help = "Skip files whose base name matches GLOB"
    ).multiple()

    private val prettyPrint: Boolean by option(
        "--pretty", help = "Pretty-print output JSON"
    ).flag()

    private val showProgress: Boolean by option(
        "--progress", help = "Print progress to STDERR"
    ).flag()

    private val confidenceThreshold: Double by option(
        "--threshold",
        help = "Minimum confidence value (inclusive between 0 and 1) " +
                "required for a detected query to be included in final " +
                "output (default is 0.5)"
    ).double().default(0.5)

    private val parallelize: Boolean by option(
        "--parallel", help = "Analyze multiple files in parallel"
    ).flag()

    private val debug: Boolean by option(
        "--debug", "--verbose", help = "Print debug logs to STDERR"
    ).flag()

    override fun run() {
        if (debug) {
            System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "Debug")
        }
        val logger = KotlinLogging.logger { }
        logger.debug("Starting SQL Extraction from command line")

        val files = filesExpander.expandAndFilter(filePaths, recursive, includes, excludes)
        if (showProgress) {
            System.err.println("0.0% Analyzing files...")
        }

        val output = sqlExtractor.process(files, confidenceThreshold, showProgress, parallelize)
        logger.debug { "output: ${Gson().toJson(output)}" }

        val builder = GsonBuilder()
        if (prettyPrint) {
            builder.setPrettyPrinting()
        }
        val json = builder.create().toJson(output)

        println(json)
    }
}
