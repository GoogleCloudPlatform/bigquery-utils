package com.google.cloud.sqlecosystem.sqlextraction.output

import java.lang.Integer.max
import java.lang.Integer.min

/**
 * All values inclusive
 */
data class Location(
    val startLine: Int,
    val startColumn: Int,
    val endLine: Int,
    val endColumn: Int
) {
    companion object {
        /** Returns the union of [a] and [b] */
        fun combine(a: Location, b: Location): Location {
            return Location(
                startLine = min(a.startLine, b.startLine),
                startColumn = when {
                    a.startLine < b.startLine -> a.startColumn
                    a.startLine > b.startLine -> b.startColumn
                    else -> min(a.startColumn, b.startColumn)
                },
                endLine = max(a.endLine, b.endLine),
                endColumn = when {
                    a.endLine > b.endLine -> a.endColumn
                    a.endLine < b.endLine -> b.endColumn
                    else -> max(a.endColumn, b.endColumn)
                }
            )
        }

        /** Reduces [locations] into the union using [combine] */
        fun combine(locations: Iterable<Location>): Location {
            return locations.reduce { a, b -> combine(a, b) }
        }
    }
}