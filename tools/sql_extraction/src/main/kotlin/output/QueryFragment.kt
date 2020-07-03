package com.google.cloud.sqlecosystem.sqlextraction.output

enum class FragmentCount { SINGLE, OPTIONAL, MULTIPLE, UNKNOWN }
enum class ComplexType { AND, OR, UNKNOWN }

data class QueryFragment(
    val count: FragmentCount = FragmentCount.SINGLE,
    val location: Location,
    val literal: String? = null,
    val complex: List<QueryFragment>? = null,
    val type: ComplexType? = null
) {
    companion object {
        fun createLiteral(
            count: FragmentCount = FragmentCount.SINGLE,
            location: Location,
            literal: String
        ) = QueryFragment(count, location, literal)

        fun createComplex(
            count: FragmentCount = FragmentCount.SINGLE,
            complex: Iterable<QueryFragment>,
            type: ComplexType = ComplexType.UNKNOWN
        ) = QueryFragment(
            count = count,
            location = Location.combine(complex.map { it.location }),
            complex = complex.toList(),
            type = type
        )

        /**
         * Combines [left] and [right] into a complex fragment.
         * If either fragments are null, a new fragment will not be constructed.
         */
        fun combine(
            count: FragmentCount,
            left: QueryFragment?,
            right: QueryFragment?,
            type: ComplexType = ComplexType.UNKNOWN
        ): QueryFragment? {
            if (left == null)
                return right
            if (right == null)
                return left

            val list = ArrayList<QueryFragment>()
            for (fragment in sequenceOf(left, right)) {
                if (fragment.type == type && fragment.count == count) {
                    list.addAll(fragment.complex!!)
                } else {
                    list.add(fragment)
                }
            }

            return createComplex(count, list, type)
        }
    }
}
