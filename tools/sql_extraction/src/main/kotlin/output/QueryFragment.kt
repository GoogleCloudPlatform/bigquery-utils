package com.google.cloud.sqlecosystem.sqlextraction.output

enum class FragmentCount { SINGLE, OPTIONAL, MULTIPLE, UNKNOWN }
enum class ComplexType { AND, OR, UNKNOWN }

data class QueryFragment(
    val count: FragmentCount = FragmentCount.SINGLE,
    var location: Location,
    val literal: String? = null,
    val complex: MutableList<QueryFragment>? = null,
    val type: ComplexType? = null
) {
    companion object {
        fun createLiteral(
            count: FragmentCount,
            location: Location,
            literal: String
        ) = QueryFragment(count, location, literal)

        fun createComplex(
            count: FragmentCount,
            complex: Iterable<QueryFragment>,
            type: ComplexType = ComplexType.UNKNOWN
        ) = QueryFragment(
            count = count,
            location = Location.combine(complex.map { it.location }),
            complex = complex.toMutableList(),
            type = type
        )

        /**
         * Combines [base] and [addition] into a complex fragment
         * either by mutating [base] or by returning a newly created fragment
         */
        fun combine(
            count: FragmentCount,
            base: QueryFragment,
            addition: QueryFragment,
            type: ComplexType = ComplexType.UNKNOWN
        ): QueryFragment {
            return if (base.type != type || base.count != count) {
                createComplex(count, listOf(base, addition), type)
            } else {
                base.location = Location.combine(base.location, addition.location)
                base.complex!!.add(addition)
                base
            }
        }
    }
}
