/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef ARRAY_OF_DOUBLES_SKETCH_HPP_
#define ARRAY_OF_DOUBLES_SKETCH_HPP_

#include "array_tuple_sketch.hpp"
#include "array_tuple_union.hpp"
#include "array_tuple_intersection.hpp"
#include "array_tuple_a_not_b.hpp"

namespace datasketches {

/// convenience alias with default allocator, default policy for update_array_of_doubles_sketch
using default_array_of_doubles_update_policy = default_array_tuple_update_policy<array<double>>;

/// convenience alias with default allocator, equivalent to ArrayOfDoublesUpdatableSketch in Java
using update_array_of_doubles_sketch = update_array_tuple_sketch<array<double>>;

/// convenience alias with default allocator, equivalent to ArrayOfDoublesCompactSketch in Java
using compact_array_of_doubles_sketch = compact_array_tuple_sketch<array<double>>;

/// convenience alias, default policy for array_of_doubles_union
using default_array_of_doubles_union_policy = default_array_tuple_union_policy<array<double>>;

/// convenience alias with default allocator, equivalent to ArrayOfDoublesUnion in Java
using array_of_doubles_union = array_tuple_union<array<double>>;

/// convenience alias with default allocator, equivalent to ArrayOfDoublesIntersection in Java
/// no default policy since it is not clear in general
template<typename Policy> using array_of_doubles_intersection = array_tuple_intersection<array<double>, Policy>;

/// convenience alias with default allocator, equivalent to ArrayOfDoublesAnotB in Java
using array_of_doubles_a_not_b = array_tuple_a_not_b<array<double>>;

} /* namespace datasketches */

#endif
