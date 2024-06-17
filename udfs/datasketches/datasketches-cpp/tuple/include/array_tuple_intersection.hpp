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

#ifndef ARRAY_TUPLE_INTERSECTION_HPP_
#define ARRAY_TUPLE_INTERSECTION_HPP_

#include <vector>
#include <memory>

#include "array_tuple_sketch.hpp"
#include "tuple_intersection.hpp"

namespace datasketches {

/// array tuple intersection
template<
  typename Array,
  typename Policy,
  typename Allocator = typename Array::allocator_type
>
class array_tuple_intersection: public tuple_intersection<Array, Policy, Allocator> {
public:
  using Base = tuple_intersection<Array, Policy, Allocator>;
  using CompactSketch = compact_array_tuple_sketch<Array, Allocator>;
  using resize_factor = theta_constants::resize_factor;

  /**
   * Constructor
   * @param seed for the hash function that was used to create the sketch
   * @param policy user-defined way of combining Summary during intersection
   * @param allocator to use for allocating and deallocating memory
   */
  explicit array_tuple_intersection(uint64_t seed = DEFAULT_SEED, const Policy& policy = Policy(), const Allocator& allocator = Allocator());

  /**
   * Produces a copy of the current state of the intersection.
   * If update() was not called, the state is the infinite "universe",
   * which is considered an undefined state, and throws an exception.
   * @param ordered optional flag to specify if an ordered sketch should be produced
   * @return the result of the intersection as a compact sketch
   */
  CompactSketch get_result(bool ordered = true) const;
};

} /* namespace datasketches */

#include "array_tuple_intersection_impl.hpp"

#endif
