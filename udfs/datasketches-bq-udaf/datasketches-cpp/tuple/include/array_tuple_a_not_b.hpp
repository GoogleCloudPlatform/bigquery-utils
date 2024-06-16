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

#ifndef ARRAY_TUPLE_A_NOT_B_HPP_
#define ARRAY_TUPLE_A_NOT_B_HPP_

#include <vector>
#include <memory>

#include "array_tuple_sketch.hpp"
#include "tuple_a_not_b.hpp"

namespace datasketches {

/// array tuple A-not-B
template<typename Array, typename Allocator = typename Array::allocator_type>
class array_tuple_a_not_b: tuple_a_not_b<Array, Allocator> {
public:
  using Base = tuple_a_not_b<Array, Allocator>;
  using CompactSketch = compact_array_tuple_sketch<Array, Allocator>;

  /**
   * Constructor
   * @param seed for the hash function that was used to create the sketch
   * @param allocator to use for allocating and deallocating memory
   */
  explicit array_tuple_a_not_b(uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
   * Computes the A-not-B set operation given two sketches.
   * @param a sketch A
   * @param b sketch B
   * @param ordered optional flag to specify if an ordered sketch should be produced
   * @return the result of A-not-B as a compact sketch
   */
  template<typename FwdSketch, typename Sketch>
  CompactSketch compute(FwdSketch&& a, const Sketch& b, bool ordered = true) const;
};

} /* namespace datasketches */

#include "array_tuple_a_not_b_impl.hpp"

#endif
