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

#ifndef CPC_UNION_HPP_
#define CPC_UNION_HPP_

#include <string>

#include "cpc_sketch.hpp"
#include "common_defs.hpp"

namespace datasketches {

/// CPC union alias with default allocator
using cpc_union = cpc_union_alloc<std::allocator<uint8_t>>;

/**
 * High performance C++ implementation of Compressed Probabilistic Counting (CPC) Union
 *
 * author Kevin Lang
 * author Alexander Saydakov
 */
template<typename A>
class cpc_union_alloc {
public:
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<A>::template rebind_alloc<uint8_t>>;
  using vector_u64 = std::vector<uint64_t, typename std::allocator_traits<A>::template rebind_alloc<uint64_t>>;

  /**
   * Creates an instance of the union given the lg_k parameter and hash seed.
   * @param lg_k base 2 logarithm of the number of bins in the sketch
   * @param seed for hash function
   * @param allocator instance of an allocator
   */
  explicit cpc_union_alloc(uint8_t lg_k = cpc_constants::DEFAULT_LG_K, uint64_t seed = DEFAULT_SEED, const A& allocator = A());

  /**
   * Copy constructor
   * @param other union to be copied
   */
  cpc_union_alloc(const cpc_union_alloc<A>& other);

  /**
   * Move constructor
   * @param other union to be moved
   */
  cpc_union_alloc(cpc_union_alloc<A>&& other) noexcept;

  ~cpc_union_alloc();

  /**
   * Copy assignment
   * @param other union to be copied
   * @return reference to this union
   */
  cpc_union_alloc<A>& operator=(const cpc_union_alloc<A>& other);

  /**
   * Move assignment
   * @param other union to be moved
   * @return reference to this union
   */
  cpc_union_alloc<A>& operator=(cpc_union_alloc<A>&& other) noexcept;

  /**
   * This method is to update the union with a given sketch (lvalue)
   * @param sketch to update the union with
   */
  void update(const cpc_sketch_alloc<A>& sketch);

  /**
   * This method is to update the union with a given sketch (rvalue)
   * @param sketch to update the union with
   */
  void update(cpc_sketch_alloc<A>&& sketch);

  /**
   * This method produces a copy of the current state of the union as a sketch.
   * @return the result of the union
   */
  cpc_sketch_alloc<A> get_result() const;

private:
  using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
  using AllocU64 = typename std::allocator_traits<A>::template rebind_alloc<uint64_t>;
  using AllocCpc = typename std::allocator_traits<A>::template rebind_alloc<cpc_sketch_alloc<A>>;

  uint8_t lg_k;
  uint64_t seed;
  cpc_sketch_alloc<A>* accumulator;
  vector_u64 bit_matrix;

  template<typename S> void internal_update(S&& sketch); // to support both rvalue and lvalue

  cpc_sketch_alloc<A> get_result_from_accumulator() const;
  cpc_sketch_alloc<A> get_result_from_bit_matrix() const;

  void switch_to_bit_matrix();
  void walk_table_updating_sketch(const u32_table<A>& table);
  void or_table_into_matrix(const u32_table<A>& table);
  void or_window_into_matrix(const vector_bytes& sliding_window, uint8_t offset, uint8_t src_lg_k);
  void or_matrix_into_matrix(const vector_u64& src_matrix, uint8_t src_lg_k);
  void reduce_k(uint8_t new_lg_k);
};

} /* namespace datasketches */

#include "cpc_union_impl.hpp"

#endif
