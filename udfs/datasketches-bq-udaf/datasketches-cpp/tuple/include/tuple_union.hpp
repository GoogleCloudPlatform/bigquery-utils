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

#ifndef TUPLE_UNION_HPP_
#define TUPLE_UNION_HPP_

#include "tuple_sketch.hpp"
#include "theta_union_base.hpp"

namespace datasketches {

// for types with defined + operation
template<typename Summary>
struct default_tuple_union_policy {
  void operator()(Summary& summary, const Summary& other) const {
    summary += other;
  }
};

/**
 * Tuple Union.
 * Computes union of Tuple sketches. There is no constructor. Use builder instead.
 */
template<
  typename Summary,
  typename Policy = default_tuple_union_policy<Summary>,
  typename Allocator = std::allocator<Summary>
>
class tuple_union {
public:
  using Entry = std::pair<uint64_t, Summary>;
  using ExtractKey = pair_extract_key<uint64_t, Summary>;
  using Sketch = tuple_sketch<Summary, Allocator>;
  using CompactSketch = compact_tuple_sketch<Summary, Allocator>;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using resize_factor = theta_constants::resize_factor;

  // reformulate the external policy that operates on Summary
  // in terms of operations on Entry
  struct internal_policy {
    internal_policy(const Policy& external_policy): external_policy_(external_policy) {}
    void operator()(Entry& internal_entry, const Entry& incoming_entry) const {
      external_policy_(internal_entry.second, incoming_entry.second);
    }
    void operator()(Entry& internal_entry, Entry&& incoming_entry) const {
      external_policy_(internal_entry.second, std::move(incoming_entry.second));
    }
    const Policy& get_external_policy() const { return external_policy_; }
    Policy external_policy_;
  };

  using State = theta_union_base<Entry, ExtractKey, internal_policy, Sketch, CompactSketch, AllocEntry>;

  // No constructor here. Use builder instead.
  class builder;

  /**
   * Update the union with a given sketch
   * @param sketch to update the union with
   */
  template<typename FwdSketch>
  void update(FwdSketch&& sketch);

  /**
   * Produces a copy of the current state of the union as a compact sketch.
   * @param ordered optional flag to specify if an ordered sketch should be produced
   * @return the result of the union
   */
  CompactSketch get_result(bool ordered = true) const;

  /**
   * Reset the union to the initial empty state
   */
  void reset();

protected:
  State state_;

  // for builder
  tuple_union(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta, uint64_t seed, const Policy& policy, const Allocator& allocator);
};

/// Tuple union builder
template<typename S, typename P, typename A>
class tuple_union<S, P, A>::builder: public tuple_base_builder<builder, P, A> {
public:
  /**
   * Constructor.
   * Creates and instance of the builder with default parameters.
   * @param policy
   * @param allocator
   */
  builder(const P& policy = P(), const A& allocator = A());

  /**
   * Create an instance of the union with predefined parameters.
   * @return an instance of the union
   */
  tuple_union build() const;
};

} /* namespace datasketches */

#include "tuple_union_impl.hpp"

#endif
