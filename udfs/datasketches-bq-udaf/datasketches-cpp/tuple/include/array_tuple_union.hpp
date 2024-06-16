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

#ifndef ARRAY_TUPLE_UNION_HPP_
#define ARRAY_TUPLE_UNION_HPP_

#include <vector>
#include <memory>
#include "array_tuple_sketch.hpp"

#include "tuple_union.hpp"

namespace datasketches {

/// default array tuple union policy
template<typename Array>
struct default_array_tuple_union_policy {
  default_array_tuple_union_policy(uint8_t num_values = 1): num_values_(num_values) {}

  void operator()(Array& array, const Array& other) const {
    for (uint8_t i = 0; i < num_values_; ++i) {
      array[i] += other[i];
    }
  }
  uint8_t get_num_values() const {
    return num_values_;
  }
private:
  uint8_t num_values_;
};

/// array tuple union
template<
  typename Array,
  typename Policy = default_array_tuple_union_policy<Array>,
  typename Allocator = typename Array::allocator_type
>
class array_tuple_union: public tuple_union<Array, Policy, Allocator> {
public:
  using value_type = typename Array::value_type;
  using Base = tuple_union<Array, Policy, Allocator>;
  using CompactSketch = compact_array_tuple_sketch<Array, Allocator>;
  using resize_factor = theta_constants::resize_factor;

  class builder;

  CompactSketch get_result(bool ordered = true) const;

private:
  // for builder
  array_tuple_union(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta, uint64_t seed, const Policy& policy, const Allocator& allocator);
};

template<typename Array, typename Policy, typename Allocator>
class array_tuple_union<Array, Policy, Allocator>::builder: public tuple_base_builder<builder, Policy, Allocator> {
public:
  builder(const Policy& policy = Policy(), const Allocator& allocator = Allocator());
  array_tuple_union build() const;
};

} /* namespace datasketches */

#include "array_tuple_union_impl.hpp"

#endif
