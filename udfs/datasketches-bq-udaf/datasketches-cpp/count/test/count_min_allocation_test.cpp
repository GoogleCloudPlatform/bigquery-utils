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

#include <catch2/catch.hpp>
#include <vector>
#include <cstring>
#include <sstream>
#include <fstream>

#include "count_min.hpp"
#include "common_defs.hpp"
#include "test_allocator.hpp"

namespace datasketches {

using count_min_sketch_test_alloc = count_min_sketch<uint64_t, test_allocator<uint64_t>>;
using alloc = test_allocator<uint64_t>;

TEST_CASE("CountMin sketch test allocator: serialize-deserialize empty", "[cm_sketch_alloc]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    uint8_t n_hashes = 1;
    uint32_t n_buckets = 5;
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    count_min_sketch_test_alloc c(n_hashes, n_buckets, DEFAULT_SEED, alloc(0));
    c.serialize(s);
    count_min_sketch_test_alloc d = count_min_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0)) ;
    REQUIRE(c.get_num_hashes() == d.get_num_hashes());
    REQUIRE(c.get_num_buckets() == d.get_num_buckets());
    REQUIRE(c.get_seed() == d.get_seed());
    uint64_t zero = 0;
    REQUIRE(c.get_estimate(zero) == d.get_estimate(zero));
    REQUIRE(c.get_total_weight() == d.get_total_weight());

    // Check that all entries are equal and 0
    for (auto di: d) {
      REQUIRE(di == 0);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("CountMin sketch test allocator: serialize-deserialize non-empty", "[cm_sketch_alloc]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    uint8_t n_hashes = 3;
    uint32_t n_buckets = 1024;
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    count_min_sketch_test_alloc c(n_hashes, n_buckets, DEFAULT_SEED, alloc(0));
    for (uint64_t i = 0; i < 10; ++i) c.update(i, 10 * i * i);
    c.serialize(s);
    count_min_sketch_test_alloc d = count_min_sketch_test_alloc::deserialize(s, DEFAULT_SEED, alloc(0));
    REQUIRE(c.get_num_hashes() == d.get_num_hashes());
    REQUIRE(c.get_num_buckets() == d.get_num_buckets());
    REQUIRE(c.get_seed() == d.get_seed());
    REQUIRE(c.get_total_weight() == d.get_total_weight());
    for (uint64_t i = 0; i < 10; ++i) {
      REQUIRE(c.get_estimate(i) == d.get_estimate(i));
    }

    auto c_it = c.begin();
    auto d_it = d.begin();
    while (c_it != c.end()) {
      REQUIRE(*c_it == *d_it);
      ++c_it;
      ++d_it;
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("CountMin sketch test allocator: bytes serialize-deserialize empty", "[cm_sketch_alloc]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    uint8_t n_hashes = 3;
    uint32_t n_buckets = 32;
    count_min_sketch_test_alloc c(n_hashes, n_buckets, DEFAULT_SEED, alloc(0));
    auto bytes = c.serialize();

    REQUIRE_THROWS_AS(count_min_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED-1, alloc(0)), std::invalid_argument);
    auto d = count_min_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, alloc(0));
    REQUIRE(c.get_num_hashes() == d.get_num_hashes());
    REQUIRE(c.get_num_buckets() == d.get_num_buckets());
    REQUIRE(c.get_seed() == d.get_seed());
    uint64_t zero = 0;
    REQUIRE(c.get_estimate(zero) == d.get_estimate(zero));
    REQUIRE(c.get_total_weight() == d.get_total_weight());

    // Check that all entries are equal and 0
    for (auto di: d) {
      REQUIRE(di == 0);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("CountMin sketch test allocator: bytes serialize-deserialize non-empty", "[cm_sketch_alloc]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    uint8_t n_hashes = 5;
    uint32_t n_buckets = 64;
    count_min_sketch_test_alloc c(n_hashes, n_buckets, DEFAULT_SEED, alloc(0));
    for (uint64_t i = 0; i < 10; ++i) c.update(i, 10 * i * i);

    auto bytes = c.serialize();
    REQUIRE_THROWS_AS(count_min_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED-1, alloc(0)), std::invalid_argument);
    auto d = count_min_sketch_test_alloc::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED, alloc(0));

    REQUIRE(c.get_num_hashes() == d.get_num_hashes());
    REQUIRE(c.get_num_buckets() == d.get_num_buckets());
    REQUIRE(c.get_seed() == d.get_seed());
    REQUIRE(c.get_total_weight() == d.get_total_weight());

    // Check that all entries are equal
    auto c_it = c.begin();
    auto d_it = d.begin();
    while (c_it != c.end()) {
      REQUIRE(*c_it == *d_it);
      ++c_it;
      ++d_it;
    }

    // Check that the estimates agree
    for (uint64_t i = 0; i < 10; ++i) {
      REQUIRE(c.get_estimate(i) == d.get_estimate(i));
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} // namespace datasketches
