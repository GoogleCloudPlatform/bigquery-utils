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

#include <ebpps_sketch.hpp>

#include <catch2/catch.hpp>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <random>
#include <stdexcept>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

static constexpr double EPS = 1e-13;

static ebpps_sketch<int> create_unweighted_sketch(uint32_t k, uint64_t n) {
  ebpps_sketch<int> sk(k);
  for (uint64_t i = 0; i < n; ++i) {
    sk.update(static_cast<int>(i), 1.0);
  }
  return sk;
}

template<typename T, typename A>
static void check_if_equal(ebpps_sketch<T, A>& sk1, ebpps_sketch<T, A>& sk2) {
  REQUIRE(sk1.get_k() == sk2.get_k());
  REQUIRE(sk1.get_n() == sk2.get_n());
  REQUIRE(sk1.get_c() == sk2.get_c());
  REQUIRE(sk1.get_cumulative_weight() == sk2.get_cumulative_weight());

  auto it1 = sk1.begin();
  auto it2 = sk2.begin();
  size_t count = 0;

  while ((it1 != sk1.end()) && (it2 != sk2.end())) {
    REQUIRE(*it1 == *it2);
    ++it1;
    ++it2;
    ++count;
  }

  REQUIRE(((count == std::floor(sk1.get_c())) || (count == std::ceil(sk1.get_c()))));

  // if c != floor(c) one sketch may not have reached the end,
  // but that's not testable from the external API
}

TEST_CASE("ebpps sketch: invalid k", "[ebpps_sketch]") {
  REQUIRE_THROWS_AS(ebpps_sketch<int>(0), std::invalid_argument);
  REQUIRE_THROWS_AS(ebpps_sketch<int>(ebpps_constants::MAX_K + 1), std::invalid_argument);
}

TEST_CASE("ebpps sketch: invalid weights", "[ebpps_sketch]") {
  uint32_t k = 100;
  ebpps_sketch<int> sk = create_unweighted_sketch(k, 3);
  REQUIRE(sk.get_n() == 3);
  REQUIRE(sk.get_cumulative_weight() == 3.0);
  sk.update(-1, 0.0); // no-op
  REQUIRE(sk.get_n() == 3);
  REQUIRE(sk.get_cumulative_weight() == 3.0);

  REQUIRE_THROWS_AS(sk.update(-2, -1.0), std::invalid_argument);

  ebpps_sketch<float> sk2(k);
  REQUIRE_THROWS_AS(sk2.update(-2, std::numeric_limits<float>::infinity()), std::invalid_argument);
  REQUIRE_THROWS_AS(sk2.update(-2, nanf("")), std::invalid_argument);
}

TEST_CASE("ebpps sketch: insert items", "[ebpps_sketch]") {
  size_t n = 0;
  uint32_t k = 5;
  ebpps_sketch<int> sk = create_unweighted_sketch(k, n);
  REQUIRE(sk.get_allocator() == std::allocator<int>());
  REQUIRE(sk.get_k() == k);
  REQUIRE(sk.get_n() == 0);
  REQUIRE(sk.get_c() == 0.0);
  REQUIRE(sk.get_cumulative_weight() == 0.0);
  REQUIRE(sk.is_empty());

  n = k;
  sk = create_unweighted_sketch(k, n);
  REQUIRE_FALSE(sk.is_empty());
  REQUIRE(sk.get_n() == n);
  REQUIRE(sk.get_cumulative_weight() == static_cast<double>(n));
  for (int val : sk.get_result())
    REQUIRE(val < static_cast<int>(n));

  n = k * 10;
  sk = create_unweighted_sketch(k, n);
  REQUIRE_FALSE(sk.is_empty());
  REQUIRE(sk.get_n() == n);
  REQUIRE(sk.get_cumulative_weight() == static_cast<double>(n));
  
  auto result = sk.get_result();
  REQUIRE(result.size() == sk.get_k()); // uniform weights so should be exactly k
  for (int val : sk.get_result())
    REQUIRE(val < static_cast<int>(n));
}

TEST_CASE("ebpps sketch: serialize/deserialize string", "[ebpps_sketch]") {
  // since C <= k we don't have the usual sketch notion of exact vs estimation
  // mode at any time. The only real serializaiton cases are empty and non-empty
  // with and without a partial item
  uint32_t k = 10;
  ebpps_sketch<std::string> sk(k);

  // empty  
  auto bytes = sk.serialize();
  REQUIRE(bytes.size() == sk.get_serialized_size_bytes());
  REQUIRE_THROWS_AS(ebpps_sketch<std::string>::deserialize(bytes.data(), bytes.size()-1), std::out_of_range);
  auto sk_bytes = ebpps_sketch<std::string>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_bytes);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(ss);
  auto sk_stream = ebpps_sketch<std::string>::deserialize(ss);
  check_if_equal(sk, sk_stream);
  check_if_equal(sk_bytes, sk_stream); // should be redundant

  for (uint32_t i = 0; i < k; ++i)
    sk.update(std::to_string(i));

  // non-empty, no partial item
  bytes = sk.serialize();
  REQUIRE(bytes.size() == sk.get_serialized_size_bytes());
  REQUIRE_THROWS_AS(ebpps_sketch<std::string>::deserialize(bytes.data(), bytes.size()-1), std::out_of_range);
  sk_bytes = ebpps_sketch<std::string>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_bytes);

  ss.str("");
  sk.serialize(ss);
  sk_stream = ebpps_sketch<std::string>::deserialize(ss);
  check_if_equal(sk, sk_stream);
  check_if_equal(sk_bytes, sk_stream); // should be redundant

  // non-empty with partial item
  sk.update(std::to_string(2 * k), 2.5);
  REQUIRE(sk.get_cumulative_weight() == Approx(k + 2.5).margin(EPS));
  bytes = sk.serialize();
  REQUIRE(bytes.size() == sk.get_serialized_size_bytes());
  REQUIRE_THROWS_AS(ebpps_sketch<std::string>::deserialize(bytes.data(), bytes.size()-1), std::out_of_range);
  sk_bytes = ebpps_sketch<std::string>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_bytes);

  ss.str("");
  sk.serialize(ss);
  sk_stream = ebpps_sketch<std::string>::deserialize(ss);
  check_if_equal(sk, sk_stream);
  check_if_equal(sk_bytes, sk_stream); // should be redundant
}

TEST_CASE("ebpps sketch: serialize/deserialize ints", "[ebpps_sketch]") {
  uint32_t k = 10;
  ebpps_sketch<uint32_t> sk(k);

  for (uint32_t i = 0; i < k; ++i)
    sk.update(i);
  sk.update(2 * k, 3.5);
  REQUIRE(sk.get_cumulative_weight() == Approx(k + 3.5).margin(EPS));

  auto bytes = sk.serialize();
  REQUIRE(bytes.size() == sk.get_serialized_size_bytes());
  REQUIRE_THROWS_AS(ebpps_sketch<uint32_t>::deserialize(bytes.data(), bytes.size()-1), std::out_of_range);
  auto sk_bytes = ebpps_sketch<uint32_t>::deserialize(bytes.data(), bytes.size());
  check_if_equal(sk, sk_bytes);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(ss);
  auto sk_stream = ebpps_sketch<uint32_t>::deserialize(ss);
  check_if_equal(sk, sk_stream);
  check_if_equal(sk_bytes, sk_stream); // should be redundant

  sk.reset();
  REQUIRE(sk.get_k() == k);
  REQUIRE(sk.get_n() == 0);
  REQUIRE(sk.get_c() == 0.0);
  REQUIRE(sk.get_cumulative_weight() == 0.0);
  REQUIRE(sk.is_empty());
}

TEST_CASE("ebpps sketch: merge large into small", "[ebpps_sketch]") {
  uint32_t k = 100;

  // lvalue merge  
  ebpps_sketch<int> sk1(k / 2);
  sk1.update(-1, k / 10.0); // one heavy item, but less than sk2 weight  
  ebpps_sketch<int> sk2 = create_unweighted_sketch(k, k);

  sk1.merge(sk2);
  REQUIRE(sk1.get_k() == k / 2);
  REQUIRE(sk1.get_n() == k + 1);
  REQUIRE(sk1.get_c() < k);
  REQUIRE(sk1.get_cumulative_weight() == Approx(1.1 * k).margin(EPS));
  
  // rvalue merge
  sk1 = create_unweighted_sketch(k / 2, 0);
  sk1.update(-1, k / 4.0);
  sk1.update(-2, k / 8.0);
  // sk2 should have been unchaged
  REQUIRE(sk2.get_n() == k);
  REQUIRE(sk2.get_c() == Approx(k).margin(EPS));

  sk1.merge(std::move(sk2));
  REQUIRE(sk1.get_k() == k / 2);
  REQUIRE(sk1.get_n() == k + 2);
  REQUIRE(sk1.get_c() < k);
  // cumulative weight is now (1.5 + 0.2) k
  REQUIRE(sk1.get_cumulative_weight() == Approx(1.375 * k).margin(EPS));
}

TEST_CASE("ebpps sketch: merge small into large", "[ebpps_sketch]") {
  uint32_t k = 100;

  // lvalue merge  
  ebpps_sketch<int> sk1 = create_unweighted_sketch(k, k);
  ebpps_sketch<int> sk2(k / 2);
  sk2.update(-1, k / 10.0); // one heavy item, but less than sk1 weight

  sk1.merge(sk2);
  REQUIRE(sk1.get_k() == k / 2);
  REQUIRE(sk1.get_n() == k + 1);
  REQUIRE(sk1.get_c() < k);
  REQUIRE(sk1.get_cumulative_weight() == Approx(1.1 * k).margin(EPS));
  
  // rvalue merge
  sk1 = create_unweighted_sketch(k, 3 * k / 2);
  // sk2 should have been unchaged
  REQUIRE(sk2.get_n() == 1);
  REQUIRE(sk2.get_c() == 1.0);
  sk2.update(-2, k / 10.0);

  sk1.merge(std::move(sk2));
  REQUIRE(sk1.get_k() == k / 2);
  REQUIRE(sk1.get_n() == (3 * k / 2) + 2);
  REQUIRE(sk1.get_c() < k);
  // cumulative weight is now (1.5 + 0.2) k
  REQUIRE(sk1.get_cumulative_weight() == Approx(1.7 * k).margin(EPS));
}

}