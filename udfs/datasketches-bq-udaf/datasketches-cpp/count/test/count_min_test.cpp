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

namespace datasketches {

TEST_CASE("CM init - throws") {
    REQUIRE_THROWS_AS(count_min_sketch<uint64_t>(5, 1), std::invalid_argument);
    REQUIRE_THROWS_AS(count_min_sketch<uint64_t>(4, 268435456), std::invalid_argument);
}

TEST_CASE("CM init") {
    uint8_t n_hashes = 3;
    uint32_t n_buckets = 5;
    uint64_t seed = 1234567;
    count_min_sketch<uint64_t> c(n_hashes, n_buckets, seed);
    REQUIRE(c.get_num_hashes() == n_hashes);
    REQUIRE(c.get_num_buckets() == n_buckets);
    REQUIRE(c.get_seed() == seed);
    REQUIRE(c.is_empty());

    for (auto x: c) {
      REQUIRE(x == 0);
    }

    // Check the default seed is appropriately set.
    count_min_sketch<uint64_t> c1(n_hashes, n_buckets);
    REQUIRE(c1.get_seed() == DEFAULT_SEED);
}

TEST_CASE("CM parameter suggestions", "[error parameters]") {

    // Bucket suggestions
    REQUIRE_THROWS(count_min_sketch<uint64_t>::suggest_num_buckets(-1.0), "Confidence must be between 0 and 1.0 (inclusive)." );
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.2) == 14);
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.1) == 28);
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.05) == 55);
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_buckets(0.01) == 272);

    // Check that the sketch get_epsilon acts inversely to suggest_num_buckets
    uint8_t n_hashes = 3;
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 14).get_relative_error() <= 0.2);
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 28).get_relative_error() <= 0.1);
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 55).get_relative_error() <= 0.05);
    REQUIRE(count_min_sketch<uint64_t>(n_hashes, 272).get_relative_error() <= 0.01);

    // Hash suggestions
    REQUIRE_THROWS(count_min_sketch<uint64_t>::suggest_num_hashes(10.0), "Confidence must be between 0 and 1.0 (inclusive)." );
    REQUIRE_THROWS(count_min_sketch<uint64_t>::suggest_num_hashes(-1.0), "Confidence must be between 0 and 1.0 (inclusive)." );
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_hashes(0.682689492) == 2); // 1 STDDEV
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_hashes(0.954499736) == 4); // 2 STDDEV
    REQUIRE(count_min_sketch<uint64_t>::suggest_num_hashes(0.997300204) == 6); // 3 STDDEV
}

TEST_CASE("CM one update: uint64_t") {
  uint8_t n_hashes = 3;
  uint32_t n_buckets = 5;
  uint64_t seed = 9223372036854775807; //1234567;
  uint64_t inserted_weight = 0;
  count_min_sketch<uint64_t> c(n_hashes, n_buckets, seed);
  std::string x = "x";

  REQUIRE(c.is_empty());
  REQUIRE(c.get_estimate("x") == 0); // No items in sketch so estimates should be zero
  c.update(x);
  REQUIRE(!c.is_empty());
  REQUIRE(c.get_estimate(x) == 1);
  inserted_weight += 1;

  uint64_t w = 9;
  inserted_weight += w;
  c.update(x, w);
  REQUIRE(c.get_estimate(x) == inserted_weight);

    // Doubles are converted to uint64_t
    double w1 = 10.0;
    inserted_weight += static_cast<uint64_t>(w1);
    c.update(x, static_cast<uint64_t>(w1));
    REQUIRE(c.get_estimate(x) == inserted_weight);
    REQUIRE(c.get_total_weight() == inserted_weight);
    REQUIRE(c.get_estimate(x) <= c.get_upper_bound(x));
    REQUIRE(c.get_estimate(x) >= c.get_lower_bound(x));
}

TEST_CASE("CM frequency cancellation") {
  count_min_sketch<int64_t> c(1, 5);
  c.update("x");
  c.update("y", -1);
  REQUIRE(c.get_total_weight() == 2);
  REQUIRE(c.get_estimate("x") == 1);
  REQUIRE(c.get_estimate("y") == -1);
}

TEST_CASE("CM frequency estimates") {
    int number_of_items = 10;
    std::vector<uint64_t> data(number_of_items);
    std::vector<uint64_t> frequencies(number_of_items);

    // Populate data vector
    for (int i = 0; i < number_of_items; ++i) {
      data[i] = i;
      frequencies[i] = 1ULL << (number_of_items - i);
    }

    double relative_error = 0.1;
    double confidence = 0.99;
    uint32_t n_buckets = count_min_sketch<uint64_t>::suggest_num_buckets(relative_error);
    uint8_t n_hashes = count_min_sketch<uint64_t>::suggest_num_hashes(confidence);

    count_min_sketch<uint64_t> c(n_hashes, n_buckets);
    for (int i = 0; i < number_of_items; ++i) {
      uint64_t value = data[i];
      uint64_t freq = frequencies[i];
      c.update(value, freq);
    }

    for (const auto i: data) {
      uint64_t est = c.get_estimate(i);
      uint64_t upp = c.get_upper_bound(i);
      uint64_t low = c.get_lower_bound(i);
      REQUIRE(est <= upp);
      REQUIRE(est >= low);
    }
}

TEST_CASE("CM merge - reject", "[reject cases]") {
    double relative_error = 0.25;
    double confidence = 0.9;
    uint32_t n_buckets = count_min_sketch<uint64_t>::suggest_num_buckets(relative_error);
    uint8_t n_hashes = count_min_sketch<uint64_t>::suggest_num_hashes(confidence);
    count_min_sketch<uint64_t> s(n_hashes, n_buckets, 9082435234709287);

    // Generate sketches that we cannot merge into ie they disagree on at least one of the config entries
    count_min_sketch<uint64_t> s1(n_hashes+1, n_buckets); // incorrect number of hashes
    count_min_sketch<uint64_t> s2(n_hashes, n_buckets + 1); // incorrect number of buckets
    count_min_sketch<uint64_t> s3(n_hashes, n_buckets, 1); // incorrect seed
    std::vector<count_min_sketch<uint64_t>> sketches = {s1, s2, s3};

    // Fail cases
    REQUIRE_THROWS(s.merge(s), "Cannot merge a sketch with itself." );
    for (count_min_sketch<uint64_t> sk : sketches) {
      REQUIRE_THROWS(s.merge(sk), "Incompatible sketch config." );
    }
}

TEST_CASE("CM merge - pass", "[acceptable cases]") {
    double relative_error = 0.25;
    double confidence = 0.9;
    uint32_t n_buckets = count_min_sketch<uint64_t>::suggest_num_buckets(relative_error);
    uint8_t n_hashes = count_min_sketch<uint64_t>::suggest_num_hashes(confidence);
    count_min_sketch<uint64_t> s(n_hashes, n_buckets);
    uint8_t s_hashes = s.get_num_hashes();
    uint32_t s_buckets = s.get_num_buckets();
    count_min_sketch<uint64_t> t(s_hashes, s_buckets);

    // Merge in an all-zeros sketch t.  Should not change the total weight.
    s.merge(t);
    REQUIRE(s.get_total_weight() == 0 );

    std::vector<uint64_t> data = {2,3,5,7};
    for (auto d: data) {
      s.update(d);
      t.update(d);
    }
    s.merge(t);

    REQUIRE(s.get_total_weight() == 2 * t.get_total_weight());

    // Estimator checks.
    for (auto x: data) {
      REQUIRE(s.get_estimate(x) <= s.get_upper_bound(x));
      REQUIRE(s.get_estimate(x) <= 2); // True frequency x == 2 for all x.
    }
  }

TEST_CASE("CountMin sketch: serialize-deserialize empty", "[cm_sketch]") {
    uint8_t n_hashes = 1;
    uint32_t n_buckets = 5;
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    count_min_sketch<uint64_t> c(n_hashes, n_buckets);
    c.serialize(s);
    count_min_sketch<uint64_t> d = count_min_sketch<uint64_t>::deserialize(s, DEFAULT_SEED);
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
    std::ofstream os("count_min-empty.bin");
    c.serialize(os);
}

TEST_CASE("CountMin sketch: serialize-deserialize non-empty", "[cm_sketch]") {
  uint8_t n_hashes = 3;
  uint32_t n_buckets = 1024;
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  count_min_sketch<uint64_t> c(n_hashes, n_buckets);
  for (uint64_t i = 0; i < 10; ++i) c.update(i, 10 * i * i);
  c.serialize(s);
  count_min_sketch<uint64_t> d = count_min_sketch<uint64_t>::deserialize(s, DEFAULT_SEED);
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

  std::ofstream os("count_min-non-empty.bin");
  c.serialize(os);
}

TEST_CASE("CountMin sketch: bytes serialize-deserialize empty", "[cm_sketch]") {
  uint8_t n_hashes = 3;
  uint32_t n_buckets = 32;
  count_min_sketch<uint64_t> c(n_hashes, n_buckets);
  auto bytes = c.serialize();

  REQUIRE_THROWS_AS(count_min_sketch<uint64_t>::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED-1), std::invalid_argument);
  auto d = count_min_sketch<uint64_t>::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED);
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


TEST_CASE("CountMin sketch: bytes serialize-deserialize non-empty", "[cm_sketch]") {
  uint8_t n_hashes = 5;
  uint32_t n_buckets = 64;
  count_min_sketch<uint64_t> c(n_hashes, n_buckets);
  for(uint64_t i=0; i < 10; ++i) c.update(i,10*i*i);

  auto bytes = c.serialize();
  REQUIRE_THROWS_AS(count_min_sketch<uint64_t>::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED-1), std::invalid_argument);
  auto d = count_min_sketch<uint64_t>::deserialize(bytes.data(), bytes.size(), DEFAULT_SEED);

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

} /* namespace datasketches */
