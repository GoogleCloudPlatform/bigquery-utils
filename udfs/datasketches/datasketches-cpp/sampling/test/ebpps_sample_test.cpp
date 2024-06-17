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

#include <ebpps_sample.hpp>

#include <catch2/catch.hpp>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <random>
#include <stdexcept>

namespace datasketches {

static constexpr double EPS = 1e-15;

TEST_CASE("ebpps sample: basic initialization", "[ebpps_sketch]") {
  ebpps_sample<int> sample = ebpps_sample<int>(0);
  REQUIRE(sample.get_c() == 0.0);
  REQUIRE(sample.get_num_retained_items() == 0);
  REQUIRE(sample.get_sample().size() == 0);
}

TEST_CASE("ebpps sample: pre-initialized", "[ebpps_sketch]") {
  double theta = 1.0;
  ebpps_sample<int> sample(1);
  sample.replace_content(-1, theta);
  REQUIRE(sample.get_c() == theta);
  REQUIRE(sample.get_num_retained_items() == 1);
  REQUIRE(sample.get_sample().size() == 1);
  REQUIRE(sample.has_partial_item() == false);
  
  theta = 1e-300;
  sample.replace_content(-1, theta);
  REQUIRE(sample.get_c() == theta);
  REQUIRE(sample.get_num_retained_items() == 1);
  REQUIRE(sample.get_sample().size() == 0); // assuming the random number is > 1e-300
  REQUIRE(sample.has_partial_item());
}

TEST_CASE("ebpps sample: downsampling", "[ebpps_sketch]") {
  ebpps_sample<char> sample(1);
  sample.replace_content('a', 1.0);

  sample.downsample(2.0); // no-op
  REQUIRE(sample.get_c() == 1.0);
  REQUIRE(sample.get_num_retained_items() == 1);
  REQUIRE(sample.has_partial_item() == false);

  // downsample and result in an empty sample
  random_utils::override_seed(12);
  std::vector<char> items = {'a', 'b'};
  optional<char> opt; // empty
  sample = ebpps_sample<char>(std::move(items), std::move(opt), 1.8);
  sample.downsample(0.5);
  REQUIRE(sample.get_c() == 0.9);
  REQUIRE(sample.get_num_retained_items() == 0);
  REQUIRE(sample.has_partial_item() == false);

  // downsample and result in a sample with a partial item
  items = {'a', 'b'};
  opt.reset();
  sample = ebpps_sample<char>(std::move(items), std::move(opt), 1.5);
  sample.downsample(0.5);
  REQUIRE(sample.get_c() == 0.75);
  REQUIRE(sample.get_num_retained_items() == 1);
  REQUIRE(sample.has_partial_item() == true);
  for (char c : sample) {
    REQUIRE((c == 'a' || c == 'b'));
  }

  // downsample to an exact integer c (7.5 * 0.8 = 6.0)
  items = {'a', 'b', 'c', 'd', 'e', 'f', 'g'};
  opt.emplace('h');
  auto ref_items = items; // copy to check contents
  ref_items.emplace_back('h'); // include partial item
  sample = ebpps_sample<char>(std::move(items), std::move(opt), 7.5);
  sample.downsample(0.8);
  REQUIRE(sample.get_c() == 6.0);
  REQUIRE(sample.get_num_retained_items() == 6);
  REQUIRE(sample.has_partial_item() == false);
  for (char c : sample) {
    REQUIRE(std::find(ref_items.begin(), ref_items.end(), c) != ref_items.end());
  }

  // downsample to c > 1 with partial item
  items = ref_items; // includes previous optional item
  opt.emplace('i');
  sample = ebpps_sample<char>(std::move(items), std::move(opt), 8.5);
  REQUIRE(sample.get_partial_item() == 'i');
  sample.downsample(0.8);
  REQUIRE(sample.get_c() == Approx(6.8).margin(EPS));
  REQUIRE(sample.get_num_retained_items() == 7);
  REQUIRE(sample.has_partial_item() == true);
  ref_items.emplace_back('i');
  for (char c : sample) {
    REQUIRE(std::find(ref_items.begin(), ref_items.end(), c) != ref_items.end());
  }

  random_utils::override_seed(random_utils::rd());
}

TEST_CASE("ebpps sample: merge unit samples", "[ebpps_sketch]") {
  uint32_t k = 8;
  ebpps_sample<int> sample = ebpps_sample<int>(k);
  
  ebpps_sample<int> s(1);
  for (uint32_t i = 1; i <= k; ++i) {
    s.replace_content(i, 1.0);
    sample.merge(s);
    REQUIRE(sample.get_c() == static_cast<double>(i));
    REQUIRE(sample.get_num_retained_items() == i);
  }

  sample.reset();
  REQUIRE(sample.get_c() == 0);
  REQUIRE(sample.get_num_retained_items() == 0);
  REQUIRE(sample.has_partial_item() == false);
}

} // namespace datasketches
