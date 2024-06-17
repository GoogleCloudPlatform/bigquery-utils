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
#include <fstream>
#include <kll_sketch.hpp>

namespace datasketches {

TEST_CASE("kll sketch float generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    kll_sketch<float> sketch;
    for (unsigned i = 1; i <= n; ++i) sketch.update(i);
    std::ofstream os("kll_float_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

TEST_CASE("kll sketch double generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    kll_sketch<double> sketch;
    for (unsigned i = 1; i <= n; ++i) sketch.update(i);
    std::ofstream os("kll_double_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

struct compare_as_number {
  bool operator()(const std::string& a, const std::string& b) const {
    return std::stoi(a) < std::stoi(b);
  }
};

TEST_CASE("kll sketch string generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    kll_sketch<std::string, compare_as_number> sketch;
    for (unsigned i = 1; i <= n; ++i) sketch.update(std::to_string(i));
    std::ofstream os("kll_string_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

} /* namespace datasketches */
