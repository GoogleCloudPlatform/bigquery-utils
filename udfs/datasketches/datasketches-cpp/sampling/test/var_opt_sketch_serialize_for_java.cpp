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
#include <var_opt_sketch.hpp>

namespace datasketches {

TEST_CASE("varopt sketch long generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    var_opt_sketch<long> sketch(32);
    for (unsigned i = 1; i <= n; ++i) sketch.update(i);
    std::ofstream os("varopt_sketch_long_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

TEST_CASE("varopt sketch string exact", "[serialize_for_java]") {
  var_opt_sketch<std::string> sketch(1024);
  for (unsigned i = 1; i <= 200; ++i) sketch.update(std::to_string(i), 1000.0 / i);
  std::ofstream os("varopt_sketch_string_exact_cpp.sk", std::ios::binary);
  sketch.serialize(os);
}

TEST_CASE("varopt sketch long sampling", "[serialize_for_java]") {
  var_opt_sketch<long> sketch(1024);
  for (unsigned i = 0; i < 2000; ++i) sketch.update(i);
  // negative heavy items to allow a simple predicate to filter
  sketch.update(-1L, 100000.0);
  sketch.update(-2L, 110000.0);
  sketch.update(-3L, 120000.0);
  std::ofstream os("varopt_sketch_long_sampling_cpp.sk", std::ios::binary);
  sketch.serialize(os);
}

} /* namespace datasketches */
