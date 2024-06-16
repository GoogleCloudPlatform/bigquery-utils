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
#include <var_opt_union.hpp>

namespace datasketches {

TEST_CASE("var opt union double sampling", "[serialize_for_java]") {
  const unsigned k_small = 16;
  const unsigned k_max = 128;
  const unsigned n1 = 32;
  const unsigned n2 = 64;

  // small k sketch, but sampling
  var_opt_sketch<double> sketch1(k_small);
  for (unsigned i = 0; i < n1; ++i) sketch1.update(i);
  // negative heavy item to allow a simple predicate to filter
  sketch1.update(-1, n1 * n1);

  // another one, but different n to get a different per-item weight
  var_opt_sketch<double> sketch2(k_small);
  for (unsigned i = 0; i < n2; ++i) sketch2.update(i);

  var_opt_union<double> u(k_max);
  u.update(sketch1);
  u.update(sketch2);

  // must reduce k in the process
  auto result = u.get_result();
  REQUIRE(result.get_k() < k_max);
  REQUIRE(result.get_k() >= k_small);
  REQUIRE(result.get_n() == 97);

  std::ofstream os("varopt_union_double_sampling_cpp.sk", std::ios::binary);
  u.serialize(os);
}

} /* namespace datasketches */
