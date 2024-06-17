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
#include <test_type.hpp>
#include <test_allocator.hpp>

#include <catch2/catch.hpp>

#include <sstream>

namespace datasketches {

using ebpps_test_sketch = ebpps_sketch<test_type, test_allocator<test_type>>;
using alloc = test_allocator<test_type>;

TEST_CASE("ebpps allocation test", "[ebpps_sketch][test_type]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    ebpps_test_sketch sk1(10, 0);
    for (int i = 0; i < 100; ++i)
      sk1.update(i);
    auto bytes1 = sk1.serialize(0, test_type_serde());
    auto sk2 = ebpps_test_sketch::deserialize(bytes1.data(), bytes1.size(), test_type_serde(), 0);

    std::stringstream ss;
    sk1.serialize(ss, test_type_serde());
    auto sk3 = ebpps_test_sketch::deserialize(ss, test_type_serde(), alloc(0));

    sk1.merge(sk2); // same size into sk1
    sk3.merge(sk1); // larger into sk3

    auto bytes2 = sk1.serialize(0, test_type_serde());
    auto sk4 = ebpps_test_sketch::deserialize(bytes2.data(), bytes2.size(), test_type_serde(), 0);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE( "ebpps merge", "[ebpps_sketch][test_type]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    uint32_t n = 20;
    uint32_t k = 5;
    ebpps_test_sketch sk1(k, 0);
    ebpps_test_sketch sk2(k, 0);

    // move udpates
    for (int i = 0; i < (int) n; ++i) {
      sk1.update(i);
      sk2.update(-i);
      sk1.update(n + i); // sk1 heavier than sk2
    }
    REQUIRE(sk1.get_n() == 2 * n);
    REQUIRE(sk2.get_n() == n);

    // move merge -- lighter into heavier
    sk1.merge(std::move(sk2));
    REQUIRE(sk1.get_n() == 3 * n);

    // move constructor
    ebpps_test_sketch sk3(std::move(sk1));
    REQUIRE(sk3.get_n() == 3 * n);

    // move assignment
    ebpps_test_sketch sk4(k, 0);
    sk4 = std::move(sk2);
    REQUIRE(sk4.get_n() == n);

    // move merge -- heavier into lighter
    sk4.merge(sk3);
    REQUIRE(sk4.get_n() == 4 * n);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

}
