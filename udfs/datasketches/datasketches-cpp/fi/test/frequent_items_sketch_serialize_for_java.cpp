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
#include <frequent_items_sketch.hpp>

namespace datasketches {

TEST_CASE("frequent longs sketch generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    frequent_items_sketch<long> sketch(6);
    for (unsigned i = 1; i <= n; ++i) sketch.update(i);
    REQUIRE(sketch.is_empty() == (n == 0));
    if (n > 10) {
      REQUIRE(sketch.get_maximum_error() > 0);
    } else {
      REQUIRE(sketch.get_maximum_error() == 0);
    }
    REQUIRE(sketch.get_total_weight() == n);
    std::ofstream os("frequent_long_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

TEST_CASE("frequent strings sketch generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    frequent_items_sketch<std::string> sketch(6);
    for (unsigned i = 1; i <= n; ++i) sketch.update(std::to_string(i));
    REQUIRE(sketch.is_empty() == (n == 0));
    if (n > 10) {
      REQUIRE(sketch.get_maximum_error() > 0);
    } else {
      REQUIRE(sketch.get_maximum_error() == 0);
    }
    REQUIRE(sketch.get_total_weight() == n);
    std::ofstream os("frequent_string_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

TEST_CASE("frequent strings sketch ascii", "[serialize_for_java]") {
  frequent_items_sketch<std::string> sketch(6);
  sketch.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1);
  sketch.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2);
  sketch.update("ccccccccccccccccccccccccccccc", 3);
  sketch.update("ddddddddddddddddddddddddddddd", 4);
  std::ofstream os("frequent_string_ascii_cpp.sk", std::ios::binary);
  sketch.serialize(os);
}

TEST_CASE("frequent strings sketch utf8", "[serialize_for_java]") {
  frequent_items_sketch<std::string> sketch(6);
  sketch.update("абвгд", 1);
  sketch.update("еёжзи", 2);
  sketch.update("йклмн", 3);
  sketch.update("опрст", 4);
  sketch.update("уфхцч", 5);
  sketch.update("шщъыь", 6);
  sketch.update("эюя", 7);
  std::ofstream os("frequent_string_utf8_cpp.sk", std::ios::binary);
  sketch.serialize(os);
}

} /* namespace datasketches */
