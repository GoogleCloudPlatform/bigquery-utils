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

#include "hll.hpp"

namespace datasketches {

static const int nArr[] = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

TEST_CASE("hll to/from byte array: double serialize", "[hll_byte_array]") {
  hll_sketch sk(9, HLL_8);
  for (int i = 0; i < 1024; ++i) {
    sk.update(i);
  }
  
  std::stringstream ss1;
  sk.serialize_updatable(ss1);
  auto ser1 = sk.serialize_updatable();

  std::stringstream ss;
  sk.serialize_updatable(ss);
  std::string str = ss.str();

  hll_sketch sk2 = hll_sketch::deserialize(ser1.data(), ser1.size());
  auto ser2 = sk.serialize_updatable();

  REQUIRE(ser1.size() == ser2.size());
  size_t len = ser1.size();
  uint8_t* b1 = ser1.data();
  uint8_t* b2 = ser2.data();

  for (size_t i = 0; i < len; ++i) {
    REQUIRE(b2[i] == b1[i]);
  }
}

static void checkSketchEquality(hll_sketch& sk1, hll_sketch& sk2) {
  REQUIRE(sk1.get_lg_config_k() == sk2.get_lg_config_k());
  REQUIRE(sk1.get_lower_bound(1) == sk2.get_lower_bound(1));
  REQUIRE(sk1.get_estimate() == sk2.get_estimate());
  REQUIRE(sk1.get_upper_bound(1) == sk2.get_upper_bound(1));
  REQUIRE(sk1.get_target_type() == sk2.get_target_type());
}

static void toFrom(const uint8_t lgConfigK, const target_hll_type tgtHllType, const int n) {
  hll_sketch src(lgConfigK, tgtHllType);
  for (int i = 0; i < n; ++i) {
    src.update(i);
  }

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  src.serialize_compact(ss);
  hll_sketch dst = hll_sketch::deserialize(ss);
  checkSketchEquality(src, dst);

  auto bytes1 = src.serialize_compact();
  dst = hll_sketch::deserialize(bytes1.data(), bytes1.size());
  checkSketchEquality(src, dst);

  ss.clear();
  src.serialize_updatable(ss);
  dst = hll_sketch::deserialize(ss);
  checkSketchEquality(src, dst);

  auto bytes2 = src.serialize_updatable();
  dst = hll_sketch::deserialize(bytes2.data(), bytes2.size());
  checkSketchEquality(src, dst);
}

TEST_CASE("hll to/from byte array: to from sketch", "[hll_byte_array]") {
  for (int i = 0; i < 10; ++i) {
    int n = nArr[i];
    for (uint8_t lgK = 4; lgK <= 13; ++lgK) {
      toFrom(lgK, HLL_4, n);
      toFrom(lgK, HLL_6, n);
      toFrom(lgK, HLL_8, n);
    }
  }
}

} /* namespace datasketches */
