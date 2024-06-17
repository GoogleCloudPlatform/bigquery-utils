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
#include <hll.hpp>

namespace datasketches {

TEST_CASE("hll sketch generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    hll_sketch hll4(12, HLL_4);
    hll_sketch hll6(12, HLL_6);
    hll_sketch hll8(12, HLL_8);
    for (unsigned i = 0; i < n; ++i) {
      hll4.update(i);
      hll6.update(i);
      hll8.update(i);
    }
    {
      std::ofstream os("hll4_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
      hll4.serialize_compact(os);
    }
    {
      std::ofstream os("hll6_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
      hll6.serialize_compact(os);
    }
    {
      std::ofstream os("hll8_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
      hll8.serialize_compact(os);
    }
  }
}

} /* namespace datasketches */
