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
#include <tuple_sketch.hpp>

namespace datasketches {

TEST_CASE("tuple sketch int generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    auto sketch = update_tuple_sketch<int>::builder().build();
    for (unsigned i = 0; i < n; ++i) sketch.update(i, i);
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    std::ofstream os("tuple_int_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.compact().serialize(os);
  }
}

} /* namespace datasketches */
