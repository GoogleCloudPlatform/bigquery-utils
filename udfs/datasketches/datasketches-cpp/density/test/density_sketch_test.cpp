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

#include <cmath>
#include <catch2/catch.hpp>

#include <density_sketch.hpp>

namespace datasketches {

TEST_CASE("density sketch: empty", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);
  REQUIRE(sketch.is_empty());
  REQUIRE_THROWS_AS(sketch.get_estimate({0, 0, 0}), std::runtime_error);
}

TEST_CASE("density sketch: one item", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);

  // dimension mismatch
  REQUIRE_THROWS_AS(sketch.update(std::vector<float>({0, 0})), std::invalid_argument);

  sketch.update(std::vector<float>({0, 0, 0}));
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_estimate({0, 0, 0}) == 1);
  REQUIRE(sketch.get_estimate({0.01, 0.01, 0.01}) > 0.95);
  REQUIRE(sketch.get_estimate({1, 1, 1}) < 0.05);
}

TEST_CASE("density sketch: merge", "[density_sketch]") {
  density_sketch<float> sketch1(10, 4);
  sketch1.update(std::vector<float>({0, 0, 0, 0}));
  sketch1.update(std::vector<float>({1, 2, 3, 4}));

  density_sketch<float> sketch2(10, 4);
  sketch2.update(std::vector<float>({5, 6, 7, 8}));

  sketch1.merge(sketch2);

  REQUIRE(sketch1.get_n() == 3);
  REQUIRE(sketch1.get_num_retained() == 3);
}

TEST_CASE("density sketch: iterator", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);
  unsigned n = 1000;
  for (unsigned i = 1; i <= n; ++i) sketch.update(std::vector<float>(3, i));
  REQUIRE(sketch.get_n() == n);
  REQUIRE(sketch.is_estimation_mode());
  //std::cout << sketch.to_string(true, true);
  unsigned count = 0;
  for (auto pair: sketch) {
    ++count;
    // just to assert something about the output
    REQUIRE(pair.first.size() == sketch.get_dim());
  }
  REQUIRE(count == sketch.get_num_retained());
}

// spherical kernel for testing, returns 1 for vectors within radius and 0 otherwise
template<typename T>
struct spherical_kernel {
  spherical_kernel(T radius = 1.0) : _radius_squared(radius * radius) {}
  T operator()(const std::vector<T>& v1, const std::vector<T>& v2) const {
    return std::inner_product(v1.begin(), v1.end(), v2.begin(), 0.0, std::plus<T>(), [](T a, T b){return (a-b)*(a-b);}) <= _radius_squared ? 1.0 : 0.0;
  }
  private:
    T _radius_squared;
};

TEST_CASE("custom kernel", "[density_sketch]") {
  density_sketch<float, spherical_kernel<float>> sketch(10, 3, spherical_kernel<float>(0.5));

  // update with (1,1,1) and test points inside and outside the kernel
  sketch.update(std::vector<float>(3, 1.0));
  REQUIRE(sketch.get_estimate(std::vector<float>(3, 1.001)) == 1.0);
  REQUIRE(sketch.get_estimate(std::vector<float>(3, 2.0)) == 0.0);

  // rest of test follows iterator test above
  unsigned n = 1000;
  for (unsigned i = 2; i <= n; ++i) sketch.update(std::vector<float>(3, i));
  REQUIRE(sketch.get_n() == n);
  REQUIRE(sketch.is_estimation_mode());
  unsigned count = 0;
  for (auto pair: sketch) {
    ++count;
    // just to assert something about the output
    REQUIRE(pair.first.size() == sketch.get_dim());
  }
  REQUIRE(count == sketch.get_num_retained());
}

TEST_CASE("serialize empty", "[density_sketch]") {
  density_sketch<double> sk(10, 2);
  auto bytes = sk.serialize();
  auto sk2 = density_sketch<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(sk2.is_empty());
  REQUIRE(!sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(s);
  auto sk3 = density_sketch<double>::deserialize(s);
  REQUIRE(sk3.is_empty());
  REQUIRE(!sk3.is_estimation_mode());
  REQUIRE(sk.get_k() == sk3.get_k());
  REQUIRE(sk.get_dim() == sk3.get_dim());
  REQUIRE(sk.get_n() == sk3.get_n());
  REQUIRE(sk.get_num_retained() == sk3.get_num_retained());
}

TEST_CASE("serialize bytes", "[density_sketch]") {
  uint16_t k = 10;
  uint32_t dim = 3;
  density_sketch<double> sk(k, dim);

  for (uint16_t i = 0; i < k; ++i) {
    double val = static_cast<double>(i);
    sk.update(std::vector<double>({val, std::sqrt(val), -val}));
  }
  REQUIRE(!sk.is_estimation_mode());

  // exact mode
  auto bytes = sk.serialize();
  auto sk2 = density_sketch<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(!sk2.is_empty());
  REQUIRE(!sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  auto it1 = sk.begin();
  auto it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }

  // estimation mode
  size_t n = 1031;
  for (uint32_t i = k; i < n; ++i) {
    double val = static_cast<double>(i);
    sk.update(std::vector<double>({val, std::sqrt(val), -val}));
  }
  REQUIRE(sk.is_estimation_mode());

  bytes = sk.serialize();
  sk2 = density_sketch<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(!sk2.is_empty());
  REQUIRE(sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  it1 = sk.begin();
  it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }
}

TEST_CASE("serialize stream", "[density_sketch]") {
  uint16_t k = 10;
  uint32_t dim = 3;
  density_sketch<float> sk(k, dim);

  for (uint16_t i = 0; i < k; ++i) {
    float val = static_cast<float>(i);
    sk.update(std::vector<float>({val, std::sin(val), std::cos(val)}));
  }
  REQUIRE(!sk.is_estimation_mode());

  // exact mode
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(s);
  auto sk2 = density_sketch<float>::deserialize(s);
  REQUIRE(!sk2.is_empty());
  REQUIRE(!sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  auto it1 = sk.begin();
  auto it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }

  // estimation mode
  size_t n = 1031;
  for (uint32_t i = k; i < n; ++i) {
    float val = static_cast<float>(i);
    sk.update(std::vector<float>({val, std::sqrt(val), -val}));
  }
  REQUIRE(sk.is_estimation_mode());

  std::stringstream s2(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(s2);
  sk2 = density_sketch<float>::deserialize(s2);
  REQUIRE(!sk2.is_empty());
  REQUIRE(sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  it1 = sk.begin();
  it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }
}

} /* namespace datasketches */
