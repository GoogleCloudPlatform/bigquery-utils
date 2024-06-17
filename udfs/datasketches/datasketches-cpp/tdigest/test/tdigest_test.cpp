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
#include <iostream>
#include <fstream>

#include "tdigest.hpp"

namespace datasketches {

TEST_CASE("empty", "[tdigest]") {
  tdigest_double td(10);
//  std::cout << td.to_string();
  REQUIRE(td.is_empty());
  REQUIRE(td.get_k() == 10);
  REQUIRE(td.get_total_weight() == 0);
  REQUIRE_THROWS_AS(td.get_min_value(), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_max_value(), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_rank(0), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_quantile(0.5), std::runtime_error);
}

TEST_CASE("one value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  REQUIRE(td.get_k() == 100);
  REQUIRE(td.get_total_weight() == 1);
  REQUIRE(td.get_min_value() == 1);
  REQUIRE(td.get_max_value() == 1);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.5);
  REQUIRE(td.get_rank(1.01) == 1);
  REQUIRE(td.get_quantile(0) == 1);
  REQUIRE(td.get_quantile(0.5) == 1);
  REQUIRE(td.get_quantile(1) == 1);
}

TEST_CASE("many values", "[tdigest]") {
  const size_t n = 10000;
  tdigest_double td;
  for (size_t i = 0; i < n; ++i) td.update(i);
//  std::cout << td.to_string(true);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE_FALSE(td.is_empty());
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
  REQUIRE(td.get_quantile(0) == 0);
  REQUIRE(td.get_quantile(0.5) == Approx(n / 2).epsilon(0.03));
  REQUIRE(td.get_quantile(0.9) == Approx(n * 0.9).epsilon(0.01));
  REQUIRE(td.get_quantile(0.95) == Approx(n * 0.95).epsilon(0.01));
  REQUIRE(td.get_quantile(1) == n - 1);
}

TEST_CASE("rank - two values", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(2);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.25);
  REQUIRE(td.get_rank(1.25) == 0.375);
  REQUIRE(td.get_rank(1.5) == 0.5);
  REQUIRE(td.get_rank(1.75) == 0.625);
  REQUIRE(td.get_rank(2) == 0.75);
  REQUIRE(td.get_rank(2.01) == 1);
}

TEST_CASE("rank - repeated value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(1);
  td.update(1);
  td.update(1);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.5);
  REQUIRE(td.get_rank(1.01) == 1);
}

TEST_CASE("rank - repeated block", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(2);
  td.update(2);
  td.update(3);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.125);
  REQUIRE(td.get_rank(2) == 0.5);
  REQUIRE(td.get_rank(3) == 0.875);
  REQUIRE(td.get_rank(3.01) == 1);
}

TEST_CASE("merge small", "[tdigest]") {
  tdigest_double td1(10);
  td1.update(1);
  td1.update(2);
  tdigest_double td2(10);
  td2.update(2);
  td2.update(3);
  td1.merge(td2);
  REQUIRE(td1.get_min_value() == 1);
  REQUIRE(td1.get_max_value() == 3);
  REQUIRE(td1.get_total_weight() == 4);
  REQUIRE(td1.get_rank(0.99) == 0);
  REQUIRE(td1.get_rank(1) == 0.125);
  REQUIRE(td1.get_rank(2) == 0.5);
  REQUIRE(td1.get_rank(3) == 0.875);
  REQUIRE(td1.get_rank(3.01) == 1);
}

TEST_CASE("merge large", "[tdigest]") {
  const size_t n = 10000;
  tdigest_double td1;
  tdigest_double td2;
  for (size_t i = 0; i < n / 2; ++i) {
    td1.update(i);
    td2.update(n / 2 + i);
  }
//  std::cout << td1.to_string();
//  std::cout << td2.to_string();
  td1.merge(td2);
//  td1.compress();
//  std::cout << td1.to_string(true);
  REQUIRE(td1.get_total_weight() == n);
  REQUIRE(td1.get_min_value() == 0);
  REQUIRE(td1.get_max_value() == n - 1);
  REQUIRE(td1.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td1.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td1.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td1.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td1.get_rank(n) == 1);
}

TEST_CASE("serialize deserialize stream empty", "[tdigest]") {
  tdigest<double> td(100);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
}

TEST_CASE("serialize deserialize stream single value", "[tdigest]") {
  tdigest<double> td;
  td.update(123);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize stream single value buffered", "[tdigest]") {
  tdigest<double> td;
  td.update(123);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s, true);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize stream many values", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 1000; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize stream many values with buffer", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 10000; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s, true);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize bytes empty", "[tdigest]") {
  tdigest<double> td(100);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
}

TEST_CASE("serialize deserialize bytes single value", "[tdigest]") {
  tdigest<double> td(200);
  td.update(123);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize bytes single value buffered", "[tdigest]") {
  tdigest<double> td(200);
  td.update(123);
  auto bytes = td.serialize(0, true);
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize bytes many values", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 1000; ++i) td.update(i);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize bytes many values with buffer", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 10000; ++i) td.update(i);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize steam and bytes equivalence empty", "[tdigest]") {
  tdigest<double> td(100);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto bytes = td.serialize();

  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto deserialized_td1 = tdigest<double>::deserialize(s);
  auto deserialized_td2 = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));

  REQUIRE(deserialized_td1.is_empty());
  REQUIRE(deserialized_td2.is_empty());
  REQUIRE(deserialized_td1.get_k() == 100);
  REQUIRE(deserialized_td2.get_k() == 100);
  REQUIRE(deserialized_td1.get_total_weight() == 0);
  REQUIRE(deserialized_td2.get_total_weight() == 0);
}

TEST_CASE("serialize deserialize steam and bytes equivalence", "[tdigest]") {
  tdigest<double> td(100);
  const int n = 1000;
  for (int i = 0; i < n; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto bytes = td.serialize();

  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto deserialized_td1 = tdigest<double>::deserialize(s);
  auto deserialized_td2 = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));

  REQUIRE_FALSE(deserialized_td1.is_empty());
  REQUIRE(deserialized_td1.get_k() == 100);
  REQUIRE(deserialized_td1.get_total_weight() == n);
  REQUIRE(deserialized_td1.get_min_value() == 0);
  REQUIRE(deserialized_td1.get_max_value() == n - 1);

  REQUIRE_FALSE(deserialized_td2.is_empty());
  REQUIRE(deserialized_td2.get_k() == 100);
  REQUIRE(deserialized_td2.get_total_weight() == n);
  REQUIRE(deserialized_td2.get_min_value() == 0);
  REQUIRE(deserialized_td2.get_max_value() == n - 1);

  REQUIRE(deserialized_td1.get_rank(n / 2) == deserialized_td2.get_rank(n / 2));
  REQUIRE(deserialized_td1.get_quantile(0.5) == deserialized_td2.get_quantile(0.5));
}

TEST_CASE("serialize deserialize steam and bytes equivalence with buffer", "[tdigest]") {
  tdigest<double> td(100);
  const int n = 10000;
  for (int i = 0; i < n; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s, true);
  auto bytes = td.serialize(0, true);

  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto deserialized_td1 = tdigest<double>::deserialize(s);
  auto deserialized_td2 = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));

  REQUIRE_FALSE(deserialized_td1.is_empty());
  REQUIRE(deserialized_td1.get_k() == 100);
  REQUIRE(deserialized_td1.get_total_weight() == n);
  REQUIRE(deserialized_td1.get_min_value() == 0);
  REQUIRE(deserialized_td1.get_max_value() == n - 1);

  REQUIRE_FALSE(deserialized_td2.is_empty());
  REQUIRE(deserialized_td2.get_k() == 100);
  REQUIRE(deserialized_td2.get_total_weight() == n);
  REQUIRE(deserialized_td2.get_min_value() == 0);
  REQUIRE(deserialized_td2.get_max_value() == n - 1);

  REQUIRE(deserialized_td1.get_rank(n / 2) == deserialized_td2.get_rank(n / 2));
  REQUIRE(deserialized_td1.get_quantile(0.5) == deserialized_td2.get_quantile(0.5));
}

TEST_CASE("deserialize from reference implementation stream double", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_double.sk", std::ios::binary);
  const auto td = tdigest<double>::deserialize(is);
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("deserialize from reference implementation stream float", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_float.sk", std::ios::binary);
  const auto td = tdigest<float>::deserialize(is);
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("deserialize from reference implementation bytes double", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_double.sk", std::ios::binary);
  std::vector<char> bytes((std::istreambuf_iterator<char>(is)), (std::istreambuf_iterator<char>()));
  const auto td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("deserialize from reference implementation bytes float", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_float.sk", std::ios::binary);
  std::vector<char> bytes((std::istreambuf_iterator<char>(is)), (std::istreambuf_iterator<char>()));
  const auto td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

} /* namespace datasketches */
