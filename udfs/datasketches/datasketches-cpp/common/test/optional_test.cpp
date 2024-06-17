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

#include "optional.hpp"

namespace datasketches {

class tt {
public:
  tt() = delete; // make sure it cannot be default constructed
  tt(int val): val_(val) {}
  tt(const tt& other): val_(other.val_) { std::cout << "tt copy constructor\n"; }
  tt(tt&& other): val_(other.val_) { std::cout << "tt move constructor\n"; }
  tt& operator=(const tt& other) { val_ = other.val_; std::cout << "tt copy assignment\n"; return *this; }
  tt& operator=(tt&& other) { val_ = other.val_; std::cout << "tt move assignment\n"; return *this; }
  int get_val() const { return val_; }
private:
  int val_;
};

TEST_CASE("optional", "[common]") {
  optional<tt> opt;
  REQUIRE_FALSE(opt);
  opt.emplace(5);
  REQUIRE(bool(opt));
  REQUIRE((*opt).get_val() == 5);
  REQUIRE(opt->get_val() == 5);
  opt.reset();
  REQUIRE_FALSE(opt);

  optional<tt> opt2(opt);
  REQUIRE_FALSE(opt2);

  opt2.emplace(3);
  if (opt2) *opt2 = 6; // good if it is initialized
  REQUIRE(opt2->get_val() == 6);

  opt.reset();
  REQUIRE_FALSE(opt);
  optional<tt> opt3(std::move(opt));
  REQUIRE_FALSE(opt3);
  *opt3 = 7; // don't do this! may be dangerous for arbitrary T, and it still thinks it is not initialized
  REQUIRE_FALSE(opt3);
  opt3.emplace(8);
  REQUIRE(bool(opt3));
  REQUIRE(opt3->get_val() == 8);

  std::swap(opt2, opt3);
  REQUIRE(opt2->get_val() == 8);
  REQUIRE(opt3->get_val() == 6);

  std::swap(opt2, opt);
  REQUIRE_FALSE(opt2);
  REQUIRE(bool(opt));
  REQUIRE(opt->get_val() == 8);
}

TEST_CASE("optional conversion", "[common]") {
  optional<float> opt_f(1);
  optional<double> opt_d(opt_f);
  REQUIRE(bool(opt_d));
  REQUIRE(*opt_d == static_cast<double>(*opt_f));
}

} /* namespace datasketches */
