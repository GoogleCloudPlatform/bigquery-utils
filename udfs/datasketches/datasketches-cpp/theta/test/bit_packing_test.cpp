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
#include <bit_packing.hpp>

namespace datasketches {

// for every number of bits from 1 to 63
// generate pseudo-random data, pack, unpack and compare

// inverse golden ratio (0.618.. of max uint64_t)
static const uint64_t IGOLDEN64 = 0x9e3779b97f4a7c13ULL;

TEST_CASE("pack unpack bits") {
  for (uint8_t bits = 1; bits <= 63; ++bits) {
    const uint64_t mask = (1ULL << bits) - 1;
    std::vector<uint64_t> input(8, 0);
    const uint64_t igolden64 = IGOLDEN64;
    uint64_t value = 0xaa55aa55aa55aa55ULL; // arbitrary starting value
    for (int i = 0; i < 8; ++i) {
      input[i] = value & mask;
      value += igolden64;
    }
    std::vector<uint8_t> bytes(8 * sizeof(uint64_t), 0);
    uint8_t offset = 0;
    uint8_t* ptr = bytes.data();
    for (int i = 0; i < 8; ++i) {
      offset = pack_bits(input[i], bits, ptr, offset);
    }

    std::vector<uint64_t> output(8, 0);
    offset = 0;
    const uint8_t* cptr = bytes.data();
    for (int i = 0; i < 8; ++i) {
      offset = unpack_bits(output[i], bits, cptr, offset);
    }
    for (int i = 0; i < 8; ++i) {
      REQUIRE((input[i] & mask) == output[i]);
    }
  }
}

TEST_CASE("pack unpack blocks") {
  for (uint8_t bits = 1; bits <= 63; ++bits) {
    const uint64_t mask = (1ULL << bits) - 1;
    std::vector<uint64_t> input(8, 0);
    const uint64_t igolden64 = IGOLDEN64;
    uint64_t value = 0xaa55aa55aa55aa55ULL; // arbitrary starting value
    for (int i = 0; i < 8; ++i) {
      input[i] = value & mask;
      value += igolden64;
    }
    std::vector<uint8_t> bytes(8 * sizeof(uint64_t), 0);
    pack_bits_block8(input.data(), bytes.data(), bits);
    std::vector<uint64_t> output(8, 0);
    unpack_bits_block8(output.data(), bytes.data(), bits);
    for (int i = 0; i < 8; ++i) {
      REQUIRE((input[i] & mask) == output[i]);
    }
  }
}

} /* namespace datasketches */
