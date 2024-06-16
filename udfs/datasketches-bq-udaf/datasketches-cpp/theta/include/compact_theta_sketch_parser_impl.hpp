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

#ifndef COMPACT_THETA_SKETCH_PARSER_IMPL_HPP_
#define COMPACT_THETA_SKETCH_PARSER_IMPL_HPP_

#include <iostream>
#include <iomanip>
#include <stdexcept>

namespace datasketches {

template<typename T>
T whole_bytes_to_hold_bits(T bits) {
  static_assert(std::is_integral<T>::value, "integral type expected");
  return (bits >> 3) + ((bits & 7) > 0);
}

template<bool dummy>
auto compact_theta_sketch_parser<dummy>::parse(const void* ptr, size_t size, uint64_t seed, bool dump_on_error) -> compact_theta_sketch_data {
  check_memory_size(ptr, size, 8, dump_on_error);
  checker<true>::check_sketch_type(reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_TYPE_BYTE], COMPACT_SKETCH_TYPE);
  uint8_t serial_version = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_SERIAL_VERSION_BYTE];
  switch(serial_version) {
  case 4: {
    // version 4 sketches are ordered and always have entries (single item in exact mode is v3)
    const uint16_t seed_hash = reinterpret_cast<const uint16_t*>(ptr)[COMPACT_SKETCH_SEED_HASH_U16];
    checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));
    const bool has_theta = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_PRE_LONGS_BYTE] > 1;
    uint64_t theta = theta_constants::MAX_THETA;
    if (has_theta) {
      check_memory_size(ptr, size, 16, dump_on_error);
      theta = reinterpret_cast<const uint64_t*>(ptr)[COMPACT_SKETCH_V4_THETA_U64];
    }
    const uint8_t num_entries_bytes = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_V4_NUM_ENTRIES_BYTES_BYTE];
    size_t data_offset_bytes = has_theta ? COMPACT_SKETCH_V4_PACKED_DATA_ESTIMATION_BYTE : COMPACT_SKETCH_V4_PACKED_DATA_EXACT_BYTE;
    check_memory_size(ptr, size, data_offset_bytes + num_entries_bytes, dump_on_error);
    uint32_t num_entries = 0;
    const uint8_t* num_entries_ptr = reinterpret_cast<const uint8_t*>(ptr) + data_offset_bytes;
    for (unsigned i = 0; i < num_entries_bytes; ++i) {
      num_entries |= (*num_entries_ptr++) << (i << 3);
    }
    data_offset_bytes += num_entries_bytes;
    const uint8_t entry_bits = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_V4_ENTRY_BITS_BYTE];
    const size_t expected_bits = entry_bits * num_entries;
    const size_t expected_size_bytes = data_offset_bytes + whole_bytes_to_hold_bits(expected_bits);
    check_memory_size(ptr, size, expected_size_bytes, dump_on_error);
    return {false, true, seed_hash, num_entries, theta,
      reinterpret_cast<const uint8_t*>(ptr) + data_offset_bytes, entry_bits};
  }
  case 3: {
      uint64_t theta = theta_constants::MAX_THETA;
      const uint16_t seed_hash = reinterpret_cast<const uint16_t*>(ptr)[COMPACT_SKETCH_SEED_HASH_U16];
      if (reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_FLAGS_BYTE] & (1 << COMPACT_SKETCH_IS_EMPTY_FLAG)) {
        return {true, true, seed_hash, 0, theta, nullptr, 64};
      }
      checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));
      const bool has_theta = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_PRE_LONGS_BYTE] > 2;
      if (has_theta) {
        check_memory_size(ptr, size, (COMPACT_SKETCH_THETA_U64 + 1) * sizeof(uint64_t), dump_on_error);
        theta = reinterpret_cast<const uint64_t*>(ptr)[COMPACT_SKETCH_THETA_U64];
      }
      if (reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_PRE_LONGS_BYTE] == 1) {
        check_memory_size(ptr, size, 16, dump_on_error);
        return {false, true, seed_hash, 1, theta, reinterpret_cast<const uint64_t*>(ptr) + COMPACT_SKETCH_SINGLE_ENTRY_U64, 64};
      }
      const uint32_t num_entries = reinterpret_cast<const uint32_t*>(ptr)[COMPACT_SKETCH_NUM_ENTRIES_U32];
      const size_t entries_start_u64 = has_theta ? COMPACT_SKETCH_ENTRIES_ESTIMATION_U64 : COMPACT_SKETCH_ENTRIES_EXACT_U64;
      const uint64_t* entries = reinterpret_cast<const uint64_t*>(ptr) + entries_start_u64;
      const size_t expected_size_bytes = (entries_start_u64 + num_entries) * sizeof(uint64_t);
      check_memory_size(ptr, size, expected_size_bytes, dump_on_error);
      const bool is_ordered = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_FLAGS_BYTE] & (1 << COMPACT_SKETCH_IS_ORDERED_FLAG);
      return {false, is_ordered, seed_hash, num_entries, theta, entries, 64};
  }
  case 1:  {
      uint16_t seed_hash = compute_seed_hash(seed);
      const uint32_t num_entries = reinterpret_cast<const uint32_t*>(ptr)[COMPACT_SKETCH_NUM_ENTRIES_U32];
      uint64_t theta = reinterpret_cast<const uint64_t*>(ptr)[COMPACT_SKETCH_THETA_U64];
      bool is_empty = (num_entries == 0) && (theta == theta_constants::MAX_THETA);
      if (is_empty) return {true, true, seed_hash, 0, theta, nullptr, 64};
      const uint64_t* entries = reinterpret_cast<const uint64_t*>(ptr) + COMPACT_SKETCH_ENTRIES_ESTIMATION_U64;
      const size_t expected_size_bytes = (COMPACT_SKETCH_ENTRIES_ESTIMATION_U64 + num_entries) * sizeof(uint64_t);
      check_memory_size(ptr, size, expected_size_bytes, dump_on_error);
      return {false, true, seed_hash, num_entries, theta, entries, 64};
  }
  case 2:  {
      uint8_t preamble_size = reinterpret_cast<const uint8_t*>(ptr)[COMPACT_SKETCH_PRE_LONGS_BYTE];
      const uint16_t seed_hash = reinterpret_cast<const uint16_t*>(ptr)[COMPACT_SKETCH_SEED_HASH_U16];
      checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));
      if (preamble_size == 1) {
          return {true, true, seed_hash, 0, theta_constants::MAX_THETA, nullptr, 64};
      } else if (preamble_size == 2) {
          const uint32_t num_entries = reinterpret_cast<const uint32_t*>(ptr)[COMPACT_SKETCH_NUM_ENTRIES_U32];
          if (num_entries == 0) {
              return {true, true, seed_hash, 0, theta_constants::MAX_THETA, nullptr, 64};
          } else {
              const size_t expected_size_bytes = (preamble_size + num_entries) << 3;
              check_memory_size(ptr, size, expected_size_bytes, dump_on_error);
              const uint64_t* entries = reinterpret_cast<const uint64_t*>(ptr) + COMPACT_SKETCH_ENTRIES_EXACT_U64;
              return {false, true, seed_hash, num_entries, theta_constants::MAX_THETA, entries, 64};
          }
      } else if (preamble_size == 3) {
          const uint32_t num_entries = reinterpret_cast<const uint32_t*>(ptr)[COMPACT_SKETCH_NUM_ENTRIES_U32];
          uint64_t theta = reinterpret_cast<const uint64_t*>(ptr)[COMPACT_SKETCH_THETA_U64];
          bool is_empty = (num_entries == 0) && (theta == theta_constants::MAX_THETA);
          if (is_empty) return {true, true, seed_hash, 0, theta, nullptr, 64};
          const uint64_t* entries = reinterpret_cast<const uint64_t*>(ptr) + COMPACT_SKETCH_ENTRIES_ESTIMATION_U64;
          const size_t expected_size_bytes = (COMPACT_SKETCH_ENTRIES_ESTIMATION_U64 + num_entries) * sizeof(uint64_t);
          check_memory_size(ptr, size, expected_size_bytes, dump_on_error);
          return {false, true, seed_hash, num_entries, theta, entries, 64};
      } else {
          throw std::invalid_argument(std::to_string(preamble_size) + " longs of premable, but expected 1, 2, or 3");
      }
  }
  default:
    throw std::invalid_argument("unsupported serial version " + std::to_string(serial_version));
  }
}

template<bool dummy>
void compact_theta_sketch_parser<dummy>::check_memory_size(const void* ptr, size_t actual_bytes, size_t expected_bytes, bool dump_on_error) {
  if (actual_bytes < expected_bytes) throw std::out_of_range("at least " + std::to_string(expected_bytes)
      + " bytes expected, actual " + std::to_string(actual_bytes)
      + (dump_on_error ? (", sketch dump: " + hex_dump(reinterpret_cast<const uint8_t*>(ptr), actual_bytes)) : ""));
}

template<bool dummy>
std::string compact_theta_sketch_parser<dummy>::hex_dump(const uint8_t* ptr, size_t size) {
  std::stringstream s;
  s << std::hex << std::setfill('0') << std::uppercase;
  for (size_t i = 0; i < size; ++i) s << std::setw(2) << (ptr[i] & 0xff);
  return s.str();
}

} /* namespace datasketches */

#endif
