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

#ifndef CPC_COMMON_HPP_
#define CPC_COMMON_HPP_

#include <memory>

#include "MurmurHash3.h"

namespace datasketches {

/// CPC constants
namespace cpc_constants {
  /// min log2 of K
  const uint8_t MIN_LG_K = 4;
  /// max log2 of K
  const uint8_t MAX_LG_K = 26;
  /// default log2 of K
  const uint8_t DEFAULT_LG_K = 11;
}

// forward declaration
template<typename A> class u32_table;

template<typename A>
struct compressed_state {
  using vector_u32 = std::vector<uint32_t, typename std::allocator_traits<A>::template rebind_alloc<uint32_t>>;

  explicit compressed_state(const A& allocator): table_data(allocator), table_data_words(0), table_num_entries(0),
      window_data(allocator), window_data_words(0) {}
  vector_u32 table_data;
  uint32_t table_data_words;
  uint32_t table_num_entries; // can be different from the number of entries in the sketch in hybrid mode
  vector_u32 window_data;
  uint32_t window_data_words;
};

template<typename A>
struct uncompressed_state {
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<A>::template rebind_alloc<uint8_t>>;

  explicit uncompressed_state(const A& allocator): table(allocator), window(allocator) {}
  u32_table<A> table;
  vector_bytes window;
};

} /* namespace datasketches */

#endif
