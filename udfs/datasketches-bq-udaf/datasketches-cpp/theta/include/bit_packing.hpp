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

#ifndef BIT_PACKING_HPP_
#define BIT_PACKING_HPP_

#include <memory>

namespace datasketches {

static inline uint8_t pack_bits(uint64_t value, uint8_t bits, uint8_t*& ptr, uint8_t offset) {
  if (offset > 0) {
    const uint8_t chunk_bits = 8 - offset;
    const uint8_t mask = (1 << chunk_bits) - 1;
    if (bits < chunk_bits) {
      *ptr |= (value << (chunk_bits - bits)) & mask;
      return offset + bits;
    }
    *ptr++ |= (value >> (bits - chunk_bits)) & mask;
    bits -= chunk_bits;
  }
  while (bits >= 8) {
    *ptr++ = static_cast<uint8_t>(value >> (bits - 8));
    bits -= 8;
  }
  if (bits > 0) {
    *ptr = static_cast<uint8_t>(value << (8 - bits));
    return bits;
  }
  return 0;
}

static inline uint8_t unpack_bits(uint64_t& value, uint8_t bits, const uint8_t*& ptr, uint8_t offset) {
  const uint8_t avail_bits = 8 - offset;
  const uint8_t chunk_bits = std::min(avail_bits, bits);
  const uint8_t mask = (1 << chunk_bits) - 1;
  value = (*ptr >> (avail_bits - chunk_bits)) & mask;
  ptr += avail_bits == chunk_bits;
  offset = (offset + chunk_bits) & 7;
  bits -= chunk_bits;
  while (bits >= 8) {
    value <<= 8;
    value |= *ptr++;
    bits -= 8;
  }
  if (bits > 0) {
    value <<= bits;
    value |= *ptr >> (8 - bits);
    return bits;
  }
  return offset;
}

// pack given number of bits from a block of 8 64-bit values into bytes
// we don't need 0 and 64 bits
// we assume that higher bits (which we are not packing) are zeros
// this assumption allows to avoid masking operations

static inline void pack_bits_1(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr |= static_cast<uint8_t>(values[1] << 6);
  *ptr |= static_cast<uint8_t>(values[2] << 5);
  *ptr |= static_cast<uint8_t>(values[3] << 4);
  *ptr |= static_cast<uint8_t>(values[4] << 3);
  *ptr |= static_cast<uint8_t>(values[5] << 2);
  *ptr |= static_cast<uint8_t>(values[6] << 1);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_2(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr |= static_cast<uint8_t>(values[1] << 4);
  *ptr |= static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3]);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr |= static_cast<uint8_t>(values[5] << 4);
  *ptr |= static_cast<uint8_t>(values[6] << 2);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_3(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr |= static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr |= static_cast<uint8_t>(values[3] << 4);
  *ptr |= static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr |= static_cast<uint8_t>(values[6] << 3);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_4(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1]);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3]);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5]);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_5(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr |= static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr |= static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_6(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3]);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_7(const uint64_t* values, uint8_t* ptr) {
  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr |= static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_8(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0]);
  *ptr++ = static_cast<uint8_t>(values[1]);
  *ptr++ = static_cast<uint8_t>(values[2]);
  *ptr++ = static_cast<uint8_t>(values[3]);
  *ptr++ = static_cast<uint8_t>(values[4]);
  *ptr++ = static_cast<uint8_t>(values[5]);
  *ptr++ = static_cast<uint8_t>(values[6]);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_9(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_10(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_11(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 9);

  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 10);

  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_12(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 4);

  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 8);

  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 4);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 4);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 8);

  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 4);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_13(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 10);

  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] >> 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 9);

  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 11);

  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_14(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 12);

  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 10);

  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 12);

  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 10);

  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_15(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 14);

  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 13);

  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 11);

  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 10);

  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 9);

  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_16(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 8);
  *ptr++ = static_cast<uint8_t>(values[0]);

  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 8);
  *ptr++ = static_cast<uint8_t>(values[2]);

  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 8);
  *ptr++ = static_cast<uint8_t>(values[4]);

  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 8);
  *ptr++ = static_cast<uint8_t>(values[6]);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_17(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 9);

  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 10);

  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 11);

  *ptr++ = static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 13);

  *ptr++ = static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 14);

  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 15);

  *ptr++ = static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_18(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 10);

  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 12);

  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 14);

  *ptr++ = static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 16);

  *ptr++ = static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 10);

  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 12);

  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 14);

  *ptr++ = static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_19(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 11);

  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 14);

  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 17);

  *ptr++ = static_cast<uint8_t>(values[2] >> 9);

  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 15);

  *ptr++ |= static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 18);

  *ptr++ = static_cast<uint8_t>(values[5] >> 10);

  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 13);

  *ptr++ = static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_20(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 12);

  *ptr++ = static_cast<uint8_t>(values[0] >> 4);

  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 16);

  *ptr++ = static_cast<uint8_t>(values[1] >> 8);

  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 12);

  *ptr++ = static_cast<uint8_t>(values[2] >> 4);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 16);

  *ptr++ = static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 12);

  *ptr++ = static_cast<uint8_t>(values[4] >> 4);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 16);

  *ptr++ = static_cast<uint8_t>(values[5] >> 8);

  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 12);

  *ptr++ = static_cast<uint8_t>(values[6] >> 4);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_21(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 13);

  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 18);

  *ptr++ = static_cast<uint8_t>(values[1] >> 10);

  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 15);

  *ptr++ = static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 20);

  *ptr++ = static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 17);

  *ptr++ = static_cast<uint8_t>(values[4] >> 9);

  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 14);

  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 19);

  *ptr++ = static_cast<uint8_t>(values[6] >> 11);

  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_22(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 14);

  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 20);

  *ptr++ = static_cast<uint8_t>(values[1] >> 12);

  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 18);

  *ptr++ = static_cast<uint8_t>(values[2] >> 10);

  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 16);

  *ptr++ = static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 14);

  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 20);

  *ptr++ = static_cast<uint8_t>(values[5] >> 12);

  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 18);

  *ptr++ = static_cast<uint8_t>(values[6] >> 10);

  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_23(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 15);

  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 22);

  *ptr++ = static_cast<uint8_t>(values[1] >> 14);

  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 21);

  *ptr++ = static_cast<uint8_t>(values[2] >> 13);

  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 20);

  *ptr++ = static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 19);

  *ptr++ = static_cast<uint8_t>(values[4] >> 11);

  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 18);

  *ptr++ = static_cast<uint8_t>(values[5] >> 10);

  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 17);

  *ptr++ = static_cast<uint8_t>(values[6] >> 9);

  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_24(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 16);
  *ptr++ = static_cast<uint8_t>(values[0] >> 8);
  *ptr++ = static_cast<uint8_t>(values[0]);

  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 16);
  *ptr++ = static_cast<uint8_t>(values[2] >> 8);
  *ptr++ = static_cast<uint8_t>(values[2]);

  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 16);
  *ptr++ = static_cast<uint8_t>(values[4] >> 8);
  *ptr++ = static_cast<uint8_t>(values[4]);

  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 16);
  *ptr++ = static_cast<uint8_t>(values[6] >> 8);
  *ptr++ = static_cast<uint8_t>(values[6]);

  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_25(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 17);

  *ptr++ = static_cast<uint8_t>(values[0] >> 9);

  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 18);

  *ptr++ = static_cast<uint8_t>(values[1] >> 10);

  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 19);

  *ptr++ = static_cast<uint8_t>(values[2] >> 11);

  *ptr++ = static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 20);

  *ptr++ = static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 21);

  *ptr++ = static_cast<uint8_t>(values[4] >> 13);

  *ptr++ = static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 22);

  *ptr++ = static_cast<uint8_t>(values[5] >> 14);

  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 23);

  *ptr++ = static_cast<uint8_t>(values[6] >> 15);

  *ptr++ = static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);

  *ptr++ = static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_26(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 18);

  *ptr++ = static_cast<uint8_t>(values[0] >> 10);

  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 20);

  *ptr++ = static_cast<uint8_t>(values[1] >> 12);

  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 22);

  *ptr++ = static_cast<uint8_t>(values[2] >> 14);

  *ptr++ = static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 24);

  *ptr++ = static_cast<uint8_t>(values[3] >> 16);

  *ptr++ = static_cast<uint8_t>(values[3] >> 8);

  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 18);

  *ptr++ = static_cast<uint8_t>(values[4] >> 10);

  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 20);

  *ptr++ = static_cast<uint8_t>(values[5] >> 12);

  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 22);

  *ptr++ = static_cast<uint8_t>(values[6] >> 14);

  *ptr++ = static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);

  *ptr++ = static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_27(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 19);

  *ptr++ = static_cast<uint8_t>(values[0] >> 11);

  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 22);

  *ptr++ = static_cast<uint8_t>(values[1] >> 14);

  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 25);

  *ptr++ = static_cast<uint8_t>(values[2] >> 17);

  *ptr++ = static_cast<uint8_t>(values[2] >> 9);

  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 20);

  *ptr++ = static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 23);

  *ptr++ = static_cast<uint8_t>(values[4] >> 15);

  *ptr++ = static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 26);

  *ptr++ = static_cast<uint8_t>(values[5] >> 18);

  *ptr++ = static_cast<uint8_t>(values[5] >> 10);

  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 21);

  *ptr++ = static_cast<uint8_t>(values[6] >> 13);

  *ptr++ = static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);

  *ptr++ = static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_28(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 20);
  *ptr++ = static_cast<uint8_t>(values[0] >> 12);
  *ptr++ = static_cast<uint8_t>(values[0] >> 4);
  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);
  *ptr++ = static_cast<uint8_t>(values[2] >> 20);
  *ptr++ = static_cast<uint8_t>(values[2] >> 12);
  *ptr++ = static_cast<uint8_t>(values[2] >> 4);
  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);
  *ptr++ = static_cast<uint8_t>(values[4] >> 20);
  *ptr++ = static_cast<uint8_t>(values[4] >> 12);
  *ptr++ = static_cast<uint8_t>(values[4] >> 4);
  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);
  *ptr++ = static_cast<uint8_t>(values[6] >> 20);
  *ptr++ = static_cast<uint8_t>(values[6] >> 12);
  *ptr++ = static_cast<uint8_t>(values[6] >> 4);
  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_29(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 21);

  *ptr++ = static_cast<uint8_t>(values[0] >> 13);

  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 26);

  *ptr++ = static_cast<uint8_t>(values[1] >> 18);

  *ptr++ = static_cast<uint8_t>(values[1] >> 10);

  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 23);

  *ptr++ = static_cast<uint8_t>(values[2] >> 15);

  *ptr++ = static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 28);

  *ptr++ = static_cast<uint8_t>(values[3] >> 20);

  *ptr++ = static_cast<uint8_t>(values[3] >> 12);

  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 25);

  *ptr++ = static_cast<uint8_t>(values[4] >> 17);

  *ptr++ = static_cast<uint8_t>(values[4] >> 9);

  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 22);

  *ptr++ = static_cast<uint8_t>(values[5] >> 14);

  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 27);

  *ptr++ = static_cast<uint8_t>(values[6] >> 19);

  *ptr++ = static_cast<uint8_t>(values[6] >> 11);

  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);

  *ptr++ = static_cast<uint8_t>(values[7] >> 16);

  *ptr++ = static_cast<uint8_t>(values[7] >> 8);

  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_30(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 22);
  *ptr++ = static_cast<uint8_t>(values[0] >> 14);
  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 26);
  *ptr++ = static_cast<uint8_t>(values[2] >> 18);
  *ptr++ = static_cast<uint8_t>(values[2] >> 10);
  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 22);
  *ptr++ = static_cast<uint8_t>(values[4] >> 14);
  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 26);
  *ptr++ = static_cast<uint8_t>(values[6] >> 18);
  *ptr++ = static_cast<uint8_t>(values[6] >> 10);
  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_31(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 23);
  *ptr++ = static_cast<uint8_t>(values[0] >> 15);
  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 29);
  *ptr++ = static_cast<uint8_t>(values[2] >> 21);
  *ptr++ = static_cast<uint8_t>(values[2] >> 13);
  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 27);
  *ptr++ = static_cast<uint8_t>(values[4] >> 19);
  *ptr++ = static_cast<uint8_t>(values[4] >> 11);
  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 25);
  *ptr++ = static_cast<uint8_t>(values[6] >> 17);
  *ptr++ = static_cast<uint8_t>(values[6] >> 9);
  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_32(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 24);
  *ptr++ = static_cast<uint8_t>(values[0] >> 16);
  *ptr++ = static_cast<uint8_t>(values[0] >> 8);
  *ptr++ = static_cast<uint8_t>(values[0]);

  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 24);
  *ptr++ = static_cast<uint8_t>(values[2] >> 16);
  *ptr++ = static_cast<uint8_t>(values[2] >> 8);
  *ptr++ = static_cast<uint8_t>(values[2]);

  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 24);
  *ptr++ = static_cast<uint8_t>(values[4] >> 16);
  *ptr++ = static_cast<uint8_t>(values[4] >> 8);
  *ptr++ = static_cast<uint8_t>(values[4]);

  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 24);
  *ptr++ = static_cast<uint8_t>(values[6] >> 16);
  *ptr++ = static_cast<uint8_t>(values[6] >> 8);
  *ptr++ = static_cast<uint8_t>(values[6]);

  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_33(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 25);
  *ptr++ = static_cast<uint8_t>(values[0] >> 17);
  *ptr++ = static_cast<uint8_t>(values[0] >> 9);
  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 27);
  *ptr++ = static_cast<uint8_t>(values[2] >> 19);
  *ptr++ = static_cast<uint8_t>(values[2] >> 11);
  *ptr++ = static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 29);
  *ptr++ = static_cast<uint8_t>(values[4] >> 21);
  *ptr++ = static_cast<uint8_t>(values[4] >> 13);
  *ptr++ = static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 31);
  *ptr++ = static_cast<uint8_t>(values[6] >> 23);
  *ptr++ = static_cast<uint8_t>(values[6] >> 15);
  *ptr++ = static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_34(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 26);
  *ptr++ = static_cast<uint8_t>(values[0] >> 18);
  *ptr++ = static_cast<uint8_t>(values[0] >> 10);
  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 30);
  *ptr++ = static_cast<uint8_t>(values[2] >> 22);
  *ptr++ = static_cast<uint8_t>(values[2] >> 14);
  *ptr++ = static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 26);
  *ptr++ = static_cast<uint8_t>(values[4] >> 18);
  *ptr++ = static_cast<uint8_t>(values[4] >> 10);
  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 30);
  *ptr++ = static_cast<uint8_t>(values[6] >> 22);
  *ptr++ = static_cast<uint8_t>(values[6] >> 14);
  *ptr++ = static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_35(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 27);
  *ptr++ = static_cast<uint8_t>(values[0] >> 19);
  *ptr++ = static_cast<uint8_t>(values[0] >> 11);
  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 33);
  *ptr++ = static_cast<uint8_t>(values[2] >> 25);
  *ptr++ = static_cast<uint8_t>(values[2] >> 17);
  *ptr++ = static_cast<uint8_t>(values[2] >> 9);
  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 31);
  *ptr++ = static_cast<uint8_t>(values[4] >> 23);
  *ptr++ = static_cast<uint8_t>(values[4] >> 15);
  *ptr++ = static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 29);
  *ptr++ = static_cast<uint8_t>(values[6] >> 21);
  *ptr++ = static_cast<uint8_t>(values[6] >> 13);
  *ptr++ = static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_36(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 28);
  *ptr++ = static_cast<uint8_t>(values[0] >> 20);
  *ptr++ = static_cast<uint8_t>(values[0] >> 12);
  *ptr++ = static_cast<uint8_t>(values[0] >> 4);

  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 28);
  *ptr++ = static_cast<uint8_t>(values[2] >> 20);
  *ptr++ = static_cast<uint8_t>(values[2] >> 12);
  *ptr++ = static_cast<uint8_t>(values[2] >> 4);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 28);
  *ptr++ = static_cast<uint8_t>(values[4] >> 20);
  *ptr++ = static_cast<uint8_t>(values[4] >> 12);
  *ptr++ = static_cast<uint8_t>(values[4] >> 4);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 28);
  *ptr++ = static_cast<uint8_t>(values[6] >> 20);
  *ptr++ = static_cast<uint8_t>(values[6] >> 12);
  *ptr++ = static_cast<uint8_t>(values[6] >> 4);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_37(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 29);
  *ptr++ = static_cast<uint8_t>(values[0] >> 21);
  *ptr++ = static_cast<uint8_t>(values[0] >> 13);
  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 31);
  *ptr++ = static_cast<uint8_t>(values[2] >> 23);
  *ptr++ = static_cast<uint8_t>(values[2] >> 15);
  *ptr++ = static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 33);
  *ptr++ = static_cast<uint8_t>(values[4] >> 25);
  *ptr++ = static_cast<uint8_t>(values[4] >> 17);
  *ptr++ = static_cast<uint8_t>(values[4] >> 9);
  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 35);
  *ptr++ = static_cast<uint8_t>(values[6] >> 27);
  *ptr++ = static_cast<uint8_t>(values[6] >> 19);
  *ptr++ = static_cast<uint8_t>(values[6] >> 11);
  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_38(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 30);
  *ptr++ = static_cast<uint8_t>(values[0] >> 22);
  *ptr++ = static_cast<uint8_t>(values[0] >> 14);
  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 34);
  *ptr++ = static_cast<uint8_t>(values[2] >> 26);
  *ptr++ = static_cast<uint8_t>(values[2] >> 18);
  *ptr++ = static_cast<uint8_t>(values[2] >> 10);
  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 30);
  *ptr++ = static_cast<uint8_t>(values[4] >> 22);
  *ptr++ = static_cast<uint8_t>(values[4] >> 14);
  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 34);
  *ptr++ = static_cast<uint8_t>(values[6] >> 26);
  *ptr++ = static_cast<uint8_t>(values[6] >> 18);
  *ptr++ = static_cast<uint8_t>(values[6] >> 10);
  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_39(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 31);
  *ptr++ = static_cast<uint8_t>(values[0] >> 23);
  *ptr++ = static_cast<uint8_t>(values[0] >> 15);
  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 37);
  *ptr++ = static_cast<uint8_t>(values[2] >> 29);
  *ptr++ = static_cast<uint8_t>(values[2] >> 21);
  *ptr++ = static_cast<uint8_t>(values[2] >> 13);
  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 35);
  *ptr++ = static_cast<uint8_t>(values[4] >> 27);
  *ptr++ = static_cast<uint8_t>(values[4] >> 19);
  *ptr++ = static_cast<uint8_t>(values[4] >> 11);
  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 33);
  *ptr++ = static_cast<uint8_t>(values[6] >> 25);
  *ptr++ = static_cast<uint8_t>(values[6] >> 17);
  *ptr++ = static_cast<uint8_t>(values[6] >> 9);
  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_40(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 32);
  *ptr++ = static_cast<uint8_t>(values[0] >> 24);
  *ptr++ = static_cast<uint8_t>(values[0] >> 16);
  *ptr++ = static_cast<uint8_t>(values[0] >> 8);
  *ptr++ = static_cast<uint8_t>(values[0]);

  *ptr++ = static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 32);
  *ptr++ = static_cast<uint8_t>(values[2] >> 24);
  *ptr++ = static_cast<uint8_t>(values[2] >> 16);
  *ptr++ = static_cast<uint8_t>(values[2] >> 8);
  *ptr++ = static_cast<uint8_t>(values[2]);

  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 32);
  *ptr++ = static_cast<uint8_t>(values[4] >> 24);
  *ptr++ = static_cast<uint8_t>(values[4] >> 16);
  *ptr++ = static_cast<uint8_t>(values[4] >> 8);
  *ptr++ = static_cast<uint8_t>(values[4]);

  *ptr++ = static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 32);
  *ptr++ = static_cast<uint8_t>(values[6] >> 24);
  *ptr++ = static_cast<uint8_t>(values[6] >> 16);
  *ptr++ = static_cast<uint8_t>(values[6] >> 8);
  *ptr++ = static_cast<uint8_t>(values[6]);

  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_41(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 33);
  *ptr++ = static_cast<uint8_t>(values[0] >> 25);
  *ptr++ = static_cast<uint8_t>(values[0] >> 17);
  *ptr++ = static_cast<uint8_t>(values[0] >> 9);
  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 35);
  *ptr++ = static_cast<uint8_t>(values[2] >> 27);
  *ptr++ = static_cast<uint8_t>(values[2] >> 19);
  *ptr++ = static_cast<uint8_t>(values[2] >> 11);
  *ptr++ = static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 37);
  *ptr++ = static_cast<uint8_t>(values[4] >> 29);
  *ptr++ = static_cast<uint8_t>(values[4] >> 21);
  *ptr++ = static_cast<uint8_t>(values[4] >> 13);
  *ptr++ = static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 38);
  *ptr++ = static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 39);
  *ptr++ = static_cast<uint8_t>(values[6] >> 31);
  *ptr++ = static_cast<uint8_t>(values[6] >> 23);
  *ptr++ = static_cast<uint8_t>(values[6] >> 15);
  *ptr++ = static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_42(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 34);
  *ptr++ = static_cast<uint8_t>(values[0] >> 26);
  *ptr++ = static_cast<uint8_t>(values[0] >> 18);
  *ptr++ = static_cast<uint8_t>(values[0] >> 10);
  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 38);
  *ptr++ = static_cast<uint8_t>(values[2] >> 30);
  *ptr++ = static_cast<uint8_t>(values[2] >> 22);
  *ptr++ = static_cast<uint8_t>(values[2] >> 14);
  *ptr++ = static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 34);
  *ptr++ = static_cast<uint8_t>(values[4] >> 26);
  *ptr++ = static_cast<uint8_t>(values[4] >> 18);
  *ptr++ = static_cast<uint8_t>(values[4] >> 10);
  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 38);
  *ptr++ = static_cast<uint8_t>(values[6] >> 30);
  *ptr++ = static_cast<uint8_t>(values[6] >> 22);
  *ptr++ = static_cast<uint8_t>(values[6] >> 14);
  *ptr++ = static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_43(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 35);
  *ptr++ = static_cast<uint8_t>(values[0] >> 27);
  *ptr++ = static_cast<uint8_t>(values[0] >> 19);
  *ptr++ = static_cast<uint8_t>(values[0] >> 11);
  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 41);
  *ptr++ = static_cast<uint8_t>(values[2] >> 33);
  *ptr++ = static_cast<uint8_t>(values[2] >> 25);
  *ptr++ = static_cast<uint8_t>(values[2] >> 17);
  *ptr++ = static_cast<uint8_t>(values[2] >> 9);
  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 39);
  *ptr++ = static_cast<uint8_t>(values[4] >> 31);
  *ptr++ = static_cast<uint8_t>(values[4] >> 23);
  *ptr++ = static_cast<uint8_t>(values[4] >> 15);
  *ptr++ = static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 42);
  *ptr++ = static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 37);
  *ptr++ = static_cast<uint8_t>(values[6] >> 29);
  *ptr++ = static_cast<uint8_t>(values[6] >> 21);
  *ptr++ = static_cast<uint8_t>(values[6] >> 13);
  *ptr++ = static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_44(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 36);
  *ptr++ = static_cast<uint8_t>(values[0] >> 28);
  *ptr++ = static_cast<uint8_t>(values[0] >> 20);
  *ptr++ = static_cast<uint8_t>(values[0] >> 12);
  *ptr++ = static_cast<uint8_t>(values[0] >> 4);

  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 40);
  *ptr++ = static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 36);
  *ptr++ = static_cast<uint8_t>(values[2] >> 28);
  *ptr++ = static_cast<uint8_t>(values[2] >> 20);
  *ptr++ = static_cast<uint8_t>(values[2] >> 12);
  *ptr++ = static_cast<uint8_t>(values[2] >> 4);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 36);
  *ptr++ = static_cast<uint8_t>(values[4] >> 28);
  *ptr++ = static_cast<uint8_t>(values[4] >> 20);
  *ptr++ = static_cast<uint8_t>(values[4] >> 12);
  *ptr++ = static_cast<uint8_t>(values[4] >> 4);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 40);
  *ptr++ = static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 36);
  *ptr++ = static_cast<uint8_t>(values[6] >> 28);
  *ptr++ = static_cast<uint8_t>(values[6] >> 20);
  *ptr++ = static_cast<uint8_t>(values[6] >> 12);
  *ptr++ = static_cast<uint8_t>(values[6] >> 4);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_45(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 37);
  *ptr++ = static_cast<uint8_t>(values[0] >> 29);
  *ptr++ = static_cast<uint8_t>(values[0] >> 21);
  *ptr++ = static_cast<uint8_t>(values[0] >> 13);
  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 42);
  *ptr++ = static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 39);
  *ptr++ = static_cast<uint8_t>(values[2] >> 31);
  *ptr++ = static_cast<uint8_t>(values[2] >> 23);
  *ptr++ = static_cast<uint8_t>(values[2] >> 15);
  *ptr++ = static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 41);
  *ptr++ = static_cast<uint8_t>(values[4] >> 33);
  *ptr++ = static_cast<uint8_t>(values[4] >> 25);
  *ptr++ = static_cast<uint8_t>(values[4] >> 17);
  *ptr++ = static_cast<uint8_t>(values[4] >> 9);
  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 38);
  *ptr++ = static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 43);
  *ptr++ = static_cast<uint8_t>(values[6] >> 35);
  *ptr++ = static_cast<uint8_t>(values[6] >> 27);
  *ptr++ = static_cast<uint8_t>(values[6] >> 19);
  *ptr++ = static_cast<uint8_t>(values[6] >> 11);
  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_46(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 38);
  *ptr++ = static_cast<uint8_t>(values[0] >> 30);
  *ptr++ = static_cast<uint8_t>(values[0] >> 22);
  *ptr++ = static_cast<uint8_t>(values[0] >> 14);
  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 44);
  *ptr++ = static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 42);
  *ptr++ = static_cast<uint8_t>(values[2] >> 34);
  *ptr++ = static_cast<uint8_t>(values[2] >> 26);
  *ptr++ = static_cast<uint8_t>(values[2] >> 18);
  *ptr++ = static_cast<uint8_t>(values[2] >> 10);
  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 38);
  *ptr++ = static_cast<uint8_t>(values[4] >> 30);
  *ptr++ = static_cast<uint8_t>(values[4] >> 22);
  *ptr++ = static_cast<uint8_t>(values[4] >> 14);
  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 44);
  *ptr++ = static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 42);
  *ptr++ = static_cast<uint8_t>(values[6] >> 34);
  *ptr++ = static_cast<uint8_t>(values[6] >> 26);
  *ptr++ = static_cast<uint8_t>(values[6] >> 18);
  *ptr++ = static_cast<uint8_t>(values[6] >> 10);
  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_47(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 39);
  *ptr++ = static_cast<uint8_t>(values[0] >> 31);
  *ptr++ = static_cast<uint8_t>(values[0] >> 23);
  *ptr++ = static_cast<uint8_t>(values[0] >> 15);
  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 46);
  *ptr++ = static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 45);
  *ptr++ = static_cast<uint8_t>(values[2] >> 37);
  *ptr++ = static_cast<uint8_t>(values[2] >> 29);
  *ptr++ = static_cast<uint8_t>(values[2] >> 21);
  *ptr++ = static_cast<uint8_t>(values[2] >> 13);
  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 43);
  *ptr++ = static_cast<uint8_t>(values[4] >> 35);
  *ptr++ = static_cast<uint8_t>(values[4] >> 27);
  *ptr++ = static_cast<uint8_t>(values[4] >> 19);
  *ptr++ = static_cast<uint8_t>(values[4] >> 11);
  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 42);
  *ptr++ = static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 41);
  *ptr++ = static_cast<uint8_t>(values[6] >> 33);
  *ptr++ = static_cast<uint8_t>(values[6] >> 25);
  *ptr++ = static_cast<uint8_t>(values[6] >> 17);
  *ptr++ = static_cast<uint8_t>(values[6] >> 9);
  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_48(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 40);
  *ptr++ = static_cast<uint8_t>(values[0] >> 32);
  *ptr++ = static_cast<uint8_t>(values[0] >> 24);
  *ptr++ = static_cast<uint8_t>(values[0] >> 16);
  *ptr++ = static_cast<uint8_t>(values[0] >> 8);
  *ptr++ = static_cast<uint8_t>(values[0]);

  *ptr++ = static_cast<uint8_t>(values[1] >> 40);
  *ptr++ = static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 40);
  *ptr++ = static_cast<uint8_t>(values[2] >> 32);
  *ptr++ = static_cast<uint8_t>(values[2] >> 24);
  *ptr++ = static_cast<uint8_t>(values[2] >> 16);
  *ptr++ = static_cast<uint8_t>(values[2] >> 8);
  *ptr++ = static_cast<uint8_t>(values[2]);

  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 40);
  *ptr++ = static_cast<uint8_t>(values[4] >> 32);
  *ptr++ = static_cast<uint8_t>(values[4] >> 24);
  *ptr++ = static_cast<uint8_t>(values[4] >> 16);
  *ptr++ = static_cast<uint8_t>(values[4] >> 8);
  *ptr++ = static_cast<uint8_t>(values[4]);

  *ptr++ = static_cast<uint8_t>(values[5] >> 40);
  *ptr++ = static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 40);
  *ptr++ = static_cast<uint8_t>(values[6] >> 32);
  *ptr++ = static_cast<uint8_t>(values[6] >> 24);
  *ptr++ = static_cast<uint8_t>(values[6] >> 16);
  *ptr++ = static_cast<uint8_t>(values[6] >> 8);
  *ptr++ = static_cast<uint8_t>(values[6]);

  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_49(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 41);
  *ptr++ = static_cast<uint8_t>(values[0] >> 33);
  *ptr++ = static_cast<uint8_t>(values[0] >> 25);
  *ptr++ = static_cast<uint8_t>(values[0] >> 17);
  *ptr++ = static_cast<uint8_t>(values[0] >> 9);
  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 42);
  *ptr++ = static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 43);
  *ptr++ = static_cast<uint8_t>(values[2] >> 35);
  *ptr++ = static_cast<uint8_t>(values[2] >> 27);
  *ptr++ = static_cast<uint8_t>(values[2] >> 19);
  *ptr++ = static_cast<uint8_t>(values[2] >> 11);
  *ptr++ = static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 45);
  *ptr++ = static_cast<uint8_t>(values[4] >> 37);
  *ptr++ = static_cast<uint8_t>(values[4] >> 29);
  *ptr++ = static_cast<uint8_t>(values[4] >> 21);
  *ptr++ = static_cast<uint8_t>(values[4] >> 13);
  *ptr++ = static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 46);
  *ptr++ = static_cast<uint8_t>(values[5] >> 38);
  *ptr++ = static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 47);
  *ptr++ = static_cast<uint8_t>(values[6] >> 39);
  *ptr++ = static_cast<uint8_t>(values[6] >> 31);
  *ptr++ = static_cast<uint8_t>(values[6] >> 23);
  *ptr++ = static_cast<uint8_t>(values[6] >> 15);
  *ptr++ = static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_50(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 42);
  *ptr++ = static_cast<uint8_t>(values[0] >> 34);
  *ptr++ = static_cast<uint8_t>(values[0] >> 26);
  *ptr++ = static_cast<uint8_t>(values[0] >> 18);
  *ptr++ = static_cast<uint8_t>(values[0] >> 10);
  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 44);
  *ptr++ = static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 46);
  *ptr++ = static_cast<uint8_t>(values[2] >> 38);
  *ptr++ = static_cast<uint8_t>(values[2] >> 30);
  *ptr++ = static_cast<uint8_t>(values[2] >> 22);
  *ptr++ = static_cast<uint8_t>(values[2] >> 14);
  *ptr++ = static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 42);
  *ptr++ = static_cast<uint8_t>(values[4] >> 34);
  *ptr++ = static_cast<uint8_t>(values[4] >> 26);
  *ptr++ = static_cast<uint8_t>(values[4] >> 18);
  *ptr++ = static_cast<uint8_t>(values[4] >> 10);
  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 44);
  *ptr++ = static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 46);
  *ptr++ = static_cast<uint8_t>(values[6] >> 38);
  *ptr++ = static_cast<uint8_t>(values[6] >> 30);
  *ptr++ = static_cast<uint8_t>(values[6] >> 22);
  *ptr++ = static_cast<uint8_t>(values[6] >> 14);
  *ptr++ = static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_51(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 43);
  *ptr++ = static_cast<uint8_t>(values[0] >> 35);
  *ptr++ = static_cast<uint8_t>(values[0] >> 27);
  *ptr++ = static_cast<uint8_t>(values[0] >> 19);
  *ptr++ = static_cast<uint8_t>(values[0] >> 11);
  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 46);
  *ptr++ = static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 49);
  *ptr++ = static_cast<uint8_t>(values[2] >> 41);
  *ptr++ = static_cast<uint8_t>(values[2] >> 33);
  *ptr++ = static_cast<uint8_t>(values[2] >> 25);
  *ptr++ = static_cast<uint8_t>(values[2] >> 17);
  *ptr++ = static_cast<uint8_t>(values[2] >> 9);
  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 47);
  *ptr++ = static_cast<uint8_t>(values[4] >> 39);
  *ptr++ = static_cast<uint8_t>(values[4] >> 31);
  *ptr++ = static_cast<uint8_t>(values[4] >> 23);
  *ptr++ = static_cast<uint8_t>(values[4] >> 15);
  *ptr++ = static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 50);
  *ptr++ = static_cast<uint8_t>(values[5] >> 42);
  *ptr++ = static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 45);
  *ptr++ = static_cast<uint8_t>(values[6] >> 37);
  *ptr++ = static_cast<uint8_t>(values[6] >> 29);
  *ptr++ = static_cast<uint8_t>(values[6] >> 21);
  *ptr++ = static_cast<uint8_t>(values[6] >> 13);
  *ptr++ = static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_52(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 44);
  *ptr++ = static_cast<uint8_t>(values[0] >> 36);
  *ptr++ = static_cast<uint8_t>(values[0] >> 28);
  *ptr++ = static_cast<uint8_t>(values[0] >> 20);
  *ptr++ = static_cast<uint8_t>(values[0] >> 12);
  *ptr++ = static_cast<uint8_t>(values[0] >> 4);

  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 48);
  *ptr++ = static_cast<uint8_t>(values[1] >> 40);
  *ptr++ = static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 44);
  *ptr++ = static_cast<uint8_t>(values[2] >> 36);
  *ptr++ = static_cast<uint8_t>(values[2] >> 28);
  *ptr++ = static_cast<uint8_t>(values[2] >> 20);
  *ptr++ = static_cast<uint8_t>(values[2] >> 12);
  *ptr++ = static_cast<uint8_t>(values[2] >> 4);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 44);
  *ptr++ = static_cast<uint8_t>(values[4] >> 36);
  *ptr++ = static_cast<uint8_t>(values[4] >> 28);
  *ptr++ = static_cast<uint8_t>(values[4] >> 20);
  *ptr++ = static_cast<uint8_t>(values[4] >> 12);
  *ptr++ = static_cast<uint8_t>(values[4] >> 4);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 48);
  *ptr++ = static_cast<uint8_t>(values[5] >> 40);
  *ptr++ = static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 44);
  *ptr++ = static_cast<uint8_t>(values[6] >> 36);
  *ptr++ = static_cast<uint8_t>(values[6] >> 28);
  *ptr++ = static_cast<uint8_t>(values[6] >> 20);
  *ptr++ = static_cast<uint8_t>(values[6] >> 12);
  *ptr++ = static_cast<uint8_t>(values[6] >> 4);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_53(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 45);
  *ptr++ = static_cast<uint8_t>(values[0] >> 37);
  *ptr++ = static_cast<uint8_t>(values[0] >> 29);
  *ptr++ = static_cast<uint8_t>(values[0] >> 21);
  *ptr++ = static_cast<uint8_t>(values[0] >> 13);
  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 50);
  *ptr++ = static_cast<uint8_t>(values[1] >> 42);
  *ptr++ = static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 47);
  *ptr++ = static_cast<uint8_t>(values[2] >> 39);
  *ptr++ = static_cast<uint8_t>(values[2] >> 31);
  *ptr++ = static_cast<uint8_t>(values[2] >> 23);
  *ptr++ = static_cast<uint8_t>(values[2] >> 15);
  *ptr++ = static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 52);
  *ptr++ = static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 49);
  *ptr++ = static_cast<uint8_t>(values[4] >> 41);
  *ptr++ = static_cast<uint8_t>(values[4] >> 33);
  *ptr++ = static_cast<uint8_t>(values[4] >> 25);
  *ptr++ = static_cast<uint8_t>(values[4] >> 17);
  *ptr++ = static_cast<uint8_t>(values[4] >> 9);
  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 46);
  *ptr++ = static_cast<uint8_t>(values[5] >> 38);
  *ptr++ = static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 51);
  *ptr++ = static_cast<uint8_t>(values[6] >> 43);
  *ptr++ = static_cast<uint8_t>(values[6] >> 35);
  *ptr++ = static_cast<uint8_t>(values[6] >> 27);
  *ptr++ = static_cast<uint8_t>(values[6] >> 19);
  *ptr++ = static_cast<uint8_t>(values[6] >> 11);
  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_54(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 46);
  *ptr++ = static_cast<uint8_t>(values[0] >> 38);
  *ptr++ = static_cast<uint8_t>(values[0] >> 30);
  *ptr++ = static_cast<uint8_t>(values[0] >> 22);
  *ptr++ = static_cast<uint8_t>(values[0] >> 14);
  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 52);
  *ptr++ = static_cast<uint8_t>(values[1] >> 44);
  *ptr++ = static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 50);
  *ptr++ = static_cast<uint8_t>(values[2] >> 42);
  *ptr++ = static_cast<uint8_t>(values[2] >> 34);
  *ptr++ = static_cast<uint8_t>(values[2] >> 26);
  *ptr++ = static_cast<uint8_t>(values[2] >> 18);
  *ptr++ = static_cast<uint8_t>(values[2] >> 10);
  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 46);
  *ptr++ = static_cast<uint8_t>(values[4] >> 38);
  *ptr++ = static_cast<uint8_t>(values[4] >> 30);
  *ptr++ = static_cast<uint8_t>(values[4] >> 22);
  *ptr++ = static_cast<uint8_t>(values[4] >> 14);
  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 52);
  *ptr++ = static_cast<uint8_t>(values[5] >> 44);
  *ptr++ = static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 50);
  *ptr++ = static_cast<uint8_t>(values[6] >> 42);
  *ptr++ = static_cast<uint8_t>(values[6] >> 34);
  *ptr++ = static_cast<uint8_t>(values[6] >> 26);
  *ptr++ = static_cast<uint8_t>(values[6] >> 18);
  *ptr++ = static_cast<uint8_t>(values[6] >> 10);
  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_55(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 47);
  *ptr++ = static_cast<uint8_t>(values[0] >> 39);
  *ptr++ = static_cast<uint8_t>(values[0] >> 31);
  *ptr++ = static_cast<uint8_t>(values[0] >> 23);
  *ptr++ = static_cast<uint8_t>(values[0] >> 15);
  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 54);
  *ptr++ = static_cast<uint8_t>(values[1] >> 46);
  *ptr++ = static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 53);
  *ptr++ = static_cast<uint8_t>(values[2] >> 45);
  *ptr++ = static_cast<uint8_t>(values[2] >> 37);
  *ptr++ = static_cast<uint8_t>(values[2] >> 29);
  *ptr++ = static_cast<uint8_t>(values[2] >> 21);
  *ptr++ = static_cast<uint8_t>(values[2] >> 13);
  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 52);
  *ptr++ = static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 51);
  *ptr++ = static_cast<uint8_t>(values[4] >> 43);
  *ptr++ = static_cast<uint8_t>(values[4] >> 35);
  *ptr++ = static_cast<uint8_t>(values[4] >> 27);
  *ptr++ = static_cast<uint8_t>(values[4] >> 19);
  *ptr++ = static_cast<uint8_t>(values[4] >> 11);
  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 50);
  *ptr++ = static_cast<uint8_t>(values[5] >> 42);
  *ptr++ = static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 49);
  *ptr++ = static_cast<uint8_t>(values[6] >> 41);
  *ptr++ = static_cast<uint8_t>(values[6] >> 33);
  *ptr++ = static_cast<uint8_t>(values[6] >> 25);
  *ptr++ = static_cast<uint8_t>(values[6] >> 17);
  *ptr++ = static_cast<uint8_t>(values[6] >> 9);
  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_56(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 48);
  *ptr++ = static_cast<uint8_t>(values[0] >> 40);
  *ptr++ = static_cast<uint8_t>(values[0] >> 32);
  *ptr++ = static_cast<uint8_t>(values[0] >> 24);
  *ptr++ = static_cast<uint8_t>(values[0] >> 16);
  *ptr++ = static_cast<uint8_t>(values[0] >> 8);
  *ptr++ = static_cast<uint8_t>(values[0]);

  *ptr++ = static_cast<uint8_t>(values[1] >> 48);
  *ptr++ = static_cast<uint8_t>(values[1] >> 40);
  *ptr++ = static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 48);
  *ptr++ = static_cast<uint8_t>(values[2] >> 40);
  *ptr++ = static_cast<uint8_t>(values[2] >> 32);
  *ptr++ = static_cast<uint8_t>(values[2] >> 24);
  *ptr++ = static_cast<uint8_t>(values[2] >> 16);
  *ptr++ = static_cast<uint8_t>(values[2] >> 8);
  *ptr++ = static_cast<uint8_t>(values[2]);

  *ptr++ = static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 48);
  *ptr++ = static_cast<uint8_t>(values[4] >> 40);
  *ptr++ = static_cast<uint8_t>(values[4] >> 32);
  *ptr++ = static_cast<uint8_t>(values[4] >> 24);
  *ptr++ = static_cast<uint8_t>(values[4] >> 16);
  *ptr++ = static_cast<uint8_t>(values[4] >> 8);
  *ptr++ = static_cast<uint8_t>(values[4]);

  *ptr++ = static_cast<uint8_t>(values[5] >> 48);
  *ptr++ = static_cast<uint8_t>(values[5] >> 40);
  *ptr++ = static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 48);
  *ptr++ = static_cast<uint8_t>(values[6] >> 40);
  *ptr++ = static_cast<uint8_t>(values[6] >> 32);
  *ptr++ = static_cast<uint8_t>(values[6] >> 24);
  *ptr++ = static_cast<uint8_t>(values[6] >> 16);
  *ptr++ = static_cast<uint8_t>(values[6] >> 8);
  *ptr++ = static_cast<uint8_t>(values[6]);

  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_57(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 49);
  *ptr++ = static_cast<uint8_t>(values[0] >> 41);
  *ptr++ = static_cast<uint8_t>(values[0] >> 33);
  *ptr++ = static_cast<uint8_t>(values[0] >> 25);
  *ptr++ = static_cast<uint8_t>(values[0] >> 17);
  *ptr++ = static_cast<uint8_t>(values[0] >> 9);
  *ptr++ = static_cast<uint8_t>(values[0] >> 1);

  *ptr = static_cast<uint8_t>(values[0] << 7);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 50);
  *ptr++ = static_cast<uint8_t>(values[1] >> 42);
  *ptr++ = static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 51);
  *ptr++ = static_cast<uint8_t>(values[2] >> 43);
  *ptr++ = static_cast<uint8_t>(values[2] >> 35);
  *ptr++ = static_cast<uint8_t>(values[2] >> 27);
  *ptr++ = static_cast<uint8_t>(values[2] >> 19);
  *ptr++ = static_cast<uint8_t>(values[2] >> 11);
  *ptr++ = static_cast<uint8_t>(values[2] >> 3);

  *ptr = static_cast<uint8_t>(values[2] << 5);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 52);
  *ptr++ = static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 53);
  *ptr++ = static_cast<uint8_t>(values[4] >> 45);
  *ptr++ = static_cast<uint8_t>(values[4] >> 37);
  *ptr++ = static_cast<uint8_t>(values[4] >> 29);
  *ptr++ = static_cast<uint8_t>(values[4] >> 21);
  *ptr++ = static_cast<uint8_t>(values[4] >> 13);
  *ptr++ = static_cast<uint8_t>(values[4] >> 5);

  *ptr = static_cast<uint8_t>(values[4] << 3);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 54);
  *ptr++ = static_cast<uint8_t>(values[5] >> 46);
  *ptr++ = static_cast<uint8_t>(values[5] >> 38);
  *ptr++ = static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 55);
  *ptr++ = static_cast<uint8_t>(values[6] >> 47);
  *ptr++ = static_cast<uint8_t>(values[6] >> 39);
  *ptr++ = static_cast<uint8_t>(values[6] >> 31);
  *ptr++ = static_cast<uint8_t>(values[6] >> 23);
  *ptr++ = static_cast<uint8_t>(values[6] >> 15);
  *ptr++ = static_cast<uint8_t>(values[6] >> 7);

  *ptr = static_cast<uint8_t>(values[6] << 1);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_58(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 50);
  *ptr++ = static_cast<uint8_t>(values[0] >> 42);
  *ptr++ = static_cast<uint8_t>(values[0] >> 34);
  *ptr++ = static_cast<uint8_t>(values[0] >> 26);
  *ptr++ = static_cast<uint8_t>(values[0] >> 18);
  *ptr++ = static_cast<uint8_t>(values[0] >> 10);
  *ptr++ = static_cast<uint8_t>(values[0] >> 2);

  *ptr = static_cast<uint8_t>(values[0] << 6);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 52);
  *ptr++ = static_cast<uint8_t>(values[1] >> 44);
  *ptr++ = static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 54);
  *ptr++ = static_cast<uint8_t>(values[2] >> 46);
  *ptr++ = static_cast<uint8_t>(values[2] >> 38);
  *ptr++ = static_cast<uint8_t>(values[2] >> 30);
  *ptr++ = static_cast<uint8_t>(values[2] >> 22);
  *ptr++ = static_cast<uint8_t>(values[2] >> 14);
  *ptr++ = static_cast<uint8_t>(values[2] >> 6);

  *ptr = static_cast<uint8_t>(values[2] << 2);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 56);
  *ptr++ = static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 50);
  *ptr++ = static_cast<uint8_t>(values[4] >> 42);
  *ptr++ = static_cast<uint8_t>(values[4] >> 34);
  *ptr++ = static_cast<uint8_t>(values[4] >> 26);
  *ptr++ = static_cast<uint8_t>(values[4] >> 18);
  *ptr++ = static_cast<uint8_t>(values[4] >> 10);
  *ptr++ = static_cast<uint8_t>(values[4] >> 2);

  *ptr = static_cast<uint8_t>(values[4] << 6);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 52);
  *ptr++ = static_cast<uint8_t>(values[5] >> 44);
  *ptr++ = static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 54);
  *ptr++ = static_cast<uint8_t>(values[6] >> 46);
  *ptr++ = static_cast<uint8_t>(values[6] >> 38);
  *ptr++ = static_cast<uint8_t>(values[6] >> 30);
  *ptr++ = static_cast<uint8_t>(values[6] >> 22);
  *ptr++ = static_cast<uint8_t>(values[6] >> 14);
  *ptr++ = static_cast<uint8_t>(values[6] >> 6);

  *ptr = static_cast<uint8_t>(values[6] << 2);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_59(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 51);
  *ptr++ = static_cast<uint8_t>(values[0] >> 43);
  *ptr++ = static_cast<uint8_t>(values[0] >> 35);
  *ptr++ = static_cast<uint8_t>(values[0] >> 27);
  *ptr++ = static_cast<uint8_t>(values[0] >> 19);
  *ptr++ = static_cast<uint8_t>(values[0] >> 11);
  *ptr++ = static_cast<uint8_t>(values[0] >> 3);

  *ptr = static_cast<uint8_t>(values[0] << 5);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 54);
  *ptr++ = static_cast<uint8_t>(values[1] >> 46);
  *ptr++ = static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 57);
  *ptr++ = static_cast<uint8_t>(values[2] >> 49);
  *ptr++ = static_cast<uint8_t>(values[2] >> 41);
  *ptr++ = static_cast<uint8_t>(values[2] >> 33);
  *ptr++ = static_cast<uint8_t>(values[2] >> 25);
  *ptr++ = static_cast<uint8_t>(values[2] >> 17);
  *ptr++ = static_cast<uint8_t>(values[2] >> 9);
  *ptr++ = static_cast<uint8_t>(values[2] >> 1);

  *ptr = static_cast<uint8_t>(values[2] << 7);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 52);
  *ptr++ = static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 55);
  *ptr++ = static_cast<uint8_t>(values[4] >> 47);
  *ptr++ = static_cast<uint8_t>(values[4] >> 39);
  *ptr++ = static_cast<uint8_t>(values[4] >> 31);
  *ptr++ = static_cast<uint8_t>(values[4] >> 23);
  *ptr++ = static_cast<uint8_t>(values[4] >> 15);
  *ptr++ = static_cast<uint8_t>(values[4] >> 7);

  *ptr = static_cast<uint8_t>(values[4] << 1);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 58);
  *ptr++ = static_cast<uint8_t>(values[5] >> 50);
  *ptr++ = static_cast<uint8_t>(values[5] >> 42);
  *ptr++ = static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 53);
  *ptr++ = static_cast<uint8_t>(values[6] >> 45);
  *ptr++ = static_cast<uint8_t>(values[6] >> 37);
  *ptr++ = static_cast<uint8_t>(values[6] >> 29);
  *ptr++ = static_cast<uint8_t>(values[6] >> 21);
  *ptr++ = static_cast<uint8_t>(values[6] >> 13);
  *ptr++ = static_cast<uint8_t>(values[6] >> 5);

  *ptr = static_cast<uint8_t>(values[6] << 3);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_60(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 52);
  *ptr++ = static_cast<uint8_t>(values[0] >> 44);
  *ptr++ = static_cast<uint8_t>(values[0] >> 36);
  *ptr++ = static_cast<uint8_t>(values[0] >> 28);
  *ptr++ = static_cast<uint8_t>(values[0] >> 20);
  *ptr++ = static_cast<uint8_t>(values[0] >> 12);
  *ptr++ = static_cast<uint8_t>(values[0] >> 4);

  *ptr = static_cast<uint8_t>(values[0] << 4);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 56);
  *ptr++ = static_cast<uint8_t>(values[1] >> 48);
  *ptr++ = static_cast<uint8_t>(values[1] >> 40);
  *ptr++ = static_cast<uint8_t>(values[1] >> 32);
  *ptr++ = static_cast<uint8_t>(values[1] >> 24);
  *ptr++ = static_cast<uint8_t>(values[1] >> 16);
  *ptr++ = static_cast<uint8_t>(values[1] >> 8);
  *ptr++ = static_cast<uint8_t>(values[1]);

  *ptr++ = static_cast<uint8_t>(values[2] >> 52);
  *ptr++ = static_cast<uint8_t>(values[2] >> 44);
  *ptr++ = static_cast<uint8_t>(values[2] >> 36);
  *ptr++ = static_cast<uint8_t>(values[2] >> 28);
  *ptr++ = static_cast<uint8_t>(values[2] >> 20);
  *ptr++ = static_cast<uint8_t>(values[2] >> 12);
  *ptr++ = static_cast<uint8_t>(values[2] >> 4);

  *ptr = static_cast<uint8_t>(values[2] << 4);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 56);
  *ptr++ = static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 52);
  *ptr++ = static_cast<uint8_t>(values[4] >> 44);
  *ptr++ = static_cast<uint8_t>(values[4] >> 36);
  *ptr++ = static_cast<uint8_t>(values[4] >> 28);
  *ptr++ = static_cast<uint8_t>(values[4] >> 20);
  *ptr++ = static_cast<uint8_t>(values[4] >> 12);
  *ptr++ = static_cast<uint8_t>(values[4] >> 4);

  *ptr = static_cast<uint8_t>(values[4] << 4);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 56);
  *ptr++ = static_cast<uint8_t>(values[5] >> 48);
  *ptr++ = static_cast<uint8_t>(values[5] >> 40);
  *ptr++ = static_cast<uint8_t>(values[5] >> 32);
  *ptr++ = static_cast<uint8_t>(values[5] >> 24);
  *ptr++ = static_cast<uint8_t>(values[5] >> 16);
  *ptr++ = static_cast<uint8_t>(values[5] >> 8);
  *ptr++ = static_cast<uint8_t>(values[5]);

  *ptr++ = static_cast<uint8_t>(values[6] >> 52);
  *ptr++ = static_cast<uint8_t>(values[6] >> 44);
  *ptr++ = static_cast<uint8_t>(values[6] >> 36);
  *ptr++ = static_cast<uint8_t>(values[6] >> 28);
  *ptr++ = static_cast<uint8_t>(values[6] >> 20);
  *ptr++ = static_cast<uint8_t>(values[6] >> 12);
  *ptr++ = static_cast<uint8_t>(values[6] >> 4);

  *ptr = static_cast<uint8_t>(values[6] << 4);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_61(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 53);
  *ptr++ = static_cast<uint8_t>(values[0] >> 45);
  *ptr++ = static_cast<uint8_t>(values[0] >> 37);
  *ptr++ = static_cast<uint8_t>(values[0] >> 29);
  *ptr++ = static_cast<uint8_t>(values[0] >> 21);
  *ptr++ = static_cast<uint8_t>(values[0] >> 13);
  *ptr++ = static_cast<uint8_t>(values[0] >> 5);

  *ptr = static_cast<uint8_t>(values[0] << 3);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 58);
  *ptr++ = static_cast<uint8_t>(values[1] >> 50);
  *ptr++ = static_cast<uint8_t>(values[1] >> 42);
  *ptr++ = static_cast<uint8_t>(values[1] >> 34);
  *ptr++ = static_cast<uint8_t>(values[1] >> 26);
  *ptr++ = static_cast<uint8_t>(values[1] >> 18);
  *ptr++ = static_cast<uint8_t>(values[1] >> 10);
  *ptr++ = static_cast<uint8_t>(values[1] >> 2);

  *ptr = static_cast<uint8_t>(values[1] << 6);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 55);
  *ptr++ = static_cast<uint8_t>(values[2] >> 47);
  *ptr++ = static_cast<uint8_t>(values[2] >> 39);
  *ptr++ = static_cast<uint8_t>(values[2] >> 31);
  *ptr++ = static_cast<uint8_t>(values[2] >> 23);
  *ptr++ = static_cast<uint8_t>(values[2] >> 15);
  *ptr++ = static_cast<uint8_t>(values[2] >> 7);

  *ptr = static_cast<uint8_t>(values[2] << 1);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 60);
  *ptr++ = static_cast<uint8_t>(values[3] >> 52);
  *ptr++ = static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 57);
  *ptr++ = static_cast<uint8_t>(values[4] >> 49);
  *ptr++ = static_cast<uint8_t>(values[4] >> 41);
  *ptr++ = static_cast<uint8_t>(values[4] >> 33);
  *ptr++ = static_cast<uint8_t>(values[4] >> 25);
  *ptr++ = static_cast<uint8_t>(values[4] >> 17);
  *ptr++ = static_cast<uint8_t>(values[4] >> 9);
  *ptr++ = static_cast<uint8_t>(values[4] >> 1);

  *ptr = static_cast<uint8_t>(values[4] << 7);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 54);
  *ptr++ = static_cast<uint8_t>(values[5] >> 46);
  *ptr++ = static_cast<uint8_t>(values[5] >> 38);
  *ptr++ = static_cast<uint8_t>(values[5] >> 30);
  *ptr++ = static_cast<uint8_t>(values[5] >> 22);
  *ptr++ = static_cast<uint8_t>(values[5] >> 14);
  *ptr++ = static_cast<uint8_t>(values[5] >> 6);

  *ptr = static_cast<uint8_t>(values[5] << 2);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 59);
  *ptr++ = static_cast<uint8_t>(values[6] >> 51);
  *ptr++ = static_cast<uint8_t>(values[6] >> 43);
  *ptr++ = static_cast<uint8_t>(values[6] >> 35);
  *ptr++ = static_cast<uint8_t>(values[6] >> 27);
  *ptr++ = static_cast<uint8_t>(values[6] >> 19);
  *ptr++ = static_cast<uint8_t>(values[6] >> 11);
  *ptr++ = static_cast<uint8_t>(values[6] >> 3);

  *ptr = static_cast<uint8_t>(values[6] << 5);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_62(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 54);
  *ptr++ = static_cast<uint8_t>(values[0] >> 46);
  *ptr++ = static_cast<uint8_t>(values[0] >> 38);
  *ptr++ = static_cast<uint8_t>(values[0] >> 30);
  *ptr++ = static_cast<uint8_t>(values[0] >> 22);
  *ptr++ = static_cast<uint8_t>(values[0] >> 14);
  *ptr++ = static_cast<uint8_t>(values[0] >> 6);

  *ptr = static_cast<uint8_t>(values[0] << 2);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 60);
  *ptr++ = static_cast<uint8_t>(values[1] >> 52);
  *ptr++ = static_cast<uint8_t>(values[1] >> 44);
  *ptr++ = static_cast<uint8_t>(values[1] >> 36);
  *ptr++ = static_cast<uint8_t>(values[1] >> 28);
  *ptr++ = static_cast<uint8_t>(values[1] >> 20);
  *ptr++ = static_cast<uint8_t>(values[1] >> 12);
  *ptr++ = static_cast<uint8_t>(values[1] >> 4);

  *ptr = static_cast<uint8_t>(values[1] << 4);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 58);
  *ptr++ = static_cast<uint8_t>(values[2] >> 50);
  *ptr++ = static_cast<uint8_t>(values[2] >> 42);
  *ptr++ = static_cast<uint8_t>(values[2] >> 34);
  *ptr++ = static_cast<uint8_t>(values[2] >> 26);
  *ptr++ = static_cast<uint8_t>(values[2] >> 18);
  *ptr++ = static_cast<uint8_t>(values[2] >> 10);
  *ptr++ = static_cast<uint8_t>(values[2] >> 2);

  *ptr = static_cast<uint8_t>(values[2] << 6);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 56);
  *ptr++ = static_cast<uint8_t>(values[3] >> 48);
  *ptr++ = static_cast<uint8_t>(values[3] >> 40);
  *ptr++ = static_cast<uint8_t>(values[3] >> 32);
  *ptr++ = static_cast<uint8_t>(values[3] >> 24);
  *ptr++ = static_cast<uint8_t>(values[3] >> 16);
  *ptr++ = static_cast<uint8_t>(values[3] >> 8);
  *ptr++ = static_cast<uint8_t>(values[3]);

  *ptr++ = static_cast<uint8_t>(values[4] >> 54);
  *ptr++ = static_cast<uint8_t>(values[4] >> 46);
  *ptr++ = static_cast<uint8_t>(values[4] >> 38);
  *ptr++ = static_cast<uint8_t>(values[4] >> 30);
  *ptr++ = static_cast<uint8_t>(values[4] >> 22);
  *ptr++ = static_cast<uint8_t>(values[4] >> 14);
  *ptr++ = static_cast<uint8_t>(values[4] >> 6);

  *ptr = static_cast<uint8_t>(values[4] << 2);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 60);
  *ptr++ = static_cast<uint8_t>(values[5] >> 52);
  *ptr++ = static_cast<uint8_t>(values[5] >> 44);
  *ptr++ = static_cast<uint8_t>(values[5] >> 36);
  *ptr++ = static_cast<uint8_t>(values[5] >> 28);
  *ptr++ = static_cast<uint8_t>(values[5] >> 20);
  *ptr++ = static_cast<uint8_t>(values[5] >> 12);
  *ptr++ = static_cast<uint8_t>(values[5] >> 4);

  *ptr = static_cast<uint8_t>(values[5] << 4);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 58);
  *ptr++ = static_cast<uint8_t>(values[6] >> 50);
  *ptr++ = static_cast<uint8_t>(values[6] >> 42);
  *ptr++ = static_cast<uint8_t>(values[6] >> 34);
  *ptr++ = static_cast<uint8_t>(values[6] >> 26);
  *ptr++ = static_cast<uint8_t>(values[6] >> 18);
  *ptr++ = static_cast<uint8_t>(values[6] >> 10);
  *ptr++ = static_cast<uint8_t>(values[6] >> 2);

  *ptr = static_cast<uint8_t>(values[6] << 6);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void pack_bits_63(const uint64_t* values, uint8_t* ptr) {
  *ptr++ = static_cast<uint8_t>(values[0] >> 55);
  *ptr++ = static_cast<uint8_t>(values[0] >> 47);
  *ptr++ = static_cast<uint8_t>(values[0] >> 39);
  *ptr++ = static_cast<uint8_t>(values[0] >> 31);
  *ptr++ = static_cast<uint8_t>(values[0] >> 23);
  *ptr++ = static_cast<uint8_t>(values[0] >> 15);
  *ptr++ = static_cast<uint8_t>(values[0] >> 7);

  *ptr = static_cast<uint8_t>(values[0] << 1);
  *ptr++ |= static_cast<uint8_t>(values[1] >> 62);
  *ptr++ = static_cast<uint8_t>(values[1] >> 54);
  *ptr++ = static_cast<uint8_t>(values[1] >> 46);
  *ptr++ = static_cast<uint8_t>(values[1] >> 38);
  *ptr++ = static_cast<uint8_t>(values[1] >> 30);
  *ptr++ = static_cast<uint8_t>(values[1] >> 22);
  *ptr++ = static_cast<uint8_t>(values[1] >> 14);
  *ptr++ = static_cast<uint8_t>(values[1] >> 6);

  *ptr = static_cast<uint8_t>(values[1] << 2);
  *ptr++ |= static_cast<uint8_t>(values[2] >> 61);
  *ptr++ = static_cast<uint8_t>(values[2] >> 53);
  *ptr++ = static_cast<uint8_t>(values[2] >> 45);
  *ptr++ = static_cast<uint8_t>(values[2] >> 37);
  *ptr++ = static_cast<uint8_t>(values[2] >> 29);
  *ptr++ = static_cast<uint8_t>(values[2] >> 21);
  *ptr++ = static_cast<uint8_t>(values[2] >> 13);
  *ptr++ = static_cast<uint8_t>(values[2] >> 5);

  *ptr = static_cast<uint8_t>(values[2] << 3);
  *ptr++ |= static_cast<uint8_t>(values[3] >> 60);
  *ptr++ = static_cast<uint8_t>(values[3] >> 52);
  *ptr++ = static_cast<uint8_t>(values[3] >> 44);
  *ptr++ = static_cast<uint8_t>(values[3] >> 36);
  *ptr++ = static_cast<uint8_t>(values[3] >> 28);
  *ptr++ = static_cast<uint8_t>(values[3] >> 20);
  *ptr++ = static_cast<uint8_t>(values[3] >> 12);
  *ptr++ = static_cast<uint8_t>(values[3] >> 4);

  *ptr = static_cast<uint8_t>(values[3] << 4);
  *ptr++ |= static_cast<uint8_t>(values[4] >> 59);
  *ptr++ = static_cast<uint8_t>(values[4] >> 51);
  *ptr++ = static_cast<uint8_t>(values[4] >> 43);
  *ptr++ = static_cast<uint8_t>(values[4] >> 35);
  *ptr++ = static_cast<uint8_t>(values[4] >> 27);
  *ptr++ = static_cast<uint8_t>(values[4] >> 19);
  *ptr++ = static_cast<uint8_t>(values[4] >> 11);
  *ptr++ = static_cast<uint8_t>(values[4] >> 3);

  *ptr = static_cast<uint8_t>(values[4] << 5);
  *ptr++ |= static_cast<uint8_t>(values[5] >> 58);
  *ptr++ = static_cast<uint8_t>(values[5] >> 50);
  *ptr++ = static_cast<uint8_t>(values[5] >> 42);
  *ptr++ = static_cast<uint8_t>(values[5] >> 34);
  *ptr++ = static_cast<uint8_t>(values[5] >> 26);
  *ptr++ = static_cast<uint8_t>(values[5] >> 18);
  *ptr++ = static_cast<uint8_t>(values[5] >> 10);
  *ptr++ = static_cast<uint8_t>(values[5] >> 2);

  *ptr = static_cast<uint8_t>(values[5] << 6);
  *ptr++ |= static_cast<uint8_t>(values[6] >> 57);
  *ptr++ = static_cast<uint8_t>(values[6] >> 49);
  *ptr++ = static_cast<uint8_t>(values[6] >> 41);
  *ptr++ = static_cast<uint8_t>(values[6] >> 33);
  *ptr++ = static_cast<uint8_t>(values[6] >> 25);
  *ptr++ = static_cast<uint8_t>(values[6] >> 17);
  *ptr++ = static_cast<uint8_t>(values[6] >> 9);
  *ptr++ = static_cast<uint8_t>(values[6] >> 1);

  *ptr = static_cast<uint8_t>(values[6] << 7);
  *ptr++ |= static_cast<uint8_t>(values[7] >> 56);
  *ptr++ = static_cast<uint8_t>(values[7] >> 48);
  *ptr++ = static_cast<uint8_t>(values[7] >> 40);
  *ptr++ = static_cast<uint8_t>(values[7] >> 32);
  *ptr++ = static_cast<uint8_t>(values[7] >> 24);
  *ptr++ = static_cast<uint8_t>(values[7] >> 16);
  *ptr++ = static_cast<uint8_t>(values[7] >> 8);
  *ptr = static_cast<uint8_t>(values[7]);
}

static inline void unpack_bits_1(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 7;
  values[1] = (*ptr >> 6) & 1;
  values[2] = (*ptr >> 5) & 1;
  values[3] = (*ptr >> 4) & 1;
  values[4] = (*ptr >> 3) & 1;
  values[5] = (*ptr >> 2) & 1;
  values[6] = (*ptr >> 1) & 1;
  values[7] = *ptr & 1;
}

static inline void unpack_bits_2(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 6;
  values[1] = (*ptr >> 4) & 3;
  values[2] = (*ptr >> 2) & 3;
  values[3] = *ptr++ & 3;
  values[4] = *ptr >> 6;
  values[5] = (*ptr >> 4) & 3;
  values[6] = (*ptr >> 2) & 3;
  values[7] = *ptr & 3;
}

static inline void unpack_bits_3(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 5;
  values[1] = (*ptr >> 2) & 7;
  values[2] = (*ptr++ & 3) << 1;
  values[2] |= *ptr >> 7;
  values[3] = (*ptr >> 4) & 7;
  values[4] = (*ptr >> 1) & 7;
  values[5] = (*ptr++ & 1) << 2;
  values[5] |= *ptr >> 6;
  values[6] = (*ptr >> 3) & 7;
  values[7] = *ptr & 7;
}

static inline void unpack_bits_4(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 4;
  values[1] = *ptr++ & 0xf;
  values[2] = *ptr >> 4;
  values[3] = *ptr++ & 0xf;
  values[4] = *ptr >> 4;
  values[5] = *ptr++ & 0xf;
  values[6] = *ptr >> 4;
  values[7] = *ptr & 0xf;
}

static inline void unpack_bits_5(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 3;

  values[1] = (*ptr++ & 7) << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr >> 1) & 0x1f;

  values[3] = (*ptr++ & 1) << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 1;
  values[4] |= *ptr >> 7;

  values[5] = (*ptr >> 2) & 0x1f;

  values[6] = (*ptr++ & 3) << 3;
  values[6] |= *ptr >> 5;

  values[7] = *ptr & 0x1f;
}

static inline void unpack_bits_6(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 2;

  values[1] = (*ptr++ & 3) << 4;
  values[1] |= *ptr >> 4;

  values[2] = (*ptr++ & 0xf) << 2;
  values[2] |= *ptr >> 6;

  values[3] = *ptr++ & 0x3f;

  values[4] = *ptr >> 2;

  values[5] = (*ptr++ & 3) << 4;
  values[5] |= *ptr >> 4;

  values[6] = (*ptr++ & 0xf) << 2;
  values[6] |= *ptr >> 6;

  values[7] = *ptr & 0x3f;
}

static inline void unpack_bits_7(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr >> 1;

  values[1] = (*ptr++ & 1) << 6;
  values[1] |= *ptr >> 2;

  values[2] = (*ptr++ & 3) << 5;
  values[2] |= *ptr >> 3;

  values[3] = (*ptr++ & 7) << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 3;
  values[4] |= *ptr >> 5;

  values[5] = (*ptr++ & 0x1f) << 2;
  values[5] |= *ptr >> 6;

  values[6] = (*ptr++ & 0x3f) << 1;
  values[6] |= *ptr >> 7;

  values[7] = *ptr & 0x7f;
}

static inline void unpack_bits_8(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++;
  values[1] = *ptr++;
  values[2] = *ptr++;
  values[3] = *ptr++;
  values[4] = *ptr++;
  values[5] = *ptr++;
  values[6] = *ptr++;
  values[7] = *ptr;
}

static inline void unpack_bits_9(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = (*ptr++ & 0x7f) << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr++ & 0x3f) << 3;
  values[2] |= *ptr >> 5;

  values[3] = (*ptr++ & 0x1f) << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 5;
  values[4] |= *ptr >> 3;

  values[5] = (*ptr++ & 7) << 6;
  values[5] |= *ptr >> 2;

  values[6] = (*ptr++ & 3) << 7;
  values[6] |= *ptr >> 1;

  values[7] = (*ptr++ & 1) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_10(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = (*ptr++ & 0x3f) << 4;
  values[1] |= *ptr >> 4;

  values[2] = (*ptr++ & 0xf) << 6;
  values[2] |= *ptr >> 2;

  values[3] = (*ptr++ & 3) << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = (*ptr++ & 0x3f) << 4;
  values[5] |= *ptr >> 4;

  values[6] = (*ptr++ & 0xf) << 6;
  values[6] |= *ptr >> 2;

  values[7] = (*ptr++ & 3) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_11(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = (*ptr++ & 0x1f) << 6;
  values[1] |= *ptr >> 2;

  values[2] = (*ptr++ & 3) << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = (*ptr++ & 0x7f) << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 7;
  values[4] |= *ptr >> 1;

  values[5] = (*ptr++ & 1) << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = (*ptr++ & 0x3f) << 5;
  values[6] |= *ptr >> 3;

  values[7] = (*ptr++ & 7) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_12(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = (*ptr++ & 0xf) << 8;
  values[1] |= *ptr++;

  values[2] = *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = (*ptr++ & 0xf) << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = (*ptr++ & 0xf) << 8;
  values[5] |= *ptr++;

  values[6] = *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = (*ptr++ & 0xf) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_13(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = (*ptr++ & 7) << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr++ & 0x3f) << 7;
  values[2] |= *ptr >> 1;

  values[3] = (*ptr++ & 1) << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = (*ptr++ & 0x7f) << 6;
  values[5] |= *ptr >> 2;

  values[6] = (*ptr++ & 3) << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = (*ptr++ & 0x1f) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_14(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = (*ptr++ & 3) << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = (*ptr++ & 0xf) << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = (*ptr++ & 0x3f) << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = (*ptr++ & 3) << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = (*ptr++ & 0xf) << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = (*ptr++ & 0x3f) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_15(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = (*ptr++ & 1) << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = (*ptr++ & 3) << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = (*ptr++ & 7) << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = (*ptr++ & 0x1f) << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = (*ptr++ & 0x3f) << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = (*ptr++ & 0x7f) << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_16(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 8;
  values[0] |= *ptr++;
  values[1] = *ptr++ << 8;
  values[1] |= *ptr++;
  values[2] = *ptr++ << 8;
  values[2] |= *ptr++;
  values[3] = *ptr++ << 8;
  values[3] |= *ptr++;
  values[4] = *ptr++ << 8;
  values[4] |= *ptr++;
  values[5] = *ptr++ << 8;
  values[5] |= *ptr++;
  values[6] = *ptr++ << 8;
  values[6] |= *ptr++;
  values[7] = *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_17(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 9;
  values[0] |= *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = (*ptr++ & 0x7f) << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr++ & 0x3f) << 11;
  values[2] |= *ptr++ << 3;
  values[2] |= *ptr >> 5;

  values[3] = (*ptr++ & 0x1f) << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 13;
  values[4] |= *ptr++ << 5;
  values[4] |= *ptr >> 3;

  values[5] = (*ptr++ & 7) << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = (*ptr++ & 3) << 15;
  values[6] |= *ptr++ << 7;
  values[6] |= *ptr >> 1;

  values[7] = (*ptr++ & 1) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_18(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 10;
  values[0] |= *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = (*ptr++ & 0x3f) << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = (*ptr++ & 0xf) << 14;
  values[2] |= *ptr++ << 6;
  values[2] |= *ptr >> 2;

  values[3] = (*ptr++ & 3) << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 10;
  values[4] |= *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = (*ptr++ & 0x3f) << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = (*ptr++ & 0xf) << 14;
  values[6] |= *ptr++ << 6;
  values[6] |= *ptr >> 2;

  values[7] = (*ptr++ & 3) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_19(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 11;
  values[0] |= *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = (*ptr++ & 0x1f) << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = (*ptr++ & 3) << 17;
  values[2] |= *ptr++ << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = (*ptr++ & 0x7f) << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 15;
  values[4] |= *ptr++ << 7;
  values[4] |= *ptr >> 1;

  values[5] = (*ptr++ & 1) << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = (*ptr++ & 0x3f) << 13;
  values[6] |= *ptr++ << 5;
  values[6] |= *ptr >> 3;

  values[7] = (*ptr++ & 7) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_20(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 12;
  values[0] |= *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = (*ptr++ & 0xf) << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;

  values[2] = *ptr++ << 12;
  values[2] |= *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = (*ptr++ & 0xf) << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 12;
  values[4] |= *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = (*ptr++ & 0xf) << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;

  values[6] = *ptr++ << 12;
  values[6] |= *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = (*ptr++ & 0xf) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_21(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 13;
  values[0] |= *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = (*ptr++ & 7) << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr++ & 0x3f) << 15;
  values[2] |= *ptr++ << 7;
  values[2] |= *ptr >> 1;

  values[3] = (*ptr++ & 1) << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 17;
  values[4] |= *ptr++ << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = (*ptr++ & 0x7f) << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = (*ptr++ & 3) << 19;
  values[6] |= *ptr++ << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = (*ptr++ & 0x1f) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_22(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 14;
  values[0] |= *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = (*ptr++ & 3) << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = (*ptr++ & 0xf) << 18;
  values[2] |= *ptr++ << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = (*ptr++ & 0x3f) << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 14;
  values[4] |= *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = (*ptr++ & 3) << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = (*ptr++ & 0xf) << 18;
  values[6] |= *ptr++ << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = (*ptr++ & 0x3f) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_23(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 15;
  values[0] |= *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = (*ptr++ & 1) << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = (*ptr++ & 3) << 21;
  values[2] |= *ptr++ << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = (*ptr++ & 7) << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 19;
  values[4] |= *ptr++ << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = (*ptr++ & 0x1f) << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = (*ptr++ & 0x3f) << 17;
  values[6] |= *ptr++ << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = (*ptr++ & 0x7f) << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_24(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 16;
  values[0] |= *ptr++ << 8;
  values[0] |= *ptr++;
  values[1] = *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;
  values[2] = *ptr++ << 16;
  values[2] |= *ptr++ << 8;
  values[2] |= *ptr++;
  values[3] = *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;
  values[4] = *ptr++ << 16;
  values[4] |= *ptr++ << 8;
  values[4] |= *ptr++;
  values[5] = *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;
  values[6] = *ptr++ << 16;
  values[6] |= *ptr++ << 8;
  values[6] |= *ptr++;
  values[7] = *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_25(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 17;
  values[0] |= *ptr++ << 9;
  values[0] |= *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = (*ptr++ & 0x7f) << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr++ & 0x3f) << 19;
  values[2] |= *ptr++ << 11;
  values[2] |= *ptr++ << 3;
  values[2] |= *ptr >> 5;

  values[3] = (*ptr++ & 0x1f) << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 21;
  values[4] |= *ptr++ << 13;
  values[4] |= *ptr++ << 5;
  values[4] |= *ptr >> 3;

  values[5] = (*ptr++ & 7) << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = (*ptr++ & 3) << 23;
  values[6] |= *ptr++ << 15;
  values[6] |= *ptr++ << 7;
  values[6] |= *ptr >> 1;

  values[7] = static_cast<uint64_t>(*ptr++ & 1) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_26(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 18;
  values[0] |= *ptr++ << 10;
  values[0] |= *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = (*ptr++ & 0x3f) << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = (*ptr++ & 0xf) << 22;
  values[2] |= *ptr++ << 14;
  values[2] |= *ptr++ << 6;
  values[2] |= *ptr >> 2;

  values[3] = static_cast<uint64_t>(*ptr++ & 3) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 18;
  values[4] |= *ptr++ << 10;
  values[4] |= *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = (*ptr++ & 0x3f) << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = (*ptr++ & 0xf) << 22;
  values[6] |= *ptr++ << 14;
  values[6] |= *ptr++ << 6;
  values[6] |= *ptr >> 2;

  values[7] = static_cast<uint64_t>(*ptr++ & 3) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_27(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 19;
  values[0] |= *ptr++ << 11;
  values[0] |= *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = (*ptr++ & 0x1f) << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 25;
  values[2] |= *ptr++ << 17;
  values[2] |= *ptr++ << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = (*ptr++ & 0x7f) << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = (*ptr++ & 0xf) << 23;
  values[4] |= *ptr++ << 15;
  values[4] |= *ptr++ << 7;
  values[4] |= *ptr >> 1;

  values[5] = static_cast<uint64_t>(*ptr++ & 1) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = (*ptr++ & 0x3f) << 21;
  values[6] |= *ptr++ << 13;
  values[6] |= *ptr++ << 5;
  values[6] |= *ptr >> 3;

  values[7] = static_cast<uint64_t>(*ptr++ & 7) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_28(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 20;
  values[0] |= *ptr++ << 12;
  values[0] |= *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = static_cast<uint64_t>(*ptr++ & 0xf) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;

  values[2] = *ptr++ << 20;
  values[2] |= *ptr++ << 12;
  values[2] |= *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = static_cast<uint64_t>(*ptr++ & 0xf) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 20;
  values[4] |= *ptr++ << 12;
  values[4] |= *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = static_cast<uint64_t>(*ptr++ & 0xf) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;

  values[6] = *ptr++ << 20;
  values[6] |= *ptr++ << 12;
  values[6] |= *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = static_cast<uint64_t>(*ptr++ & 0xf) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_29(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 21;
  values[0] |= *ptr++ << 13;
  values[0] |= *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = static_cast<uint64_t>(*ptr++ & 7) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = (*ptr++ & 0x3f) << 23;
  values[2] |= *ptr++ << 15;
  values[2] |= *ptr++ << 7;
  values[2] |= *ptr >> 1;

  values[3] = static_cast<uint64_t>(*ptr++ & 1) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 25;
  values[4] |= *ptr++ << 17;
  values[4] |= *ptr++ << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = (*ptr++ & 0x7f) << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 27;
  values[6] |= *ptr++ << 19;
  values[6] |= *ptr++ << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x1f) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_30(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 22;
  values[0] |= *ptr++ << 14;
  values[0] |= *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = static_cast<uint64_t>(*ptr++ & 3) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 26;
  values[2] |= *ptr++ << 18;
  values[2] |= *ptr++ << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x3f) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = *ptr++ << 22;
  values[4] |= *ptr++ << 14;
  values[4] |= *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = static_cast<uint64_t>(*ptr++ & 3) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 26;
  values[6] |= *ptr++ << 18;
  values[6] |= *ptr++ << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x3f) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_31(uint64_t* values, const uint8_t* ptr) {
  values[0] = *ptr++ << 23;
  values[0] |= *ptr++ << 15;
  values[0] |= *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = static_cast<uint64_t>(*ptr++ & 1) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 29;
  values[2] |= *ptr++ << 21;
  values[2] |= *ptr++ << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = static_cast<uint64_t>(*ptr++ & 7) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 27;
  values[4] |= *ptr++ << 19;
  values[4] |= *ptr++ << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x1f) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 25;
  values[6] |= *ptr++ << 17;
  values[6] |= *ptr++ << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x7f) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_32(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 24;
  values[0] |= *ptr++ << 16;
  values[0] |= *ptr++ << 8;
  values[0] |= *ptr++;
  values[1] = static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;
  values[2] = static_cast<uint64_t>(*ptr++) << 24;
  values[2] |= *ptr++ << 16;
  values[2] |= *ptr++ << 8;
  values[2] |= *ptr++;
  values[3] = static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;
  values[4] = static_cast<uint64_t>(*ptr++) << 24;
  values[4] |= *ptr++ << 16;
  values[4] |= *ptr++ << 8;
  values[4] |= *ptr++;
  values[5] = static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;
  values[6] = static_cast<uint64_t>(*ptr++) << 24;
  values[6] |= *ptr++ << 16;
  values[6] |= *ptr++ << 8;
  values[6] |= *ptr++;
  values[7] = static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_33(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 25;
  values[0] |= *ptr++ << 17;
  values[0] |= *ptr++ << 9;
  values[0] |= *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x7f) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 27;
  values[2] |= *ptr++ << 19;
  values[2] |= *ptr++ << 11;
  values[2] |= *ptr++ << 3;
  values[2] |= *ptr >> 5;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x1f) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 29;
  values[4] |= *ptr++ << 21;
  values[4] |= *ptr++ << 13;
  values[4] |= *ptr++ << 5;
  values[4] |= *ptr >> 3;

  values[5] = static_cast<uint64_t>(*ptr++ & 7) << 30;
  values[5] |= *ptr++ << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 31;
  values[6] |= *ptr++ << 23;
  values[6] |= *ptr++ << 15;
  values[6] |= *ptr++ << 7;
  values[6] |= *ptr >> 1;

  values[7] = static_cast<uint64_t>(*ptr++ & 1) << 32;
  values[7] |= *ptr++ << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_34(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 26;
  values[0] |= *ptr++ << 18;
  values[0] |= *ptr++ << 10;
  values[0] |= *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x3f) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 30;
  values[2] |= *ptr++ << 22;
  values[2] |= *ptr++ << 14;
  values[2] |= *ptr++ << 6;
  values[2] |= *ptr >> 2;

  values[3] = static_cast<uint64_t>(*ptr++ & 3) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 26;
  values[4] |= *ptr++ << 18;
  values[4] |= *ptr++ << 10;
  values[4] |= *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x3f) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 30;
  values[6] |= *ptr++ << 22;
  values[6] |= *ptr++ << 14;
  values[6] |= *ptr++ << 6;
  values[6] |= *ptr >> 2;

  values[7] = static_cast<uint64_t>(*ptr++ & 3) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr++;
}

static inline void unpack_bits_35(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 27;
  values[0] |= *ptr++ << 19;
  values[0] |= *ptr++ << 11;
  values[0] |= *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x1f) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 2) << 33;
  values[2] |= static_cast<uint64_t>(*ptr++) << 25;
  values[2] |= *ptr++ << 17;
  values[2] |= *ptr++ << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x7f) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 31;
  values[4] |= *ptr++ << 23;
  values[4] |= *ptr++ << 15;
  values[4] |= *ptr++ << 7;
  values[4] |= *ptr >> 1;

  values[5] = static_cast<uint64_t>(*ptr++ & 1) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 29;
  values[6] |= *ptr++ << 21;
  values[6] |= *ptr++ << 13;
  values[6] |= *ptr++ << 5;
  values[6] |= *ptr >> 3;

  values[7] = static_cast<uint64_t>(*ptr++ & 7) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_36(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 28;
  values[0] |= *ptr++ << 20;
  values[0] |= *ptr++ << 12;
  values[0] |= *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = static_cast<uint64_t>(*ptr++ & 0xf) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;

  values[2] = static_cast<uint64_t>(*ptr++) << 28;
  values[2] |= *ptr++ << 20;
  values[2] |= *ptr++ << 12;
  values[2] |= *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = static_cast<uint64_t>(*ptr++ & 0xf) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 28;
  values[4] |= *ptr++ << 20;
  values[4] |= *ptr++ << 12;
  values[4] |= *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = static_cast<uint64_t>(*ptr++ & 0xf) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;

  values[6] = static_cast<uint64_t>(*ptr++) << 28;
  values[6] |= *ptr++ << 20;
  values[6] |= *ptr++ << 12;
  values[6] |= *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = static_cast<uint64_t>(*ptr++ & 0xf) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_37(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 29;
  values[0] |= *ptr++ << 21;
  values[0] |= *ptr++ << 13;
  values[0] |= *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = static_cast<uint64_t>(*ptr++ & 7) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 31;
  values[2] |= static_cast<uint64_t>(*ptr++) << 23;
  values[2] |= *ptr++ << 15;
  values[2] |= *ptr++ << 7;
  values[2] |= *ptr >> 1;

  values[3] = static_cast<uint64_t>(*ptr++ & 1) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 33;
  values[4] |= static_cast<uint64_t>(*ptr++) << 25;
  values[4] |= *ptr++ << 17;
  values[4] |= *ptr++ << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x7f) << 30;
  values[5] |= static_cast<uint64_t>(*ptr++) << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 35;
  values[6] |= static_cast<uint64_t>(*ptr++) << 27;
  values[6] |= *ptr++ << 19;
  values[6] |= *ptr++ << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x1f) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_38(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 30;
  values[0] |= *ptr++ << 22;
  values[0] |= *ptr++ << 14;
  values[0] |= *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = static_cast<uint64_t>(*ptr++ & 3) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 34;
  values[2] |= static_cast<uint64_t>(*ptr++) << 26;
  values[2] |= *ptr++ << 18;
  values[2] |= *ptr++ << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x3f) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 30;
  values[4] |= *ptr++ << 22;
  values[4] |= *ptr++ << 14;
  values[4] |= *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = static_cast<uint64_t>(*ptr++ & 3) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 34;
  values[6] |= static_cast<uint64_t>(*ptr++) << 26;
  values[6] |= *ptr++ << 18;
  values[6] |= *ptr++ << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x3f) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_39(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 31;
  values[0] |= *ptr++ << 23;
  values[0] |= *ptr++ << 15;
  values[0] |= *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = static_cast<uint64_t>(*ptr++ & 1) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 37;
  values[2] |= static_cast<uint64_t>(*ptr++) << 29;
  values[2] |= *ptr++ << 21;
  values[2] |= *ptr++ << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = static_cast<uint64_t>(*ptr++ & 7) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 35;
  values[4] |= static_cast<uint64_t>(*ptr++) << 27;
  values[4] |= *ptr++ << 19;
  values[4] |= *ptr++ << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x1f) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 33;
  values[6] |= static_cast<uint64_t>(*ptr++) << 25;
  values[6] |= *ptr++ << 17;
  values[6] |= *ptr++ << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x7f) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_40(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 32;
  values[0] |= static_cast<uint64_t>(*ptr++) << 24;
  values[0] |= *ptr++ << 16;
  values[0] |= *ptr++ << 8;
  values[0] |= *ptr++;
  values[1] = static_cast<uint64_t>(*ptr++) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;
  values[2] = static_cast<uint64_t>(*ptr++) << 32;
  values[2] |= static_cast<uint64_t>(*ptr++) << 24;
  values[2] |= *ptr++ << 16;
  values[2] |= *ptr++ << 8;
  values[2] |= *ptr++;
  values[3] = static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;
  values[4] = static_cast<uint64_t>(*ptr++) << 32;
  values[4] |= static_cast<uint64_t>(*ptr++) << 24;
  values[4] |= *ptr++ << 16;
  values[4] |= *ptr++ << 8;
  values[4] |= *ptr++;
  values[5] = static_cast<uint64_t>(*ptr++) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;
  values[6] = static_cast<uint64_t>(*ptr++) << 32;
  values[6] |= static_cast<uint64_t>(*ptr++) << 24;
  values[6] |= *ptr++ << 16;
  values[6] |= *ptr++ << 8;
  values[6] |= *ptr++;
  values[7] = static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_41(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 33;
  values[0] |= static_cast<uint64_t>(*ptr++) << 25;
  values[0] |= *ptr++ << 17;
  values[0] |= *ptr++ << 9;
  values[0] |= *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x7f) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 35;
  values[2] |= static_cast<uint64_t>(*ptr++) << 27;
  values[2] |= *ptr++ << 19;
  values[2] |= *ptr++ << 11;
  values[2] |= *ptr++ << 3;
  values[2] |= *ptr >> 5;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x1f) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 37;
  values[4] |= static_cast<uint64_t>(*ptr++) << 29;
  values[4] |= *ptr++ << 21;
  values[4] |= *ptr++ << 13;
  values[4] |= *ptr++ << 5;
  values[4] |= *ptr >> 3;

  values[5] = static_cast<uint64_t>(*ptr++ & 7) << 38;
  values[5] |= static_cast<uint64_t>(*ptr++) << 30;
  values[5] |= *ptr++ << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 39;
  values[6] |= static_cast<uint64_t>(*ptr++) << 31;
  values[6] |= *ptr++ << 23;
  values[6] |= *ptr++ << 15;
  values[6] |= *ptr++ << 7;
  values[6] |= *ptr >> 1;

  values[7] = static_cast<uint64_t>(*ptr++ & 1) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_42(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 34;
  values[0] |= static_cast<uint64_t>(*ptr++) << 26;
  values[0] |= *ptr++ << 18;
  values[0] |= *ptr++ << 10;
  values[0] |= *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x3f) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 38;
  values[2] |= static_cast<uint64_t>(*ptr++) << 30;
  values[2] |= *ptr++ << 22;
  values[2] |= *ptr++ << 14;
  values[2] |= *ptr++ << 6;
  values[2] |= *ptr >> 2;

  values[3] = static_cast<uint64_t>(*ptr++ & 3) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 34;
  values[4] |= static_cast<uint64_t>(*ptr++) << 26;
  values[4] |= *ptr++ << 18;
  values[4] |= *ptr++ << 10;
  values[4] |= *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x3f) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 38;
  values[6] |= static_cast<uint64_t>(*ptr++) << 30;
  values[6] |= *ptr++ << 22;
  values[6] |= *ptr++ << 14;
  values[6] |= *ptr++ << 6;
  values[6] |= *ptr >> 2;

  values[7] = static_cast<uint64_t>(*ptr++ & 3) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_43(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 35;
  values[0] |= static_cast<uint64_t>(*ptr++) << 27;
  values[0] |= *ptr++ << 19;
  values[0] |= *ptr++ << 11;
  values[0] |= *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x1f) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 41;
  values[2] |= static_cast<uint64_t>(*ptr++) << 33;
  values[2] |= static_cast<uint64_t>(*ptr++) << 25;
  values[2] |= *ptr++ << 17;
  values[2] |= *ptr++ << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x7f) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 39;
  values[4] |= static_cast<uint64_t>(*ptr++) << 31;
  values[4] |= *ptr++ << 23;
  values[4] |= *ptr++ << 15;
  values[4] |= *ptr++ << 7;
  values[4] |= *ptr >> 1;

  values[5] = static_cast<uint64_t>(*ptr++ & 1) << 42;
  values[5] |= static_cast<uint64_t>(*ptr++) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 37;
  values[6] |= static_cast<uint64_t>(*ptr++) << 29;
  values[6] |= *ptr++ << 21;
  values[6] |= *ptr++ << 13;
  values[6] |= *ptr++ << 5;
  values[6] |= *ptr >> 3;

  values[7] = static_cast<uint64_t>(*ptr++ & 7) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_44(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 36;
  values[0] |= static_cast<uint64_t>(*ptr++) << 28;
  values[0] |= *ptr++ << 20;
  values[0] |= *ptr++ << 12;
  values[0] |= *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = static_cast<uint64_t>(*ptr++ & 0xf) << 40;
  values[1] |= static_cast<uint64_t>(*ptr++) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;

  values[2] = static_cast<uint64_t>(*ptr++) << 36;
  values[2] |= static_cast<uint64_t>(*ptr++) << 28;
  values[2] |= *ptr++ << 20;
  values[2] |= *ptr++ << 12;
  values[2] |= *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = static_cast<uint64_t>(*ptr++ & 0xf) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 36;
  values[4] |= static_cast<uint64_t>(*ptr++) << 28;
  values[4] |= *ptr++ << 20;
  values[4] |= *ptr++ << 12;
  values[4] |= *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = static_cast<uint64_t>(*ptr++ & 0xf) << 40;
  values[5] |= static_cast<uint64_t>(*ptr++) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;

  values[6] = static_cast<uint64_t>(*ptr++) << 36;
  values[6] |= static_cast<uint64_t>(*ptr++) << 28;
  values[6] |= *ptr++ << 20;
  values[6] |= *ptr++ << 12;
  values[6] |= *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = static_cast<uint64_t>(*ptr++ & 0xf) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_45(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 37;
  values[0] |= static_cast<uint64_t>(*ptr++) << 29;
  values[0] |= *ptr++ << 21;
  values[0] |= *ptr++ << 13;
  values[0] |= *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = static_cast<uint64_t>(*ptr++ & 7) << 42;
  values[1] |= static_cast<uint64_t>(*ptr++) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 39;
  values[2] |= static_cast<uint64_t>(*ptr++) << 31;
  values[2] |= static_cast<uint64_t>(*ptr++) << 23;
  values[2] |= *ptr++ << 15;
  values[2] |= *ptr++ << 7;
  values[2] |= *ptr >> 1;

  values[3] = static_cast<uint64_t>(*ptr++ & 1) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 41;
  values[4] |= static_cast<uint64_t>(*ptr++) << 33;
  values[4] |= static_cast<uint64_t>(*ptr++) << 25;
  values[4] |= *ptr++ << 17;
  values[4] |= *ptr++ << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x7f) << 38;
  values[5] |= static_cast<uint64_t>(*ptr++) << 30;
  values[5] |= static_cast<uint64_t>(*ptr++) << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 43;
  values[6] |= static_cast<uint64_t>(*ptr++) << 35;
  values[6] |= static_cast<uint64_t>(*ptr++) << 27;
  values[6] |= *ptr++ << 19;
  values[6] |= *ptr++ << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x1f) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_46(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 38;
  values[0] |= static_cast<uint64_t>(*ptr++) << 30;
  values[0] |= *ptr++ << 22;
  values[0] |= *ptr++ << 14;
  values[0] |= *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = static_cast<uint64_t>(*ptr++ & 3) << 44;
  values[1] |= static_cast<uint64_t>(*ptr++) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 42;
  values[2] |= static_cast<uint64_t>(*ptr++) << 34;
  values[2] |= static_cast<uint64_t>(*ptr++) << 26;
  values[2] |= *ptr++ << 18;
  values[2] |= *ptr++ << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x3f) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 38;
  values[4] |= static_cast<uint64_t>(*ptr++) << 30;
  values[4] |= *ptr++ << 22;
  values[4] |= *ptr++ << 14;
  values[4] |= *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = static_cast<uint64_t>(*ptr++ & 3) << 44;
  values[5] |= static_cast<uint64_t>(*ptr++) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 42;
  values[6] |= static_cast<uint64_t>(*ptr++) << 34;
  values[6] |= static_cast<uint64_t>(*ptr++) << 26;
  values[6] |= *ptr++ << 18;
  values[6] |= *ptr++ << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x3f) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_47(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 39;
  values[0] |= static_cast<uint64_t>(*ptr++) << 31;
  values[0] |= *ptr++ << 23;
  values[0] |= *ptr++ << 15;
  values[0] |= *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = static_cast<uint64_t>(*ptr++ & 1) << 46;
  values[1] |= static_cast<uint64_t>(*ptr++) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 45;
  values[2] |= static_cast<uint64_t>(*ptr++) << 37;
  values[2] |= static_cast<uint64_t>(*ptr++) << 29;
  values[2] |= *ptr++ << 21;
  values[2] |= *ptr++ << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = static_cast<uint64_t>(*ptr++ & 7) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 43;
  values[4] |= static_cast<uint64_t>(*ptr++) << 35;
  values[4] |= static_cast<uint64_t>(*ptr++) << 27;
  values[4] |= *ptr++ << 19;
  values[4] |= *ptr++ << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x1f) << 42;
  values[5] |= static_cast<uint64_t>(*ptr++) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 41;
  values[6] |= static_cast<uint64_t>(*ptr++) << 33;
  values[6] |= static_cast<uint64_t>(*ptr++) << 25;
  values[6] |= *ptr++ << 17;
  values[6] |= *ptr++ << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x7f) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_48(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 40;
  values[0] |= static_cast<uint64_t>(*ptr++) << 32;
  values[0] |= static_cast<uint64_t>(*ptr++) << 24;
  values[0] |= *ptr++ << 16;
  values[0] |= *ptr++ << 8;
  values[0] |= *ptr++;
  values[1] = static_cast<uint64_t>(*ptr++) << 40;
  values[1] |= static_cast<uint64_t>(*ptr++) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;
  values[2] = static_cast<uint64_t>(*ptr++) << 40;
  values[2] |= static_cast<uint64_t>(*ptr++) << 32;
  values[2] |= static_cast<uint64_t>(*ptr++) << 24;
  values[2] |= *ptr++ << 16;
  values[2] |= *ptr++ << 8;
  values[2] |= *ptr++;
  values[3] = static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;
  values[4] = static_cast<uint64_t>(*ptr++) << 40;
  values[4] |= static_cast<uint64_t>(*ptr++) << 32;
  values[4] |= static_cast<uint64_t>(*ptr++) << 24;
  values[4] |= *ptr++ << 16;
  values[4] |= *ptr++ << 8;
  values[4] |= *ptr++;
  values[5] = static_cast<uint64_t>(*ptr++) << 40;
  values[5] |= static_cast<uint64_t>(*ptr++) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;
  values[6] = static_cast<uint64_t>(*ptr++) << 40;
  values[6] |= static_cast<uint64_t>(*ptr++) << 32;
  values[6] |= static_cast<uint64_t>(*ptr++) << 24;
  values[6] |= *ptr++ << 16;
  values[6] |= *ptr++ << 8;
  values[6] |= *ptr++;
  values[7] = static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_49(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 41;
  values[0] |= static_cast<uint64_t>(*ptr++) << 33;
  values[0] |= static_cast<uint64_t>(*ptr++) << 25;
  values[0] |= *ptr++ << 17;
  values[0] |= *ptr++ << 9;
  values[0] |= *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x7f) << 42;
  values[1] |= static_cast<uint64_t>(*ptr++) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 43;
  values[2] |= static_cast<uint64_t>(*ptr++) << 35;
  values[2] |= static_cast<uint64_t>(*ptr++) << 27;
  values[2] |= *ptr++ << 19;
  values[2] |= *ptr++ << 11;
  values[2] |= *ptr++ << 3;
  values[2] |= *ptr >> 5;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x1f) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 45;
  values[4] |= static_cast<uint64_t>(*ptr++) << 37;
  values[4] |= static_cast<uint64_t>(*ptr++) << 29;
  values[4] |= *ptr++ << 21;
  values[4] |= *ptr++ << 13;
  values[4] |= *ptr++ << 5;
  values[4] |= *ptr >> 3;

  values[5] = static_cast<uint64_t>(*ptr++ & 7) << 46;
  values[5] |= static_cast<uint64_t>(*ptr++) << 38;
  values[5] |= static_cast<uint64_t>(*ptr++) << 30;
  values[5] |= *ptr++ << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 47;
  values[6] |= static_cast<uint64_t>(*ptr++) << 39;
  values[6] |= static_cast<uint64_t>(*ptr++) << 31;
  values[6] |= *ptr++ << 23;
  values[6] |= *ptr++ << 15;
  values[6] |= *ptr++ << 7;
  values[6] |= *ptr >> 1;

  values[7] = static_cast<uint64_t>(*ptr++ & 1) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_50(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 42;
  values[0] |= static_cast<uint64_t>(*ptr++) << 34;
  values[0] |= static_cast<uint64_t>(*ptr++) << 26;
  values[0] |= *ptr++ << 18;
  values[0] |= *ptr++ << 10;
  values[0] |= *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x3f) << 44;
  values[1] |= static_cast<uint64_t>(*ptr++) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 46;
  values[2] |= static_cast<uint64_t>(*ptr++) << 38;
  values[2] |= static_cast<uint64_t>(*ptr++) << 30;
  values[2] |= *ptr++ << 22;
  values[2] |= *ptr++ << 14;
  values[2] |= *ptr++ << 6;
  values[2] |= *ptr >> 2;

  values[3] = static_cast<uint64_t>(*ptr++ & 3) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 42;
  values[4] |= static_cast<uint64_t>(*ptr++) << 34;
  values[4] |= static_cast<uint64_t>(*ptr++) << 26;
  values[4] |= *ptr++ << 18;
  values[4] |= *ptr++ << 10;
  values[4] |= *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x3f) << 44;
  values[5] |= static_cast<uint64_t>(*ptr++) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 46;
  values[6] |= static_cast<uint64_t>(*ptr++) << 38;
  values[6] |= static_cast<uint64_t>(*ptr++) << 30;
  values[6] |= *ptr++ << 22;
  values[6] |= *ptr++ << 14;
  values[6] |= *ptr++ << 6;
  values[6] |= *ptr >> 2;

  values[7] = static_cast<uint64_t>(*ptr++ & 3) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_51(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 43;
  values[0] |= static_cast<uint64_t>(*ptr++) << 35;
  values[0] |= static_cast<uint64_t>(*ptr++) << 27;
  values[0] |= *ptr++ << 19;
  values[0] |= *ptr++ << 11;
  values[0] |= *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x1f) << 46;
  values[1] |= static_cast<uint64_t>(*ptr++) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 49;
  values[2] |= static_cast<uint64_t>(*ptr++) << 41;
  values[2] |= static_cast<uint64_t>(*ptr++) << 33;
  values[2] |= static_cast<uint64_t>(*ptr++) << 25;
  values[2] |= *ptr++ << 17;
  values[2] |= *ptr++ << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x7f) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 47;
  values[4] |= static_cast<uint64_t>(*ptr++) << 39;
  values[4] |= static_cast<uint64_t>(*ptr++) << 31;
  values[4] |= *ptr++ << 23;
  values[4] |= *ptr++ << 15;
  values[4] |= *ptr++ << 7;
  values[4] |= *ptr >> 1;

  values[5] = static_cast<uint64_t>(*ptr++ & 1) << 50;
  values[5] |= static_cast<uint64_t>(*ptr++) << 42;
  values[5] |= static_cast<uint64_t>(*ptr++) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 45;
  values[6] |= static_cast<uint64_t>(*ptr++) << 37;
  values[6] |= static_cast<uint64_t>(*ptr++) << 29;
  values[6] |= *ptr++ << 21;
  values[6] |= *ptr++ << 13;
  values[6] |= *ptr++ << 5;
  values[6] |= *ptr >> 3;

  values[7] = static_cast<uint64_t>(*ptr++ & 7) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_52(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 44;
  values[0] |= static_cast<uint64_t>(*ptr++) << 36;
  values[0] |= static_cast<uint64_t>(*ptr++) << 28;
  values[0] |= *ptr++ << 20;
  values[0] |= *ptr++ << 12;
  values[0] |= *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = static_cast<uint64_t>(*ptr++ & 0xf) << 48;
  values[1] |= static_cast<uint64_t>(*ptr++) << 40;
  values[1] |= static_cast<uint64_t>(*ptr++) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;

  values[2] = static_cast<uint64_t>(*ptr++) << 44;
  values[2] |= static_cast<uint64_t>(*ptr++) << 36;
  values[2] |= static_cast<uint64_t>(*ptr++) << 28;
  values[2] |= *ptr++ << 20;
  values[2] |= *ptr++ << 12;
  values[2] |= *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = static_cast<uint64_t>(*ptr++ & 0xf) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 44;
  values[4] |= static_cast<uint64_t>(*ptr++) << 36;
  values[4] |= static_cast<uint64_t>(*ptr++) << 28;
  values[4] |= *ptr++ << 20;
  values[4] |= *ptr++ << 12;
  values[4] |= *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = static_cast<uint64_t>(*ptr++ & 0xf) << 48;
  values[5] |= static_cast<uint64_t>(*ptr++) << 40;
  values[5] |= static_cast<uint64_t>(*ptr++) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;

  values[6] = static_cast<uint64_t>(*ptr++) << 44;
  values[6] |= static_cast<uint64_t>(*ptr++) << 36;
  values[6] |= static_cast<uint64_t>(*ptr++) << 28;
  values[6] |= *ptr++ << 20;
  values[6] |= *ptr++ << 12;
  values[6] |= *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = static_cast<uint64_t>(*ptr++ & 0xf) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_53(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 45;
  values[0] |= static_cast<uint64_t>(*ptr++) << 37;
  values[0] |= static_cast<uint64_t>(*ptr++) << 29;
  values[0] |= *ptr++ << 21;
  values[0] |= *ptr++ << 13;
  values[0] |= *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = static_cast<uint64_t>(*ptr++ & 7) << 50;
  values[1] |= static_cast<uint64_t>(*ptr++) << 42;
  values[1] |= static_cast<uint64_t>(*ptr++) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 47;
  values[2] |= static_cast<uint64_t>(*ptr++) << 39;
  values[2] |= static_cast<uint64_t>(*ptr++) << 31;
  values[2] |= static_cast<uint64_t>(*ptr++) << 23;
  values[2] |= *ptr++ << 15;
  values[2] |= *ptr++ << 7;
  values[2] |= *ptr >> 1;

  values[3] = static_cast<uint64_t>(*ptr++ & 1) << 52;
  values[3] |= static_cast<uint64_t>(*ptr++) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 49;
  values[4] |= static_cast<uint64_t>(*ptr++) << 41;
  values[4] |= static_cast<uint64_t>(*ptr++) << 33;
  values[4] |= static_cast<uint64_t>(*ptr++) << 25;
  values[4] |= *ptr++ << 17;
  values[4] |= *ptr++ << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x7f) << 46;
  values[5] |= static_cast<uint64_t>(*ptr++) << 38;
  values[5] |= static_cast<uint64_t>(*ptr++) << 30;
  values[5] |= *ptr++ << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 51;
  values[6] |= static_cast<uint64_t>(*ptr++) << 43;
  values[6] |= static_cast<uint64_t>(*ptr++) << 35;
  values[6] |= static_cast<uint64_t>(*ptr++) << 27;
  values[6] |= *ptr++ << 19;
  values[6] |= *ptr++ << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x1f) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_54(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 46;
  values[0] |= static_cast<uint64_t>(*ptr++) << 38;
  values[0] |= static_cast<uint64_t>(*ptr++) << 30;
  values[0] |= *ptr++ << 22;
  values[0] |= *ptr++ << 14;
  values[0] |= *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = static_cast<uint64_t>(*ptr++ & 3) << 52;
  values[1] |= static_cast<uint64_t>(*ptr++) << 44;
  values[1] |= static_cast<uint64_t>(*ptr++) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 50;
  values[2] |= static_cast<uint64_t>(*ptr++) << 42;
  values[2] |= static_cast<uint64_t>(*ptr++) << 34;
  values[2] |= static_cast<uint64_t>(*ptr++) << 26;
  values[2] |= *ptr++ << 18;
  values[2] |= *ptr++ << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x3f) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 46;
  values[4] |= static_cast<uint64_t>(*ptr++) << 38;
  values[4] |= static_cast<uint64_t>(*ptr++) << 30;
  values[4] |= *ptr++ << 22;
  values[4] |= *ptr++ << 14;
  values[4] |= *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = static_cast<uint64_t>(*ptr++ & 3) << 52;
  values[5] |= static_cast<uint64_t>(*ptr++) << 44;
  values[5] |= static_cast<uint64_t>(*ptr++) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 50;
  values[6] |= static_cast<uint64_t>(*ptr++) << 42;
  values[6] |= static_cast<uint64_t>(*ptr++) << 34;
  values[6] |= static_cast<uint64_t>(*ptr++) << 26;
  values[6] |= *ptr++ << 18;
  values[6] |= *ptr++ << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x3f) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr++;
}

static inline void unpack_bits_55(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 47;
  values[0] |= static_cast<uint64_t>(*ptr++) << 39;
  values[0] |= static_cast<uint64_t>(*ptr++) << 31;
  values[0] |= *ptr++ << 23;
  values[0] |= *ptr++ << 15;
  values[0] |= *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = static_cast<uint64_t>(*ptr++ & 1) << 54;
  values[1] |= static_cast<uint64_t>(*ptr++) << 46;
  values[1] |= static_cast<uint64_t>(*ptr++) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 53;
  values[2] |= static_cast<uint64_t>(*ptr++) << 45;
  values[2] |= static_cast<uint64_t>(*ptr++) << 37;
  values[2] |= static_cast<uint64_t>(*ptr++) << 29;
  values[2] |= *ptr++ << 21;
  values[2] |= *ptr++ << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = static_cast<uint64_t>(*ptr++ & 7) << 52;
  values[3] |= static_cast<uint64_t>(*ptr++) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 51;
  values[4] |= static_cast<uint64_t>(*ptr++) << 43;
  values[4] |= static_cast<uint64_t>(*ptr++) << 35;
  values[4] |= static_cast<uint64_t>(*ptr++) << 27;
  values[4] |= *ptr++ << 19;
  values[4] |= *ptr++ << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x1f) << 50;
  values[5] |= static_cast<uint64_t>(*ptr++) << 42;
  values[5] |= static_cast<uint64_t>(*ptr++) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 49;
  values[6] |= static_cast<uint64_t>(*ptr++) << 41;
  values[6] |= static_cast<uint64_t>(*ptr++) << 33;
  values[6] |= static_cast<uint64_t>(*ptr++) << 25;
  values[6] |= *ptr++ << 17;
  values[6] |= *ptr++ << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x7f) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_56(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 48;
  values[0] |= static_cast<uint64_t>(*ptr++) << 40;
  values[0] |= static_cast<uint64_t>(*ptr++) << 32;
  values[0] |= static_cast<uint64_t>(*ptr++) << 24;
  values[0] |= *ptr++ << 16;
  values[0] |= *ptr++ << 8;
  values[0] |= *ptr++;
  values[1] = static_cast<uint64_t>(*ptr++) << 48;
  values[1] |= static_cast<uint64_t>(*ptr++) << 40;
  values[1] |= static_cast<uint64_t>(*ptr++) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;
  values[2] = static_cast<uint64_t>(*ptr++) << 48;
  values[2] |= static_cast<uint64_t>(*ptr++) << 40;
  values[2] |= static_cast<uint64_t>(*ptr++) << 32;
  values[2] |= static_cast<uint64_t>(*ptr++) << 24;
  values[2] |= *ptr++ << 16;
  values[2] |= *ptr++ << 8;
  values[2] |= *ptr++;
  values[3] = static_cast<uint64_t>(*ptr++) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;
  values[4] = static_cast<uint64_t>(*ptr++) << 48;
  values[4] |= static_cast<uint64_t>(*ptr++) << 40;
  values[4] |= static_cast<uint64_t>(*ptr++) << 32;
  values[4] |= static_cast<uint64_t>(*ptr++) << 24;
  values[4] |= *ptr++ << 16;
  values[4] |= *ptr++ << 8;
  values[4] |= *ptr++;
  values[5] = static_cast<uint64_t>(*ptr++) << 48;
  values[5] |= static_cast<uint64_t>(*ptr++) << 40;
  values[5] |= static_cast<uint64_t>(*ptr++) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;
  values[6] = static_cast<uint64_t>(*ptr++) << 48;
  values[6] |= static_cast<uint64_t>(*ptr++) << 40;
  values[6] |= static_cast<uint64_t>(*ptr++) << 32;
  values[6] |= static_cast<uint64_t>(*ptr++) << 24;
  values[6] |= *ptr++ << 16;
  values[6] |= *ptr++ << 8;
  values[6] |= *ptr++;
  values[7] = static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_57(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 49;
  values[0] |= static_cast<uint64_t>(*ptr++) << 41;
  values[0] |= static_cast<uint64_t>(*ptr++) << 33;
  values[0] |= static_cast<uint64_t>(*ptr++) << 25;
  values[0] |= *ptr++ << 17;
  values[0] |= *ptr++ << 9;
  values[0] |= *ptr++ << 1;
  values[0] |= *ptr >> 7;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x7f) << 50;
  values[1] |= static_cast<uint64_t>(*ptr++) << 42;
  values[1] |= static_cast<uint64_t>(*ptr++) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 51;
  values[2] |= static_cast<uint64_t>(*ptr++) << 43;
  values[2] |= static_cast<uint64_t>(*ptr++) << 35;
  values[2] |= static_cast<uint64_t>(*ptr++) << 27;
  values[2] |= *ptr++ << 19;
  values[2] |= *ptr++ << 11;
  values[2] |= *ptr++ << 3;
  values[2] |= *ptr >> 5;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x1f) << 52;
  values[3] |= static_cast<uint64_t>(*ptr++) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 53;
  values[4] |= static_cast<uint64_t>(*ptr++) << 45;
  values[4] |= static_cast<uint64_t>(*ptr++) << 37;
  values[4] |= static_cast<uint64_t>(*ptr++) << 29;
  values[4] |= *ptr++ << 21;
  values[4] |= *ptr++ << 13;
  values[4] |= *ptr++ << 5;
  values[4] |= *ptr >> 3;

  values[5] = static_cast<uint64_t>(*ptr++ & 7) << 54;
  values[5] |= static_cast<uint64_t>(*ptr++) << 46;
  values[5] |= static_cast<uint64_t>(*ptr++) << 38;
  values[5] |= static_cast<uint64_t>(*ptr++) << 30;
  values[5] |= *ptr++ << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 55;
  values[6] |= static_cast<uint64_t>(*ptr++) << 47;
  values[6] |= static_cast<uint64_t>(*ptr++) << 39;
  values[6] |= static_cast<uint64_t>(*ptr++) << 31;
  values[6] |= *ptr++ << 23;
  values[6] |= *ptr++ << 15;
  values[6] |= *ptr++ << 7;
  values[6] |= *ptr >> 1;

  values[7] = static_cast<uint64_t>(*ptr++ & 1) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_58(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 50;
  values[0] |= static_cast<uint64_t>(*ptr++) << 42;
  values[0] |= static_cast<uint64_t>(*ptr++) << 34;
  values[0] |= static_cast<uint64_t>(*ptr++) << 26;
  values[0] |= *ptr++ << 18;
  values[0] |= *ptr++ << 10;
  values[0] |= *ptr++ << 2;
  values[0] |= *ptr >> 6;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x3f) << 52;
  values[1] |= static_cast<uint64_t>(*ptr++) << 44;
  values[1] |= static_cast<uint64_t>(*ptr++) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 54;
  values[2] |= static_cast<uint64_t>(*ptr++) << 46;
  values[2] |= static_cast<uint64_t>(*ptr++) << 38;
  values[2] |= static_cast<uint64_t>(*ptr++) << 30;
  values[2] |= *ptr++ << 22;
  values[2] |= *ptr++ << 14;
  values[2] |= *ptr++ << 6;
  values[2] |= *ptr >> 2;

  values[3] = static_cast<uint64_t>(*ptr++ & 3) << 56;
  values[3] |= static_cast<uint64_t>(*ptr++) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 50;
  values[4] |= static_cast<uint64_t>(*ptr++) << 42;
  values[4] |= static_cast<uint64_t>(*ptr++) << 34;
  values[4] |= static_cast<uint64_t>(*ptr++) << 26;
  values[4] |= *ptr++ << 18;
  values[4] |= *ptr++ << 10;
  values[4] |= *ptr++ << 2;
  values[4] |= *ptr >> 6;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x3f) << 52;
  values[5] |= static_cast<uint64_t>(*ptr++) << 44;
  values[5] |= static_cast<uint64_t>(*ptr++) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 54;
  values[6] |= static_cast<uint64_t>(*ptr++) << 46;
  values[6] |= static_cast<uint64_t>(*ptr++) << 38;
  values[6] |= static_cast<uint64_t>(*ptr++) << 30;
  values[6] |= *ptr++ << 22;
  values[6] |= *ptr++ << 14;
  values[6] |= *ptr++ << 6;
  values[6] |= *ptr >> 2;

  values[7] = static_cast<uint64_t>(*ptr++ & 3) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr++;
}

static inline void unpack_bits_59(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 51;
  values[0] |= static_cast<uint64_t>(*ptr++) << 43;
  values[0] |= static_cast<uint64_t>(*ptr++) << 35;
  values[0] |= static_cast<uint64_t>(*ptr++) << 27;
  values[0] |= *ptr++ << 19;
  values[0] |= *ptr++ << 11;
  values[0] |= *ptr++ << 3;
  values[0] |= *ptr >> 5;

  values[1] = static_cast<uint64_t>(*ptr++ & 0x1f) << 54;
  values[1] |= static_cast<uint64_t>(*ptr++) << 46;
  values[1] |= static_cast<uint64_t>(*ptr++) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 57;
  values[2] |= static_cast<uint64_t>(*ptr++) << 49;
  values[2] |= static_cast<uint64_t>(*ptr++) << 41;
  values[2] |= static_cast<uint64_t>(*ptr++) << 33;
  values[2] |= static_cast<uint64_t>(*ptr++) << 25;
  values[2] |= *ptr++ << 17;
  values[2] |= *ptr++ << 9;
  values[2] |= *ptr++ << 1;
  values[2] |= *ptr >> 7;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x7f) << 52;
  values[3] |= static_cast<uint64_t>(*ptr++) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 55;
  values[4] |= static_cast<uint64_t>(*ptr++) << 47;
  values[4] |= static_cast<uint64_t>(*ptr++) << 39;
  values[4] |= static_cast<uint64_t>(*ptr++) << 31;
  values[4] |= *ptr++ << 23;
  values[4] |= *ptr++ << 15;
  values[4] |= *ptr++ << 7;
  values[4] |= *ptr >> 1;

  values[5] = static_cast<uint64_t>(*ptr++ & 1) << 58;
  values[5] |= static_cast<uint64_t>(*ptr++) << 50;
  values[5] |= static_cast<uint64_t>(*ptr++) << 42;
  values[5] |= static_cast<uint64_t>(*ptr++) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 53;
  values[6] |= static_cast<uint64_t>(*ptr++) << 45;
  values[6] |= static_cast<uint64_t>(*ptr++) << 37;
  values[6] |= static_cast<uint64_t>(*ptr++) << 29;
  values[6] |= *ptr++ << 21;
  values[6] |= *ptr++ << 13;
  values[6] |= *ptr++ << 5;
  values[6] |= *ptr >> 3;

  values[7] = static_cast<uint64_t>(*ptr++ & 7) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_60(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 52;
  values[0] |= static_cast<uint64_t>(*ptr++) << 44;
  values[0] |= static_cast<uint64_t>(*ptr++) << 36;
  values[0] |= static_cast<uint64_t>(*ptr++) << 28;
  values[0] |= *ptr++ << 20;
  values[0] |= *ptr++ << 12;
  values[0] |= *ptr++ << 4;
  values[0] |= *ptr >> 4;

  values[1] = static_cast<uint64_t>(*ptr++ & 0xf) << 56;
  values[1] |= static_cast<uint64_t>(*ptr++) << 48;
  values[1] |= static_cast<uint64_t>(*ptr++) << 40;
  values[1] |= static_cast<uint64_t>(*ptr++) << 32;
  values[1] |= static_cast<uint64_t>(*ptr++) << 24;
  values[1] |= *ptr++ << 16;
  values[1] |= *ptr++ << 8;
  values[1] |= *ptr++;

  values[2] = static_cast<uint64_t>(*ptr++) << 52;
  values[2] |= static_cast<uint64_t>(*ptr++) << 44;
  values[2] |= static_cast<uint64_t>(*ptr++) << 36;
  values[2] |= static_cast<uint64_t>(*ptr++) << 28;
  values[2] |= *ptr++ << 20;
  values[2] |= *ptr++ << 12;
  values[2] |= *ptr++ << 4;
  values[2] |= *ptr >> 4;

  values[3] = static_cast<uint64_t>(*ptr++ & 0xf) << 56;
  values[3] |= static_cast<uint64_t>(*ptr++) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 52;
  values[4] |= static_cast<uint64_t>(*ptr++) << 44;
  values[4] |= static_cast<uint64_t>(*ptr++) << 36;
  values[4] |= static_cast<uint64_t>(*ptr++) << 28;
  values[4] |= *ptr++ << 20;
  values[4] |= *ptr++ << 12;
  values[4] |= *ptr++ << 4;
  values[4] |= *ptr >> 4;

  values[5] = static_cast<uint64_t>(*ptr++ & 0xf) << 56;
  values[5] |= static_cast<uint64_t>(*ptr++) << 48;
  values[5] |= static_cast<uint64_t>(*ptr++) << 40;
  values[5] |= static_cast<uint64_t>(*ptr++) << 32;
  values[5] |= static_cast<uint64_t>(*ptr++) << 24;
  values[5] |= *ptr++ << 16;
  values[5] |= *ptr++ << 8;
  values[5] |= *ptr++;

  values[6] = static_cast<uint64_t>(*ptr++) << 52;
  values[6] |= static_cast<uint64_t>(*ptr++) << 44;
  values[6] |= static_cast<uint64_t>(*ptr++) << 36;
  values[6] |= static_cast<uint64_t>(*ptr++) << 28;
  values[6] |= *ptr++ << 20;
  values[6] |= *ptr++ << 12;
  values[6] |= *ptr++ << 4;
  values[6] |= *ptr >> 4;

  values[7] = static_cast<uint64_t>(*ptr++ & 0xf) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_61(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 53;
  values[0] |= static_cast<uint64_t>(*ptr++) << 45;
  values[0] |= static_cast<uint64_t>(*ptr++) << 37;
  values[0] |= static_cast<uint64_t>(*ptr++) << 29;
  values[0] |= *ptr++ << 21;
  values[0] |= *ptr++ << 13;
  values[0] |= *ptr++ << 5;
  values[0] |= *ptr >> 3;

  values[1] = static_cast<uint64_t>(*ptr++ & 7) << 58;
  values[1] |= static_cast<uint64_t>(*ptr++) << 50;
  values[1] |= static_cast<uint64_t>(*ptr++) << 42;
  values[1] |= static_cast<uint64_t>(*ptr++) << 34;
  values[1] |= static_cast<uint64_t>(*ptr++) << 26;
  values[1] |= *ptr++ << 18;
  values[1] |= *ptr++ << 10;
  values[1] |= *ptr++ << 2;
  values[1] |= *ptr >> 6;

  values[2] = static_cast<uint64_t>(*ptr++ & 0x3f) << 55;
  values[2] |= static_cast<uint64_t>(*ptr++) << 47;
  values[2] |= static_cast<uint64_t>(*ptr++) << 39;
  values[2] |= static_cast<uint64_t>(*ptr++) << 31;
  values[2] |= *ptr++ << 23;
  values[2] |= *ptr++ << 15;
  values[2] |= *ptr++ << 7;
  values[2] |= *ptr >> 1;

  values[3] = static_cast<uint64_t>(*ptr++ & 1) << 60;
  values[3] |= static_cast<uint64_t>(*ptr++) << 52;
  values[3] |= static_cast<uint64_t>(*ptr++) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 57;
  values[4] |= static_cast<uint64_t>(*ptr++) << 49;
  values[4] |= static_cast<uint64_t>(*ptr++) << 41;
  values[4] |= static_cast<uint64_t>(*ptr++) << 33;
  values[4] |= static_cast<uint64_t>(*ptr++) << 25;
  values[4] |= *ptr++ << 17;
  values[4] |= *ptr++ << 9;
  values[4] |= *ptr++ << 1;
  values[4] |= *ptr >> 7;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x7f) << 54;
  values[5] |= static_cast<uint64_t>(*ptr++) << 46;
  values[5] |= static_cast<uint64_t>(*ptr++) << 38;
  values[5] |= static_cast<uint64_t>(*ptr++) << 30;
  values[5] |= *ptr++ << 22;
  values[5] |= *ptr++ << 14;
  values[5] |= *ptr++ << 6;
  values[5] |= *ptr >> 2;

  values[6] = static_cast<uint64_t>(*ptr++ & 3) << 59;
  values[6] |= static_cast<uint64_t>(*ptr++) << 51;
  values[6] |= static_cast<uint64_t>(*ptr++) << 43;
  values[6] |= static_cast<uint64_t>(*ptr++) << 35;
  values[6] |= static_cast<uint64_t>(*ptr++) << 27;
  values[6] |= *ptr++ << 19;
  values[6] |= *ptr++ << 11;
  values[6] |= *ptr++ << 3;
  values[6] |= *ptr >> 5;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x1f) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_62(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 54;
  values[0] |= static_cast<uint64_t>(*ptr++) << 46;
  values[0] |= static_cast<uint64_t>(*ptr++) << 38;
  values[0] |= static_cast<uint64_t>(*ptr++) << 30;
  values[0] |= *ptr++ << 22;
  values[0] |= *ptr++ << 14;
  values[0] |= *ptr++ << 6;
  values[0] |= *ptr >> 2;

  values[1] = static_cast<uint64_t>(*ptr++ & 3) << 60;
  values[1] |= static_cast<uint64_t>(*ptr++) << 52;
  values[1] |= static_cast<uint64_t>(*ptr++) << 44;
  values[1] |= static_cast<uint64_t>(*ptr++) << 36;
  values[1] |= static_cast<uint64_t>(*ptr++) << 28;
  values[1] |= *ptr++ << 20;
  values[1] |= *ptr++ << 12;
  values[1] |= *ptr++ << 4;
  values[1] |= *ptr >> 4;

  values[2] = static_cast<uint64_t>(*ptr++ & 0xf) << 58;
  values[2] |= static_cast<uint64_t>(*ptr++) << 50;
  values[2] |= static_cast<uint64_t>(*ptr++) << 42;
  values[2] |= static_cast<uint64_t>(*ptr++) << 34;
  values[2] |= static_cast<uint64_t>(*ptr++) << 26;
  values[2] |= *ptr++ << 18;
  values[2] |= *ptr++ << 10;
  values[2] |= *ptr++ << 2;
  values[2] |= *ptr >> 6;

  values[3] = static_cast<uint64_t>(*ptr++ & 0x3f) << 56;
  values[3] |= static_cast<uint64_t>(*ptr++) << 48;
  values[3] |= static_cast<uint64_t>(*ptr++) << 40;
  values[3] |= static_cast<uint64_t>(*ptr++) << 32;
  values[3] |= static_cast<uint64_t>(*ptr++) << 24;
  values[3] |= *ptr++ << 16;
  values[3] |= *ptr++ << 8;
  values[3] |= *ptr++;

  values[4] = static_cast<uint64_t>(*ptr++) << 54;
  values[4] |= static_cast<uint64_t>(*ptr++) << 46;
  values[4] |= static_cast<uint64_t>(*ptr++) << 38;
  values[4] |= static_cast<uint64_t>(*ptr++) << 30;
  values[4] |= *ptr++ << 22;
  values[4] |= *ptr++ << 14;
  values[4] |= *ptr++ << 6;
  values[4] |= *ptr >> 2;

  values[5] = static_cast<uint64_t>(*ptr++ & 3) << 60;
  values[5] |= static_cast<uint64_t>(*ptr++) << 52;
  values[5] |= static_cast<uint64_t>(*ptr++) << 44;
  values[5] |= static_cast<uint64_t>(*ptr++) << 36;
  values[5] |= static_cast<uint64_t>(*ptr++) << 28;
  values[5] |= *ptr++ << 20;
  values[5] |= *ptr++ << 12;
  values[5] |= *ptr++ << 4;
  values[5] |= *ptr >> 4;

  values[6] = static_cast<uint64_t>(*ptr++ & 0xf) << 58;
  values[6] |= static_cast<uint64_t>(*ptr++) << 50;
  values[6] |= static_cast<uint64_t>(*ptr++) << 42;
  values[6] |= static_cast<uint64_t>(*ptr++) << 34;
  values[6] |= static_cast<uint64_t>(*ptr++) << 26;
  values[6] |= *ptr++ << 18;
  values[6] |= *ptr++ << 10;
  values[6] |= *ptr++ << 2;
  values[6] |= *ptr >> 6;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x3f) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void unpack_bits_63(uint64_t* values, const uint8_t* ptr) {
  values[0] = static_cast<uint64_t>(*ptr++) << 55;
  values[0] |= static_cast<uint64_t>(*ptr++) << 47;
  values[0] |= static_cast<uint64_t>(*ptr++) << 39;
  values[0] |= static_cast<uint64_t>(*ptr++) << 31;
  values[0] |= *ptr++ << 23;
  values[0] |= *ptr++ << 15;
  values[0] |= *ptr++ << 7;
  values[0] |= *ptr >> 1;

  values[1] = static_cast<uint64_t>(*ptr++ & 1) << 62;
  values[1] |= static_cast<uint64_t>(*ptr++) << 54;
  values[1] |= static_cast<uint64_t>(*ptr++) << 46;
  values[1] |= static_cast<uint64_t>(*ptr++) << 38;
  values[1] |= static_cast<uint64_t>(*ptr++) << 30;
  values[1] |= *ptr++ << 22;
  values[1] |= *ptr++ << 14;
  values[1] |= *ptr++ << 6;
  values[1] |= *ptr >> 2;

  values[2] = static_cast<uint64_t>(*ptr++ & 3) << 61;
  values[2] |= static_cast<uint64_t>(*ptr++) << 53;
  values[2] |= static_cast<uint64_t>(*ptr++) << 45;
  values[2] |= static_cast<uint64_t>(*ptr++) << 37;
  values[2] |= static_cast<uint64_t>(*ptr++) << 29;
  values[2] |= *ptr++ << 21;
  values[2] |= *ptr++ << 13;
  values[2] |= *ptr++ << 5;
  values[2] |= *ptr >> 3;

  values[3] = static_cast<uint64_t>(*ptr++ & 7) << 60;
  values[3] |= static_cast<uint64_t>(*ptr++) << 52;
  values[3] |= static_cast<uint64_t>(*ptr++) << 44;
  values[3] |= static_cast<uint64_t>(*ptr++) << 36;
  values[3] |= static_cast<uint64_t>(*ptr++) << 28;
  values[3] |= *ptr++ << 20;
  values[3] |= *ptr++ << 12;
  values[3] |= *ptr++ << 4;
  values[3] |= *ptr >> 4;

  values[4] = static_cast<uint64_t>(*ptr++ & 0xf) << 59;
  values[4] |= static_cast<uint64_t>(*ptr++) << 51;
  values[4] |= static_cast<uint64_t>(*ptr++) << 43;
  values[4] |= static_cast<uint64_t>(*ptr++) << 35;
  values[4] |= static_cast<uint64_t>(*ptr++) << 27;
  values[4] |= *ptr++ << 19;
  values[4] |= *ptr++ << 11;
  values[4] |= *ptr++ << 3;
  values[4] |= *ptr >> 5;

  values[5] = static_cast<uint64_t>(*ptr++ & 0x1f) << 58;
  values[5] |= static_cast<uint64_t>(*ptr++) << 50;
  values[5] |= static_cast<uint64_t>(*ptr++) << 42;
  values[5] |= static_cast<uint64_t>(*ptr++) << 34;
  values[5] |= static_cast<uint64_t>(*ptr++) << 26;
  values[5] |= *ptr++ << 18;
  values[5] |= *ptr++ << 10;
  values[5] |= *ptr++ << 2;
  values[5] |= *ptr >> 6;

  values[6] = static_cast<uint64_t>(*ptr++ & 0x3f) << 57;
  values[6] |= static_cast<uint64_t>(*ptr++) << 49;
  values[6] |= static_cast<uint64_t>(*ptr++) << 41;
  values[6] |= static_cast<uint64_t>(*ptr++) << 33;
  values[6] |= static_cast<uint64_t>(*ptr++) << 25;
  values[6] |= *ptr++ << 17;
  values[6] |= *ptr++ << 9;
  values[6] |= *ptr++ << 1;
  values[6] |= *ptr >> 7;

  values[7] = static_cast<uint64_t>(*ptr++ & 0x7f) << 56;
  values[7] |= static_cast<uint64_t>(*ptr++) << 48;
  values[7] |= static_cast<uint64_t>(*ptr++) << 40;
  values[7] |= static_cast<uint64_t>(*ptr++) << 32;
  values[7] |= static_cast<uint64_t>(*ptr++) << 24;
  values[7] |= *ptr++ << 16;
  values[7] |= *ptr++ << 8;
  values[7] |= *ptr;
}

static inline void pack_bits_block8(const uint64_t* values, uint8_t* ptr, uint8_t bits) {
  switch (bits) {
    case 1: pack_bits_1(values, ptr); break;
    case 2: pack_bits_2(values, ptr); break;
    case 3: pack_bits_3(values, ptr); break;
    case 4: pack_bits_4(values, ptr); break;
    case 5: pack_bits_5(values, ptr); break;
    case 6: pack_bits_6(values, ptr); break;
    case 7: pack_bits_7(values, ptr); break;
    case 8: pack_bits_8(values, ptr); break;
    case 9: pack_bits_9(values, ptr); break;
    case 10: pack_bits_10(values, ptr); break;
    case 11: pack_bits_11(values, ptr); break;
    case 12: pack_bits_12(values, ptr); break;
    case 13: pack_bits_13(values, ptr); break;
    case 14: pack_bits_14(values, ptr); break;
    case 15: pack_bits_15(values, ptr); break;
    case 16: pack_bits_16(values, ptr); break;
    case 17: pack_bits_17(values, ptr); break;
    case 18: pack_bits_18(values, ptr); break;
    case 19: pack_bits_19(values, ptr); break;
    case 20: pack_bits_20(values, ptr); break;
    case 21: pack_bits_21(values, ptr); break;
    case 22: pack_bits_22(values, ptr); break;
    case 23: pack_bits_23(values, ptr); break;
    case 24: pack_bits_24(values, ptr); break;
    case 25: pack_bits_25(values, ptr); break;
    case 26: pack_bits_26(values, ptr); break;
    case 27: pack_bits_27(values, ptr); break;
    case 28: pack_bits_28(values, ptr); break;
    case 29: pack_bits_29(values, ptr); break;
    case 30: pack_bits_30(values, ptr); break;
    case 31: pack_bits_31(values, ptr); break;
    case 32: pack_bits_32(values, ptr); break;
    case 33: pack_bits_33(values, ptr); break;
    case 34: pack_bits_34(values, ptr); break;
    case 35: pack_bits_35(values, ptr); break;
    case 36: pack_bits_36(values, ptr); break;
    case 37: pack_bits_37(values, ptr); break;
    case 38: pack_bits_38(values, ptr); break;
    case 39: pack_bits_39(values, ptr); break;
    case 40: pack_bits_40(values, ptr); break;
    case 41: pack_bits_41(values, ptr); break;
    case 42: pack_bits_42(values, ptr); break;
    case 43: pack_bits_43(values, ptr); break;
    case 44: pack_bits_44(values, ptr); break;
    case 45: pack_bits_45(values, ptr); break;
    case 46: pack_bits_46(values, ptr); break;
    case 47: pack_bits_47(values, ptr); break;
    case 48: pack_bits_48(values, ptr); break;
    case 49: pack_bits_49(values, ptr); break;
    case 50: pack_bits_50(values, ptr); break;
    case 51: pack_bits_51(values, ptr); break;
    case 52: pack_bits_52(values, ptr); break;
    case 53: pack_bits_53(values, ptr); break;
    case 54: pack_bits_54(values, ptr); break;
    case 55: pack_bits_55(values, ptr); break;
    case 56: pack_bits_56(values, ptr); break;
    case 57: pack_bits_57(values, ptr); break;
    case 58: pack_bits_58(values, ptr); break;
    case 59: pack_bits_59(values, ptr); break;
    case 60: pack_bits_60(values, ptr); break;
    case 61: pack_bits_61(values, ptr); break;
    case 62: pack_bits_62(values, ptr); break;
    case 63: pack_bits_63(values, ptr); break;
    default: throw std::logic_error("wrong number of bits " + std::to_string(bits));
  }
}

static inline void unpack_bits_block8(uint64_t* values, const uint8_t* ptr, uint8_t bits) {
  switch (bits) {
    case 1: unpack_bits_1(values, ptr); break;
    case 2: unpack_bits_2(values, ptr); break;
    case 3: unpack_bits_3(values, ptr); break;
    case 4: unpack_bits_4(values, ptr); break;
    case 5: unpack_bits_5(values, ptr); break;
    case 6: unpack_bits_6(values, ptr); break;
    case 7: unpack_bits_7(values, ptr); break;
    case 8: unpack_bits_8(values, ptr); break;
    case 9: unpack_bits_9(values, ptr); break;
    case 10: unpack_bits_10(values, ptr); break;
    case 11: unpack_bits_11(values, ptr); break;
    case 12: unpack_bits_12(values, ptr); break;
    case 13: unpack_bits_13(values, ptr); break;
    case 14: unpack_bits_14(values, ptr); break;
    case 15: unpack_bits_15(values, ptr); break;
    case 16: unpack_bits_16(values, ptr); break;
    case 17: unpack_bits_17(values, ptr); break;
    case 18: unpack_bits_18(values, ptr); break;
    case 19: unpack_bits_19(values, ptr); break;
    case 20: unpack_bits_20(values, ptr); break;
    case 21: unpack_bits_21(values, ptr); break;
    case 22: unpack_bits_22(values, ptr); break;
    case 23: unpack_bits_23(values, ptr); break;
    case 24: unpack_bits_24(values, ptr); break;
    case 25: unpack_bits_25(values, ptr); break;
    case 26: unpack_bits_26(values, ptr); break;
    case 27: unpack_bits_27(values, ptr); break;
    case 28: unpack_bits_28(values, ptr); break;
    case 29: unpack_bits_29(values, ptr); break;
    case 30: unpack_bits_30(values, ptr); break;
    case 31: unpack_bits_31(values, ptr); break;
    case 32: unpack_bits_32(values, ptr); break;
    case 33: unpack_bits_33(values, ptr); break;
    case 34: unpack_bits_34(values, ptr); break;
    case 35: unpack_bits_35(values, ptr); break;
    case 36: unpack_bits_36(values, ptr); break;
    case 37: unpack_bits_37(values, ptr); break;
    case 38: unpack_bits_38(values, ptr); break;
    case 39: unpack_bits_39(values, ptr); break;
    case 40: unpack_bits_40(values, ptr); break;
    case 41: unpack_bits_41(values, ptr); break;
    case 42: unpack_bits_42(values, ptr); break;
    case 43: unpack_bits_43(values, ptr); break;
    case 44: unpack_bits_44(values, ptr); break;
    case 45: unpack_bits_45(values, ptr); break;
    case 46: unpack_bits_46(values, ptr); break;
    case 47: unpack_bits_47(values, ptr); break;
    case 48: unpack_bits_48(values, ptr); break;
    case 49: unpack_bits_49(values, ptr); break;
    case 50: unpack_bits_50(values, ptr); break;
    case 51: unpack_bits_51(values, ptr); break;
    case 52: unpack_bits_52(values, ptr); break;
    case 53: unpack_bits_53(values, ptr); break;
    case 54: unpack_bits_54(values, ptr); break;
    case 55: unpack_bits_55(values, ptr); break;
    case 56: unpack_bits_56(values, ptr); break;
    case 57: unpack_bits_57(values, ptr); break;
    case 58: unpack_bits_58(values, ptr); break;
    case 59: unpack_bits_59(values, ptr); break;
    case 60: unpack_bits_60(values, ptr); break;
    case 61: unpack_bits_61(values, ptr); break;
    case 62: unpack_bits_62(values, ptr); break;
    case 63: unpack_bits_63(values, ptr); break;
    default: throw std::logic_error("wrong number of bits " + std::to_string(bits));
  }
}

} // namespace

#endif // BIT_PACKING_HPP_
