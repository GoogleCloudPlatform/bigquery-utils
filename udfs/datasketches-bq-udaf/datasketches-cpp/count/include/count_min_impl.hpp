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

#ifndef COUNT_MIN_IMPL_HPP_
#define COUNT_MIN_IMPL_HPP_

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>

#include "MurmurHash3.h"
#include "count_min.hpp"
#include "memory_operations.hpp"

namespace datasketches {

template<typename W, typename A>
count_min_sketch<W,A>::count_min_sketch(uint8_t num_hashes, uint32_t num_buckets, uint64_t seed, const A& allocator):
_allocator(allocator),
_num_hashes(num_hashes),
_num_buckets(num_buckets),
_sketch_array((num_hashes*num_buckets < 1<<30) ? num_hashes*num_buckets : 0, 0, _allocator),
_seed(seed),
_total_weight(0) {
  if (num_buckets < 3) throw std::invalid_argument("Using fewer than 3 buckets incurs relative error greater than 1.");

  // This check is to ensure later compatibility with a Java implementation whose maximum size can only
  // be 2^31-1.  We check only against 2^30 for simplicity.
  if (num_buckets * num_hashes >= 1 << 30) {
    throw std::invalid_argument("These parameters generate a sketch that exceeds 2^30 elements."
                                "Try reducing either the number of buckets or the number of hash functions.");
  }

  std::default_random_engine rng(_seed);
  std::uniform_int_distribution<uint64_t> extra_hash_seeds(0, std::numeric_limits<uint64_t>::max());
  hash_seeds.reserve(num_hashes);

  for (uint64_t i=0; i < num_hashes; ++i) {
    hash_seeds.push_back(extra_hash_seeds(rng) + _seed); // Adds the global seed to all hash functions.
  }
}

template<typename W, typename A>
uint8_t count_min_sketch<W,A>::get_num_hashes() const {
  return _num_hashes;
}

template<typename W, typename A>
uint32_t count_min_sketch<W,A>::get_num_buckets() const {
   return _num_buckets;
}

template<typename W, typename A>
uint64_t count_min_sketch<W,A>::get_seed() const {
  return _seed;
}

template<typename W, typename A>
double count_min_sketch<W,A>::get_relative_error() const {
  return exp(1.0) / double(_num_buckets);
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_total_weight() const {
  return _total_weight;
}

template<typename W, typename A>
uint32_t count_min_sketch<W,A>::suggest_num_buckets(double relative_error) {
  /*
   * Function to help users select a number of buckets for a given error.
   * TODO: Change this when we use only power of 2 buckets.
   */
  if (relative_error < 0.) {
    throw std::invalid_argument("Relative error must be at least 0.");
  }
  return static_cast<uint32_t>(ceil(exp(1.0) / relative_error));
}

template<typename W, typename A>
uint8_t count_min_sketch<W,A>::suggest_num_hashes(double confidence) {
  /*
   * Function to help users select a number of hashes for a given confidence
   * e.g. confidence = 1 - failure probability
   * failure probability == delta in the literature.
   */
  if (confidence < 0. || confidence > 1.0) {
    throw std::invalid_argument("Confidence must be between 0 and 1.0 (inclusive).");
  }
  return std::min<uint8_t>(ceil(log(1.0 / (1.0 - confidence))), UINT8_MAX);
}

template<typename W, typename A>
std::vector<uint64_t> count_min_sketch<W,A>::get_hashes(const void* item, size_t size) const {
  /*
   * Returns the hash locations for the input item using the original hashing
   * scheme from [1].
   * Generate _num_hashes separate hashes from calls to murmurmhash.
   * This could be optimized by keeping both of the 64bit parts of the hash
   * function, rather than generating a new one for every level.
   *
   *
   * Postscript.
   * Note that a tradeoff can be achieved over the update time and space
   * complexity of the sketch by using a combinatorial hashing scheme from
   * https://github.com/Claudenw/BloomFilter/wiki/Bloom-Filters----An-overview
   * https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
   */
  uint64_t bucket_index;
  std::vector<uint64_t> sketch_update_locations;
  sketch_update_locations.reserve(_num_hashes);

  uint64_t hash_seed_index = 0;
  for (const auto &it: hash_seeds) {
    HashState hashes;
    MurmurHash3_x64_128(item, size, it, hashes); // ? BEWARE OVERFLOW.
    uint64_t hash = hashes.h1;
    bucket_index = hash % _num_buckets;
    sketch_update_locations.push_back((hash_seed_index * _num_buckets) + bucket_index);
    hash_seed_index += 1;
  }
  return sketch_update_locations;
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_estimate(uint64_t item) const {return get_estimate(&item, sizeof(item));}

template<typename W, typename A>
W count_min_sketch<W,A>::get_estimate(int64_t item) const {return get_estimate(&item, sizeof(item));}

template<typename W, typename A>
W count_min_sketch<W,A>::get_estimate(const std::string& item) const {
  if (item.empty()) return 0; // Empty strings are not inserted into the sketch.
  return get_estimate(item.c_str(), item.length());
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_estimate(const void* item, size_t size) const {
  /*
   * Returns the estimated frequency of the item
   */
  std::vector<uint64_t> hash_locations = get_hashes(item, size);
  std::vector<W> estimates;
  for (const auto h: hash_locations) {
    estimates.push_back(_sketch_array[h]);
  }
  return *std::min_element(estimates.begin(), estimates.end());
}

template<typename W, typename A>
void count_min_sketch<W,A>::update(uint64_t item, W weight) {
  update(&item, sizeof(item), weight);
}

template<typename W, typename A>
void count_min_sketch<W,A>::update(int64_t item, W weight) {
  update(&item, sizeof(item), weight);
}

template<typename W, typename A>
void count_min_sketch<W,A>::update(const std::string& item, W weight) {
  if (item.empty()) return;
  update(item.c_str(), item.length(), weight);
}

template<typename W, typename A>
void count_min_sketch<W,A>::update(const void* item, size_t size, W weight) {
  /*
   * Gets the item's hash locations and then increments the sketch in those
   * locations by the weight.
   */
  _total_weight += weight >= 0 ? weight : -weight;
  std::vector<uint64_t> hash_locations = get_hashes(item, size);
  for (const auto h: hash_locations) {
    _sketch_array[h] += weight;
  }
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_upper_bound(uint64_t item) const {return get_upper_bound(&item, sizeof(item));}

template<typename W, typename A>
W count_min_sketch<W,A>::get_upper_bound(int64_t item) const {return get_upper_bound(&item, sizeof(item));}

template<typename W, typename A>
W count_min_sketch<W,A>::get_upper_bound(const std::string& item) const {
  if (item.empty()) return 0; // Empty strings are not inserted into the sketch.
  return get_upper_bound(item.c_str(), item.length());
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_upper_bound(const void* item, size_t size) const {
  return static_cast<W>(get_estimate(item, size) + get_relative_error() * get_total_weight());
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_lower_bound(uint64_t item) const {return get_lower_bound(&item, sizeof(item));}

template<typename W, typename A>
W count_min_sketch<W,A>::get_lower_bound(int64_t item) const {return get_lower_bound(&item, sizeof(item));}

template<typename W, typename A>
W count_min_sketch<W,A>::get_lower_bound(const std::string& item) const {
  if (item.empty()) return 0; // Empty strings are not inserted into the sketch.
  return get_lower_bound(item.c_str(), item.length());
}

template<typename W, typename A>
W count_min_sketch<W,A>::get_lower_bound(const void* item, size_t size) const {
  return get_estimate(item, size);
}

template<typename W, typename A>
void count_min_sketch<W,A>::merge(const count_min_sketch &other_sketch) {
  /*
  * Merges this sketch into other_sketch sketch by elementwise summing of buckets
  */
  if (this == &other_sketch) {
    throw std::invalid_argument( "Cannot merge a sketch with itself." );
  }

  bool acceptable_config =
    (get_num_hashes() == other_sketch.get_num_hashes())   &&
    (get_num_buckets() == other_sketch.get_num_buckets()) &&
    (get_seed() == other_sketch.get_seed());
  if (!acceptable_config) {
    throw std::invalid_argument( "Incompatible sketch configuration." );
  }

  // Merge step - iterate over the other vector and add the weights to this sketch
  auto it = _sketch_array.begin(); // This is a std::vector iterator.
  auto other_it = other_sketch.begin(); //This is a const iterator over the other sketch.
  while (it != _sketch_array.end()) {
    *it += *other_it;
    ++it;
    ++other_it;
  }
  _total_weight += other_sketch.get_total_weight();
}

// Iterators
template<typename W, typename A>
typename count_min_sketch<W,A>::const_iterator count_min_sketch<W,A>::begin() const {
  return _sketch_array.begin();
}

template<typename W, typename A>
typename count_min_sketch<W,A>::const_iterator count_min_sketch<W,A>::end() const {
return _sketch_array.end();
}

template<typename W, typename A>
void count_min_sketch<W,A>::serialize(std::ostream& os) const {
  // Long 0
  //const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_SHORT : PREAMBLE_LONGS_FULL;
  const uint8_t preamble_longs = PREAMBLE_LONGS_SHORT;
  const uint8_t ser_ver = SERIAL_VERSION_1;
  const uint8_t family_id = FAMILY_ID;
  const uint8_t flags_byte = (is_empty() ? 1 << flags::IS_EMPTY : 0);
  const uint32_t unused32 = NULL_32;
  write(os, preamble_longs);
  write(os, ser_ver);
  write(os, family_id);
  write(os, flags_byte);
  write(os, unused32);

  // Long 1
  const uint32_t nbuckets = _num_buckets;
  const uint8_t nhashes = _num_hashes;
  const uint16_t seed_hash(compute_seed_hash(_seed));
  const uint8_t unused8 =  NULL_8;
  write(os, nbuckets);
  write(os, nhashes);
  write(os, seed_hash);
  write(os, unused8);
  if (is_empty()) return; // sketch is empty, no need to write further bytes.

  // Long 2
  write(os, _total_weight);

  // Long 3 onwards: remaining bytes are consumed by writing the weight and the array values.
  auto it = _sketch_array.begin();
  while (it != _sketch_array.end()) {
    write(os, *it);
    ++it;
  }
}

template<typename W, typename A>
auto count_min_sketch<W,A>::deserialize(std::istream& is, uint64_t seed, const A& allocator) -> count_min_sketch {

  // First 8 bytes are 4 bytes of preamble and 4 unused bytes.
  const auto preamble_longs = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto family_id = read<uint8_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  read<uint32_t>(is); // 4 unused bytes

  check_header_validity(preamble_longs, serial_version, family_id, flags_byte);

  // Sketch parameters
  const auto nbuckets = read<uint32_t>(is);
  const auto nhashes = read<uint8_t>(is);
  const auto seed_hash = read<uint16_t>(is);
  read<uint8_t>(is); // 1 unused byte

  if (seed_hash != compute_seed_hash(seed)) {
    throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash) + ", "
                                + std::to_string(compute_seed_hash(seed)));
  }
  count_min_sketch c(nhashes, nbuckets, seed, allocator);
  const bool is_empty = (flags_byte & (1 << flags::IS_EMPTY)) > 0;
  if (is_empty == 1) return c; // sketch is empty, no need to read further.

  // Set the sketch weight and read in the sketch values
  const auto weight = read<W>(is);
  c._total_weight += weight;
  read(is, c._sketch_array.data(), sizeof(W) * c._sketch_array.size());

  return c;
}

template<typename W, typename A>
size_t count_min_sketch<W,A>::get_serialized_size_bytes() const {
  // The header is always 2 longs, whether empty or full
  const size_t preamble_longs = PREAMBLE_LONGS_SHORT;

  // If the sketch is empty, we're done. Otherwise, we need the total weight
  // held by the sketch as well as a data table of size (num_buckets * num_hashes)
  return (preamble_longs * sizeof(uint64_t)) + (is_empty() ? 0 : sizeof(W) * (1 + _num_buckets * _num_hashes));
}

template<typename W, typename A>
auto count_min_sketch<W,A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  vector_bytes bytes(header_size_bytes + get_serialized_size_bytes(), 0, _allocator);
  uint8_t *ptr = bytes.data() + header_size_bytes;

  // Long 0
  const uint8_t preamble_longs = PREAMBLE_LONGS_SHORT;
  ptr += copy_to_mem(preamble_longs, ptr);
  const uint8_t ser_ver = SERIAL_VERSION_1;
  ptr += copy_to_mem(ser_ver, ptr);
  const uint8_t family_id = FAMILY_ID;
  ptr += copy_to_mem(family_id, ptr);
  const uint8_t flags_byte = (is_empty() ? 1 << flags::IS_EMPTY : 0);
  ptr += copy_to_mem(flags_byte, ptr);
  const uint32_t unused32 = NULL_32;
  ptr += copy_to_mem(unused32, ptr);

  // Long 1
  const uint32_t nbuckets = _num_buckets;
  const uint8_t nhashes = _num_hashes;
  const uint16_t seed_hash(compute_seed_hash(_seed));
  const uint8_t null_characters_8 =  NULL_8;
  ptr += copy_to_mem(nbuckets, ptr);
  ptr += copy_to_mem(nhashes, ptr);
  ptr += copy_to_mem(seed_hash, ptr);
  ptr += copy_to_mem(null_characters_8, ptr);
  if (is_empty()) return bytes; // sketch is empty, no need to write further bytes.

  // Long 2
  const W t_weight = _total_weight;
  ptr += copy_to_mem(t_weight, ptr);

  // Long  3 onwards: remaining bytes are consumed by writing the weight and the array values.
  auto it = _sketch_array.begin();
  while (it != _sketch_array.end()) {
    ptr += copy_to_mem(*it, ptr);
    ++it;
  }

  return bytes;
}

template<typename W, typename A>
auto count_min_sketch<W,A>::deserialize(const void* bytes, size_t size, uint64_t seed, const A& allocator) -> count_min_sketch {
  ensure_minimum_memory(size, PREAMBLE_LONGS_SHORT * sizeof(uint64_t));

  const char* ptr = static_cast<const char*>(bytes);

  // First 8 bytes are 4 bytes of preamble and 4 unused bytes.
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, preamble_longs);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, flags_byte);
  ptr += sizeof(uint32_t);

  check_header_validity(preamble_longs, serial_version, family_id, flags_byte);

  // Second 8 bytes are the sketch parameters with a final, unused byte.
  uint32_t nbuckets;
  uint8_t nhashes;
  uint16_t seed_hash;
  ptr += copy_from_mem(ptr, nbuckets);
  ptr += copy_from_mem(ptr, nhashes);
  ptr += copy_from_mem(ptr, seed_hash);
  ptr += sizeof(uint8_t);

  if (seed_hash != compute_seed_hash(seed)) {
    throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash) + ", "
                                + std::to_string(compute_seed_hash(seed)));
  }
  count_min_sketch c(nhashes, nbuckets, seed, allocator);
  const bool is_empty = (flags_byte & (1 << flags::IS_EMPTY)) > 0;
  if (is_empty) return c; // sketch is empty, no need to read further.

  ensure_minimum_memory(size, sizeof(W) * (1 + nbuckets * nhashes));

  // Long 2 is the weight.
  W weight;
  ptr += copy_from_mem(ptr, weight);
  c._total_weight += weight;

  // All remaining bytes are the sketch table entries.
  for (size_t i = 0; i<c._num_buckets*c._num_hashes; ++i) {
    ptr += copy_from_mem(ptr, c._sketch_array[i]);
  }
  return c;
}

template<typename W, typename A>
bool count_min_sketch<W,A>::is_empty() const {
  return _total_weight == 0;
}

template<typename W, typename A>
string<A> count_min_sketch<W,A>::to_string() const {
  // count the number of used entries in the sketch
  uint64_t num_nonzero = 0;
  for (const auto entry: _sketch_array) {
    if (entry != static_cast<W>(0.0))
      ++num_nonzero;
  }

  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Count Min sketch summary:" << std::endl;
  os << "   num hashes     : " << static_cast<uint32_t>(_num_hashes) << std::endl;
  os << "   num buckets    : " << _num_buckets << std::endl;
  os << "   capacity bins  : " << _sketch_array.size() << std::endl;
  os << "   filled bins    : " << num_nonzero << std::endl;
  os << "   pct filled     : " << std::setprecision(3) << (num_nonzero * 100.0) / _sketch_array.size() << "%" << std::endl;
  os << "### End sketch summary" << std::endl;

  return string<A>(os.str().c_str(), _allocator);
}

template<typename W, typename A>
void count_min_sketch<W,A>::check_header_validity(uint8_t preamble_longs, uint8_t serial_version,  uint8_t family_id, uint8_t flags_byte) {
  const bool empty = (flags_byte & (1 << flags::IS_EMPTY)) > 0;

  const uint8_t sw = (empty ? 1 : 0) + (2 * serial_version) + (4 * family_id) + (32 * (preamble_longs & 0x3F));
  bool valid = true;

  switch (sw) { // exhaustive list and description of all valid cases
    case 138 : break; // !empty, ser_ver==1, family==18, preLongs=2;
    case 139 : break; // empty, ser_ver==1, family==18, preLongs=2;
    //case 170 : break; // !empty, ser_ver==1, family==18, preLongs=3;
    default : // all other case values are invalid
      valid = false;
  }

  if (!valid) {
    std::ostringstream os;
    os << "Possible sketch corruption. Inconsistent state: "
       << "preamble_longs = " << static_cast<uint32_t>(preamble_longs)
       << ", empty = " << (empty ? "true" : "false")
       << ", serialization_version = " << static_cast<uint32_t>(serial_version);
    throw std::invalid_argument(os.str());
  }
}

} /* namespace datasketches */

#endif
