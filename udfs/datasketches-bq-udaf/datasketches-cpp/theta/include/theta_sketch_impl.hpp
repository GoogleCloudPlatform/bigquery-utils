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

#ifndef THETA_SKETCH_IMPL_HPP_
#define THETA_SKETCH_IMPL_HPP_

#include <sstream>
#include <vector>
#include <stdexcept>

#include "serde.hpp"
#include "binomial_bounds.hpp"
#include "theta_helpers.hpp"
#include "count_zeros.hpp"
#include "bit_packing.hpp"

namespace datasketches {

template<typename A>
bool base_theta_sketch_alloc<A>::is_estimation_mode() const {
  return get_theta64() < theta_constants::MAX_THETA && !is_empty();
}

template<typename A>
double base_theta_sketch_alloc<A>::get_theta() const {
  return static_cast<double>(get_theta64()) /
         static_cast<double>(theta_constants::MAX_THETA);
}

template<typename A>
double base_theta_sketch_alloc<A>::get_estimate() const {
  return get_num_retained() / get_theta();
}

template<typename A>
double base_theta_sketch_alloc<A>::get_lower_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_lower_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename A>
double base_theta_sketch_alloc<A>::get_upper_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_upper_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename A>
string<A> base_theta_sketch_alloc<A>::to_string(bool print_details) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Theta sketch summary:" << std::endl;
  os << "   num retained entries : " << this->get_num_retained() << std::endl;
  os << "   seed hash            : " << this->get_seed_hash() << std::endl;
  os << "   empty?               : " << (this->is_empty() ? "true" : "false") << std::endl;
  os << "   ordered?             : " << (this->is_ordered() ? "true" : "false") << std::endl;
  os << "   estimation mode?     : " << (this->is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << this->get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << this->get_theta64() << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  print_specifics(os);
  os << "### End sketch summary" << std::endl;
  if (print_details) {
      print_items(os);
  }
  return string<A>(os.str().c_str(), this->get_allocator());
}

template<typename A>
void theta_sketch_alloc<A>::print_items(std::ostringstream& os) const {
    os << "### Retained entries" << std::endl;
    for (const auto& hash: *this) {
      os << hash << std::endl;
    }
    os << "### End retained entries" << std::endl;
}


// update sketch

template<typename A>
update_theta_sketch_alloc<A>::update_theta_sketch_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf,
    float p, uint64_t theta, uint64_t seed, const A& allocator):
table_(lg_cur_size, lg_nom_size, rf, p, theta, seed, allocator)
{}

template<typename A>
A update_theta_sketch_alloc<A>::get_allocator() const {
  return table_.allocator_;
}

template<typename A>
bool update_theta_sketch_alloc<A>::is_empty() const {
  return table_.is_empty_;
}

template<typename A>
bool update_theta_sketch_alloc<A>::is_ordered() const {
  return table_.num_entries_ > 1 ? false : true;
}

template<typename A>
uint64_t update_theta_sketch_alloc<A>::get_theta64() const {
  return is_empty() ? theta_constants::MAX_THETA : table_.theta_;
}

template<typename A>
uint32_t update_theta_sketch_alloc<A>::get_num_retained() const {
  return table_.num_entries_;
}

template<typename A>
uint16_t update_theta_sketch_alloc<A>::get_seed_hash() const {
  return compute_seed_hash(table_.seed_);
}

template<typename A>
uint8_t update_theta_sketch_alloc<A>::get_lg_k() const {
  return table_.lg_nom_size_;
}

template<typename A>
auto update_theta_sketch_alloc<A>::get_rf() const -> resize_factor {
  return table_.rf_;
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint64_t value) {
  update(&value, sizeof(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int64_t value) {
  update(&value, sizeof(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint32_t value) {
  update(static_cast<int32_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int32_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint16_t value) {
  update(static_cast<int16_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int16_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(uint8_t value) {
  update(static_cast<int8_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(int8_t value) {
  update(static_cast<int64_t>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(double value) {
  update(canonical_double(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(float value) {
  update(static_cast<double>(value));
}

template<typename A>
void update_theta_sketch_alloc<A>::update(const std::string& value) {
  if (value.empty()) return;
  update(value.c_str(), value.length());
}

template<typename A>
void update_theta_sketch_alloc<A>::update(const void* data, size_t length) {
  const uint64_t hash = table_.hash_and_screen(data, length);
  if (hash == 0) return;
  auto result = table_.find(hash);
  if (!result.second) {
    table_.insert(result.first, hash);
  }
}

template<typename A>
void update_theta_sketch_alloc<A>::trim() {
  table_.trim();
}

template<typename A>
void update_theta_sketch_alloc<A>::reset() {
  table_.reset();
}

template<typename A>
auto update_theta_sketch_alloc<A>::begin() -> iterator {
  return iterator(table_.entries_, 1 << table_.lg_cur_size_, 0);
}

template<typename A>
auto update_theta_sketch_alloc<A>::end() -> iterator {
  return iterator(nullptr, 0, 1 << table_.lg_cur_size_);
}

template<typename A>
auto update_theta_sketch_alloc<A>::begin() const -> const_iterator {
  return const_iterator(table_.entries_, 1 << table_.lg_cur_size_, 0);
}

template<typename A>
auto update_theta_sketch_alloc<A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, 1 << table_.lg_cur_size_);
}

template<typename A>
compact_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::compact(bool ordered) const {
  return compact_theta_sketch_alloc<A>(*this, ordered);
}

template<typename A>
void update_theta_sketch_alloc<A>::print_specifics(std::ostringstream& os) const {
  os << "   lg nominal size      : " << static_cast<int>(table_.lg_nom_size_) << std::endl;
  os << "   lg current size      : " << static_cast<int>(table_.lg_cur_size_) << std::endl;
  os << "   resize factor        : " << (1 << table_.rf_) << std::endl;
}

// builder

template<typename A>
update_theta_sketch_alloc<A>::builder::builder(const A& allocator): theta_base_builder<builder, A>(allocator) {}

template<typename A>
update_theta_sketch_alloc<A> update_theta_sketch_alloc<A>::builder::build() const {
  return update_theta_sketch_alloc(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->starting_theta(), this->seed_, this->allocator_);
}

// compact sketch

template<typename A>
template<typename Other>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(const Other& other, bool ordered):
is_empty_(other.is_empty()),
is_ordered_(other.is_ordered() || ordered),
seed_hash_(other.get_seed_hash()),
theta_(other.get_theta64()),
entries_(other.get_allocator())
{
  if (!other.is_empty()) {
    entries_.reserve(other.get_num_retained());
    std::copy(other.begin(), other.end(), std::back_inserter(entries_));
    if (ordered && !other.is_ordered()) std::sort(entries_.begin(), entries_.end());
  }
}

template<typename A>
compact_theta_sketch_alloc<A>::compact_theta_sketch_alloc(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta,
    std::vector<uint64_t, A>&& entries):
is_empty_(is_empty),
is_ordered_(is_ordered || (entries.size() <= 1ULL)),
seed_hash_(seed_hash),
theta_(theta),
entries_(std::move(entries))
{}

template<typename A>
A compact_theta_sketch_alloc<A>::get_allocator() const {
  return entries_.get_allocator();
}

template<typename A>
bool compact_theta_sketch_alloc<A>::is_empty() const {
  return is_empty_;
}

template<typename A>
bool compact_theta_sketch_alloc<A>::is_ordered() const {
  return is_ordered_;
}

template<typename A>
uint64_t compact_theta_sketch_alloc<A>::get_theta64() const {
  return theta_;
}

template<typename A>
uint32_t compact_theta_sketch_alloc<A>::get_num_retained() const {
  return static_cast<uint32_t>(entries_.size());
}

template<typename A>
uint16_t compact_theta_sketch_alloc<A>::get_seed_hash() const {
  return seed_hash_;
}

template<typename A>
auto compact_theta_sketch_alloc<A>::begin() -> iterator {
  return iterator(entries_.data(), static_cast<uint32_t>(entries_.size()), 0);
}

template<typename A>
auto compact_theta_sketch_alloc<A>::end() -> iterator {
  return iterator(nullptr, 0, static_cast<uint32_t>(entries_.size()));
}

template<typename A>
auto compact_theta_sketch_alloc<A>::begin() const -> const_iterator {
  return const_iterator(entries_.data(), static_cast<uint32_t>(entries_.size()), 0);
}

template<typename A>
auto compact_theta_sketch_alloc<A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, static_cast<uint32_t>(entries_.size()));
}

template<typename A>
void compact_theta_sketch_alloc<A>::print_specifics(std::ostringstream&) const {}

template<typename A>
void compact_theta_sketch_alloc<A>::serialize(std::ostream& os) const {
  const uint8_t preamble_longs = this->is_estimation_mode() ? 3 : this->is_empty() || entries_.size() == 1 ? 1 : 2;
  write(os, preamble_longs);
  write(os, UNCOMPRESSED_SERIAL_VERSION);
  write(os, SKETCH_TYPE);
  write<uint16_t>(os, 0); // unused
  const uint8_t flags_byte(
    (1 << flags::IS_COMPACT) |
    (1 << flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << flags::IS_ORDERED : 0)
  );
  write(os, flags_byte);
  write(os, get_seed_hash());
  if (preamble_longs > 1) {
    write(os, static_cast<uint32_t>(entries_.size()));
    write<uint32_t>(os, 0); // unused
  }
  if (this->is_estimation_mode()) write(os, this->theta_);
  if (entries_.size() > 0) write(os, entries_.data(), entries_.size() * sizeof(uint64_t));
}

template<typename A>
auto compact_theta_sketch_alloc<A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const uint8_t preamble_longs = this->is_estimation_mode() ? 3 : this->is_empty() || entries_.size() == 1 ? 1 : 2;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs
      + sizeof(uint64_t) * entries_.size();
  vector_bytes bytes(size, 0, entries_.get_allocator());
  uint8_t* ptr = bytes.data() + header_size_bytes;

  *ptr++ = preamble_longs;
  *ptr++ = UNCOMPRESSED_SERIAL_VERSION;
  *ptr++ = SKETCH_TYPE;
  ptr += sizeof(uint16_t); // unused
  const uint8_t flags_byte(
    (1 << flags::IS_COMPACT) |
    (1 << flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << flags::IS_ORDERED : 0)
  );
  *ptr++ = flags_byte;
  ptr += copy_to_mem(get_seed_hash(), ptr);
  if (preamble_longs > 1) {
    ptr += copy_to_mem(static_cast<uint32_t>(entries_.size()), ptr);
    ptr += sizeof(uint32_t); // unused
  }
  if (this->is_estimation_mode()) ptr += copy_to_mem(theta_, ptr);
  if (entries_.size() > 0) ptr += copy_to_mem(entries_.data(), ptr, entries_.size() * sizeof(uint64_t));
  return bytes;
}

template<typename A>
bool compact_theta_sketch_alloc<A>::is_suitable_for_compression() const {
  if (!this->is_ordered() || entries_.size() == 0 ||
        (entries_.size() == 1 && !this->is_estimation_mode())) return false;
  return true;
}

template<typename A>
void compact_theta_sketch_alloc<A>::serialize_compressed(std::ostream& os) const {
  if (is_suitable_for_compression()) return serialize_version_4(os);
  return serialize(os);
}

template<typename A>
auto compact_theta_sketch_alloc<A>::serialize_compressed(unsigned header_size_bytes) const -> vector_bytes {
  if (is_suitable_for_compression()) return serialize_version_4(header_size_bytes);
  return serialize(header_size_bytes);
}

template<typename A>
uint8_t compact_theta_sketch_alloc<A>::compute_min_leading_zeros() const {
  // compression is based on leading zeros in deltas between ordered hash values
  // assumes ordered sketch
  uint64_t previous = 0;
  uint64_t ored = 0;
  for (const uint64_t entry: entries_) {
    const uint64_t delta = entry - previous;
    ored |= delta;
    previous = entry;
  }
  return count_leading_zeros_in_u64(ored);
}

template<typename A>
void compact_theta_sketch_alloc<A>::serialize_version_4(std::ostream& os) const {
  const uint8_t preamble_longs = this->is_estimation_mode() ? 2 : 1;
  const uint8_t entry_bits = 64 - compute_min_leading_zeros();

  // store num_entries as whole bytes since whole-byte blocks will follow (most probably)
  const uint8_t num_entries_bytes = whole_bytes_to_hold_bits<uint8_t>(32 - count_leading_zeros_in_u32(static_cast<uint32_t>(entries_.size())));

  write(os, preamble_longs);
  write(os, COMPRESSED_SERIAL_VERSION);
  write(os, SKETCH_TYPE);
  write(os, entry_bits);
  write(os, num_entries_bytes);
  const uint8_t flags_byte(
    (1 << flags::IS_COMPACT) |
    (1 << flags::IS_READ_ONLY) |
    (1 << flags::IS_ORDERED)
  );
  write(os, flags_byte);
  write(os, get_seed_hash());
  if (this->is_estimation_mode()) write(os, this->theta_);
  uint32_t num_entries = static_cast<uint32_t>(entries_.size());
  for (unsigned i = 0; i < num_entries_bytes; ++i) {
    write<uint8_t>(os, num_entries & 0xff);
    num_entries >>= 8;
  }

  uint64_t previous = 0;
  uint64_t deltas[8];
  vector_bytes buffer(entry_bits, 0, entries_.get_allocator()); // block of 8 entries takes entry_bits bytes

  // pack blocks of 8 deltas
  unsigned i;
  for (i = 0; i + 7 < entries_.size(); i += 8) {
    for (unsigned j = 0; j < 8; ++j) {
      deltas[j] = entries_[i + j] - previous;
      previous = entries_[i + j];
    }
    pack_bits_block8(deltas, buffer.data(), entry_bits);
    write(os, buffer.data(), buffer.size());
  }

  // pack extra deltas if fewer than 8 of them left
  if (i < entries_.size()) {
    uint8_t offset = 0;
    uint8_t* ptr = buffer.data();
    for (; i < entries_.size(); ++i) {
      const uint64_t delta = entries_[i] - previous;
      previous = entries_[i];
      offset = pack_bits(delta, entry_bits, ptr, offset);
    }
    write(os, buffer.data(), ptr - buffer.data());
  }
}

template<typename A>
auto compact_theta_sketch_alloc<A>::serialize_version_4(unsigned header_size_bytes) const -> vector_bytes {
  const uint8_t preamble_longs = this->is_estimation_mode() ? 2 : 1;
  const uint8_t entry_bits = 64 - compute_min_leading_zeros();
  const size_t compressed_bits = entry_bits * entries_.size();

  // store num_entries as whole bytes since whole-byte blocks will follow (most probably)
  const uint8_t num_entries_bytes = whole_bytes_to_hold_bits<uint8_t>(32 - count_leading_zeros_in_u32(static_cast<uint32_t>(entries_.size())));

  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs + num_entries_bytes
      + whole_bytes_to_hold_bits(compressed_bits);
  vector_bytes bytes(size, 0, entries_.get_allocator());
  uint8_t* ptr = bytes.data() + header_size_bytes;

  *ptr++ = preamble_longs;
  *ptr++ = COMPRESSED_SERIAL_VERSION;
  *ptr++ = SKETCH_TYPE;
  *ptr++ = entry_bits;
  *ptr++ = num_entries_bytes;
  const uint8_t flags_byte(
    (1 << flags::IS_COMPACT) |
    (1 << flags::IS_READ_ONLY) |
    (1 << flags::IS_ORDERED)
  );
  *ptr++ = flags_byte;
  ptr += copy_to_mem(get_seed_hash(), ptr);
  if (this->is_estimation_mode()) {
    ptr += copy_to_mem(theta_, ptr);
  }
  uint32_t num_entries = static_cast<uint32_t>(entries_.size());
  for (unsigned i = 0; i < num_entries_bytes; ++i) {
    *ptr++ = num_entries & 0xff;
    num_entries >>= 8;
  }

  uint64_t previous = 0;
  uint64_t deltas[8];

  // pack blocks of 8 deltas
  unsigned i;
  for (i = 0; i + 7 < entries_.size(); i += 8) {
    for (unsigned j = 0; j < 8; ++j) {
      deltas[j] = entries_[i + j] - previous;
      previous = entries_[i + j];
    }
    pack_bits_block8(deltas, ptr, entry_bits);
    ptr += entry_bits;
  }

  // pack extra deltas if fewer than 8 of them left
  uint8_t offset = 0;
  for (; i < entries_.size(); ++i) {
    const uint64_t delta = entries_[i] - previous;
    previous = entries_[i];
    offset = pack_bits(delta, entry_bits, ptr, offset);
  }
  return bytes;
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize(std::istream& is, uint64_t seed, const A& allocator) {
  const auto preamble_longs = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto type = read<uint8_t>(is);
  checker<true>::check_sketch_type(type, SKETCH_TYPE);
  switch (serial_version) {
    case 4:
      return deserialize_v4(preamble_longs, is, seed, allocator);
    case 3:
      return deserialize_v3(preamble_longs, is, seed, allocator);
    case 1:
      return deserialize_v1(preamble_longs, is, seed, allocator);
    case 2:
      return deserialize_v2(preamble_longs, is, seed, allocator);
    default:
      throw std::invalid_argument("unexpected sketch serialization version " + std::to_string(serial_version));
  }
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize_v1(
    uint8_t, std::istream& is, uint64_t seed, const A& allocator)
{
  const auto seed_hash = compute_seed_hash(seed);
  read<uint8_t>(is); // unused
  read<uint32_t>(is); // unused
  const auto num_entries = read<uint32_t>(is);
  read<uint32_t>(is); //unused
  const auto theta = read<uint64_t>(is);
  std::vector<uint64_t, A> entries(num_entries, 0, allocator);
  bool is_empty = (num_entries == 0) && (theta == theta_constants::MAX_THETA);
  if (!is_empty) read(is, entries.data(), sizeof(uint64_t) * entries.size());
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return compact_theta_sketch_alloc(is_empty, true, seed_hash, theta, std::move(entries));
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize_v2(
    uint8_t preamble_longs, std::istream& is, uint64_t seed, const A& allocator)
{
  read<uint8_t>(is); // unused
  read<uint16_t>(is); // unused
  const uint16_t seed_hash = read<uint16_t>(is);
  checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));
  if (preamble_longs == 1) {
    if (!is.good()) throw std::runtime_error("error reading from std::istream");
    std::vector<uint64_t, A> entries(0, 0, allocator);
    return compact_theta_sketch_alloc(true, true, seed_hash, theta_constants::MAX_THETA, std::move(entries));
  } else if (preamble_longs == 2) {
    const uint32_t num_entries = read<uint32_t>(is);
    read<uint32_t>(is); // unused
    std::vector<uint64_t, A> entries(num_entries, 0, allocator);
    if (num_entries == 0) {
      return compact_theta_sketch_alloc(true, true, seed_hash, theta_constants::MAX_THETA, std::move(entries));
    }
    read(is, entries.data(), entries.size() * sizeof(uint64_t));
    if (!is.good()) throw std::runtime_error("error reading from std::istream");
    return compact_theta_sketch_alloc(false, true, seed_hash, theta_constants::MAX_THETA, std::move(entries));
  } else if (preamble_longs == 3) {
    const uint32_t num_entries = read<uint32_t>(is);
    read<uint32_t>(is); // unused
    const auto theta = read<uint64_t>(is);
    bool is_empty = (num_entries == 0) && (theta == theta_constants::MAX_THETA);
    std::vector<uint64_t, A> entries(num_entries, 0, allocator);
    if (is_empty) {
      if (!is.good()) throw std::runtime_error("error reading from std::istream");
      return compact_theta_sketch_alloc(true, true, seed_hash, theta, std::move(entries));
    } else {
      read(is, entries.data(), sizeof(uint64_t) * entries.size());
      if (!is.good()) throw std::runtime_error("error reading from std::istream");
      return compact_theta_sketch_alloc(false, true, seed_hash, theta, std::move(entries));
    }
  } else {
    throw std::invalid_argument(std::to_string(preamble_longs) + " longs of premable, but expected 1, 2, or 3");
  }
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize_v3(
    uint8_t preamble_longs, std::istream& is, uint64_t seed, const A& allocator)
{
  read<uint16_t>(is); // unused
  const auto flags_byte = read<uint8_t>(is);
  const auto seed_hash = read<uint16_t>(is);
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  if (!is_empty) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));
  uint64_t theta = theta_constants::MAX_THETA;
  uint32_t num_entries = 0;
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_entries = 1;
    } else {
      num_entries = read<uint32_t>(is);
      read<uint32_t>(is); // unused
      if (preamble_longs > 2) theta = read<uint64_t>(is);
    }
  }
  std::vector<uint64_t, A> entries(num_entries, 0, allocator);
  if (!is_empty) read(is, entries.data(), sizeof(uint64_t) * entries.size());
  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return compact_theta_sketch_alloc(is_empty, is_ordered, seed_hash, theta, std::move(entries));
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize_v4(
    uint8_t preamble_longs, std::istream& is, uint64_t seed, const A& allocator)
{
  const auto entry_bits = read<uint8_t>(is);
  const auto num_entries_bytes = read<uint8_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  const auto seed_hash = read<uint16_t>(is);
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  if (!is_empty) checker<true>::check_seed_hash(seed_hash, compute_seed_hash(seed));
  uint64_t theta = theta_constants::MAX_THETA;
  if (preamble_longs > 1) theta = read<uint64_t>(is);
  uint32_t num_entries = 0;
  for (unsigned i = 0; i < num_entries_bytes; ++i) {
    num_entries |= read<uint8_t>(is) << (i << 3);
  }
  vector_bytes buffer(entry_bits, 0, allocator); // block of 8 entries takes entry_bits bytes
  std::vector<uint64_t, A> entries(num_entries, 0, allocator);

  // unpack blocks of 8 deltas
  unsigned i;
  for (i = 0; i + 7 < num_entries; i += 8) {
    read(is, buffer.data(), buffer.size());
    unpack_bits_block8(&entries[i], buffer.data(), entry_bits);
  }
  // unpack extra deltas if fewer than 8 of them left
  if (i < num_entries) read(is, buffer.data(), whole_bytes_to_hold_bits((num_entries - i) * entry_bits));
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  const uint8_t* ptr = buffer.data();
  uint8_t offset = 0;
  for (; i < num_entries; ++i) {
    offset = unpack_bits(entries[i], entry_bits, ptr, offset);
  }
  // undo deltas
  uint64_t previous = 0;
  for (i = 0; i < num_entries; ++i) {
    entries[i] += previous;
    previous = entries[i];
  }
  const bool is_ordered = flags_byte & (1 << flags::IS_ORDERED);
  return compact_theta_sketch_alloc(is_empty, is_ordered, seed_hash, theta, std::move(entries));
}

template<typename A>
compact_theta_sketch_alloc<A> compact_theta_sketch_alloc<A>::deserialize(const void* bytes, size_t size, uint64_t seed, const A& allocator) {
  auto data = compact_theta_sketch_parser<true>::parse(bytes, size, seed, false);
  if (data.entry_bits == 64) { // versions 1 to 3
    const uint64_t* entries = reinterpret_cast<const uint64_t*>(data.entries_start_ptr);
    return compact_theta_sketch_alloc(data.is_empty, data.is_ordered, data.seed_hash, data.theta,
        std::vector<uint64_t, A>(entries, entries + data.num_entries, allocator));
  } else { // version 4
    std::vector<uint64_t, A> entries(data.num_entries, 0, allocator);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data.entries_start_ptr);
    // unpack blocks of 8 deltas
    unsigned i;
    for (i = 0; i + 7 < data.num_entries; i += 8) {
      unpack_bits_block8(&entries[i], ptr, data.entry_bits);
      ptr += data.entry_bits;
    }
    // unpack extra deltas if fewer than 8 of them left
    uint8_t offset = 0;
    for (; i < data.num_entries; ++i) {
      offset = unpack_bits(entries[i], data.entry_bits, ptr, offset);
    }
    // undo deltas
    uint64_t previous = 0;
    for (i = 0; i < data.num_entries; ++i) {
      entries[i] += previous;
      previous = entries[i];
    }
    return compact_theta_sketch_alloc(data.is_empty, data.is_ordered, data.seed_hash, data.theta, std::move(entries));
  }
}

// wrapped compact sketch

template<typename A>
wrapped_compact_theta_sketch_alloc<A>::wrapped_compact_theta_sketch_alloc(const data_type& data):
data_(data)
{}

template<typename A>
const wrapped_compact_theta_sketch_alloc<A> wrapped_compact_theta_sketch_alloc<A>::wrap(const void* bytes, size_t size, uint64_t seed, bool dump_on_error) {
  return wrapped_compact_theta_sketch_alloc(compact_theta_sketch_parser<true>::parse(bytes, size, seed, dump_on_error));
}

template<typename A>
A wrapped_compact_theta_sketch_alloc<A>::get_allocator() const {
  return A();
}

template<typename A>
bool wrapped_compact_theta_sketch_alloc<A>::is_empty() const {
  return data_.is_empty;
}

template<typename A>
bool wrapped_compact_theta_sketch_alloc<A>::is_ordered() const {
  return data_.is_ordered;
}

template<typename A>
uint64_t wrapped_compact_theta_sketch_alloc<A>::get_theta64() const {
  return data_.theta;
}

template<typename A>
uint32_t wrapped_compact_theta_sketch_alloc<A>::get_num_retained() const {
  return data_.num_entries;
}

template<typename A>
uint16_t wrapped_compact_theta_sketch_alloc<A>::get_seed_hash() const {
  return data_.seed_hash;
}

template<typename A>
auto wrapped_compact_theta_sketch_alloc<A>::begin() const -> const_iterator {
  return const_iterator(data_.entries_start_ptr, data_.entry_bits, data_.num_entries, 0);
}

template<typename A>
auto wrapped_compact_theta_sketch_alloc<A>::end() const -> const_iterator {
  return const_iterator(data_.entries_start_ptr, data_.entry_bits, data_.num_entries, data_.num_entries);
}

template<typename A>
void wrapped_compact_theta_sketch_alloc<A>::print_specifics(std::ostringstream&) const {}

template<typename A>
void wrapped_compact_theta_sketch_alloc<A>::print_items(std::ostringstream& os) const {
    os << "### Retained entries" << std::endl;
    for (const auto hash: *this) {
      os << hash << std::endl;
    }
    os << "### End retained entries" << std::endl;
}

// assumes index == 0 or index == num_entries
template<typename Allocator>
wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::const_iterator(
    const void* ptr, uint8_t entry_bits, uint32_t num_entries, uint32_t index):
ptr_(ptr),
entry_bits_(entry_bits),
num_entries_(num_entries),
index_(index),
previous_(0),
is_block_mode_(num_entries_ >= 8),
buf_i_(0),
offset_(0)
{
  if (entry_bits == 64) { // no compression
    ptr_ = reinterpret_cast<const uint64_t*>(ptr) + index;
  } else if (index < num_entries) {
    if (is_block_mode_) {
      unpack_bits_block8(buffer_, reinterpret_cast<const uint8_t*>(ptr_), entry_bits_);
      ptr_ = reinterpret_cast<const uint8_t*>(ptr_) + entry_bits_;
      for (int i = 0; i < 8; ++i) {
        buffer_[i] += previous_;
        previous_ = buffer_[i];
      }
    } else {
      offset_ = unpack_bits(buffer_[0], entry_bits_, reinterpret_cast<const uint8_t*&>(ptr_), offset_);
      buffer_[0] += previous_;
      previous_ = buffer_[0];
    }
  }
}

template<typename Allocator>
auto wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::operator++() -> const_iterator& {
  if (entry_bits_ == 64) { // no compression
    ptr_ = reinterpret_cast<const uint64_t*>(ptr_) + 1;
    return *this;
  }
  ++index_;
  if (index_ < num_entries_) {
    if (is_block_mode_) {
      ++buf_i_;
      if (buf_i_ == 8) {
        buf_i_ = 0;
        if (index_ + 8 < num_entries_) {
          unpack_bits_block8(buffer_, reinterpret_cast<const uint8_t*>(ptr_), entry_bits_);
          ptr_ = reinterpret_cast<const uint8_t*>(ptr_) + entry_bits_;
          for (int i = 0; i < 8; ++i) {
            buffer_[i] += previous_;
            previous_ = buffer_[i];
          }
        } else {
          is_block_mode_ = false;
          offset_ = unpack_bits(buffer_[0], entry_bits_, reinterpret_cast<const uint8_t*&>(ptr_), offset_);
          buffer_[0] += previous_;
          previous_ = buffer_[0];
        }
      }
    } else {
      offset_ = unpack_bits(buffer_[0], entry_bits_, reinterpret_cast<const uint8_t*&>(ptr_), offset_);
      buffer_[0] += previous_;
      previous_ = buffer_[0];
    }
  }
  return *this;
}

template<typename Allocator>
auto wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::operator++(int) -> const_iterator {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename Allocator>
bool wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::operator!=(const const_iterator& other) const {
  if (entry_bits_ == 64) return ptr_ != other.ptr_;
  return index_ != other.index_;
}

template<typename Allocator>
bool wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::operator==(const const_iterator& other) const {
  if (entry_bits_ == 64) return ptr_ == other.ptr_;
  return index_ == other.index_;
}

template<typename Allocator>
auto wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::operator*() const -> reference {
  if (entry_bits_ == 64) return *reinterpret_cast<const uint64_t*>(ptr_);
  return buffer_[buf_i_];
}

template<typename Allocator>
auto wrapped_compact_theta_sketch_alloc<Allocator>::const_iterator::operator->() const -> pointer {
  if (entry_bits_ == 64) return reinterpret_cast<const uint64_t*>(ptr_);
  return buffer_ + buf_i_;
}

} /* namespace datasketches */

#endif
