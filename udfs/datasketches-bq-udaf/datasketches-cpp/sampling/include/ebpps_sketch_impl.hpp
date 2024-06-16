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

#ifndef _EBPPS_SKETCH_IMPL_HPP_
#define _EBPPS_SKETCH_IMPL_HPP_

#include <memory>
#include <sstream>
#include <cmath>
#include <random>
#include <algorithm>
#include <stdexcept>
#include <utility>

#include "ebpps_sketch.hpp"

namespace datasketches {

template<typename T, typename A>
ebpps_sketch<T, A>::ebpps_sketch(uint32_t k, const A& allocator) :
  allocator_(allocator),
  k_(k),
  n_(0),
  cumulative_wt_(0.0),
  wt_max_(0.0),
  rho_(1.0),
  sample_(check_k(k), allocator),
  tmp_(1, allocator)
  {}

template<typename T, typename A>
ebpps_sketch<T,A>::ebpps_sketch(uint32_t k, uint64_t n, double cumulative_wt,
                                double wt_max, double rho,
                                ebpps_sample<T,A>&& sample, const A& allocator) :
  allocator_(allocator),
  k_(k),
  n_(n),
  cumulative_wt_(cumulative_wt),
  wt_max_(wt_max),
  rho_(rho),
  sample_(sample),
  tmp_(1, allocator)
  {}

template<typename T, typename A>
uint32_t ebpps_sketch<T, A>::get_k() const {
  return k_;
}

template<typename T, typename A>
uint64_t ebpps_sketch<T, A>::get_n() const {
  return n_;
}

template<typename T, typename A>
double ebpps_sketch<T, A>::get_c() const {
  return sample_.get_c();
}

template<typename T, typename A>
double ebpps_sketch<T, A>::get_cumulative_weight() const {
  return cumulative_wt_;
}

template<typename T, typename A>
bool ebpps_sketch<T, A>::is_empty() const {
  return n_ == 0;
}

template<typename T, typename A>
void ebpps_sketch<T, A>::reset() {
  n_ = 0;
  cumulative_wt_ = 0.0;
  wt_max_ = 0.0;
  rho_ = 1.0;
  sample_.reset();
}

template<typename T, typename A>
string<A> ebpps_sketch<T, A>::to_string() const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### EBPPS Sketch SUMMARY:" << std::endl;
  os << "   k            : " << k_ << std::endl;
  os << "   n            : " << n_ << std::endl;
  os << "   cum. weight  : " << cumulative_wt_ << std::endl;
  os << "   wt_mac       : " << wt_max_ << std::endl;
  os << "   rho          : " << rho_ << std::endl;
  os << "   C            : " << sample_.get_c() << std::endl;
  os << "### END SKETCH SUMMARY" << std::endl;
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename A>
string<A> ebpps_sketch<T, A>::items_to_string() const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Sketch Items" << std::endl;
  os << sample_.to_string(); // assumes std::endl at end
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename A>
A ebpps_sketch<T, A>::get_allocator() const {
  return allocator_;
}

template<typename T, typename A>
void ebpps_sketch<T, A>::update(const T& item, double weight) {
  return internal_update(item, weight);
}

template<typename T, typename A>
void ebpps_sketch<T, A>::update(T&& item, double weight) {
  return internal_update(std::move(item), weight);
}

template<typename T, typename A>
template<typename FwdItem>
void ebpps_sketch<T, A>::internal_update(FwdItem&& item, double weight) {
  if (weight < 0.0 || std::isnan(weight) || std::isinf(weight)) {
    throw std::invalid_argument("Item weights must be nonnegative and finite. Found: "
                                + std::to_string(weight));
  } else if (weight == 0.0) {
    return;
  }

  const double new_cum_wt = cumulative_wt_ + weight;
  const double new_wt_max = std::max(wt_max_, weight);
  const double new_rho = std::min(1.0 / new_wt_max, k_ / new_cum_wt);

  if (cumulative_wt_ > 0.0)
    sample_.downsample(new_rho / rho_);
  
  tmp_.replace_content(conditional_forward<FwdItem>(item), new_rho * weight);
  sample_.merge(tmp_);

  cumulative_wt_ = new_cum_wt;
  wt_max_ = new_wt_max;
  rho_ = new_rho;
  ++n_;
}

template<typename T, typename A>
auto ebpps_sketch<T,A>::get_result() const -> result_type {
  return sample_.get_sample();
}

/* Merging
 * There is a trivial merge algorithm that involves downsampling each sketch A and B
 * as A.cum_wt / (A.cum_wt + B.cum_wt) and B.cum_wt / (A.cum_wt + B.cum_wt),
 * respectively. That merge does preserve first-order probabilities, specifically
 * the probability proportional to size property, and like all other known merge
 * algorithms distorts second-order probabilities (co-occurrences). There are
 * pathological cases, most obvious with k=2 and A.cum_wt == B.cum_wt where that
 * approach will always take exactly 1 item from A and 1 from B, meaning the
 * co-occurrence rate for two items from either sketch is guaranteed to be 0.0.
 * 
 * With EBPPS, once an item is accepted into the sketch we no longer need to
 * track the item's weight: All accepted items are treated equally. As a result, we
 * can take inspiration from the reservoir sampling merge in the datasketches-java
 * library. We need to merge the smaller sketch into the larger one, swapping as
 * needed to ensure that, at which point we simply call update() with the items
 * in the smaller sketch as long as we adjust the weight appropriately.
 * Merging smaller into larger is essential to ensure that no item has a
 * contribution to C > 1.0.
 */

template<typename T, typename A>
void ebpps_sketch<T, A>::merge(ebpps_sketch<T, A>&& sk) {
  if (sk.get_cumulative_weight() == 0.0) return;
  else if (sk.get_cumulative_weight() > get_cumulative_weight()) {
    // need to swap this with sk to merge smaller into larger
    std::swap(*this, sk);
  }

  internal_merge(sk);
}

template<typename T, typename A>
void ebpps_sketch<T, A>::merge(const ebpps_sketch<T, A>& sk) {
  if (sk.get_cumulative_weight() == 0.0) return;
  else if (sk.get_cumulative_weight() > get_cumulative_weight()) {
    // need to swap this with sk to merge, so make a copy, swap,
    // and use that to merge
    ebpps_sketch sk_copy(sk);
    swap(*this, sk_copy);
    internal_merge(sk_copy);
  } else {
    internal_merge(sk);
  }
}

template<typename T, typename A>
template<typename O>
void ebpps_sketch<T, A>::internal_merge(O&& sk) {
  // assumes that sk.cumulative_wt_ <= cumulative_wt_,
  // which must be checked before calling this
  
  const ebpps_sample<T,A>& other_sample = sk.sample_;

  const double final_cum_wt = cumulative_wt_ + sk.cumulative_wt_;
  const double new_wt_max = std::max(wt_max_, sk.wt_max_);
  k_ = std::min(k_, sk.k_);
  const uint64_t new_n = n_ + sk.n_;

  // Insert sk's items with the cumulative weight
  // split between the input items. We repeat the same process
  // for full items and the partial item, scaling the input
  // weight appropriately.
  // We handle all C input items, meaning we always process
  // the partial item using a scaled down weight.
  // Handling the partial item by probabilistically including
  // it as a full item would be correct on average but would
  // introduce bias for any specific merge operation.
  const double avg_wt = sk.get_cumulative_weight() / sk.get_c();
  auto items = other_sample.get_full_items();
  for (size_t i = 0; i < items.size(); ++i) {
    // new_wt_max is pre-computed
    const double new_cum_wt = cumulative_wt_ + avg_wt;
    const double new_rho = std::min(1.0 / new_wt_max, k_ / new_cum_wt);

    if (cumulative_wt_ > 0.0)
      sample_.downsample(new_rho / rho_);
  
    tmp_.replace_content(conditional_forward<O>(items[i]), new_rho * avg_wt);
    sample_.merge(tmp_);

    cumulative_wt_ = new_cum_wt;
    rho_ = new_rho;
  }

  // insert partial item with weight scaled by the fractional part of C
  if (other_sample.has_partial_item()) {
    double unused;
    const double other_c_frac = std::modf(other_sample.get_c(), &unused);

    const double new_cum_wt = cumulative_wt_ + (other_c_frac * avg_wt);
    const double new_rho = std::min(1.0 / new_wt_max, k_ / new_cum_wt);

    if (cumulative_wt_ > 0.0)
      sample_.downsample(new_rho / rho_);
  
    tmp_.replace_content(conditional_forward<O>(other_sample.get_partial_item()), new_rho * other_c_frac * avg_wt);
    sample_.merge(tmp_);

    cumulative_wt_ = new_cum_wt;
    rho_ = new_rho;
  }

  // avoid numeric issues by setting cumulative weight to the
  // pre-computed value
  cumulative_wt_ = final_cum_wt;
  n_ = new_n;
}

/*
 * An empty sketch requires 8 bytes.
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 * </pre>
 *
 * A non-empty sketch requires 40 bytes of preamble. C looks like part of
 * the preamble but is serialized as part of the internal sample state.
 *
 * The count of items seen is not used but preserved as the value seems like a useful
 * count to track.
 * 
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------Items Seen Count (N)--------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||----------------------------Cumulative Weight----------------------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||-----------------------------Max Item Weight-----------------------------------|
 *
 *      ||      32        |   33   |   34   |   35   |   36   |   37   |   38   |   39   |
 *  4   ||----------------------------------Rho------------------------------------------|
 *
 *      ||      40        |   41   |   42   |   43   |   44   |   45   |   46   |   47   |
 *  5   ||-----------------------------------C-------------------------------------------|
 *
 *      ||      40+                      |
 *  6+  ||  {Items Array}                |
 *      ||  {Optional Item (if needed)}  |
 * </pre>
 */

template<typename T, typename A>
template<typename SerDe>
size_t ebpps_sketch<T, A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (is_empty()) { return PREAMBLE_LONGS_EMPTY << 3; }
  return (PREAMBLE_LONGS_FULL << 3) + sample_.get_serialized_size_bytes(sd);
}

template<typename T, typename A>
template<typename SerDe>
auto ebpps_sketch<T,A>::serialize(unsigned header_size_bytes, const SerDe& sd) const -> vector_bytes {
  const uint8_t prelongs = (is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_FULL);

  const size_t size = header_size_bytes + (prelongs << 3) + sample_.get_serialized_size_bytes(sd);
  vector_bytes bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  const uint8_t* end_ptr = ptr + size;

  uint8_t flags = 0;
  if (is_empty()) {
    flags |= EMPTY_FLAG_MASK;
  } else {
    flags |= sample_.has_partial_item() ? HAS_PARTIAL_ITEM_MASK : 0;
  }

  // first prelong
  const uint8_t ser_ver = SER_VER;
  const uint8_t family = FAMILY_ID;
  ptr += copy_to_mem(prelongs, ptr);
  ptr += copy_to_mem(ser_ver, ptr);
  ptr += copy_to_mem(family, ptr);
  ptr += copy_to_mem(flags, ptr);
  ptr += copy_to_mem(k_, ptr);

  if (!is_empty()) {
    // remaining preamble
    ptr += copy_to_mem(n_, ptr);
    ptr += copy_to_mem(cumulative_wt_, ptr);
    ptr += copy_to_mem(wt_max_, ptr);
    ptr += copy_to_mem(rho_, ptr);
    ptr += sample_.serialize(ptr, end_ptr, sd);
  }

  return bytes;
}

template<typename T, typename A>
template<typename SerDe>
void ebpps_sketch<T,A>::serialize(std::ostream& os, const SerDe& sd) const {
  const uint8_t prelongs = (is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_FULL);

  uint8_t flags = 0;
  if (is_empty()) {
    flags |= EMPTY_FLAG_MASK;
  } else {
    flags |= sample_.has_partial_item() ? HAS_PARTIAL_ITEM_MASK : 0;
  }

  // first prelong
  const uint8_t ser_ver = SER_VER;
  const uint8_t family = FAMILY_ID;
  write(os, prelongs);
  write(os, ser_ver);
  write(os, family);
  write(os, flags);
  write(os, k_);

  if (!is_empty()) {
    // remaining preamble
    write(os, n_);
    write(os, cumulative_wt_);
    write(os, wt_max_);
    write(os, rho_);
    sample_.serialize(os, sd);
  }

  if (!os.good()) throw std::runtime_error("error writing to std::ostream");
}

template<typename T, typename A>
template<typename SerDe>
ebpps_sketch<T,A> ebpps_sketch<T,A>::deserialize(const void* bytes, size_t size, const SerDe& sd, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const uint8_t* ptr = static_cast<const uint8_t*>(bytes);
  const uint8_t* end_ptr = ptr + size;
  uint8_t prelongs;
  ptr += copy_from_mem(ptr, prelongs);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags;
  ptr += copy_from_mem(ptr, flags);
  uint32_t k;
  ptr += copy_from_mem(ptr, k);

  check_k(k);
  check_preamble_longs(prelongs, flags);
  check_family_and_serialization_version(family_id, serial_version);
  ensure_minimum_memory(size, prelongs << 3);

  const bool empty = flags & EMPTY_FLAG_MASK;
  if (empty)
    return ebpps_sketch(k, allocator);

  uint64_t n;
  ptr += copy_from_mem(ptr, n);
  double cumulative_wt;
  ptr += copy_from_mem(ptr, cumulative_wt);
  double wt_max;
  ptr += copy_from_mem(ptr, wt_max);
  double rho;
  ptr += copy_from_mem(ptr, rho);

  auto pair = ebpps_sample<T, A>::deserialize(ptr, end_ptr - ptr, sd, allocator);
  ebpps_sample<T, A> sample = pair.first;
  ptr += pair.second;

  if (sample.has_partial_item() != bool(flags & HAS_PARTIAL_ITEM_MASK))
    throw std::runtime_error("sketch fails internal consistency check");

  return ebpps_sketch(k, n, cumulative_wt, wt_max, rho, std::move(sample), allocator);
}

template<typename T, typename A>
template<typename SerDe>
ebpps_sketch<T,A> ebpps_sketch<T,A>::deserialize(std::istream& is, const SerDe& sd, const A& allocator) {
  const uint8_t prelongs = read<uint8_t>(is);
  const uint8_t ser_ver = read<uint8_t>(is);
  const uint8_t family = read<uint8_t>(is);
  const uint8_t flags = read<uint8_t>(is);
  const uint32_t k = read<uint32_t>(is);

  check_k(k);
  check_family_and_serialization_version(family, ser_ver);
  check_preamble_longs(prelongs, flags);

  const bool empty = (flags & EMPTY_FLAG_MASK);
  
  if (empty)
    return ebpps_sketch(k, allocator);

  const uint64_t n = read<uint64_t>(is);
  const double cumulative_wt = read<double>(is);
  const double wt_max = read<double>(is);
  const double rho = read<double>(is);

  auto sample = ebpps_sample<T,A>::deserialize(is, sd, allocator);

  if (sample.has_partial_item() != bool(flags & HAS_PARTIAL_ITEM_MASK))
    throw std::runtime_error("sketch fails internal consistency check");

  return ebpps_sketch(k, n, cumulative_wt, wt_max, rho, std::move(sample), allocator);
}

template <typename T, typename A>
inline uint32_t ebpps_sketch<T, A>::check_k(uint32_t k)
{
  if (k == 0 || k > MAX_K)
    throw std::invalid_argument("k must be strictly positive and less than " + std::to_string(MAX_K));
  return k;
}

template<typename T, typename A>
void ebpps_sketch<T, A>::check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver) {
  if (family_id == FAMILY_ID) {
    if (ser_ver != SER_VER) {
      throw std::invalid_argument("Possible corruption: EBPPS serialization version must be "
        + std::to_string(SER_VER) + ". Found: " + std::to_string(ser_ver));
    }
    return;
  }

  throw std::invalid_argument("Possible corruption: EBPPS Sketch family id must be "
    + std::to_string(FAMILY_ID) + ". Found: " + std::to_string(family_id));
}

template <typename T, typename A>
void ebpps_sketch<T, A>::check_preamble_longs(uint8_t preamble_longs, uint8_t flags)
{
  const bool is_empty(flags & EMPTY_FLAG_MASK);
  
  if (is_empty) {
    if (preamble_longs != PREAMBLE_LONGS_EMPTY) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_EMPTY) + " for an empty sketch. Found: "
        + std::to_string(preamble_longs));
    }
    if (flags & HAS_PARTIAL_ITEM_MASK) {
      throw std::invalid_argument("Possible corruption: Empty sketch must not "
        "contain indications of the presence of any item");
    }
  } else {
    if (preamble_longs != PREAMBLE_LONGS_FULL) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_FULL)
        + " for a non-empty sketch. Found: " + std::to_string(preamble_longs));
    }
  }
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sketch<T, A>::begin() const {
  return sample_.begin();
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sketch<T, A>::end() const {
  return sample_.end();
}

} // namespace datasketches

#endif // _EBPPS_SKETCH_IMPL_HPP_
