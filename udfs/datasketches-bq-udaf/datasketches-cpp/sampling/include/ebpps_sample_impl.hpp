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

#ifndef _EBPPS_SAMPLE_IMPL_HPP_
#define _EBPPS_SAMPLE_IMPL_HPP_

#include "common_defs.hpp"
#include "conditional_forward.hpp"
#include "ebpps_sample.hpp"
#include "serde.hpp"

#include <cmath>
#include <string>
#include <sstream>

namespace datasketches {

template<typename T, typename A>
ebpps_sample<T,A>::ebpps_sample(uint32_t reserved_size, const A& allocator) :
  allocator_(allocator),
  c_(0.0),
  partial_item_(),
  data_(allocator)
  {
    data_.reserve(reserved_size);
  }

template<typename T, typename A>
ebpps_sample<T,A>::ebpps_sample(std::vector<T, A>&& data, optional<T>&& partial_item, double c, const A& allocator) :
  allocator_(allocator),
  c_(c),
  partial_item_(partial_item),
  data_(data, allocator)
  {}

template<typename T, typename A>
template<typename TT>
void ebpps_sample<T,A>::replace_content(TT&& item, double theta) {
  c_ = theta;
  data_.clear();
  partial_item_.reset();
  if (theta == 1.0) {
    data_.emplace_back(std::forward<TT>(item));
  } else {
    partial_item_.emplace(std::forward<TT>(item));
  }
}

template<typename T, typename A>
auto ebpps_sample<T,A>::get_sample() const -> result_type {
  double unused;
  const double c_frac = std::modf(c_, &unused);
  const bool include_partial = next_double() < c_frac;
  const uint32_t result_size = static_cast<uint32_t>(data_.size()) + (include_partial ? 1 : 0);

  result_type result;
  result.reserve(result_size);
  std::copy(data_.begin(), data_.end(), std::back_inserter(result));
  if (include_partial)
    result.emplace_back(static_cast<const T&>(*partial_item_));
  
  return result;
}

template<typename T, typename A>
void ebpps_sample<T,A>::downsample(double theta) {
  if (theta >= 1.0) return;

  const double new_c = theta * c_;
  double new_c_int;
  const double new_c_frac = std::modf(new_c, &new_c_int);
  double c_int;
  const double c_frac = std::modf(c_, &c_int);

  if (new_c_int == 0.0) {
    // no full items retained
    if (next_double() > (c_frac / c_)) {
      swap_with_partial();
    }
    data_.clear();
  } else if (new_c_int == c_int) {
    // no items deleted
    if (next_double() > (1 - theta * c_frac)/(1 - new_c_frac)) {
      swap_with_partial();
    }
  } else {
    if (next_double() < theta * c_frac) {
      // subsample data in random order; last item is partial
      // create sample size new_c_int then swap_with_partial()
      subsample(static_cast<uint32_t>(new_c_int));
      swap_with_partial();
    } else {
      // create sample size new_c_int + 1 then move_one_to_partial)
      subsample(static_cast<uint32_t>(new_c_int) + 1);
      move_one_to_partial();
    }
  }

  if (new_c == new_c_int)
    partial_item_.reset();

  c_ = new_c;
}

template<typename T, typename A>
template<typename FwdSample>
void ebpps_sample<T,A>::merge(FwdSample&& other) {
  double c_int;
  const double c_frac = std::modf(c_, &c_int);

  double unused;
  const double other_c_frac = std::modf(other.c_, &unused);

  // update c_ here but do NOT recompute fractional part yet
  c_ += other.c_;

  for (uint32_t i = 0; i < other.data_.size(); ++i)
    data_.emplace_back(conditional_forward<FwdSample>(other.data_[i]));

  // This modifies the original algorithm slightly due to numeric
  // precision issues. Specifically, the test if c_frac + other_c_frac == 1.0
  // happens before tests for < 1.0 or > 1.0 and can also be triggered
  // if c_ == floor(c_) (the updated value of c_, not the input).
  //
  // We can still run into issues where c_frac + other_c_frac == epsilon
  // and the first case would have ideally triggered. As a result, we must
  // check if the partial item exists before adding to the data_ vector.

  if (c_frac == 0.0 && other_c_frac == 0.0) {
    partial_item_.reset();
  } else if (c_frac + other_c_frac == 1.0 || c_ == std::floor(c_)) {
    if (next_double() <= c_frac) {
      if (partial_item_)
        data_.emplace_back(std::move(*partial_item_));
    } else {
      if (other.partial_item_)
        data_.emplace_back(conditional_forward<FwdSample>(*other.partial_item_));
    }
    partial_item_.reset();
  } else if (c_frac + other_c_frac < 1.0) {
    if (next_double() > c_frac / (c_frac + other_c_frac)) {
      set_partial(conditional_forward<FwdSample>(*other.partial_item_));
    }
  } else { // c_frac + other_c_frac > 1
    if (next_double() <= (1 - c_frac) / ((1 - c_frac) + (1 - other_c_frac))) {
      data_.emplace_back(conditional_forward<FwdSample>(*other.partial_item_));
    } else {
      data_.emplace_back(std::move(*partial_item_));
      partial_item_.reset();
      set_partial(conditional_forward<FwdSample>(*other.partial_item_));
    }
  }
}

template<typename T, typename A>
string<A> ebpps_sample<T ,A>::to_string() const {
  std::ostringstream oss;
  oss << "   sample:" << std::endl;
  uint32_t idx = 0;
  for (const T& item : data_)
    oss << "\t" << idx++ << ":\t" << item << std::endl;
  oss << "   partial: ";
  if (bool(partial_item_))
    oss << (*partial_item_) << std::endl;
  else
    oss << "NULL" << std::endl;

  return oss.str();
}

template<typename T, typename A>
void ebpps_sample<T,A>::subsample(uint32_t num_samples) {
  // we can perform a Fisher-Yates style shuffle, stopping after
  // num_samples points since subsequent swaps would only be
  // between items after num_samples. This is valid since a
  // point from anywhere in the initial array would be eligible
  // to end up in the final subsample.

  if (num_samples == data_.size()) return;

  auto erase_start = data_.begin();
  uint32_t data_len = static_cast<uint32_t>(data_.size());
  for (uint32_t i = 0; i < num_samples; ++i, ++erase_start) {
    uint32_t j = i + random_idx(data_len - i);
    std::swap(data_[i], data_[j]);
  }

  data_.erase(erase_start, data_.end());
}

template<typename T, typename A>
template<typename FwdItem>
void ebpps_sample<T,A>::set_partial(FwdItem&& item) {
  if (partial_item_)
    *partial_item_ = conditional_forward<FwdItem>(item);
  else
    partial_item_.emplace(conditional_forward<FwdItem>(item));
}

template<typename T, typename A>
void ebpps_sample<T,A>::move_one_to_partial() {
  const size_t idx = random_idx(static_cast<uint32_t>(data_.size()));
  // swap selected item to end so we can delete it easily
  const size_t last_idx = data_.size() - 1;
  if (idx != last_idx) {
    std::swap(data_[idx], data_[last_idx]);
  }

  set_partial(std::move(data_[last_idx]));

  data_.pop_back();
}

template<typename T, typename A>
void ebpps_sample<T,A>::swap_with_partial() {
  if (partial_item_) {
    const size_t idx = random_idx(static_cast<uint32_t>(data_.size()));
    std::swap(data_[idx], *partial_item_);
  } else {
    move_one_to_partial();
  }
}

template<typename T, typename A>
void ebpps_sample<T,A>::reset() {
  c_ = 0.0;
  partial_item_.reset();
  data_.clear();
}

template<typename T, typename A>
double ebpps_sample<T,A>::get_c() const {
  return c_;
}

template<typename T, typename A>
auto ebpps_sample<T,A>::get_full_items() const -> result_type {
  return result_type(data_);
}

template<typename T, typename A>
bool ebpps_sample<T,A>::has_partial_item() const {
  return bool(partial_item_);
}

template<typename T, typename A>
T ebpps_sample<T,A>::get_partial_item() const {
  if (!partial_item_) throw std::runtime_error("Call to get_partial_item() when no partial item exists");
  return *partial_item_;
}

template<typename T, typename A>
uint32_t ebpps_sample<T,A>::random_idx(uint32_t max) {
  static std::uniform_int_distribution<uint32_t> dist;
  return dist(random_utils::rand, std::uniform_int_distribution<uint32_t>::param_type(0, max - 1));
}

template<typename T, typename A>
double ebpps_sample<T,A>::next_double() {
  return random_utils::next_double(random_utils::rand);
}

template<typename T, typename A>
uint32_t ebpps_sample<T,A>::get_num_retained_items() const {
  return static_cast<uint32_t>(data_.size() + (partial_item_ ? 1 : 0));
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename A>
template<typename TT, typename SerDe, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t ebpps_sample<T, A>::get_serialized_size_bytes(const SerDe&) const {
  if (c_ == 0.0)
    return 0;
  else
    return sizeof(c_) + (data_.size() + (partial_item_ ? 1 : 0)) * sizeof(T);
}

// implementation for all other types
template<typename T, typename A>
template<typename TT, typename SerDe, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t ebpps_sample<T, A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (c_ == 0.0) return 0;

  size_t num_bytes = sizeof(c_);
  for (auto it : data_)
    num_bytes += sd.size_of_item(it);

  if (partial_item_)
    num_bytes += sd.size_of_item(*partial_item_);

  return num_bytes;
}

template<typename T, typename A>
template<typename SerDe>
size_t ebpps_sample<T,A>::serialize(uint8_t* ptr, const uint8_t* end_ptr, const SerDe& sd) const {
  uint8_t* st_ptr = ptr;
  
  ensure_minimum_memory(end_ptr - ptr, sizeof(c_));
  ptr += copy_to_mem(c_, ptr);

  ptr += sd.serialize(ptr, end_ptr - ptr, data_.data(), static_cast<unsigned>(data_.size()));

  if (partial_item_) {
    ptr += sd.serialize(ptr, end_ptr - ptr, &*partial_item_, 1);
  }

  return ptr - st_ptr;
}

template<typename T, typename A>
template<typename SerDe>
void ebpps_sample<T,A>::serialize(std::ostream& os, const SerDe& sd) const {
  write(os, c_);

  sd.serialize(os, data_.data(), static_cast<unsigned>(data_.size()));

  if (partial_item_)
    sd.serialize(os, &*partial_item_, 1);

  if (!os.good()) throw std::runtime_error("error writing to std::ostream");
}

template<typename T, typename A>
template<typename SerDe>
std::pair<ebpps_sample<T, A>, size_t> ebpps_sample<T, A>::deserialize(const uint8_t* ptr, size_t size, const SerDe& sd, const A& allocator) {
  const uint8_t* st_ptr = ptr;
  const uint8_t* end_ptr = ptr + size;

  ensure_minimum_memory(size, sizeof(double));
  double c;
  ptr += copy_from_mem(ptr, c);
  if (c < 0.0)
    throw std::runtime_error("sketch image has C < 0.0 during deserializaiton");

  double c_int;
  const double c_frac = std::modf(c, &c_int);
  const bool has_partial = c_frac != 0.0;

  const uint32_t num_full_items = static_cast<uint32_t>(c_int);
  A alloc(allocator);
  std::unique_ptr<T, items_deleter> items(alloc.allocate(num_full_items), items_deleter(allocator, false, num_full_items));
  ptr += sd.deserialize(ptr, end_ptr - ptr, items.get(), num_full_items);
  // serde did not throw, enable destructors
  items.get_deleter().set_destroy(true);
  std::vector<T, A> data(std::make_move_iterator(items.get()),
                         std::make_move_iterator(items.get() + num_full_items),
                         allocator);

  optional<T> partial_item;
  if (has_partial) {
    optional<T> tmp; // space to deserialize
    ptr += sd.deserialize(ptr, end_ptr - ptr, &*tmp, 1);
    // serde did not throw so place item and clean up
    partial_item.emplace(*tmp);
    (*tmp).~T();
  }

  return std::pair<ebpps_sample<T,A>, size_t>(
    ebpps_sample<T,A>(std::move(data), std::move(partial_item), c, allocator),
    ptr - st_ptr);
}

template<typename T, typename A>
template<typename SerDe>
ebpps_sample<T, A> ebpps_sample<T, A>::deserialize(std::istream& is, const SerDe& sd, const A& allocator) {
  const double c = read<double>(is);
  if (c < 0.0)
    throw std::runtime_error("sketch image has C < 0.0 during deserializaiton");

  double c_int;
  const double c_frac = std::modf(c, &c_int);
  const bool has_partial = c_frac != 0.0;

  const uint32_t num_full_items = static_cast<uint32_t>(c_int);
  A alloc(allocator);
  std::unique_ptr<T, items_deleter> items(alloc.allocate(num_full_items), items_deleter(allocator, false, num_full_items));
  sd.deserialize(is, items.get(), num_full_items);
  // serde did not throw, enable destructors
  items.get_deleter().set_destroy(true);
  std::vector<T, A> data(std::make_move_iterator(items.get()),
                         std::make_move_iterator(items.get() + num_full_items),
                         allocator);

  optional<T> partial_item;
  if (has_partial) {
    optional<T> tmp; // space to deserialize
    sd.deserialize(is, &*tmp, 1);
    // serde did not throw so place item and clean up
    partial_item.emplace(*tmp);
    (*tmp).~T();
  }

  if (!is.good()) throw std::runtime_error("error reading from std::istream");

  return ebpps_sample<T,A>(std::move(data), std::move(partial_item), c, allocator);
}


template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sample<T, A>::begin() const {
  return const_iterator(this);
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sample<T, A>::end() const {
  return const_iterator(nullptr);
}


// -------- ebpps_sketch::const_iterator implementation ---------

template<typename T, typename A>
ebpps_sample<T, A>::const_iterator::const_iterator(const ebpps_sample* sample) :
  sample_(sample),
  idx_(0),
  use_partial_(false)
{
  if (sample == nullptr)
    return;

  // determine in advance if we use the partial item
  double c_int;
  const double c_frac = std::modf(sample_->get_c(), &c_int);
  use_partial_ = sample->next_double() < c_frac;

  // sample with no items
  if (sample_->data_.size() == 0 && use_partial_) {
    idx_ = PARTIAL_IDX;
  }

  if (sample_->c_== 0.0 || (sample_->data_.size() == 0 && !sample_->has_partial_item())) { sample_ = nullptr; }
}

template<typename T, typename A>
ebpps_sample<T, A>::const_iterator::const_iterator(const const_iterator& other) :
  sample_(other.sample_),
  idx_(other.idx_),
  use_partial_(other.use_partial_)
{}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator& ebpps_sample<T, A>::const_iterator::operator++() {
  if (sample_ == nullptr)
    return *this;
  else if (idx_ == PARTIAL_IDX) {
    idx_ = sample_->data_.size();
    sample_ = nullptr;
    return * this;
  }
 
  ++idx_;

  if (idx_ == sample_->data_.size()) {
    if (use_partial_)
      idx_ = PARTIAL_IDX;
    else
      sample_ = nullptr;
  }

  return *this;
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator& ebpps_sample<T, A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename A>
bool ebpps_sample<T, A>::const_iterator::operator==(const const_iterator& other) const {
  if (sample_ != other.sample_) return false;
  if (sample_ == nullptr) return true; // end (and we know other.sample_ is also null)
  return idx_ == other.idx_;
}

template<typename T, typename A>
bool ebpps_sample<T, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename A>
auto ebpps_sample<T, A>::const_iterator::operator*() const -> reference {
  if (idx_ == PARTIAL_IDX)
    return *(sample_->partial_item_);
  else
    return sample_->data_[idx_];
}

template<typename T, typename A>
auto ebpps_sample<T, A>::const_iterator::operator->() const -> pointer {
  return **this;
}

template<typename T, typename A>
class ebpps_sample<T, A>::items_deleter {
  public:
  items_deleter(const A& allocator, bool destroy, size_t num): allocator_(allocator), destroy_(destroy), num_(num) {}
  void operator() (T* ptr) {
    if (ptr != nullptr) {
      if (destroy_) {
        for (size_t i = 0; i < num_; ++i) {
          ptr[i].~T();
        }
      }
      allocator_.deallocate(ptr, num_);
    }
  }
  void set_destroy(bool destroy) { destroy_ = destroy; }
  private:
  A allocator_;
  bool destroy_;
  size_t num_;
};


} // namespace datasketches

#endif // _EBPPS_SAMPLE_IMPL_HPP_
