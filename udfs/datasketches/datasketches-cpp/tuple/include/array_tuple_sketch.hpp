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

#ifndef ARRAY_TUPLE_SKETCH_HPP_
#define ARRAY_TUPLE_SKETCH_HPP_

#include <vector>
#include <memory>

#include "serde.hpp"
#include "tuple_sketch.hpp"

namespace datasketches {

// This simple array is faster than std::vector and should be sufficient for this application
template<typename T, typename Allocator = std::allocator<T>>
class array {
public:
  using value_type = T;
  using allocator_type = Allocator;

  explicit array(uint8_t size, T value, const Allocator& allocator = Allocator()):
  allocator_(allocator), size_(size), array_(allocator_.allocate(size_)) {
    std::fill(array_, array_ + size_, value);
  }
  array(const array& other):
    allocator_(other.allocator_),
    size_(other.size_),
    array_(allocator_.allocate(size_))
  {
    std::copy(other.array_, other.array_ + size_, array_);
  }
  array(array&& other) noexcept:
    allocator_(std::move(other.allocator_)),
    size_(other.size_),
    array_(other.array_)
  {
    other.array_ = nullptr;
  }
  ~array() {
    if (array_ != nullptr) allocator_.deallocate(array_, size_);
  }
  array& operator=(const array& other) {
    array copy(other);
    std::swap(allocator_, copy.allocator_);
    std::swap(size_, copy.size_);
    std::swap(array_, copy.array_);
    return *this;
  }
  array& operator=(array&& other) {
    std::swap(allocator_, other.allocator_);
    std::swap(size_, other.size_);
    std::swap(array_, other.array_);
    return *this;
  }
  T& operator[](size_t index) { return array_[index]; }
  T operator[](size_t index) const { return array_[index]; }
  uint8_t size() const { return size_; }
  T* data() { return array_; }
  const T* data() const { return array_; }
  bool operator==(const array& other) const {
    for (uint8_t i = 0; i < size_; ++i) if (array_[i] != other.array_[i]) return false;
    return true;
  }
private:
  Allocator allocator_;
  uint8_t size_;
  T* array_;
};

/// default array tuple update policy
template<typename Array, typename Allocator = typename Array::allocator_type>
class default_array_tuple_update_policy {
public:
  default_array_tuple_update_policy(uint8_t num_values = 1, const Allocator& allocator = Allocator()):
    allocator_(allocator), num_values_(num_values) {}
  Array create() const {
    return Array(num_values_, 0, allocator_);
  }
  template<typename InputArray> // to allow any type with indexed access (such as double* or std::vector)
  void update(Array& array, const InputArray& update) const {
    for (uint8_t i = 0; i < num_values_; ++i) array[i] += update[i];
  }
  uint8_t get_num_values() const {
    return num_values_;
  }

private:
  Allocator allocator_;
  uint8_t num_values_;
};

// forward declaration
template<typename Array, typename Allocator> class compact_array_tuple_sketch;

/**
 * Update array tuple sketch.
 * This is a wrapper around tuple sketch to match the functionality and serialization format of ArrayOfDoublesSketch in Java.
 * For this the sketch must be configured with array<double> or std::vector<double>.
 * This is a more generic implementation for any arithmetic type (serialization assumes contiguous array size_of(T) * num_values).
 * A set of type definitions for the ArrayOfDoubles* equivalent is provided in a separate file array_of_doubles_sketch.hpp.
 * There is no constructor. Use builder instead.
 */
template<
  typename Array,
  typename Policy = default_array_tuple_update_policy<Array>,
  typename Allocator = typename Array::allocator_type
>
class update_array_tuple_sketch: public update_tuple_sketch<Array, Array, Policy, Allocator> {
public:
  using Base = update_tuple_sketch<Array, Array, Policy, Allocator>;
  using resize_factor = typename Base::resize_factor;

  class builder;

  compact_array_tuple_sketch<Array, Allocator> compact(bool ordered = true) const;

  /// @return number of values in array
  uint8_t get_num_values() const;

private:
  // for builder
  update_array_tuple_sketch(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t theta,
      uint64_t seed, const Policy& policy, const Allocator& allocator);
};

/// Update array tuple sketch builder
template<typename Array, typename Policy, typename Allocator>
class update_array_tuple_sketch<Array, Policy, Allocator>::builder: public tuple_base_builder<builder, Policy, Allocator> {
public:
  /**
   * Constructor
   * @param policy
   * @param allocator
   */
  builder(const Policy& policy = Policy(), const Allocator& allocator = Allocator());

  /// @return instance of sketch
  update_array_tuple_sketch build() const;
};

/// Compact array tuple sketch
template<
  typename Array,
  typename Allocator = typename Array::allocator_type
>
class compact_array_tuple_sketch: public compact_tuple_sketch<Array, Allocator> {
public:
  using Base = compact_tuple_sketch<Array, Allocator>;
  using Entry = typename Base::Entry;
  using AllocEntry = typename Base::AllocEntry;
  using AllocU64 = typename Base::AllocU64;
  using vector_bytes = typename Base::vector_bytes;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t SKETCH_FAMILY = 9;
  static const uint8_t SKETCH_TYPE = 3;
  enum flags { UNUSED1, UNUSED2, IS_EMPTY, HAS_ENTRIES, IS_ORDERED };

  /**
   * Copy constructor.
   * Constructs a compact sketch from another sketch (update or compact)
   * @param other sketch to be constructed from
   * @param ordered if true make the resulting sketch ordered
   */
  template<typename Sketch>
  compact_array_tuple_sketch(const Sketch& other, bool ordered = true);

  /// @return number of double values in array
  uint8_t get_num_values() const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is a blank space of a given size.
   * @param header_size_bytes space to reserve in front of the sketch
   * @return serialized sketch as a vector of bytes
   */
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the sketch
   * @param allocator instance of an Allocator
   * @return an instance of the sketch
   */
  static compact_array_tuple_sketch deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the sketch
   * @param allocator instance of an Allocator
   * @return an instance of the sketch
   */
  static compact_array_tuple_sketch deserialize(const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED,
      const Allocator& allocator = Allocator());

private:
  uint8_t num_values_;

  template<typename Ar, typename P, typename Al> friend class array_tuple_union;
  template<typename Ar, typename P, typename Al> friend class array_tuple_intersection;
  template<typename Ar, typename Al> friend class array_tuple_a_not_b;
  compact_array_tuple_sketch(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries, uint8_t num_values);
  compact_array_tuple_sketch(uint8_t num_values, Base&& base);
};

} /* namespace datasketches */

#include "array_tuple_sketch_impl.hpp"

#endif
