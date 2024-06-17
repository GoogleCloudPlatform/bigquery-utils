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

#ifndef COUNT_MIN_HPP_
#define COUNT_MIN_HPP_

#include <iterator>
#include "common_defs.hpp"

namespace datasketches {

/**
 * C++ implementation of the CountMin sketch data structure of Cormode and Muthukrishnan.
 * [1] - http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
 * The template type W is the type of the vector that contains the weights of the objects inserted into the sketch,
 * not the type of the input items themselves.
 * @author Charlie Dickens
 */
template <typename W,
          typename Allocator = std::allocator<W>>
class count_min_sketch{
  static_assert(std::is_arithmetic<W>::value, "Arithmetic type expected");
public:
  using allocator_type = Allocator;
  using const_iterator = typename std::vector<W, Allocator>::const_iterator;

  /**
   * Creates an instance of the sketch given parameters _num_hashes, _num_buckets and hash seed, `seed`.
   * @param num_hashes number of hash functions in the sketch. Equivalently the number of rows in the array
   * @param num_buckets number of buckets that hash functions map into. Equivalently the number of columns in the array
   * @param seed for hash function
   * @param allocator to acquire and release memory
   *
   * The items inserted into the sketch can be arbitrary type, so long as they are hashable via murmurhash.
   * Only update and estimate methods are added for uint64_t and string types.
   */
  count_min_sketch(uint8_t num_hashes, uint32_t num_buckets, uint64_t seed = DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
   * @return configured _num_hashes of this sketch
   */
  uint8_t get_num_hashes() const;

  /**
   * @return configured _num_buckets of this sketch
   */
  uint32_t get_num_buckets() const;

  /**
   * @return configured seed of this sketch
   */
  uint64_t get_seed()  const;

  /**
   * @return epsilon
   * The maximum permissible error for any frequency estimate query.
   * epsilon = ceil(e / _num_buckets)
   */
   double get_relative_error() const;

  /**
   * @return _total_weight
   * The total weight currently inserted into the stream.
   */
  W get_total_weight() const;

  /**
   * Suggests the number of buckets required to achieve the given relative error
   * @param relative_error the desired accuracy within which estimates should lie.
   * For example, when relative_error = 0.05, then the returned frequency estimates satisfy the
   * `relative_error` guarantee that never overestimates the weights but may underestimate the weights
   * by 5% of the total weight in the sketch.
   * @return the number of hash buckets at every level of the
   * sketch required in order to obtain the specified relative error.
   * [1] - Section 3 ``Data Structure'', page 6.
   */
  static uint32_t suggest_num_buckets(double relative_error);

  /**
   * Suggests the number of hash functions required to achieve the given confidence
   * @param confidence the desired confidence with which estimates should be correct.
   * For example, with 95% confidence, frequency estimates satisfy the `relative_error` guarantee.
   * @return the number of hash functions that are required in
   * order to achieve the specified confidence of the sketch.
   * confidence = 1 - delta, with delta denoting the sketch failure probability in the literature.
   * [1] - Section 3 ``Data Structure'', page 6.
   */
  static uint8_t suggest_num_hashes(double confidence);

  /**
   * Specific get_estimate function for uint64_t type
   * see generic get_estimate function
   * @param item uint64_t type.
   * @return an estimate of the item's frequency.
   */
  W get_estimate(uint64_t item) const;

  /**
   * Specific get_estimate function for int64_t type
   * see generic get_estimate function
   * @param item int64_t type.
   * @return an estimate of the item's frequency.
   */
  W get_estimate(int64_t item) const;

  /**
   * Specific get_estimate function for std::string type
   * see generic get_estimate function
   * @param item std::string type
   * @return an estimate of the item's frequency.
   */
  W get_estimate(const std::string& item) const;

  /**
   * This is the generic estimate query function for any of the given datatypes.
   * Query the sketch for the estimate of a given item.
   * @param item pointer to the data item to be query from the sketch.
   * @param size size of the item in bytes
   * @return the estimated frequency of the item denoted f_est satisfying
   * f_true - relative_error*_total_weight <= f_est <= f_true
   */
  W get_estimate(const void* item, size_t size) const;

  /**
   * Query the sketch for the upper bound of a given item.
   * @param item to query
   * @param size of the item in bytes
   * @return the upper bound on the true frequency of the item
   * f_true <= f_est + relative_error*_total_weight
   */
  W get_upper_bound(const void* item, size_t size) const;

  /**
   * Query the sketch for the upper bound of a given item.
   * @param item to query
   * @return the upper bound on the true frequency of the item
   * f_true <= f_est + relative_error*_total_weight
   */
  W get_upper_bound(int64_t item) const;

  /**
   * Query the sketch for the upper bound of a given item.
   * @param item to query
   * @return the upper bound on the true frequency of the item
   * f_true <= f_est + relative_error*_total_weight
   */
  W get_upper_bound(uint64_t item) const;

  /**
   * Query the sketch for the upper bound of a given item.
   * @param item to query
   * @return the upper bound on the true frequency of the item
   * f_true <= f_est + relative_error*_total_weight
   */
  W get_upper_bound(const std::string& item) const;

  /**
   * Query the sketch for the lower bound of a given item.
   * @param item to query
   * @param size of the item in bytes
   * @return the lower bound for the query result, f_est, on the true frequency, f_est of the item
   * f_true - relative_error*_total_weight <= f_est
   */
  W get_lower_bound(const void* item, size_t size) const;

  /**
   * Query the sketch for the lower bound of a given item.
   * @param item to query
   * @return the lower bound for the query result, f_est, on the true frequency, f_est of the item
   * f_true - relative_error*_total_weight <= f_est
   */
  W get_lower_bound(int64_t item) const;

  /**
   * Query the sketch for the lower bound of a given item.
   * @param item to query
   * @return the lower bound for the query result, f_est, on the true frequency, f_est of the item
   * f_true - relative_error*_total_weight <= f_est
   */
  W get_lower_bound(uint64_t item) const;

  /**
   * Query the sketch for the lower bound of a given item.
   * @param item to query
   * @return the lower bound for the query result, f_est, on the true frequency, f_est of the item
   * f_true - relative_error*_total_weight <= f_est
   */
  W get_lower_bound(const std::string& item) const;

  /**
   * Update this sketch with given data of any type.
   * This is a "universal" update that covers all cases,
   * but may produce different hashes compared to specialized update methods.
   * @param item pointer to the data item to be inserted into the sketch.
   * @param size of the data in bytes
   * @param weight arithmetic type
   */
  void update(const void* item, size_t size, W weight);

  /**
   * Update this sketch with a given item.
   * @param item to update the sketch with
   * @param weight arithmetic type
   */
  void update(uint64_t item, W weight = 1);

  /**
   * Update this sketch with a given item.
   * @param item to update the sketch with
   * @param weight arithmetic type
   */
  void update(int64_t item, W weight = 1);

  /**
   * Update this sketch with a given string.
   * @param item string to update the sketch with
   * @param weight arithmetic type
   */
  void update(const std::string& item, W weight = 1);

  /**
   * Merges another count_min_sketch into this count_min_sketch.
   * @param other_sketch
   */
  void merge(const count_min_sketch& other_sketch);

  /**
   * Returns true if this sketch is empty.
   * A Count Min Sketch is defined to be empty iff weight == 0
   * This can only ever happen if all items inserted to the sketch have weights that cancel each other out.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * @brief Returns a string describing the sketch
   * @return A string with a human-readable description of the sketch
   */
  string<Allocator> to_string() const;

  /**
   * Iterator pointing to the first item in the sketch.
   * If the sketch is empty, the returned iterator must not be dereferenced or incremented.
   * @return iterator pointing to the first item in the sketch
   */
  const_iterator begin() const;

  /**
   * Iterator pointing to the past-the-end item in the sketch.
   * The past-the-end item is the hypothetical item that would follow the last item.
   * It does not point to any item, and must not be dereferenced or incremented.
   * @return iterator pointing to the past-the-end item in the sketch
   */
  const_iterator end() const;

  /*
   * The serialized sketch binary form has the following structure
   * Byte 0:
   * 1 - if and only if the sketch is empty
   * 0 - otherwise
   *
   * Byte 1 (serial version), byte 2 (family id), byte 3 (flags):
   * 00000001 - default for now.
   *
   * Bytes 4 - 7:
   * uint8_t zero corresponding to ``empty''
   *
   * Byte 8:
   * uint_8 for number of hash functions
   *
   * Bytes 9, 13
   * 4 bytes : uint32 for number of buckets.
   *
   * Bytes 14, 15:
   * seed_hash
   *
   * Byte 16:
   * uint8_t zero corresponding to ``empty''
   *
   * All remaining bytes from 17-24 follow the pattern of
   * Bytes 17-24:
   * Sketch array entry
   *

  0   ||    0   |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
      ||is_empty|ser__ver|familyId| flags  |xxxxxxxx|xxxxxxxx|xxxxxxxx|xxxxxxxx|

  1   ||    0   |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
      ||---------- _num_buckets -----------|num_hash|__seed__ __hash__|xxxxxxxx|

  2   ||    0   |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
      ||---------------------------- total  weight ----------------------------|

  3   ||    0   |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
      ||---------------------------- sketch entries ---------------------------|
 ...

   */

  
  /**
   * Computes size needed to serialize the current state of the sketch.
   * @return size in bytes needed to serialize this sketch
   */
  size_t get_serialized_size_bytes() const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is an uninitialized space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   */
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  /**
  * This method deserializes a sketch from a given stream.
  * @param is input stream
  * @param seed the seed for the hash function that was used to create the sketch
  * @param allocator instance of an Allocator
  * @return an instance of a sketch
  */
  static count_min_sketch deserialize(std::istream& is, uint64_t seed=DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
  * This method deserializes a sketch from a given array of bytes.
  * @param bytes pointer to the array of bytes
  * @param size the size of the array
  * @param seed the seed for the hash function that was used to create the sketch
  * @param allocator instance of an Allocator
  * @return an instance of the sketch
  */
  static count_min_sketch deserialize(const void* bytes, size_t size, uint64_t seed=DEFAULT_SEED, const Allocator& allocator = Allocator());

  /**
   * @return allocator
   */
  allocator_type get_allocator() const;

private:
  Allocator _allocator;
  uint8_t _num_hashes;
  uint32_t _num_buckets;
  std::vector<W, Allocator> _sketch_array; // the array stored by the sketch
  uint64_t _seed;
  W _total_weight;
  std::vector<uint64_t> hash_seeds;

  enum flags {IS_EMPTY};
  static const uint8_t PREAMBLE_LONGS_SHORT = 2; // Empty -> need second byte for sketch parameters
  static const uint8_t PREAMBLE_LONGS_FULL = 3; // Not empty -> need (at least) third byte for total weight.
  static const uint8_t SERIAL_VERSION_1 = 1;
  static const uint8_t FAMILY_ID = 18;
  static const uint8_t NULL_8 = 0;
  static const uint32_t NULL_32 = 0;

  /**
   * Throws an error if the header is not valid.
   * @param preamble_longs
   * @param serial_version
   * @param flags_byte
   */
  static void check_header_validity(uint8_t preamble_longs, uint8_t serial_version, uint8_t family_id, uint8_t flags_byte);

  /*
   * Obtain the hash values when inserting an item into the sketch.
   * @param item pointer to the data item to be inserted into the sketch.
   * @param size of the data in bytes
   * @return vector of uint64_t which each represent the index to which `value' must update in the sketch
   */
  std::vector<uint64_t> get_hashes(const void* item, size_t size) const;

};

} /* namespace datasketches */

#include "count_min_impl.hpp"

#endif
