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

#ifndef _EBPPS_SKETCH_HPP_
#define _EBPPS_SKETCH_HPP_

#include "common_defs.hpp"
#include "ebpps_sample.hpp"
#include "optional.hpp"
#include "serde.hpp"

#include <random>
#include <string>
#include <vector>

namespace datasketches {

/// EBPPS sketch constants
namespace ebpps_constants {
  /// maximum value of parameter K
  const uint32_t MAX_K = ((uint32_t) 1 << 31) - 2;
}

/**
 * An implementation of an Exact and Bounded Sampling Proportional to Size sketch.
 * 
 * From: "Exact PPS Sampling with Bounded Sample Size",
 * B. Hentschel, P. J. Haas, Y. Tian. Information Processing Letters, 2023.
 * 
 * This sketch samples data from a stream of items proportional to the weight of each item.
 * The sample guarantees the presence of an item in the result is proportional to that item's
 * portion of the total weight seen by the sketch, and returns a sample no larger than size k.
 * 
 * The sample may be smaller than k and the resulting size of the sample potentially includes
 * a probabilistic component, meaning the resulting sample size is not always constant.
 *
 * @author Jon Malkin
 */
template<
  typename T,
  typename A = std::allocator<T>
>
class ebpps_sketch {

  public:
    static const uint32_t MAX_K = ebpps_constants::MAX_K;

    /**
     * Constructor
     * @param k sketch size
     * @param allocator instance of an allocator
     */
    explicit ebpps_sketch(uint32_t k, const A& allocator = A());

    /**
     * Updates this sketch with the given data item with the given weight.
     * This method takes an lvalue.
     * @param item an item from a stream of items
     * @param weight the weight of the item
     */
    void update(const T& item, double weight = 1.0);

    /**
     * Updates this sketch with the given data item with the given weight.
     * This method takes an rvalue.
     * @param item an item from a stream of items
     * @param weight the weight of the item
     */
    void update(T&& item, double weight = 1.0);

    /**
     * Merges the provided sketch into the current one.
     * This method takes an lvalue.
     * @param sketch the sketch to merge into the current object
     */
    void merge(const ebpps_sketch<T, A>& sketch);

    /**
     * Merges the provided sketch into the current one.
     * This method takes an rvalue.
     * @param sketch the sketch to merge into the current object
     */
    void merge(ebpps_sketch<T, A>&& sketch);

    using result_type = typename ebpps_sample<T,A>::result_type;

    /**
     * @brief Returns a copy of the current sample, as a std::vector
     */
    result_type get_result() const;

    /**
     * Returns the configured maximum sample size.
     * @return configured maximum sample size
     */
    inline uint32_t get_k() const;

    /**
     * Returns the number of items processed by the sketch, regardless of
     * item weight.
     * @return count of items processed by the sketch
     */
    inline uint64_t get_n() const;

    /**
     * Returns the cumulative weight of items processed by the sketch.
     * @return cumulative weight of items seen
     */
    inline double get_cumulative_weight() const;

    /**
     * Returns the expected number of samples returned upon a call to
     * get_result() or the creation of an iterator. The number is a
     * floating point value, where the fractional portion represents
     * the probability of including a "partial item" from the sample.
     * 
     * The value C should be no larger than the sketch's configured
     * value of k, although numerical precision limitations mean it
     * may exceed k by double precision floating point error margins
     * in certain cases.
     * @return The expected number of samples returned when querying the sketch
     */
    inline double get_c() const;
    
    /**
     * Returns true if the sketch is empty.
     * @return empty flag
     */
    inline bool is_empty() const;
    
    /**
     * Returns an instance of the allocator for this sketch.
     * @return allocator
     */
    A get_allocator() const;

    /**
     * Resets the sketch to its default, empty state.
     */
    void reset();

    /**
     * Computes size needed to serialize the current state of the sketch.
     * @param sd instance of a SerDe
     * @return size in bytes needed to serialize this sketch
     */
    template<typename SerDe = serde<T>>
    inline size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    // This is a convenience alias for users
    // The type returned by the following serialize method
    using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<A>::template rebind_alloc<uint8_t>>;

    /**
     * This method serializes the sketch as a vector of bytes.
     * An optional header can be reserved in front of the sketch.
     * It is a blank space of a given size.
     * This header is used in Datasketches PostgreSQL extension.
     * @param header_size_bytes space to reserve in front of the sketch
     * @param sd instance of a SerDe
     */
    template<typename SerDe = serde<T>>
    vector_bytes serialize(unsigned header_size_bytes = 0, const SerDe& sd = SerDe()) const;

    /**
     * This method serializes the sketch into a given stream in a binary form
     * @param os output stream
     * @param sd instance of a SerDe
     */
    template<typename SerDe = serde<T>>
    void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

    /**
     * This method deserializes a sketch from a given array of bytes.
     * @param bytes pointer to the array of bytes
     * @param size the size of the array
     * @param sd instance of a SerDe
     * @param allocator instance of an allocator
     * @return an instance of a sketch
     */
    template<typename SerDe = serde<T>>
    static ebpps_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(), const A& allocator = A());

    /**
     * This method deserializes a sketch from a given stream.
     * @param is input stream
     * @param sd instance of a SerDe
     * @param allocator instance of an allocator
     * @return an instance of a sketch
     */
    template<typename SerDe = serde<T>>
    static ebpps_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(), const A& allocator = A());

    /**
     * Prints a summary of the sketch.
     * @return the summary as a string
     */
    string<A> to_string() const;

    /**
     * Prints the raw sketch items to a string.
     * Only works for type T with a defined
     * std::ostream& operator<<(std::ostream&, const T&) and is
     * kept separate from to_string() to allow compilation even if
     * T does not have such an operator defined.
     * @return a string with the sketch items
     */
    string<A> items_to_string() const;

    /**
     * Iterator pointing to the first item in the sketch.
     * If the sketch is empty, the returned iterator must not be dereferenced or incremented.
     * @return iterator pointing to the first item in the sketch
     */
    typename ebpps_sample<T,A>::const_iterator begin() const;

    /**
     * Iterator pointing to the past-the-end item in the sketch.
     * The past-the-end item is the hypothetical item that would follow the last item.
     * It does not point to any item, and must not be dereferenced or incremented.
     * @return iterator pointing to the past-the-end item in the sketch
     */
    typename ebpps_sample<T,A>::const_iterator end() const;

  private:
    static const uint8_t PREAMBLE_LONGS_EMPTY  = 1;
    static const uint8_t PREAMBLE_LONGS_FULL   = 5; // C is part of sample_
    static const uint8_t SER_VER = 1;
    static const uint8_t FAMILY_ID  = 19;
    static const uint8_t EMPTY_FLAG_MASK  = 4;
    static const uint8_t HAS_PARTIAL_ITEM_MASK = 8;

    A allocator_;
    uint32_t k_;                    // max size of sketch, in items
    uint64_t n_;                    // total number of items processed by the sketch

    double cumulative_wt_;          // total weight of items processed by the sketch
    double wt_max_;                 // maximum weight seen so far
    double rho_;                    // latest scaling parameter for downsampling

    ebpps_sample<T,A> sample_;      // Object holding the current state of the sample

    ebpps_sample<T,A> tmp_;         // Temporary sample of size 1 used in updates

    // handles merge after ensuring other.cumulative_wt_ <= this->cumulative_wt_
    // so we can send items in individually
    template<typename O>
    void internal_merge(O&& other);

    ebpps_sketch(uint32_t k, uint64_t n, double cumulative_wt, double wt_max, double rho,
                 ebpps_sample<T,A>&& sample, const A& allocator = A());

    template<typename FwdItem>
    inline void internal_update(FwdItem&& item, double weight);

    // validation
    static uint32_t check_k(uint32_t k);
    static void check_preamble_longs(uint8_t preamble_longs, uint8_t flags);
    static void check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver);
    static uint32_t validate_and_get_target_size(uint32_t preamble_longs, uint32_t k, uint64_t n);
};

} // namespace datasketches

#include "ebpps_sketch_impl.hpp"

#endif // _EBPPS_SKETCH_HPP_
