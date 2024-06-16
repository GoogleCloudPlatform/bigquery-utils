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

#ifndef _EBPPS_SAMPLE_HPP_
#define _EBPPS_SAMPLE_HPP_

#include "common_defs.hpp"
#include "optional.hpp"
#include "serde.hpp"

#include <memory>
#include <vector>

namespace datasketches {

template<
  typename T,
  typename A = std::allocator<T>
>
class ebpps_sample {
  public:
    explicit ebpps_sample(uint32_t k, const A& allocator = A());

    // for deserialization
    class items_deleter;
    ebpps_sample(std::vector<T, A>&& data, optional<T>&& partial_item, double c, const A& allocator = A());

    // used instead of having a single-item constructor for update/merge calls
    template<typename TT>
    void replace_content(TT&& item, double theta);

    void reset();
    void downsample(double theta);

    template<typename FwdSample>
    void merge(FwdSample&& other);

    // standard way to query the sample
    using result_type = std::vector<T, A>;
    result_type get_sample() const;

    double get_c() const;
    
    // intended for internal use
    // returns only full items
    result_type get_full_items() const;

    // intended for internal use
    // handles only the partial item
    bool has_partial_item() const;
    T get_partial_item() const;
        
    string<A> to_string() const;

    /**
     * @brief Returns the number of items contained in the sample
     * Computes the number of items, full or partial, currently in the sample.
     * The result should match ceiling(c);
     * @return the number of items contained in the sample
     */
    inline uint32_t get_num_retained_items() const;

    /**
     * Computes size needed to serialize the current state of the sample.
     * This version is for fixed-size arithmetic types (integral and floating point).
     * @param sd instance of a SerDe
     * @return size in bytes needed to serialize the items in this sample
     */
    template<typename TT = T, typename SerDe = serde<T>, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
    inline size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    /**
     * Computes size needed to serialize the items in the sample.
     * This version is for all other types and can be expensive since every item needs to be looked at.
     * @param sd instance of a SerDe
     * @return size in bytes needed to serialize the items in this sample
     */
    template<typename TT = T, typename SerDe = serde<T>, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
    inline size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

    // This is a convenience alias for users
    // The type returned by the following serialize method
    using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<A>::template rebind_alloc<uint8_t>>;

    /**
     * This method serializes the sample as a vector of bytes.
     * @param ptr pointer to start of pre-allocated memory to use
     * @param end_ptr pointer to end of pre-allocated memory to use
     * @param sd instance of a SerDe
     * @return number of bytes written
     */
    template<typename SerDe = serde<T>>
    size_t serialize(uint8_t* ptr, const uint8_t* end_ptr, const SerDe& sd = SerDe()) const;

    /**
     * This method serializes the sample into a given stream in a binary form
     * @param os output stream
     * @param sd instance of a SerDe
     */
    template<typename SerDe = serde<T>>
    void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

    /**
     * This method deserializes a sample from a given array of bytes.
     * @param ptr pointer to the array of bytes
     * @param size the size of the array
     * @param sd instance of a SerDe
     * @param allocator instance of an allocator
     * @return a std::pair with a sample and the number of bytes read
     */
    template<typename SerDe = serde<T>>
    static std::pair<ebpps_sample, size_t> deserialize(const uint8_t* ptr, size_t size, const SerDe& sd = SerDe(), const A& allocator = A());

    /**
     * This method deserializes a sample from a given stream.
     * @param is input stream
     * @param sd instance of a SerDe
     * @param allocator instance of an allocator
     * @return an instance of a sample
     */
    template<typename SerDe = serde<T>>
    static ebpps_sample deserialize(std::istream& is, const SerDe& sd = SerDe(), const A& allocator = A());

    class const_iterator;

    /**
     * Iterator pointing to the first item in the sample.
     * If the sample is empty, the returned iterator must not be dereferenced or incremented
     * @param force_partial forces the inclusion of the partial item, if one exists
     * @return iterator pointing to the first item in the sample
     */
    const_iterator begin() const;

    /**
     * Iterator pointing to the past-the-end item in the sample.
     * The past-the-end item is the hypothetical item that would follow the last item.
     * It does not point to any item, and must not be dereferenced or incremented.
     * @return iterator pointing to the past-the-end item in the sample
     */
    const_iterator end() const;

  private:
    A allocator_;
    double c_;                      // Current sample size, including fractional part
    optional<T> partial_item_;      // a sample item corresponding to a partial weight
    std::vector<T, A> data_;           // stored sampled items

    template<typename FwdItem>
    inline void set_partial(FwdItem&& item);
    void swap_with_partial();
    void move_one_to_partial();
    void subsample(uint32_t num_samples);

    static inline uint32_t random_idx(uint32_t max);
    static inline double next_double();

    friend class const_iterator;
};

template<typename T, typename A>
class ebpps_sample<T, A>::const_iterator {
public:
  using iterator_category = std::input_iterator_tag;
  using value_type = const T&;
  using difference_type = void;
  using pointer = const return_value_holder<value_type>;
  using reference = value_type;

  const_iterator(const const_iterator& other);
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  reference operator*() const;
  pointer operator->() const;

private:
  static const size_t PARTIAL_IDX = static_cast<size_t>(-1);

  // default iterator over sample
  const_iterator(const ebpps_sample<T, A>* sample);

  const ebpps_sample<T, A>* sample_;
  size_t idx_;
  bool use_partial_;

  friend class ebpps_sample;
};

} // namespace datasketches

#include "ebpps_sample_impl.hpp"

#endif // _EBPPS_SAMPLE_HPP_
