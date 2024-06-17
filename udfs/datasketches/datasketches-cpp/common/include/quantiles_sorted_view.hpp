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

#ifndef QUANTILES_SORTED_VIEW_HPP_
#define QUANTILES_SORTED_VIEW_HPP_

#include <functional>
#include <cmath>

#include "common_defs.hpp"

namespace datasketches {

/**
 * Sorted view for quantiles sketches (REQ, KLL and Quantiles)
 */
template<
  typename T,
  typename Comparator, // strict weak ordering function (see C++ named requirements: Compare)
  typename Allocator
>
class quantiles_sorted_view {
public:
  /// Entry type
  using Entry = typename std::conditional<std::is_arithmetic<T>::value, std::pair<T, uint64_t>, std::pair<const T*, uint64_t>>::type;
  using AllocEntry = typename std::allocator_traits<Allocator>::template rebind_alloc<Entry>;
  using Container = std::vector<Entry, AllocEntry>;

  /// @private
  quantiles_sorted_view(uint32_t num, const Comparator& comparator, const Allocator& allocator);

  /// @private
  template<typename Iterator>
  void add(Iterator begin, Iterator end, uint64_t weight);

  /// @private
  void convert_to_cummulative();

  class const_iterator;

  /**
   * Iterator pointing to the first entry in the view.
   * If the view is empty, the returned iterator must not be dereferenced or incremented.
   * @return iterator pointing to the first entry
   */
  const_iterator begin() const;

  /**
   * Iterator pointing to the past-the-end entry in the view.
   * The past-the-end entry is the hypothetical entry that would follow the last entry.
   * It does not point to any entry, and must not be dereferenced or incremented.
   * @return iterator pointing to the past-the-end entry
   */
  const_iterator end() const;

  /// @return size of the view
  size_t size() const;

  /**
   * Returns an approximation to the normalized rank of the given item.
   *
   * <p>If the view is empty this throws std::runtime_error.
   *
   * @param item to be ranked
   * @param inclusive if true the weight of the given item is included into the rank.
   * Otherwise the rank equals the sum of the weights of all items that are less than the given item
   * according to the Comparator.
   *
   * @return an approximate normalized rank of the given item (0 to 1 inclusive)
   */
  double get_rank(const T& item, bool inclusive = true) const;

  /**
   * Quantile return type.
   * This is to return quantiles either by value (for arithmetic types) or by const reference (for all other types)
   */
  using quantile_return_type = typename std::conditional<std::is_arithmetic<T>::value, T, const T&>::type;

  /**
   * Returns an item from the sketch that is the best approximation to an item
   * from the original stream with the given normalized rank.
   *
   * <p>If the view is empty this throws std::runtime_error.
   *
   * @param rank of an item in the hypothetical sorted stream.
   * @param inclusive if true, the given rank is considered inclusive (includes weight of an item)
   *
   * @return approximate quantile associated with the given normalized rank
   */
  quantile_return_type get_quantile(double rank, bool inclusive = true) const;

  using vector_double = std::vector<double, typename std::allocator_traits<Allocator>::template rebind_alloc<double>>;

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of split points (items).
   *
   * <p>If the view is empty this throws std::runtime_error.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing items
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   *
   * @param size the number of split points in the array
   *
   * @param inclusive if true the rank of an item includes its own weight, and therefore
   * if the sketch contains items equal to a slit point, then in CDF such items are
   * included into the interval to the left of split point. Otherwise they are included into
   * the interval to the right of split point.
   *
   * @return an array of m+1 doubles, which are a consecutive approximation to the CDF
   * of the input stream given the split_points. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array. This can be viewed as array of ranks of the given split points plus one more value
   * that is always 1.
   */
  vector_double get_CDF(const T* split_points, uint32_t size, bool inclusive = true) const;

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of split points (items).
   *
   * <p>If the view is empty this throws std::runtime_error.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing items
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals (bins).
   *
   * @param size the number of split points in the array
   *
   * @param inclusive if true the rank of an item includes its own weight, and therefore
   * if the sketch contains items equal to a slit point, then in PMF such items are
   * included into the interval to the left of split point. Otherwise they are included into the interval
   * to the right of split point.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream items (the mass) that fall into one of those intervals.
   */
  vector_double get_PMF(const T* split_points, uint32_t size, bool inclusive = true) const;

private:
  Comparator comparator_;
  uint64_t total_weight_;
  Container entries_;

  static inline const T& deref_helper(const T* t) { return *t; }
  static inline T deref_helper(T t) { return t; }

  struct compare_pairs_by_first {
    explicit compare_pairs_by_first(const Comparator& comparator): comparator_(comparator) {}
    bool operator()(const Entry& a, const Entry& b) const {
      return comparator_(deref_helper(a.first), deref_helper(b.first));
    }
    Comparator comparator_;
  };

  struct compare_pairs_by_second {
    bool operator()(const Entry& a, const Entry& b) const {
      return a.second < b.second;
    }
  };

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  static inline T ref_helper(const T& t) { return t; }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  static inline const T* ref_helper(const T& t) { return std::addressof(t); }

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  static inline Entry make_dummy_entry(uint64_t weight) { return Entry(0, weight); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  static inline Entry make_dummy_entry(uint64_t weight) { return Entry(nullptr, weight); }

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline void check_split_points(const T* items, uint32_t size) {
    for (uint32_t i = 0; i < size ; i++) {
      if (std::isnan(items[i])) {
        throw std::invalid_argument("Values must not be NaN");
      }
      if ((i < (size - 1)) && !(Comparator()(items[i], items[i + 1]))) {
        throw std::invalid_argument("Values must be unique and monotonically increasing");
      }
    }
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline void check_split_points(const T* items, uint32_t size) {
    for (uint32_t i = 0; i < size ; i++) {
      if ((i < (size - 1)) && !(Comparator()(items[i], items[i + 1]))) {
        throw std::invalid_argument("Items must be unique and monotonically increasing");
      }
    }
  }
};

template<typename T, typename C, typename A>
class quantiles_sorted_view<T, C, A>::const_iterator: public quantiles_sorted_view<T, C, A>::Container::const_iterator {
public:
  using Base = typename quantiles_sorted_view<T, C, A>::Container::const_iterator;
  using value_type = typename std::conditional<std::is_arithmetic<T>::value, typename Base::value_type, std::pair<const T&, const uint64_t>>::type;

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type operator*() const { return Base::operator*(); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type operator*() const { return value_type(*(Base::operator*().first), Base::operator*().second); }

  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  const value_type* operator->() const { return Base::operator->(); }

  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  const return_value_holder<value_type> operator->() const { return **this; }

  uint64_t get_weight() const {
    if (*this == begin) return Base::operator*().second;
    return Base::operator*().second - (*this - 1).operator*().second;
  }

  uint64_t get_cumulative_weight(bool inclusive = true) const {
    return inclusive ? Base::operator*().second : Base::operator*().second - get_weight();
  }

private:
  Base begin;

  friend class quantiles_sorted_view<T, C, A>;
  const_iterator(const Base& it, const Base& begin): Base(it), begin(begin) {}
};

} /* namespace datasketches */

#include "quantiles_sorted_view_impl.hpp"

#endif
