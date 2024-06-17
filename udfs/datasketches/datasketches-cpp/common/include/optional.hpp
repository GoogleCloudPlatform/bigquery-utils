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

#ifndef _OPTIONAL_HPP_
#define _OPTIONAL_HPP_

// This is a simplistic substitute for std::optional until we require C++17

#if (__cplusplus >= 201703L || (defined(_MSVC_LANG) && _MSVC_LANG >= 201703L))
#include <optional>
using std::optional;
#else

#include <type_traits>

namespace datasketches {

template<typename T>
class optional {
public:

  optional() noexcept: initialized_(false) {}

  optional(const T& value) noexcept(std::is_nothrow_copy_constructible<T>::value) {
    new (&value_) T(value);
    initialized_ = true;
  }

  optional(T&& value) noexcept(std::is_nothrow_move_constructible<T>::value) {
    new (&value_) T(std::move(value));
    initialized_ = true;
  }

  // conversion from compatible types
  template<typename TT>
  optional(const optional<TT>& other) noexcept(std::is_nothrow_constructible<T, TT>::value): initialized_(false) {
    if (other.initialized_) {
      new (&value_) T(other.value_);
      initialized_ = true;
    }
  }

  optional(const optional& other) noexcept(std::is_nothrow_copy_constructible<T>::value): initialized_(false) {
    if (other.initialized_) {
      new (&value_) T(other.value_);
      initialized_ = true;
    }
  }

  optional(optional&& other) noexcept(std::is_nothrow_move_constructible<T>::value): initialized_(false) {
    if (other.initialized_) {
      new (&value_) T(std::move(other.value_));
      initialized_ = true;
    }
  }

  ~optional() noexcept(std::is_nothrow_destructible<T>::value) {
     if (initialized_) value_.~T();
  }

  explicit operator bool() const noexcept {
    return initialized_;
  }

  optional& operator=(const optional& other)
      noexcept(std::is_nothrow_copy_constructible<T>::value && std::is_nothrow_copy_assignable<T>::value) {
    if (initialized_) {
      if (other.initialized_) {
        value_ = other.value_;
      } else {
        reset();
      }
    } else {
      if (other.initialized_) {
        new (&value_) T(other.value_);
        initialized_ = true;
      }
    }
    return *this;
  }

  optional& operator=(optional&& other)
      noexcept(std::is_nothrow_move_constructible<T>::value && std::is_nothrow_move_assignable<T>::value) {
    if (initialized_) {
      if (other.initialized_) {
        value_ = std::move(other.value_);
      } else {
        reset();
      }
    } else {
      if (other.initialized_) {
        new (&value_) T(std::move(other.value_));
        initialized_ = true;
      }
    }
    return *this;
  }

  template<typename... Args>
  void emplace(Args&&... args) noexcept(std::is_nothrow_constructible<T, Args...>::value) {
    new (&value_) T(args...);
    initialized_ = true;
  }

  T& operator*() & noexcept { return value_; }
  const T& operator*() const & noexcept { return value_; }
  T&& operator*() && noexcept { return std::move(value_); }
  const T&& operator*() const && noexcept { return std::move(value_); }

  T* operator->() noexcept { return &value_; }
  const T* operator->() const noexcept { return &value_; }

  void reset() noexcept(std::is_nothrow_destructible<T>::value) {
    if (initialized_) value_.~T();
    initialized_ = false;
  }

private:
  union {
    T value_;
  };
  bool initialized_;

  // for converting constructor
  template<typename TT> friend class optional;
};

} // namespace

#endif // C++17

#endif // _OPTIONAL_HPP_
