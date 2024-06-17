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

#ifndef _HLL8ARRAY_INTERNAL_HPP_
#define _HLL8ARRAY_INTERNAL_HPP_

#include "Hll8Array.hpp"

namespace datasketches {

template<typename A>
Hll8Array<A>::Hll8Array(uint8_t lgConfigK, bool startFullSize, const A& allocator):
HllArray<A>(lgConfigK, target_hll_type::HLL_8, startFullSize, allocator)
{
  const int numBytes = this->hll8ArrBytes(lgConfigK);
  this->hllByteArr_.resize(numBytes, 0);
}

template<typename A>
Hll8Array<A>::Hll8Array(const HllArray<A>& other):
  HllArray<A>(other.getLgConfigK(), target_hll_type::HLL_8, other.isStartFullSize(), other.getAllocator())
{
  const int numBytes = this->hll8ArrBytes(this->lgConfigK_);
  this->hllByteArr_.resize(numBytes, 0);
  this->oooFlag_ = other.isOutOfOrderFlag();
  uint32_t num_zeros = 1 << this->lgConfigK_;
  
  for (const auto coupon : other) { // all = false, so skip empty values
    num_zeros--;
    internalCouponUpdate(coupon); // updates KxQ registers
  }
  
  this->numAtCurMin_ = num_zeros;
  this->hipAccum_ = other.getHipAccum();
  this->rebuild_kxq_curmin_ = false;
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> Hll8Array<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    Hll8Array<A>* hll = static_cast<Hll8Array<A>*>(ptr);
    using Hll8Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>>;
    Hll8Alloc hll8Alloc(hll->getAllocator());
    hll->~Hll8Array();
    hll8Alloc.deallocate(hll, 1);
  };
}

template<typename A>
Hll8Array<A>* Hll8Array<A>::copy() const {
  using Hll8Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>>;
  Hll8Alloc hll8Alloc(this->getAllocator());
  return new (hll8Alloc.allocate(1)) Hll8Array<A>(*this);
}

template<typename A>
uint8_t Hll8Array<A>::getSlot(uint32_t slotNo) const {
  return this->hllByteArr_[slotNo];
}

template<typename A>
void Hll8Array<A>::putSlot(uint32_t slotNo, uint8_t value) {
  this->hllByteArr_[slotNo] = value;
}

template<typename A>
uint32_t Hll8Array<A>::getHllByteArrBytes() const {
  return this->hll8ArrBytes(this->lgConfigK_);
}

template<typename A>
HllSketchImpl<A>* Hll8Array<A>::couponUpdate(uint32_t coupon) {
  internalCouponUpdate(coupon);
  return this;
}

template<typename A>
void Hll8Array<A>::internalCouponUpdate(uint32_t coupon) {
  const uint32_t configKmask = (1 << this->lgConfigK_) - 1;
  const uint32_t slotNo = HllUtil<A>::getLow26(coupon) & configKmask;
  const uint8_t newVal = HllUtil<A>::getValue(coupon);

  const uint8_t curVal = this->hllByteArr_[slotNo];
  if (newVal > curVal) {
    this->hllByteArr_[slotNo] = newVal;
    this->hipAndKxQIncrementalUpdate(curVal, newVal);
    this->numAtCurMin_ -= curVal == 0; // interpret numAtCurMin as num zeros
  }
}

template<typename A>
void Hll8Array<A>::mergeList(const CouponList<A>& src) {
  for (const auto coupon: src) {
    internalCouponUpdate(coupon);
  }
}

template<typename A>
void Hll8Array<A>::mergeHll(const HllArray<A>& src) {
  // at this point src_k >= dst_k
  // we can optimize further when the k values are equal
  if (this->getLgConfigK() == src.getLgConfigK()) {
    if (src.getTgtHllType() == target_hll_type::HLL_8) {
      uint32_t i = 0;
      for (const auto value: src.getHllArray()) {
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], value);
        ++i;
      }
    } else if (src.getTgtHllType() == target_hll_type::HLL_6) {
      const uint32_t src_k = 1 << src.getLgConfigK();
      uint32_t i = 0;
      const uint8_t* ptr = src.getHllArray().data();
      while (i < src_k) {
        uint8_t value = *ptr & 0x3f;
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], value);
        ++i;
        value = *ptr++ >> 6;
        value |= (*ptr & 0x0f) << 2;
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], value);
        ++i;
        value = *ptr++ >> 4;
        value |= (*ptr & 3) << 4;
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], value);
        ++i;
        value = *ptr++ >> 2;
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], value);
        ++i;
      }
    } else { // HLL_4
      const auto& src4 = static_cast<const Hll4Array<A>&>(src);
      uint32_t i = 0;
      for (const auto byte: src.getHllArray()) {
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], src4.adjustRawValue(i, byte & hll_constants::loNibbleMask));
        ++i;
        this->hllByteArr_[i] = std::max(this->hllByteArr_[i], src4.adjustRawValue(i, byte >> 4));
        ++i;
      }
    }
  } else {
    // src_k > dst_k
    const uint32_t dst_mask = (1 << this->getLgConfigK()) - 1;
    // special treatment below to optimize performance
    if (src.getTgtHllType() == target_hll_type::HLL_8) {
      uint32_t i = 0;
      for (const auto value: src.getHllArray()) {
        processValue(i++, dst_mask, value);
      }
    } else if (src.getTgtHllType() == target_hll_type::HLL_6) {
      const uint32_t src_k = 1 << src.getLgConfigK();
      uint32_t i = 0;
      const uint8_t* ptr = src.getHllArray().data();
      while (i < src_k) {
        uint8_t value = *ptr & 0x3f;
        processValue(i++, dst_mask, value);
        value = *ptr++ >> 6;
        value |= (*ptr & 0x0f) << 2;
        processValue(i++, dst_mask, value);
        value = *ptr++ >> 4;
        value |= (*ptr & 3) << 4;
        processValue(i++, dst_mask, value);
        value = *ptr++ >> 2;
        processValue(i++, dst_mask, value);
      }
    } else { // HLL_4
      const auto& src4 = static_cast<const Hll4Array<A>&>(src);
      uint32_t i = 0;
      for (const auto byte: src.getHllArray()) {
        processValue(i, dst_mask, src4.adjustRawValue(i, byte & hll_constants::loNibbleMask));
        ++i;
        processValue(i, dst_mask, src4.adjustRawValue(i, byte >> 4));
        ++i;
      }
    }
  }
  this->setRebuildKxqCurminFlag(true);
}


template<typename A>
void Hll8Array<A>::processValue(uint32_t slot, uint32_t mask, uint8_t new_val) {
  const size_t index = slot & mask;
  this->hllByteArr_[index] = std::max(this->hllByteArr_[index], new_val);
}

}

#endif // _HLL8ARRAY_INTERNAL_HPP_
