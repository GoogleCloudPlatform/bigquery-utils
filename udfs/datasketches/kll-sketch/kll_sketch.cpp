/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#if __EMSCRIPTEN__
#include <emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif
#include <algorithm>
#include <cassert>
#include <strstream>
#include "kll_sketch.hpp"

using kll_sketch = datasketches::kll_sketch<float>;

extern "C" {
// helper because we get the K as INT64
EMSCRIPTEN_KEEPALIVE int32_t clamp_k(int64_t k) {
  if (k <= 0) {
    return datasketches::kll_constants::DEFAULT_K;
  } else if (k < datasketches::kll_constants::MIN_K) {
    return datasketches::kll_constants::MIN_K;
  } else if (k > datasketches::kll_constants::MAX_K) {
    return datasketches::kll_constants::MAX_K;
  }
  return k;
}

EMSCRIPTEN_KEEPALIVE kll_sketch *
    kll_sketch_initialize(int32_t k) {
  return new kll_sketch(k);
}

EMSCRIPTEN_KEEPALIVE void kll_sketch_update_int64(
    kll_sketch *sketch, int64_t value) {
  sketch->update(value);
}

EMSCRIPTEN_KEEPALIVE void kll_sketch_update_double(
    kll_sketch *sketch, double value) {
  sketch->update(value);
}

EMSCRIPTEN_KEEPALIVE int kll_sketch_serialize(
    kll_sketch *sketch,
    char *buffer, size_t buffer_size) {
  std::strstream stream(buffer, buffer_size);
  sketch->serialize(stream);
  return stream.tellp();
}

EMSCRIPTEN_KEEPALIVE kll_sketch * kll_sketch_deserialize(
    void * buffer, size_t len) {
  return new kll_sketch(kll_sketch::deserialize(buffer, len));
}

EMSCRIPTEN_KEEPALIVE double
    kll_sketch_get_quantile(kll_sketch *sketch, double rank) {
  return sketch->get_quantile(rank);
}

EMSCRIPTEN_KEEPALIVE kll_sketch* merge_sketch(
    kll_sketch *sketch1,
    kll_sketch *sketch2) {
  sketch1->merge(*sketch2);
  return sketch1;
}

EMSCRIPTEN_KEEPALIVE size_t kll_sketch_serialized_size_bytes(kll_sketch *sketch) {
  return sketch->get_serialized_size_bytes();
}


EMSCRIPTEN_KEEPALIVE int merged_sketch_serialize(
    kll_sketch *sketch1,
    kll_sketch *sketch2,
    int32_t k, char *buffer, size_t buffer_size) {
  return kll_sketch_serialize(merge_sketch(sketch1,sketch2), buffer, buffer_size);
}

EMSCRIPTEN_KEEPALIVE void kll_sketch_destroy(kll_sketch *sketch) {
  delete sketch;
}

}

#if !__EMSCRIPTEN__
int main(int argc, char **argv) {
  puts("OH HAI");
}
#endif
