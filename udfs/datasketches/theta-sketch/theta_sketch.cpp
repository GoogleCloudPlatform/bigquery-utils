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
#include "theta_constants.hpp"
#include "theta_sketch.hpp"
#include "theta_union.hpp"
#include "theta_intersection.hpp"
#include "theta_a_not_b.hpp"

using datasketches::theta_sketch;
using datasketches::update_theta_sketch;
using datasketches::compact_theta_sketch;
using datasketches::wrapped_compact_theta_sketch;
using datasketches::theta_union;
using datasketches::theta_intersection;
using datasketches::theta_a_not_b;

extern "C" {
// helper because we get the lg_k as INT64
EMSCRIPTEN_KEEPALIVE int32_t clamp_lg_k(int64_t lg_k) {
  if (lg_k <= 0) {
    return datasketches::theta_constants::DEFAULT_LG_K;
  } else if (lg_k < datasketches::theta_constants::MIN_LG_K) {
    return datasketches::theta_constants::MIN_LG_K;
  } else if (lg_k > datasketches::theta_constants::MAX_LG_K) {
    return datasketches::theta_constants::MAX_LG_K;
  }
  return lg_k;
}

EMSCRIPTEN_KEEPALIVE int compact_sketch_serialize(
    compact_theta_sketch *compact,
    char *buffer, size_t buffer_size) {
  std::strstream stream(buffer, buffer_size);
  compact->serialize(stream);
  return stream.tellp();
}

EMSCRIPTEN_KEEPALIVE compact_theta_sketch * compact_sketch_deserialize(
    void * buffer, size_t len) {
  return new compact_theta_sketch(
      compact_theta_sketch::deserialize(buffer, len));
}

EMSCRIPTEN_KEEPALIVE void compact_sketch_destroy(compact_theta_sketch *sketch) {
  delete sketch;
}

EMSCRIPTEN_KEEPALIVE update_theta_sketch *
    update_sketch_initialize(int32_t lg_k) {
  return new update_theta_sketch(
      update_theta_sketch::builder()
          .set_lg_k(clamp_lg_k(lg_k))
          .build());
}

EMSCRIPTEN_KEEPALIVE void theta_sketch_update_int64(
    update_theta_sketch *sketch, int64_t value) {
  sketch->update(value);
}

EMSCRIPTEN_KEEPALIVE void theta_sketch_update_bytes(
    update_theta_sketch *sketch, void * data, size_t length) {
  sketch->update(data, length);
}

EMSCRIPTEN_KEEPALIVE int update_sketch_serialize(
    update_theta_sketch *sketch,
    char *buffer, size_t buffer_size) {
  compact_theta_sketch compact = sketch->compact();
  return compact_sketch_serialize(&compact, buffer, buffer_size);
}

EMSCRIPTEN_KEEPALIVE int combined_sketch_serialize(
    update_theta_sketch *sketch,
    compact_theta_sketch *compact,
    int32_t lg_k, char *buffer, size_t buffer_size) {
  theta_union theta_union =
      theta_union::builder().set_lg_k(lg_k).build();
  theta_union.update(*sketch);
  theta_union.update(*compact);
  compact_theta_sketch result = theta_union.get_result();
  return compact_sketch_serialize(&result, buffer, buffer_size);
}

EMSCRIPTEN_KEEPALIVE double
    compact_sketch_get_estimate(compact_theta_sketch *sketch) {
  return sketch->get_estimate();
}

EMSCRIPTEN_KEEPALIVE void update_sketch_destroy(update_theta_sketch *sketch) {
  delete sketch;
}

EMSCRIPTEN_KEEPALIVE theta_union * theta_union_initialize(int32_t lg_k) {
  return new theta_union(theta_union::builder().set_lg_k(lg_k).build());
}

EMSCRIPTEN_KEEPALIVE void theta_union_destroy(theta_union *theta_union) {
  delete theta_union;
}

EMSCRIPTEN_KEEPALIVE void theta_union_update_buffer(
    theta_union *theta_union,
    const void *data, size_t len) {
  wrapped_compact_theta_sketch sketch =
      wrapped_compact_theta_sketch::wrap(data, len);
  theta_union->update(sketch);
}

EMSCRIPTEN_KEEPALIVE void theta_union_update_sketch(
    theta_union *theta_union, update_theta_sketch *sketch) {
  theta_union->update(*sketch);
}

EMSCRIPTEN_KEEPALIVE int theta_union_serialize_sketch(
    theta_union *theta_union, char *buffer, size_t buffer_size) {
  compact_theta_sketch sketch = theta_union->get_result();
  return compact_sketch_serialize(&sketch, buffer, buffer_size);
}

// read serialized sketch from buffer, union with sketch, serialize result
EMSCRIPTEN_KEEPALIVE int theta_combined_update_serialized(
    char *buffer, size_t buffer_size,
    int32_t compact_size,
    update_theta_sketch *sketch,
    int32_t lg_k) {
  theta_union theta_union =
      theta_union::builder().set_lg_k(lg_k).build();
  theta_union_update_buffer(&theta_union, buffer, compact_size);
  theta_union.update(*sketch);

  compact_theta_sketch result = theta_union.get_result();
  return compact_sketch_serialize(&result, buffer, buffer_size);
}

EMSCRIPTEN_KEEPALIVE theta_intersection * theta_intersection_initialize() {
  return new theta_intersection();
}

EMSCRIPTEN_KEEPALIVE void theta_intersection_update_buffer(
    theta_intersection *intersection,
    const void *data, size_t len) {
  wrapped_compact_theta_sketch sketch =
      wrapped_compact_theta_sketch::wrap(data, len);
  intersection->update(sketch);
}

EMSCRIPTEN_KEEPALIVE void theta_intersection_update_sketch(
    theta_intersection *intersection,
    compact_theta_sketch *sketch) {
  intersection->update(*sketch);
}

EMSCRIPTEN_KEEPALIVE void theta_intersection_destroy(
    theta_intersection *intersection) {
  delete intersection;
}

EMSCRIPTEN_KEEPALIVE int theta_intersection_serialize_sketch(
    theta_intersection *intersection,
    char *buffer, size_t buffer_size) {
  compact_theta_sketch sketch = intersection->get_result();
  return compact_sketch_serialize(&sketch, buffer, buffer_size);
}

EMSCRIPTEN_KEEPALIVE int theta_sketch_a_not_b(char *buf_a, size_t a_length, char *buf_b, size_t b_length) {
  theta_a_not_b difference;
  wrapped_compact_theta_sketch sketch_a = wrapped_compact_theta_sketch::wrap(buf_a, a_length);
  wrapped_compact_theta_sketch sketch_b = wrapped_compact_theta_sketch::wrap(buf_b, b_length);
  compact_theta_sketch result = difference.compute(sketch_a, sketch_b);
  return compact_sketch_serialize(&result, buf_a, a_length);
}

}

#if !__EMSCRIPTEN__
int main(int argc, char **argv) {
  puts("OH HAI");
}
#endif
