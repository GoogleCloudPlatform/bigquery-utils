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
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <cassert>
#include <strstream>
#include "theta_constants.hpp"
#include "tuple_sketch.hpp"
#include "tuple_union.hpp"

using update_tuple_sketch = datasketches::update_tuple_sketch<int64_t>;
using tuple_union = datasketches::tuple_union<int64_t>;
using compact_tuple_sketch = datasketches::compact_tuple_sketch<int64_t>;

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

EMSCRIPTEN_KEEPALIVE update_tuple_sketch *
    update_sketch_initialize(int32_t lg_k) {
  return new update_tuple_sketch(
      update_tuple_sketch::builder()
          .set_lg_k(clamp_lg_k(lg_k))
          .build());
}

EMSCRIPTEN_KEEPALIVE int compact_sketch_serialize(
    compact_tuple_sketch *compact,
    char *buffer, size_t buffer_size) {
  std::strstream stream(buffer, buffer_size);
  try {
   compact->serialize(stream);
  } catch (const std::runtime_error& e) {
      std::cerr << "WebAssembly exception in compact_sketch_serialize: " << e.what() << std::endl;
      return -1;
      }
  return stream.tellp();
}

EMSCRIPTEN_KEEPALIVE compact_tuple_sketch * compact_sketch_deserialize(
    void * buffer, size_t len) {
  return new compact_tuple_sketch(
      compact_tuple_sketch::deserialize(buffer, len));
}

EMSCRIPTEN_KEEPALIVE void compact_sketch_destroy(compact_tuple_sketch *sketch) {
  delete sketch;
}

EMSCRIPTEN_KEEPALIVE void tuple_sketch_update_int64(
    update_tuple_sketch *sketch, int64_t key, int64_t value) {
  sketch->update(key, value);
}

EMSCRIPTEN_KEEPALIVE int update_sketch_serialize(
    update_tuple_sketch *sketch,
    char *buffer, size_t buffer_size) {
  try {
    compact_tuple_sketch compact = sketch->compact();
    return compact_sketch_serialize(&compact, buffer, buffer_size);
  }  catch (const std::runtime_error& e) {
        std::cerr << "WebAssembly Caught exception: " << e.what() << std::endl;
        return -1;
  }
}

EMSCRIPTEN_KEEPALIVE int combined_sketch_serialize(
    update_tuple_sketch *sketch,
    compact_tuple_sketch *compact,
    int32_t lg_k, char *buffer, size_t buffer_size) {
  tuple_union tuple_union =
      tuple_union::builder().set_lg_k(lg_k).build();
  tuple_union.update(*sketch);
  tuple_union.update(*compact);
  compact_tuple_sketch result = tuple_union.get_result();
  return compact_sketch_serialize(&result, buffer, buffer_size);
}

EMSCRIPTEN_KEEPALIVE int64_t
    compact_sketch_get_estimate_count(compact_tuple_sketch *sketch) {
  return static_cast<int64_t>(sketch->get_estimate());
}

EMSCRIPTEN_KEEPALIVE int64_t
    compact_sketch_get_estimate_sum(compact_tuple_sketch *sketch) {
  int64_t sum = 0;
  for (const auto& entry: *sketch) {
     sum += entry.second;
  }
  return static_cast<int64_t>(sum/sketch->get_theta());
}

EMSCRIPTEN_KEEPALIVE int64_t
    compact_sketch_get_estimate_avg(compact_tuple_sketch *sketch) {
  int64_t sum = 0;
  for (const auto& entry: *sketch) {
     sum += entry.second;
  }
  return static_cast<int64_t>(sum/sketch->get_num_retained());
}

EMSCRIPTEN_KEEPALIVE void update_sketch_destroy(update_tuple_sketch *sketch) {
  delete sketch;
}

EMSCRIPTEN_KEEPALIVE tuple_union * tuple_union_initialize(int32_t lg_k) {
  return new tuple_union(tuple_union::builder().set_lg_k(lg_k).build());
}

EMSCRIPTEN_KEEPALIVE void tuple_union_destroy(tuple_union *tuple_union) {
  delete tuple_union;
}

EMSCRIPTEN_KEEPALIVE void tuple_union_update_buffer(
    tuple_union *tuple_union,
    void *data, size_t len) {
  compact_tuple_sketch *sketch =
      compact_sketch_deserialize(data, len);
  tuple_union->update(*sketch);
}

EMSCRIPTEN_KEEPALIVE void tuple_union_update_sketch(
    tuple_union *tuple_union, update_tuple_sketch *sketch) {
  tuple_union->update(*sketch);
}

EMSCRIPTEN_KEEPALIVE int tuple_union_serialize_sketch(
    tuple_union *tuple_union, char *buffer, size_t buffer_size) {
  compact_tuple_sketch sketch = tuple_union->get_result();
  return compact_sketch_serialize(&sketch, buffer, buffer_size);
}

// read serialized sketch from buffer, union with sketch, serialize result
EMSCRIPTEN_KEEPALIVE int tuple_combined_update_serialized(
    char *buffer, size_t buffer_size,
    int32_t compact_size,
    update_tuple_sketch *sketch,
    int32_t lg_k) {
  tuple_union tuple_union =
      tuple_union::builder().set_lg_k(lg_k).build();
  tuple_union_update_buffer(&tuple_union, buffer, compact_size);
  tuple_union.update(*sketch);

  compact_tuple_sketch result = tuple_union.get_result();
  return compact_sketch_serialize(&result, buffer, buffer_size);
}

}

#if !__EMSCRIPTEN__
int main(int argc, char **argv) {
  puts("Init");
}
#endif
