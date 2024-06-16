CREATE OR REPLACE FUNCTION `$BQ_PROJECT.$BQ_DATASET`.kll_sketch_quantile(sketch BYTES, q_rank FLOAT64)
    RETURNS FLOAT64
    LANGUAGE js
    OPTIONS (library=["$GCS_PATH/kll_sketch.js"]) AS '''
var sketchBinary = intArrayFromBase64(sketch);
var ptr = Module._malloc(sketchBinary.length);
Module.HEAPU8.subarray(ptr, ptr + sketchBinary.length).set(sketchBinary);
var kll_sketch = Module._kll_sketch_deserialize(ptr, sketchBinary.length);
try {
    return Module._kll_sketch_get_quantile(kll_sketch, q_rank);
} finally {
    Module._kll_sketch_destroy(kll_sketch);
    Module._free(ptr);
  }
''';


CREATE OR REPLACE AGGREGATE FUNCTION
  `$BQ_PROJECT.$BQ_DATASET`.kll_sketch_int64(x INT64, k INT64 NOT AGGREGATE)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/kll_sketch.mjs"]) AS '''
import ModuleFactory from "$GCS_PATH/kll_sketch.mjs";

var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};
// This function returns expected size of sketch when serialized
function maxSize(sketch) {
  // https://github.com/apache/datasketches-cpp/issues/4
  // https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html
  return Module._kll_sketch_serialized_size_bytes(sketch);
}

function requireBuffer(size) {
  if (BUFFER.size < size) {
    releaseBuffer();
    BUFFER.ptr = Module._malloc(size);
    BUFFER.size = size;
  }
  return BUFFER;
}

function releaseBuffer() {
  if (BUFFER.ptr) {
    Module._free(BUFFER.ptr);
  }
  BUFFER.ptr = 0;
  BUFFER.size = 0;
}

function destroyState(state) {
  if (state.sketch) {
    Module._kll_sketch_destroy(state.sketch);
    state.sketch = 0;
  }
  state.serialized = null;
}

function updateSketch(sketch, bytes) {
  var buffer = requireBuffer(bytes.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(bytes);
  var deserialized_sketch = Module._kll_sketch_deserialize(buffer.ptr, bytes.length);
  Module._merge_sketch(sketch, deserialized_sketch);
  Module._kll_sketch_destroy(deserialized_sketch);
}

// UDAF interface

export function initialState(k) {
  k = Module._clamp_k(k);
  return {
    sketch: Module._kll_sketch_initialize(k),
    k: k,
    serialized: null,
    count: 0
  };
}

export function aggregate(state, arg) {
  if (!state.sketch) {
    state.sketch = Module._kll_sketch_initialize(state.k);
  }
  Module._kll_sketch_update_int64(state.sketch, arg);
  state.count++;
}

export function serialize(state) {
  try {
    var buffer = 0;
    var len = 0;
    if ( state.sketch && state.serialized) {
      updateSketch(state.sketch, state.serialized);
      state.serialized = null;
      buffer = requireBuffer(maxSize(state.sketch));
      len = Module._kll_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.sketch) {
      buffer = requireBuffer(maxSize(state.sketch));
      len = Module._kll_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.serialized) {
      return {
        k: state.k,
        bytes: state.serialized,
        count: state.count
      }
    } else {
      throw new Error(
          "Unexpected state in serialization " + JSON.stringify(state));
    }

    return {
      k: state.k,
      bytes: Module.HEAPU8.slice(buffer.ptr, buffer.ptr + len),
      count: state.count
    }
  } finally {
    // clean up kll sketch
    Module._kll_sketch_destroy(state.sketch);
    state.sketch = 0;
    state.serialized = null;
  }
}

// Keeping state same as agrgegation state
export function deserialize(serialized) {
  return {
    sketch: 0,
    k: serialized.k,
    serialized: serialized.bytes,
    count: serialized.count
  };
}

// Assuming Merge can only be called after deserialize
export function merge(state, other_state) {
  if (!state.sketch) {
    state.sketch = Module._kll_sketch_initialize(state.k);
  }

  if (other_state.sketch ) {
    throw new Error("Did not expect sketch in other state");
  }

  if (state.serialized) {
    // consume it
    updateSketch(state.sketch, state.serialized);
    state.serialized = null;
  }

  if (other_state.serialized) {
    updateSketch(state.sketch, other_state.serialized);
    other_state.serialized = null;
  } else {
    throw new Error("Expected serialized sketch on other_state");
  }
}


export function finalize(state) {
  var result = serialize(state);
  return result.bytes;
}
''';



CREATE OR REPLACE AGGREGATE FUNCTION
  `$BQ_PROJECT.$BQ_DATASET`.kll_sketch_float64(x FLOAT64, k INT64 NOT AGGREGATE)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/kll_sketch.mjs"]) AS '''
import ModuleFactory from "$GCS_PATH/kll_sketch.mjs";
var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};
// This function returns expected size of sketch when serialized
function maxSize(sketch) {
  // https://github.com/apache/datasketches-cpp/issues/4
  // https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html
  return Module._kll_sketch_serialized_size_bytes(sketch);
}

function requireBuffer(size) {
  if (BUFFER.size < size) {
    releaseBuffer();
    BUFFER.ptr = Module._malloc(size);
    BUFFER.size = size;
  }
  return BUFFER;
}

function releaseBuffer() {
  if (BUFFER.ptr) {
    Module._free(BUFFER.ptr);
  }
  BUFFER.ptr = 0;
  BUFFER.size = 0;
}

function destroyState(state) {
  if (state.sketch) {
    Module._kll_sketch_destroy(state.sketch);
    state.sketch = 0;
  }
  state.serialized = null;
}

function updateSketch(sketch, bytes) {
  var buffer = requireBuffer(bytes.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(bytes);
  var deserialized_sketch = Module._kll_sketch_deserialize(buffer.ptr, bytes.length);
  Module._merge_sketch(sketch, deserialized_sketch);
  Module._kll_sketch_destroy(deserialized_sketch);
}

// UDAF interface

export function initialState(k) {
  k = Module._clamp_k(k);
  return {
    sketch: Module._kll_sketch_initialize(k),
    k: k,
    serialized: null,
    count: 0
  };
}

export function aggregate(state, arg) {
  if (!state.sketch) {
    state.sketch = Module._kll_sketch_initialize(state.k);
  }
  Module._kll_sketch_update_double(state.sketch, arg);
  state.count++;
}

export function serialize(state) {
  try {
    var buffer = 0;
    var len = 0;
    if ( state.sketch && state.serialized) {
      updateSketch(state.sketch, state.serialized);
      state.serialized = null;
      buffer = requireBuffer(maxSize(state.sketch));
      len = Module._kll_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.sketch) {
      buffer = requireBuffer(maxSize(state.sketch));
      len = Module._kll_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.serialized) {
      return {
        k: state.k,
        bytes: state.serialized,
        count: state.count
      }
    } else {
      throw new Error(
          "Unexpected state in serialization " + JSON.stringify(state));
    }

    return {
      k: state.k,
      bytes: Module.HEAPU8.slice(buffer.ptr, buffer.ptr + len),
      count: state.count
    }
  } finally {
    // clean up kll sketch
    Module._kll_sketch_destroy(state.sketch);
    state.sketch = 0;
    state.serialized = null;
  }
}

// Keeping state same as agrgegation state
export function deserialize(serialized) {
  return {
    sketch: 0,
    k: serialized.k,
    serialized: serialized.bytes,
    count: serialized.count
  };
}

// Assuming Merge can only be called after deserialize
export function merge(state, other_state) {
  if (!state.sketch) {
    state.sketch = Module._kll_sketch_initialize(state.k);
  }

  if (other_state.sketch ) {
    throw new Error("Did not expect sketch in other state");
  }

  if (state.serialized) {
    // consume it
    updateSketch(state.sketch, state.serialized);
    state.serialized = null;
  }

  if (other_state.serialized) {
    updateSketch(state.sketch, other_state.serialized);
    other_state.serialized = null;
  } else {
    throw new Error("Expected serialized sketch on other_state");
  }
}


export function finalize(state) {
  var result = serialize(state);
  return result.bytes;
}
''';

CREATE OR REPLACE AGGREGATE FUNCTION
  `$BQ_PROJECT.$BQ_DATASET`.kll_sketch_merge(sketch BYTES, k INT64 NOT AGGREGATE)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/kll_sketch.mjs"]) AS '''

import ModuleFactory from "$GCS_PATH/kll_sketch.mjs";
var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};
// This function returns expected size of sketch when serialized
function maxSize(sketch) {
  // https://github.com/apache/datasketches-cpp/issues/4
  // https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html
  return Module._kll_sketch_serialized_size_bytes(sketch);
}

function requireBuffer(size) {
  if (BUFFER.size < size) {
    releaseBuffer();
    BUFFER.ptr = Module._malloc(size);
    BUFFER.size = size;
  }
  return BUFFER;
}

function releaseBuffer() {
  if (BUFFER.ptr) {
    Module._free(BUFFER.ptr);
  }
  BUFFER.ptr = 0;
  BUFFER.size = 0;
}

function destroyState(state) {
  if (state.sketch) {
    Module._kll_sketch_destroy(state.sketch);
    state.sketch = 0;
  }
  state.serialized = null;
}

function updateSketch(sketch, bytes) {
  var buffer = requireBuffer(bytes.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(bytes);
  var deserialized_sketch = Module._kll_sketch_deserialize(buffer.ptr, bytes.length);
  Module._merge_sketch(sketch, deserialized_sketch);
  Module._kll_sketch_destroy(deserialized_sketch);
}

// UDAF interface

export function initialState(k) {
  k = Module._clamp_k(k);
  return {
    sketch: Module._kll_sketch_initialize(k),
    k: k,
    serialized: null
  };
}

// Upstream: initialState, aggregate, merge, deserialize
export function aggregate(state, bytes) {
  if (!state.sketch) {
    state.sketch = Module._kll_sketch_initialize(state.k);
  }
  updateSketch(state.sketch, bytes);
}

// Upstream: initialState, aggregate, merge, deserialize
export function serialize(state) {
  try {
    var buffer = 0;
    var len = 0;
    if ( state.sketch && state.serialized) {
      updateSketch(state.sketch, state.serialized);
      state.serialized = null;
      buffer = requireBuffer(maxSize(state.sketch));
      len = Module._kll_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.sketch) {
      buffer = requireBuffer(maxSize(state.sketch));
      len = Module._kll_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.serialized) {
      return {
        k: state.k,
        bytes: state.serialized
      }
    } else {
      throw new Error(
          "Unexpected state in serialization " + JSON.stringify(state));
    }

    return {
      k: state.k,
      bytes: Module.HEAPU8.slice(buffer.ptr, buffer.ptr + len)
    }
  } finally {
    // clean up kll sketch
    Module._kll_sketch_destroy(state.sketch);
    state.sketch = 0;
    state.serialized = null;
  }
}

// Keeping state same as agrgegation state
export function deserialize(serialized) {
  return {
    sketch: 0,
    k: serialized.k,
    serialized: serialized.bytes
  };
}

// Assuming Merge can only be called after deserialize
export function merge(state, other_state) {
  if (!state.sketch) {
    state.sketch = Module._kll_sketch_initialize(state.k);
  }

  if (other_state.sketch ) {
    throw new Error("Did not expect sketch in other state");
  }

  if (state.serialized) {
    // consume it
    updateSketch(state.sketch, state.serialized);
    state.serialized = null;
  }

  if (other_state.serialized) {
    updateSketch(state.sketch, other_state.serialized);
    other_state.serialized = null;
  } else {
    throw new Error("Expected serialized sketch on other_state");
  }
}


export function finalize(state) {
  var result = serialize(state);
  return result.bytes;
}


''';