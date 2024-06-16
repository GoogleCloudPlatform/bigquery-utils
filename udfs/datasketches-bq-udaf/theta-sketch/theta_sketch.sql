CREATE OR REPLACE FUNCTION `$BQ_PROJECT.$BQ_DATASET`.theta_sketch_extract(sketch BYTES)
    RETURNS FLOAT64
    LANGUAGE js
    OPTIONS (library=["$GCS_PATH/theta_sketch.js"]) AS '''
// from emscripten
var sketchBinary = intArrayFromBase64(sketch);
var ptr = Module._malloc(sketchBinary.length);
Module.HEAPU8.subarray(ptr, ptr + sketchBinary.length).set(sketchBinary);

var compact_sketch =
    Module._compact_sketch_deserialize(ptr, sketchBinary.length);
try {
   return Module._compact_sketch_get_estimate(compact_sketch);
} finally {
  Module._compact_sketch_destroy(compact_sketch);
  Module._free(ptr);
}
''';

CREATE OR REPLACE FUNCTION `$BQ_PROJECT.$BQ_DATASET`.theta_sketch_a_not_b(sketch_A BYTES, sketch_NotB BYTES)
    RETURNS BYTES
    LANGUAGE js
    OPTIONS (library=["$GCS_PATH/theta_sketch.js"]) AS
'''
var sketchBinary_a = intArrayFromBase64(sketch_A);
var sketchBinary_b = intArrayFromBase64(sketch_NotB);
var totalSize = sketchBinary_a.length + sketchBinary_b.length;
var ptrA = Module._malloc(totalSize);
var ptrB = ptrA + sketchBinary_a.length;

Module.HEAPU8.subarray(ptrA, ptrA + sketchBinary_a.length).set(sketchBinary_a);
Module.HEAPU8.subarray(ptrB, ptrB + sketchBinary_b.length).set(sketchBinary_b);

var len = Module._theta_sketch_a_not_b(ptrA, sketchBinary_a.length, ptrB, sketchBinary_b.length);
try {
  // converting uint8 byte array to base64 string ( to be returned as "Bytes" in BQ
  return bytesToBase64(Module.HEAPU8.slice(ptrA, ptrA + len));
} finally {
  Module._free(ptrA);
}
''';

CREATE OR REPLACE AGGREGATE FUNCTION
  `$BQ_PROJECT.$BQ_DATASET`.theta_sketch_int64(x INT64, lg_k INT64 NOT AGGREGATE)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/theta_sketch.mjs"]) AS '''
import ModuleFactory from "$GCS_PATH/theta_sketch.mjs";

var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};

function maxSize(lg_k) {
  // see https://datasketches.apache.org/docs/Theta/ThetaSize.html
  return 8 + 24 + 16 * (1 << lg_k);
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
    Module._update_sketch_destroy(state.sketch);
    state.sketch = 0;
  }
  if (state.union) {
    Module._theta_union_destroy(state.union);
    state.union = 0;
  }
  state.serialized = null;
}

function updateUnion(union, bytes) {
  var buffer = requireBuffer(bytes.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(bytes);
  Module._theta_union_update_buffer(union, buffer.ptr, bytes.length);
}

// UDAF interface

export function initialState(lg_k) {
  lg_k = Module._clamp_lg_k(lg_k);
  return {
    sketch: Module._update_sketch_initialize(lg_k),
    lg_k: lg_k,
    serialized: null,
    union: 0,
  };
}

export function aggregate(state, arg) {
  if (!state.sketch) {
    state.sketch = Module._update_sketch_initialize(state.lg_k);
  }

  Module._theta_sketch_update_int64(state.sketch, arg);
}

export function serialize(state) {
  var buffer = requireBuffer(maxSize(state.lg_k));
  var len = 0;
  try {
    if (state.sketch && state.serialized) {
      // merge aggregated and serialized state
      Module.HEAPU8
        .subarray(buffer.ptr, buffer.ptr + buffer.size)
        .set(state.serialized);

      len = Module._theta_combined_update_serialized(
          buffer.ptr, buffer.size, state.serialized.length,
          state.sketch, state.lg_k);
    } else if (state.sketch) {
      len = Module._update_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.union) {
      len = Module._theta_union_serialize_sketch(
          state.union, buffer.ptr, buffer.size);
    } else if (state.serialized) {
      return {
        lg_k: state.lg_k,
        bytes: state.serialized,
      };
    } else {
      throw new Error(
          "Unexpected state in serialization " + JSON.stringify(state));
    }
    return {
      lg_k: state.lg_k,
      bytes: Module.HEAPU8.slice(buffer.ptr , buffer.ptr + len),
    };
  } finally {
    destroyState(state);
  }
}

export function deserialize(serialized) {
  return {
    sketch: 0,
    union: 0,
    serialized: serialized.bytes,
    lg_k: serialized.lg_k,
  };
}

export function merge(state, other_state) {
  if (!state.union) {
    state.union = Module._theta_union_initialize(state.lg_k);
  }

  if (state.sketch || other_state.sketch) {
    throw new Error("update_sketch not expected during merge()");
  }

  if (other_state.union) {
    throw new Error("other_state should not have union during merge()");
  }

  if (state.serialized) {
    // consume it
    updateUnion(state.union, state.serialized);
    state.serialized = null;
  }

  if (other_state.serialized) {
    updateUnion(state.union, other_state.serialized);
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

-- use this version also for strings
CREATE OR REPLACE AGGREGATE FUNCTION
  `$BQ_PROJECT.$BQ_DATASET`.theta_sketch_bytes (x BYTES, lg_k INT64 NOT AGGREGATE)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/theta_sketch.mjs"]) AS '''
import ModuleFactory from "$GCS_PATH/theta_sketch.mjs";

var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};

// allocate a shared buffer for passing strings/bytes
var STRMAX = 256;
var STRBUF = Module._malloc(STRMAX);

function maxSize(lg_k) {
  // see https://datasketches.apache.org/docs/Theta/ThetaSize.html
  return 8 + 24 + 16 * (1 << lg_k);
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
    Module._update_sketch_destroy(state.sketch);
    state.sketch = 0;
  }
  if (state.union) {
    Module._theta_union_destroy(state.union);
    state.union = 0;
  }
  state.serialized = null;
}

function updateUnion(union, bytes) {
  var buffer = requireBuffer(bytes.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(bytes);
  Module._theta_union_update_buffer(union, buffer.ptr, bytes.length);
}

// UDAF interface
export function initialState(lg_k) {
  lg_k = Module._clamp_lg_k(lg_k);
  return {
    sketch: Module._update_sketch_initialize(lg_k),
    lg_k: lg_k,
    serialized: null,
    union: 0,
  };
}

export function aggregate(state, arg) {
  if (!state.sketch) {
    state.sketch = Module._update_sketch_initialize(state.lg_k);
  }

  // make sure that the argument will fit into the string buffer
  if (arg.length > STRMAX) {
    // allocate a bigger buffer
    Module._free(STRBUF);
    while (arg.length >= STRMAX) {
      STRMAX *= 2;
    }
    STRBUF = Module._malloc(STRMAX);
  }

  Module.HEAPU8.subarray(STRBUF, STRBUF + arg.length).set(arg);
  Module._theta_sketch_update_bytes(state.sketch, STRBUF, arg.length);
}

export function serialize(state) {
  var buffer = requireBuffer(maxSize(state.lg_k));
  var len = 0;
  try {
    if (state.sketch && state.serialized) {
      // merge aggregated and serialized state
      Module.HEAPU8
        .subarray(buffer.ptr, buffer.ptr + buffer.size)
        .set(state.serialized);

      len = Module._theta_combined_update_serialized(
          buffer.ptr, buffer.size, state.serialized.length,
          state.sketch, state.lg_k);
    } else if (state.sketch) {
      len = Module._update_sketch_serialize(
          state.sketch, buffer.ptr, buffer.size);
    } else if (state.union) {
      len = Module._theta_union_serialize_sketch(
          state.union, buffer.ptr, buffer.size);
    } else if (state.serialized) {
      return {
        lg_k: state.lg_k,
        bytes: state.serialized,
      };
    } else {
      throw new Error(
          "Unexpected state in serialization " + JSON.stringify(state));
    }
    return {
      lg_k: state.lg_k,
      bytes: Module.HEAPU8.slice(buffer.ptr , buffer.ptr + len),
    };
  } finally {
    destroyState(state);
  }
}

export function deserialize(serialized) {
  return {
    sketch: 0,
    union: 0,
    serialized: serialized.bytes,
    lg_k: serialized.lg_k,
  };
}

export function merge(state, other_state) {
  if (!state.union) {
    state.union = Module._theta_union_initialize(state.lg_k);
  }

  if (state.sketch || other_state.sketch) {
    throw new Error("update_sketch not expected during merge()");
  }

  if (other_state.union) {
    throw new Error("other_state should not have union during merge()");
  }

  if (state.serialized) {
    // consume it
    updateUnion(state.union, state.serialized);
    state.serialized = null;
  }

  if (other_state.serialized) {
    updateUnion(state.union, other_state.serialized);
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
  `$BQ_PROJECT.$BQ_DATASET`.theta_sketch_union(sketch BYTES, lg_k INT64 NOT AGGREGATE)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/theta_sketch.mjs"]) AS '''
import ModuleFactory from "$GCS_PATH/theta_sketch.mjs";

var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};

function maxSize(lg_k) {
  // see https://datasketches.apache.org/docs/Theta/ThetaSize.html
  return 8 + 24 + 16 * (1 << lg_k);
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

function updateUnion(union, bytes) {
  var buffer = requireBuffer(bytes.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(bytes);
  Module._theta_union_update_buffer(union, buffer.ptr, bytes.length);
}

// Ensures we have a theta_union;
// if there is a compact_theta_sketch, copy it to the union
// and destroy it.
function ensureUnion(state) {
  if (!state.union) {
    state.union = Module._theta_union_initialize(state.lg_k);
  }
  if (state.serialized) {
    updateUnion(state.union, state.serialized);
    state.serialized = null;
  }
}

export function initialState(lg_k) {
  lg_k = Module._clamp_lg_k(lg_k);
  return {
    union: Module._theta_union_initialize(lg_k),
    serialized: null,
    lg_k: lg_k,
  };
}

export function aggregate(state, arg) {
  ensureUnion(state);
  updateUnion(state.union, arg);
}

export function serialize(state) {
  try {
    ensureUnion(state);
    var buffer = requireBuffer(maxSize(state.lg_k));
    var len = Module._theta_union_serialize_sketch(
        state.union, buffer.ptr, buffer.size);
    return {
      lg_k: state.lg_k,
      bytes: Module.HEAPU8.slice(buffer.ptr, buffer.ptr + len),
    };
  } finally {
    // clean up union
    Module._theta_union_destroy(state.union);
    state.union = 0;
  }
}

export function deserialize(serialized) {
  return {
    union:  0,
    serialized: serialized.bytes,
    lg_k: serialized.lg_k,
  };
}

export function merge(state, other_state) {
  ensureUnion(state);

  if (other_state.union) {
    throw new Error("Did not expect union in other state");
  }

  if (other_state.serialized) {
    updateUnion(state.union, other_state.serialized);
    other_state.serialized = null;
  } else {
    throw new Error("Expected serialized sketch in other_state");
  }
}
export function finalize(state) {
  return serialize(state).bytes;
}
''';

CREATE OR REPLACE AGGREGATE FUNCTION
       `$BQ_PROJECT.$BQ_DATASET`.theta_sketch_intersection(x BYTES)
  RETURNS BYTES
  LANGUAGE js
  OPTIONS (library=["$GCS_PATH/theta_sketch.mjs"]) AS '''
import ModuleFactory from "$GCS_PATH/theta_sketch.mjs";

var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};

function maxSize(lg_k) {
  // see https://datasketches.apache.org/docs/Theta/ThetaSize.html
  return 8 + 24 + 16 * (1 << lg_k);
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


function requireIntersection(state) {
  if (!state.intersection) {
    state.intersection = Module._theta_intersection_initialize();
  }
  if (state.compact) {
    Module._theta_intersection_update_sketch(state.intersection, state.compact);
    Module._compact_sketch_destroy(state.compact);
    state.compact = 0;
  }
  return state.intersection;
}

export function initialState() {
  return {
    intersection: Module._theta_intersection_initialize(),
    compact: 0,
    count: 0,
    maxSize: 0,
  };
}

export function aggregate(state, arg) {
  var intersection = requireIntersection(state);

  // the aggregated sketch may be *bigger* than the maxSize of union
  // so we need to take the arg length as leading
  var buffer = requireBuffer(arg.length);
  Module.HEAPU8.subarray(buffer.ptr, buffer.ptr + buffer.size).set(arg);

  Module._theta_intersection_update_buffer(
      intersection, buffer.ptr, arg.length);
  state.maxSize = Math.max(state.maxSize, arg.length);
  state.count++;
}

export function serialize(state) {
  try {
    var intersection = requireIntersection(state);
    var buffer = requireBuffer(state.maxSize);
    var len = Module._theta_intersection_serialize_sketch(
        intersection, buffer.ptr, buffer.size);

    return {
      count: state.count,
      bytes: Module.HEAPU8.slice(buffer.ptr, buffer.ptr + len),
    };
  } finally {
    // clean up intersection
    Module._theta_intersection_destroy(state.intersection);
    state.intersection = 0;
  }
}

export function deserialize(serialized) {

  var buffer = requireBuffer(serialized.bytes.length);
  Module.HEAPU8
      .subarray(buffer.ptr, buffer.ptr + serialized.bytes.length)
      .set(serialized.bytes);
  var compact = Module._compact_sketch_deserialize(
      buffer.ptr, serialized.bytes.length);
  return {
    intersection: 0,
    compact: compact,
    count: serialized.count,
    maxSize: serialized.bytes.length,
  };
}

export function merge(state, other_state) {
  var intersection = requireIntersection(state);

  if (other_state.intersection) {
    throw new Error("Did not expect intersection in other state");
  }

  if (other_state.compact) {
    if (other_state.count) {
      Module._theta_intersection_update_sketch(
          intersection, other_state.compact);
    }
    state.maxSize = Math.max(state.maxSize, other_state.maxSize);
    state.count += other_state.count;

    Module._compact_sketch_destroy(other_state.compact);
    other_state.compact = 0;

  } else {
    throw new Error("Expected compact sketch in other_state");
  }
}
export function finalize(state) {
  return serialize(state).bytes;
}
''';