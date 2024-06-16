import ModuleFactory from "gs://$BUCKET/tuple_sketch.mjs";

var Module = await ModuleFactory();

// Helper definitions

// shared buffer for serialization and deserialization
var BUFFER = {
  ptr: 0,
  size: 0,
};

function maxSize(lg_k) {
  // see https://datasketches.apache.org/docs/Theta/ThetaSize.html
  // Todo: Calculate max size for tuple sketches ( couldn't find documentation)
  // using 16 bytes per row throws "error writing to std::ostream with 1 items"
  // 16 bytes per entry was calculated for theta sketch, since tuple sketch has an additional summary row of input datatype,
  // doubling the size requirement as max bound
  return 8 + 24 + 32 * (1 << lg_k);
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
  Module._tuple_union_update_buffer(union, buffer.ptr, bytes.length);
}

// Ensures we have a tuple_union;
// if there is a compact_tuple_sketch, copy it to the union
// and destroy it.
function ensureUnion(state) {
  if (!state.union) {
    state.union = Module._tuple_union_initialize(state.lg_k);
  }
  if (state.serialized) {
    updateUnion(state.union, state.serialized);
    state.serialized = null;
  }
}

export function initialState(lg_k) {
  lg_k = Module._clamp_lg_k(lg_k);
  return {
    union: Module._tuple_union_initialize(lg_k),
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
    var len = Module._tuple_union_serialize_sketch(
        state.union, buffer.ptr, buffer.size);
    return {
      lg_k: state.lg_k,
      bytes: Module.HEAPU8.slice(buffer.ptr, buffer.ptr + len),
    };
  } finally {
    // clean up union
    Module._tuple_union_destroy(state.union);
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
