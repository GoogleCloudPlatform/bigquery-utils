import ModuleFactory from "../kll_sketch.mjs";

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

// tests

function intArrayFromBase64(sketch_base64) {

  var decoded = atob(sketch_base64);
  var bytes = new Uint8Array(decoded.length);
  for (var i = 0 ; i < decoded.length ; ++i) {
    bytes[i] = decoded.charCodeAt(i);
  }
  return bytes;
}
export function get_quantile(sketch, rank) {
  return Module._kll_sketch_get_quantile(sketch, rank);
}

console.log(requireBuffer(10));
let state1  = initialState(BigInt(250))
aggregate(state1, intArrayFromBase64("BQEPAvoACAAKAAAAAAAAAPoAAQDwAAAAAACAPwAAIEEAAIA/AAAAQAAAQEAAAIBAAACgQAAAwEAAAOBAAAAAQQAAEEEAACBB")); // elems 1 through 10
console.log("0.5 quantile Sketch1 ( 1- 10 ) -> " + get_quantile(state1.sketch,0.5));

let state2  = initialState(BigInt(250))
aggregate(state2, intArrayFromBase64("BQEPAvoACAAKAAAAAAAAAPoAAQDwAAAAAAAwQQAAoEEAADBBAABAQQAAUEEAAGBBAABwQQAAgEEAAIhBAACQQQAAmEEAAKBB")); // elems 11 through 20
console.log("0.5 quantile Sketch2 ( 11- 20 ) -> " + get_quantile(state2.sketch,0.5));

console.log("(Before merge) State1 = " + JSON.stringify(state1));
console.log("(Before merge) State2 = " + JSON.stringify(state2));
let deserialized_sketch2 = deserialize(serialize(state2));
console.log("(Before merge) Deser-State2 = " + JSON.stringify(deserialized_sketch2));
merge(state1,deserialized_sketch2);
console.log("(After merge) State1 = " + JSON.stringify(state1));
console.log("(After merge) Deser-State2  = " + JSON.stringify(deserialized_sketch2));
console.log("0.5 quantile After merge state1 ( 1 - 20 ) -> " + get_quantile(state1.sketch,0.5));