// from emscripten
import ModuleFactory from "../kll_sketch.mjs";
var Module = await ModuleFactory();

// Converts a string of base64 into a byte array (Uint8Array).
function intArrayFromBase64(s) {

  var decoded = atob(s);
  var bytes = new Uint8Array(decoded.length);
  for (var i = 0 ; i < decoded.length ; ++i) {
    bytes[i] = decoded.charCodeAt(i);
  }
  return bytes;
}

function get_quantile(sketch, q_rank) {
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
}


console.log("From code -> " + get_quantile("BQEPAvoACAAKAAAAAAAAAPoAAQDwAAAAAACAPwAAIEEAAIA/AAAAQAAAQEAAAIBAAACgQAAAwEAAAOBAAAAAQQAAEEEAACBB", 0.3))
console.log("From BQ   -> " + get_quantile("BQEPAPoACAAKAAAAAAAAAPoAAQDwAAAAAACAPwAAIEEAACBBAAAQQQAAAEEAAOBAAADAQAAAoEAAAIBAAABAQAAAAEAAAIA/", 0.3))