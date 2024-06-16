// from emscripten
import ModuleFactory from "../theta_sketch.mjs";
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

function get_estimate(sketch) {
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
}

// Distinct(A not B) -> Dinstinct((1,10) - (9,20)) = 8
console.log(get_estimate("AgMDAAAazJMIAAAAAAAAABX5fcu9hqEFQN4u4cnbPQhpi7mRuGhXCP4WIRP7mLwQvTJzckaRzBTDl/wSgXCdHrpAs8HaBmld4PSL6pmDw3w="));