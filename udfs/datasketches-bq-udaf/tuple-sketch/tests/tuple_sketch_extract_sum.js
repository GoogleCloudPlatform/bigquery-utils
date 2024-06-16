// from emscripten
import ModuleFactory from "../compiled/tuple_sketch.mjs";
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
    return Module._compact_sketch_get_estimate_sum(compact_sketch);
  } finally {
    Module._compact_sketch_destroy(compact_sketch);
    Module._free(ptr);
  }
}

// ===== TESTS =====
// sum : 1-10 = 55
// Sketch(1-10) ==> AgMJAQAazJMKAAAAAAAAABX5fcu9hqEFAQAAAAAAAABA3i7hyds9CAQAAAAAAAAAaYu5kbhoVwgIAAAAAAAAAP4WIRP7mLwQBgAAAAAAAAC9MnNyRpHMFAUAAAAAAAAAw5f8EoFwnR4CAAAAAAAAABrRMAuZjC8iCQAAAAAAAAC6QLPB2gZpXQMAAAAAAAAA4PSL6pmDw3wHAAAAAAAAANgtI3dLuTV+CgAAAAAAAAA=
console.log("[1-10]: Sum: expected: 55, actual: " + get_estimate("AgMJAQAazJMKAAAAAAAAABX5fcu9hqEFAQAAAAAAAABA3i7hyds9CAQAAAAAAAAAaYu5kbhoVwgIAAAAAAAAAP4WIRP7mLwQBgAAAAAAAAC9MnNyRpHMFAUAAAAAAAAAw5f8EoFwnR4CAAAAAAAAABrRMAuZjC8iCQAAAAAAAAC6QLPB2gZpXQMAAAAAAAAA4PSL6pmDw3wHAAAAAAAAANgtI3dLuTV+CgAAAAAAAAA="));

// Sum 5 - 15 = 110
// Sketch(5-15) ==> AgMJAQAazJMLAAAAAAAAAPs4eYkTJI8BCwAAAAAAAABpi7mRuGhXCAgAAAAAAAAA/hYhE/uYvBAGAAAAAAAAAL0yc3JGkcwUBQAAAAAAAABtqRa8SmZhHg8AAAAAAAAAI6VbOBr9dB8MAAAAAAAAAD663KNvb7YgDgAAAAAAAAAa0TALmYwvIgkAAAAAAAAAVx7EnK9BlzoNAAAAAAAAAOD0i+qZg8N8BwAAAAAAAADYLSN3S7k1fgoAAAAAAAAA

console.log("[5-15]: Sum: expected: 110, actual: " + get_estimate("AgMJAQAazJMLAAAAAAAAAPs4eYkTJI8BCwAAAAAAAABpi7mRuGhXCAgAAAAAAAAA/hYhE/uYvBAGAAAAAAAAAL0yc3JGkcwUBQAAAAAAAABtqRa8SmZhHg8AAAAAAAAAI6VbOBr9dB8MAAAAAAAAAD663KNvb7YgDgAAAAAAAAAa0TALmYwvIgkAAAAAAAAAVx7EnK9BlzoNAAAAAAAAAOD0i+qZg8N8BwAAAAAAAADYLSN3S7k1fgoAAAAAAAAA"));