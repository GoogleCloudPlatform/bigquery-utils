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
