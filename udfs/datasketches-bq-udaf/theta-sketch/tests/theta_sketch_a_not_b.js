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

const base64abc = [
  "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
  "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
  "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
  "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
  "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "+", "/"
];

 function bytesToBase64(bytes) {
  let result = '', i, l = bytes.length;
  for (i = 2; i < l; i += 3) {
    result += base64abc[bytes[i - 2] >> 2];
    result += base64abc[((bytes[i - 2] & 0x03) << 4) | (bytes[i - 1] >> 4)];
    result += base64abc[((bytes[i - 1] & 0x0F) << 2) | (bytes[i] >> 6)];
    result += base64abc[bytes[i] & 0x3F];
  }
  if (i === l + 1) { // 1 octet yet to write
    result += base64abc[bytes[i - 2] >> 2];
    result += base64abc[(bytes[i - 2] & 0x03) << 4];
    result += "==";
  }
  if (i === l) { // 2 octets yet to write
    result += base64abc[bytes[i - 2] >> 2];
    result += base64abc[((bytes[i - 2] & 0x03) << 4) | (bytes[i - 1] >> 4)];
    result += base64abc[(bytes[i - 1] & 0x0F) << 2];
    result += "=";
  }
  return result;
}

// Note: Bytes in BQ is represented as :
//     a) base64 encoded string in JS for UDF
//     b) Uint8Array in JS for UDAF
function a_not_b(sketch_a_bytes, sketch_b_bytes) {
  var sketchBinary_a = intArrayFromBase64(sketch_a_bytes);
  var sketchBinary_b = intArrayFromBase64(sketch_b_bytes);
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
}

// Sketch(1-10) ==> AgMDAAAazJMKAAAAAAAAABX5fcu9hqEFQN4u4cnbPQhpi7mRuGhXCP4WIRP7mLwQvTJzckaRzBTDl/wSgXCdHhrRMAuZjC8iukCzwdoGaV3g9IvqmYPDfNgtI3dLuTV+
let sketch_a = "AgMDAAAazJMKAAAAAAAAABX5fcu9hqEFQN4u4cnbPQhpi7mRuGhXCP4WIRP7mLwQvTJzckaRzBTDl/wSgXCdHhrRMAuZjC8iukCzwdoGaV3g9IvqmYPDfNgtI3dLuTV+";

// Sketch(9-20)
let sketch_b = "AgMDAAAazJMMAAAAAAAAAPs4eYkTJI8BbakWvEpmYR4jpVs4Gv10Hz663KNvb7YgGtEwC5mMLyJXHsScr0GXOmf4rfV42Ys/4e4o4grYzUS3YhopjQlLSGQ9Em6L1ztJ3J/8sdRHKG/YLSN3S7k1fg==";
let byte_array_diff = a_not_b(sketch_a, sketch_b);
console.log(byte_array_diff)