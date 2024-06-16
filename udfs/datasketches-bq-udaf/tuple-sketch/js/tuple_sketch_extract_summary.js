var sketchBinary = intArrayFromBase64(sketch);
var ptr = Module._malloc(sketchBinary.length);
Module.HEAPU8.subarray(ptr, ptr + sketchBinary.length).set(sketchBinary);

var compact_sketch =
    Module._compact_sketch_deserialize(ptr, sketchBinary.length);
try {
  return {
    "key_distinct_count" : Module._compact_sketch_get_estimate_count(compact_sketch),
    "value_sum" : Module._compact_sketch_get_estimate_sum(compact_sketch),
    "value_avg" : Module._compact_sketch_get_estimate_avg(compact_sketch)
  }
} finally {
  Module._compact_sketch_destroy(compact_sketch);
  Module._free(ptr);
}