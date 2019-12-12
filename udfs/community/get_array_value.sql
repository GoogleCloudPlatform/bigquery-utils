-- Given a key and a map, returns the ARRAY type value.
-- This is same as getValue except it returns an ARRAY type.
-- This can be used when the map has multiple values for a given key.
CREATE OR REPLACE FUNCTION fn.get_array_value(k STRING, arr ANY TYPE) AS
(
  ARRAY(SELECT value FROM UNNEST(arr) WHERE key = k)
);
