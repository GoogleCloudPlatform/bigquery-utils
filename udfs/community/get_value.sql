-- Given a key and a map, returns the SCALAR type value.
CREATE OR REPLACE FUNCTION fn.get_value(k STRING, arr ANY TYPE) AS
(
  (SELECT value FROM UNNEST(arr) WHERE key = k)
);
