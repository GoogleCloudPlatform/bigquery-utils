-- csv_to_struct:
-- Input:
-- strList: string that has map in the format a:b,c:d....
-- Output: struct for the above map.
CREATE OR REPLACE FUNCTION fn.csv_to_struct(strList STRING)
AS (
  CASE
    WHEN REGEXP_CONTAINS(strList, ',') OR REGEXP_CONTAINS(strList, ':') THEN
      (ARRAY(
        WITH list AS (
          SELECT l FROM UNNEST(SPLIT(TRIM(strList), ',')) l WHERE REGEXP_CONTAINS(l, ':')
        )
        SELECT AS STRUCT
            TRIM(SPLIT(l, ":")[OFFSET(0)]) AS key, TRIM(SPLIT(l, ":")[OFFSET(1)]) as value
        FROM list
      ))
    ELSE NULL
  END
);
