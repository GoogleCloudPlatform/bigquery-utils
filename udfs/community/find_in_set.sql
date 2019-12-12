-- find_in_set:
-- Returns the first occurance of str in strList where strList is a comma-delimited string.
-- Returns null if either argument is null.
-- Returns 0 if the first argument contains any commas.
-- For example, find_in_set('ab', 'abc,b,ab,c,def') returns 3.
-- Input:
-- str: string to search for.
-- strList: string in which to search for.
-- Output: Position of str in strList
CREATE OR REPLACE FUNCTION fn.find_in_set(str STRING, strList STRING)
AS (
  CASE
    WHEN STRPOS(str, ',') > 0 THEN 0
    ELSE
    (
      WITH list AS (
        SELECT ROW_NUMBER() OVER() id, l FROM UNNEST(SPLIT(strList, ',')) l
      )
      (SELECT id FROM list WHERE l = str)
    )
  END
);
