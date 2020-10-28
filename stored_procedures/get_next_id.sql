-- Generates next ids and inserts them into a sample table.
-- This implementation prevents against race condition.
--
-- @param INT64 id_count number of id to increase
-- @return ARRAY<int64> an array of generated ids

-- a sample id table
CREATE OR REPLACE TABLE bqutil.procedure.Ids (id INT64);

CREATE OR REPLACE PROCEDURE bqutil.procedure.GetNextIds (id_count INT64, OUT next_ids ARRAY<INT64>)
BEGIN
  DECLARE id_start INT64 DEFAULT (SELECT COUNT(*) FROM bqutil.procedure.Ids);
  SET next_ids = GENERATE_ARRAY(id_start, id_start + id_count);
 
  MERGE bqutil.procedure.Ids
  USING UNNEST(next_ids) AS new_id
  ON id = new_id
  WHEN MATCHED THEN UPDATE SET id = ERROR(FORMAT('Race when adding ID %t', new_id))
  WHEN NOT MATCHED THEN INSERT VALUES (new_id);
END;

-- a unit test of GetNextIds
BEGIN
  DECLARE i INT64 DEFAULT 1;
  DECLARE next_ids ARRAY<INT64> DEFAULT [];
  DECLARE ids ARRAY<INT64> DEFAULT [];
  WHILE i < 10 DO
    CALL bqutil.procedure.GetNextIds(10, next_ids);
    SET ids = ARRAY_CONCAT(ids, next_ids);
    SET i = i + 1;
  END WHILE;
  SELECT FORMAT('IDs are: %t', ids);
END;
