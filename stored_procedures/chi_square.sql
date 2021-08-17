-- @param STRING table_name table (or subquery) that contains the data
-- @param STRING independent_var name of the column in our table that represents our independent variable
-- @param STRING dependent_var name of the column in our table that represents our dependent variable
-- TODO: return struct rather than record (?)
-- @return RECORD <FLOAT64 chi_square, FLOAT64 degrees_freedom>

-- TODO: Use group by for scalability/performance; break into several statements
CREATE OR REPLACE PROCEDURE bqutil.procedure.chi_square (table_name STRING, independent_var STRING, dependent_var STRING )
BEGIN
EXECUTE IMMEDIATE """
    WITH contingency_table AS (
        SELECT DISTINCT
            """ || independent_var || """ as independent_var,
            """ || dependent_var || """ as dependent_var,
            COUNT(*) OVER(PARTITION BY """ || independent_var || """, """ || dependent_var || """) as count,
            COUNT(*) OVER(PARTITION BY """ || independent_var || """) independent_total,
            COUNT(*) OVER(PARTITION BY """ || dependent_var || """) dependent_total,
            COUNT(*) OVER() as total
        FROM """ || table_name || """ AS t0
    ),
    expected_table AS (
        SELECT
            independent_var,
            dependent_var,
            independent_total * dependent_total / total as count
        FROM `contingency_table`
    )
    SELECT
        SUM(POW(contingency_table.count - expected_table.count, 2) / expected_table.count) as chi_square,
        (COUNT(DISTINCT contingency_table.independent_var) - 1)
            * (COUNT(DISTINCT contingency_table.dependent_var) - 1) AS degrees_freedom
    FROM contingency_table
    INNER JOIN expected_table
        ON expected_table.independent_var = contingency_table.independent_var
        AND expected_table.dependent_var = contingency_table.dependent_var
""";
END;

-- a unit test of chi_square
-- TODO: this is pretty slow, we should do one insert with lots of records rather than looping through
BEGIN
  DECLARE i INT64 DEFAULT 0;
  CREATE TEMP TABLE categorical (sex STRING, party STRING);

  WHILE i < 2 DO
      INSERT INTO categorical (sex, party) VALUES('male', 'republican');
      SET i = i + 1;
  END WHILE;

  SET i = 0;
  WHILE i < 1 DO
      INSERT INTO categorical (sex, party) VALUES('male', 'democrat');
      SET i = i + 1;
  END WHILE;

  SET i = 0;
  WHILE i < 3 DO
      INSERT INTO categorical (sex, party) VALUES('female', 'republican');
      SET i = i + 1;
  END WHILE;

  SET i = 0;
  WHILE i < 2 DO
      INSERT INTO categorical (sex, party) VALUES('female', 'democrat');
      SET i = i + 1;
  END WHILE;

  CALL bqutil.procedure.chi_square('categorical', 'sex', 'party');
--   TODO: print assertion that result is what we expect
END;
