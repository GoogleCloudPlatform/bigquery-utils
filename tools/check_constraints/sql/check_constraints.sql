create or replace procedure constraints_staging.check_constraints(itable_name string, idataset_name string)
begin
  declare columns array<string>;
  declare count, cnt, dcnt int64;
  declare failed array<string> default [];

  # validate UNIQUE:
  execute immediate """select array(
  SELECT
   column_name
  FROM """ || idataset_name || """.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
  WHERE
   table_name = ?
   and cast(json_value(description, "$.unique") as bool) = True
  )
  """ 
  into columns
  using itable_name;
  
  set count = 0;
  while count < ARRAY_LENGTH(columns) do
    execute immediate """
      select 
        count(distinct """ || columns[OFFSET(count)] || """), 
        count(""" || columns[OFFSET(count)] || """) 
      from """ || idataset_name || "." || itable_name
    into cnt, dcnt;
    
    if cnt <> dcnt then
      set failed = failed || ["Column " || columns[OFFSET(count)] || " violates unique constraint"];
    end if;
    set count = count + 1; 
  end while;
  
  # validate CHECK:
  execute immediate """select array(
  SELECT
   json_value(description, "$.check")
  FROM """ || idataset_name || """.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
  WHERE
   table_name = ?
   and json_value(description, "$.check") is not null
  )
  """ 
  into columns
  using itable_name;

  set count = 0;
  while count < ARRAY_LENGTH(columns) do
    execute immediate """
      select 1
        from """ || idataset_name || "." || itable_name || """
        where not (""" || columns[OFFSET(count)] || """)
        limit 1"""
    into cnt;
    if cnt is not NULL then
      set failed = failed || ["Violated check constraint: " || columns[OFFSET(count)]];
    end if;
    set count = count + 1; 
  end while;

  if array_length(failed) > 0 then
    RAISE USING MESSAGE = TO_JSON_STRING(failed);
  end if;
end;

