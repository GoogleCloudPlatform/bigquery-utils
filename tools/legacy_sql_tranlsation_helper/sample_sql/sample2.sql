SELECT product, inventory, cast(null as string) as color,  FROM `projectid.test.simple_table1`
UNION ALL SELECT product, inventory, color,  FROM `projectid.test.simple_table2`