/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE OR REPLACE PROCEDURE bqutil.procedure.delete_datasets (project_id STRING, dataset_like_filter STRING)
BEGIN
    DECLARE datasets ARRAY<STRING>;
    DECLARE i INT64 DEFAULT 0;

    EXECUTE IMMEDIATE
        FORMAT("""
        (SELECT
           ARRAY_AGG(SCHEMA_NAME)
         FROM region-us.INFORMATION_SCHEMA.SCHEMATA
         WHERE SCHEMA_NAME LIKE "%s")
        """, dataset_like_filter)
        INTO datasets;

    SET i = ARRAY_LENGTH(datasets);
    WHILE i > 0 DO
        BEGIN
            EXECUTE IMMEDIATE
                FORMAT("DROP SCHEMA IF EXISTS `%s`.%s CASCADE",
                       project_id, datasets[ORDINAL(i)]);
            SET i = i - 1;
            EXCEPTION WHEN ERROR THEN
                SET i = i - 1;
                SELECT @@error.message;
                CONTINUE;
        END;
    END WHILE;
END;
