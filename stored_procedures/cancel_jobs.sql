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


CREATE OR REPLACE PROCEDURE bqutil.procedure.cancel_jobs (project_id STRING, job_id_like_filter STRING)
BEGIN
    DECLARE job_ids ARRAY<STRING>;
    DECLARE i INT64;

    EXECUTE IMMEDIATE
        FORMAT("""
        (SELECT
           ARRAY_AGG(job_id)
         FROM
           `%s`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
         WHERE
           state IN ("RUNNING","PENDING")
           AND job_id LIKE "%s"
           AND job_id <> @@script.job_id);
        """, project_id, job_id_like_filter)
        INTO job_ids;

    SET i = ARRAY_LENGTH(job_ids);
    WHILE i > 0 DO
        BEGIN
            CALL BQ.JOBS.CANCEL(project_id || '.' || job_ids[ORDINAL(i)]);
            SET i = i - 1;
            EXCEPTION WHEN ERROR THEN
                SET i = i - 1;
                SELECT @@error.message;
                CONTINUE;
        END;
    END WHILE;
END;