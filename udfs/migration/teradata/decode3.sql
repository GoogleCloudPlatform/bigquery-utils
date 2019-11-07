/*
 * Copyright 2019 Google LLC
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
CREATE OR REPLACE FUNCTION decode3(
  expr ANY TYPE,
  s1 ANY TYPE, r1 ANY TYPE,
  s2 ANY TYPE, r2 ANY TYPE,
  s3 ANY TYPE, r3 ANY TYPE,
  def ANY TYPE) AS
((
  CASE 
    WHEN expr = s1 OR (expr IS NULL AND s1 IS NULL) THEN r1
    WHEN expr = s2 OR (expr IS NULL AND s2 IS NULL) THEN r2
    WHEN expr = s3 OR (expr IS NULL AND s3 IS NULL) THEN r3
    ELSE def
  END 
))
