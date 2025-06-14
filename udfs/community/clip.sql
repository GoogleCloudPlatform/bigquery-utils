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

CREATE OR REPLACE FUNCTION fn.clip_less(x FLOAT64, a FLOAT64) AS (
  IF (x < a, a, x)
);

CREATE OR REPLACE FUNCTION fn.clip_gt(x FLOAT64, b FLOAT64) AS (
  IF (x > b, b, x)
);

CREATE OR REPLACE FUNCTION fn.clip(x FLOAT64, a FLOAT64, b FLOAT64) AS (
  fn.clip_gt(fn.clip_less(x, a), b)
);
