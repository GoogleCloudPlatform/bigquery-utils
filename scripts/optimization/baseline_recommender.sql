/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Baseline Recommendation Validator
 *
 * Purpose:
 * Analyzes historical slot usage at a minute-level granularity to recommend
 * optimal Baseline Commitments (p50) and Autoscale Limits (p95).
 *
 * Use Cases:
 * 1. Rightsizing existing reservations (Audit).
 * 2. Calculating commitments for new contracts (Capacity Planning).
 * 3. Identifying "Spiky" vs. "Stable" workloads (Volatility Analysis).
 *
 * Prerequisites:
 * - Requires 'roles/bigquery.resourceViewer' at the Organization level.
 * - This script targets the Organization view by default.
 *
 * Volatility:
 * This score works to identify which workloads are spiky(volatility_score > 3)
 * vs flat(volatility_score < 1.5), this helps setting commitments for the flat 
 * reservations without risk on losing performance.
 */

DECLARE num_days_scan INT64 DEFAULT 14; -- Lookback window (recommended: 14-30 days)
DECLARE reservation_filter STRING DEFAULT '%'; -- Filter for specific reservation IDs (optional)

SELECT
  reservation_id,
  
  -- Sizing Recommendations
  ROUND(APPROX_QUANTILES(SUM(period_slot_ms) / (1000 * 60), 100)[OFFSET(50)], 0) AS recommended_baseline_slots,
  ROUND(APPROX_QUANTILES(SUM(period_slot_ms) / (1000 * 60), 100)[OFFSET(95)], 0) AS recommended_autoscale_limit,
  
  -- Volatility Score
  ROUND(
    APPROX_QUANTILES(SUM(period_slot_ms) / (1000 * 60), 100)[OFFSET(95)] / 
    NULLIF(APPROX_QUANTILES(SUM(period_slot_ms) / (1000 * 60), 100)[OFFSET(50)], 0), 2
  ) AS volatility_score,
  
  -- Supporting Metrics
  ROUND(AVG(SUM(period_slot_ms) / (1000 * 60)), 0) AS avg_slots,
  ROUND(MAX(SUM(period_slot_ms) / (1000 * 60)), 0) AS max_slots_observed

FROM
  `region-us.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION` -- Region is selected through this FROM clause
WHERE
  job_creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL num_days_scan DAY)
  AND job_type = 'QUERY'
  AND statement_type != 'SCRIPT'
  AND reservation_id LIKE reservation_filter
GROUP BY
  reservation_id,
  TIMESTAMP_TRUNC(period_start, MINUTE)
ORDER BY
  recommended_baseline_slots DESC;