#standardSQL

CREATE OR REPLACE FUNCTION fn.chisquare_cdf(H FLOAT64, dof INT64) 
RETURNS FLOAT64 
LANGUAGE js AS """
  return 1.0 - jstat.jStat['chisquare'].cdf(H, dof)
"""
OPTIONS ( 
    library=["${JS_BUCKET}/jstat-v1.9.4.min.js"]
);
