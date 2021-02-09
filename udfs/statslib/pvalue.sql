#standardSQL

CREATE OR REPLACE FUNCTION st.pvalue(H FLOAT64, dof INT64) 
RETURNS FLOAT64 
LANGUAGE js AS """
  return 1.0 - jStat['chisquare'].cdf(H, dof)
"""
OPTIONS ( 
    library=["gs://bqutil-lib/bq_js_libs/jstat-v1.9.4.min.js"]
);