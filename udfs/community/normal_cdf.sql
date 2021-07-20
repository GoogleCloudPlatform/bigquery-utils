CREATE OR REPLACE FUNCTION fn.normal_cdf(x FLOAT64, mean FLOAT64, std FLOAT64)
RETURNS FLOAT64
LANGUAGE js AS """
  return jstat.jStat.normal.cdf( x, mean, std )
"""
OPTIONS (
	    library=["${JS_BUCKET}/jstat-v1.9.4.min.js"]
);
