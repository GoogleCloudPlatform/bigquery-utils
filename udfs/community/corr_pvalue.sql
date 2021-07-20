-- corr_pvalue:
-- Input:
--     r: correlation value, n : number of samples
-- Output: The p value of the correlation 
CREATE OR REPLACE FUNCTION fn.corr_pvalue(r FLOAT64, n INT64 ) 
RETURNS FLOAT64 
LANGUAGE js AS """
var abs_r = Math['abs'](r)
if ( abs_r < 1.0 ) {
   var t =  abs_r * Math['sqrt']( (n-2) / (1.0 - (r*r)) )
   return jstat['jStat']['ttest'](t,n-2,2);

} else if (abs_r == 1.0  ) {
   return 0.0
} else {  
   return NaN 
}
"""
OPTIONS (library=["${JS_BUCKET}/jstat-v1.9.4.min.js"]);
