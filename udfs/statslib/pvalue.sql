#standardSQL

/*
## clone git repot
> git clone https://github.com/jstat/jstat.git

## sync jstat to GCS as
> gsutil rsync -r ./jstat gs://bq-stats-test-jslib/jstat

*/

CREATE OR REPLACE FUNCTION st.pvalue(H FLOAT64, dof INT64) 
RETURNS FLOAT64 
LANGUAGE js AS """
  return 1.0 - jStat['chisquare'].cdf(H, dof)
"""
OPTIONS ( 
    library="gs://isb_nih/jstat/dist/jstat.js");