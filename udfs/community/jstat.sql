CREATE OR REPLACE FUNCTION fn.jstat(method STRING, args ARRAY<FLOAT64>)
RETURNS FLOAT64
LANGUAGE js AS """
    const methodPath = method['split']('.')
    let fn = jstat['jStat']
    for (const name of methodPath){
        fn = fn[name]
    }

  return fn(...args)
"""
OPTIONS (
	    library=["${JS_BUCKET}/jstat-v1.9.4.min.js"]
);
