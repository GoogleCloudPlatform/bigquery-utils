# Statistical UDFs

This directory contains community contributed [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions) for Statistical Analysis
to extend BigQuery for more specialized usage patterns. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`fn` dataset for reference in queries.

For example, if you'd like to reference the `int` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.fn.int(1.684)
```

## UDFs

* [kruskal_wallis_udf](#kruskal_wallisarrstructfactor-string-val-float64)

## Documentation

### [kruskal_wallis(arr(struct(factor STRING, val FLOAT64))](kruskal_wallis.sql)
Takes an array of struct where each struct (point) represents a measurement, with a group label and a measurement value

The [Kruskal–Wallis test by ranks](https://en.wikipedia.org/wiki/Kruskal%E2%80%93Wallis_one-way_analysis_of_variance), Kruskal–Wallis H test (named after William Kruskal and W. Allen Wallis), or one-way ANOVA on ranks is a non-parametric method for testing whether samples originate from the same distribution. It is used for comparing two or more independent samples of equal or different sample sizes. It extends the Mann–Whitney U test, which is used for comparing only two groups. The parametric equivalent of the Kruskal–Wallis test is the one-way analysis of variance (ANOVA).

* Input: array: struct <factor STRING, val FLOAT64>
* Output: struct<H FLOAT64, p-value FLOAT64, DOF FLOAT64>
```sql
DECLARE data ARRAY<STRUCT<factor STRING, val FLOAT64>>;

set data = [
('a',1.0),
('b',2.0),
('c',2.3),
('a',1.4),
('b',2.2),
('c',5.5),
('a',1.0),
('b',2.3),
('c',2.3),
('a',1.1),
('b',7.2),
('c',2.8)
];


SELECT `lib_stats.kruskal_wallis_udf`(data) as results;
```

results:

| results.H	| results.p	| results.DoF	|
|-----------|-----------|-------------|
| 3.4230769 | 0.1805877 | 2           |