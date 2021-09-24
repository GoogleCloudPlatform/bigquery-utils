# Redshift UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Redshift. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`rs` dataset for reference in queries.

For example, if you'd like to reference the `translate` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.rs.translate('mint tea', 'inea', 'osin')
```

## UDFs

* [initcap](#initcapstring_expr-string)
* [interval_literal_to_seconds](#interval_literal_to_secondsinterval_literal-string)
* [translate](#translateexpression-string-characters_to_replace-string-characters_to_substitute-string)
* [split_part](#split_partstring-string-delimiter-string-part-int64)


## Documentation

### [initcap(string_expr STRING)](initcap.sqlx)
Returns the decimal representation of the first character in the `string_expr`. [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/r_INITCAP.html)
```sql
SELECT bqutil.rs.initcap('À vaillant coeur rien d’impossible')
  , bqutil.rs.initcap('640 k!ouGht tO BE enough~for_anyONE')
  , bqutil.rs.initcap('Simplicity & élÉgance are unpopular because they require hard-work&discipline')
  , bqutil.rs.initcap('one+one is   "(two-one)*[two]"')
  , bqutil.rs.initcap('<lorem>ipsum@GMAIL.COM')

'À Vaillant Coeur Rien D’Impossible', '640 K!Ought To Be Enough~For_Anyone', 'Simplicity & Élégance Are Unpopular Because They Require Hard-Work&Discipline', 'One+One Is   "(Two-One)*[Two]"', '<Lorem>Ipsum@Gmail.Com'
```

### [interval_literal_to_seconds(interval_literal STRING)](interval_literal_to_seconds.sqlx)
This function parses a [Redshift interval literal](https://docs.aws.amazon.com/redshift/latest/dg/r_interval_literals.html) and converts it to seconds.
```sql
SELECT
  bqutil.rs.interval_literal_to_seconds('0.5 days, 3 hours, 59 minutes'),
  bqutil.rs.interval_literal_to_seconds('0.5 d,3h, 59m')

57540, 57540
```

### [translate(expression STRING, characters_to_replace STRING, characters_to_substitute STRING)](translate.sqlx)
For a given expression, replaces all occurrences of specified characters with specified substitutes. Existing characters are mapped to replacement characters by their positions in the `characters_to_replace` and `characters_to_substitute` arguments. If more characters are specified in the `characters_to_replace` argument than in the `characters_to_substitute` argument, the extra characters from the `characters_to_replace` argument are omitted in the return value. [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/r_TRANSLATE.html)
```sql
SELECT bqutil.rs.translate('mint tea', 'inea', 'osin')

most tin
```

### [split_part(string STRING, delimiter STRING, part INT64)](split_part.sqlx)
Splits a string on the specified delimiter and returns the part at the specified position. Position of the portion to return (counting from 1). Must be an integer greater than 0. If part is larger than the number of string portions, SPLIT_PART returns an empty string. If delimiter is not found in string, then the returned value contains the contents of the specified part, which might be the entire string or an empty value. [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/SPLIT_PART.html)
```sql
SELECT bqutil.rs.split_part('2020-02-02', '-' , 1)

2020
```
