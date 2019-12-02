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


* [interval_literal_to_seconds](#interval_literal_to_secondsinterval_literal-string)
* [translate](#translateexpression-string-characters_to_replace-string-characters_to_substitute-string)

## Documentation

### [interval_literal_to_seconds(interval_literal STRING)](interval_literal_to_seconds.sql)
This function parses a [Redshift interval literal](https://docs.aws.amazon.com/redshift/latest/dg/r_interval_literals.html) and converts it to seconds.
```sql
SELECT
  bqutil.rs.interval_literal_to_seconds('0.5 days, 3 hours, 59 minutes'),
  bqutil.rs.interval_literal_to_seconds('0.5 d,3h, 59m')

57540, 57540
```


### [translate(expression STRING, characters_to_replace STRING, characters_to_substitute STRING)](translate.sql)
For a given expression, replaces all occurrences of specified characters with specified substitutes. Existing characters are mapped to replacement characters by their positions in the `characters_to_replace` and `characters_to_substitute` arguments. If more characters are specified in the `characters_to_replace` argument than in the `characters_to_substitute` argument, the extra characters from the `characters_to_replace` argument are omitted in the return value. [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/r_TRANSLATE.html)
```sql
SELECT bqutil.rs.translate('mint tea', 'inea', 'osin')

most tin
```


### [initcap(string_expr STRING)](initcap.sql)
Returns the decimal representation of the first character in the `string_expr`. [Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/r_INITCAP.html)
```sql
SELECT bqutil.rs.initcap('À vaillant coeur rien d’impossible')
  , bqutil.rs.initcap('640 k!ouGht tO BE enough~for_anyONE')
  , bqutil.rs.initcap('Simplicity & élÉgance are unpopular because they require hard-work&discipline')
  , bqutil.rs.initcap('one+one is   "(two-one)*[two]"')
  , bqutil.rs.initcap('<lorem>ipsum@GMAIL.COM')

'À Vaillant Coeur Rien D’Impossible', '640 K!Ought To Be Enough~For_Anyone', 'Simplicity & Élégance Are Unpopular Because They Require Hard-Work&Discipline', 'One+One Is   "(Two-One)*[Two]"', '<Lorem>Ipsum@Gmail.Com'
```
