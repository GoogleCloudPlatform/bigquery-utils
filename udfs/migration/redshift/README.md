# Redshift UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Redshift. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`rs` dataset for reference in queries.

## UDFs

* [initcap](#initcapstring_expr-string)

## Documentation

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
