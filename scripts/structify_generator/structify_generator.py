# Copyright 2019 Google Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#pylint: disable=line-too-long
"""Command line utility to generate a BigQuery standard SQL Structify UDF.

Command line utility to generate a BigQuery standard SQL Structify UDF.
A structify UDF converts a struct with repeated columns into a flat array of
structs with the respective columns as member fields of the struct.

Inputs:
- Columns list: Space separated list of column names
- UDF name: Default UDF name is 'structify'. Can be overriden with this flag.
- Output path: Optional parameter to write the generated UDF to a file.

Output:
- The generated UDF is printed to STDOUT and written to file if the output path
parameter is passed.

Usage: python structify_generator.py [-h] --columns COLUMNS [COLUMNS ...]
                                     [--output_path OUTPUT_PATH]
                                     [--udf_name UDF_NAME]

Examples:

1. Use default name for generated UDF:
python structify_generator.py --columns a b c

output:
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION structify(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b),  c_t AS (SELECT ROW_NUMBER() OVER() idx, c FROM UNNEST(col.c) c)
    (SELECT AS STRUCT a, b, c
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx FULL JOIN c_t ON b_t.idx = c_t.idx
    )
  )
);

2. Override default name for generted UDF:
python structify_generator.py --columns y m d --udf_name custom_name

-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION custom_name(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b),  c_t AS (SELECT ROW_NUMBER() OVER() idx, c FROM UNNEST(col.c) c)
    (SELECT AS STRUCT a, b, c
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx FULL JOIN c_t ON b_t.idx = c_t.idx
    )
  )
);

"""
#pylint: enable=line-too-long

import argparse
import re

STRUCTIFY_UDF_TEMPLATE = """
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION %(UDF_NAME)s(col ANY TYPE)
AS (
  ARRAY(
    WITH %(UNNEST_QUERIES)s
    (SELECT AS STRUCT %(COLUMNS_STRING)s
      FROM %(JOIN_CLAUSES)s
    )
  )
);
"""

DEFAULT_UDF_NAME = 'structify'

UNNEST = '{0}_t AS (SELECT ROW_NUMBER() OVER() idx, {0} FROM UNNEST(col.{0}) {0})'

JOIN = 'FULL JOIN'

COLUMN_NAME_RE = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')


def get_structify_udf(column_names, udf_name=None):
    """Returns the structify and get_column UDFs for the given set of columns.

  Args:
    column_names: list<str>, columns of array type that need to be structified.
  Returns:
    string with structify UDF and get_column UDF.
  """
    if not udf_name:
        udf_name = DEFAULT_UDF_NAME
    else:
        if not COLUMN_NAME_RE.match(udf_name):
            raise Exception('Invalid UDF name.')
    for col in column_names:
        if not COLUMN_NAME_RE.match(col):
            raise Exception(
                'Column name does not follow BigQuery naming convention.')
    columns_string = ', '.join(column_names)
    unnest_str = build_unnest_str(column_names)
    join_str = build_join_str(column_names)
    structify_udf = STRUCTIFY_UDF_TEMPLATE % {
        'UDF_NAME': udf_name,
        'COLUMNS_STRING': columns_string,
        'UNNEST_QUERIES': unnest_str,
        'JOIN_CLAUSES': join_str
    }
    return structify_udf


def build_unnest_str(column_names):
    """Returns the get_column UDF for the given set of columns.

  Args:
    column_names: list<str>, columns of array type that need to be structified.
  Returns:
    string with structify UDF and get_column UDF.
  Raises:
    Exception: Column name does not follow BigQuery naming convention.
  """
    unnest_str = ''
    for col in column_names:
        if unnest_str:
            unnest_str = f'{unnest_str}, '
        unnest_str = f'{unnest_str} {UNNEST.format(col)}'
    return unnest_str


def build_join_str(column_names):
    """Returns the get_column UDF for the given set of columns.

  Args:
    column_names: list<str>, columns of array type that need to be structified.
  Returns:
    string with structify UDF and get_column UDF.
  Raises:
    Exception: Column name does not follow BigQuery naming convention.
  """
    join_str = ''
    for i, col in enumerate(column_names):
        if i == 0:
            join_str = f'{col}_t'
            continue
        join_str = f'{join_str} FULL JOIN {col}_t ON {column_names[i - 1]}_t.idx = {col}_t.idx'
    return join_str


def write_to_file(udfs, output_path):
    """Writes UDF string to output path.

  Args:
    udfs: str, UDF string.
    output_path: str, valid file path. If none exists new file will be created
      Existing file will be overwritten.
  """
    with open(output_path, 'w+') as f_p:
        for udf in udfs:
            f_p.write(udf)


def main():
    """Main function for UDF command line."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--columns',
                        help='List of columns to be structified.',
                        nargs='+',
                        required=True)
    parser.add_argument('--output_path',
                        help='(Optional) Output to a file.',
                        type=str)
    parser.add_argument('--udf_name',
                        help='(Optional) Output to a file.',
                        type=str,
                        required=False)
    args = parser.parse_args()
    results = get_structify_udf(args.columns, udf_name=args.udf_name)
    print(results)
    if args.output_path:
        write_to_file([results], args.output_path)


if __name__ == '__main__':
    main()
