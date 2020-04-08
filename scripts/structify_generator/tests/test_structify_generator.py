# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests for UDF generator command line utility."""

import unittest
import structify_generator


class TestUDFGenerator(unittest.TestCase):
    """Test suite for UDF command line generator utility."""
    def test_build_unnest_str_pass(self):
        """Tests the success scenarios for build_unnest_str method."""
        self.assertEqual('', structify_generator.build_unnest_str([]))

        expected = (
            ' a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM '
            'UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM '
            'UNNEST(col.b) b)')
        self.assertEqual(expected,
                         structify_generator.build_unnest_str(['a', 'b']))

        expected = (' a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM '
                    'UNNEST(col.a) a),  b_c_t AS (SELECT ROW_NUMBER() OVER() '
                    'idx, b_c FROM UNNEST(col.b_c) b_c)')
        self.assertEqual(expected,
                         structify_generator.build_unnest_str(['a', 'b_c']))

    def test_build_unnest_str_fail(self):
        """Tests the failure scenarios for build_unnest_str method."""
        pass

    def test_get_structify_udf_pass(self):
        """Tests the success scenarios for get_structify_udf method."""
        expected = """
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION structify(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b)
    (SELECT AS STRUCT a, b
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx
    )
  )
);
"""
        self.assertEqual(expected,
                         structify_generator.get_structify_udf(['a', 'b']))

        expected = """
-- inputs to structify: struct<array1<type1>, array2<type2>, array3<type3>, ...>
-- output: array<struct<type1, type2, type3, ...>
CREATE TEMP FUNCTION custom_name(col ANY TYPE)
AS (
  ARRAY(
    WITH  a_t AS (SELECT ROW_NUMBER() OVER() idx, a FROM UNNEST(col.a) a),  b_t AS (SELECT ROW_NUMBER() OVER() idx, b FROM UNNEST(col.b) b)
    (SELECT AS STRUCT a, b
      FROM a_t FULL JOIN b_t ON a_t.idx = b_t.idx
    )
  )
);
"""
        self.assertEqual(
            expected,
            structify_generator.get_structify_udf(['a', 'b'],
                                                  udf_name='custom_name'))

    def test_get_structify_udf_fail(self):
        """Tests the failure scenarios for get_structify_udf method."""
        with self.assertRaises(Exception):
            structify_generator.get_structify_udf(['a-b'])
        with self.assertRaises(Exception):
            structify_generator.get_structify_udf(['a'], udf_name='bad-name')


if __name__ == '__main__':
    unittest.main()
