# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from call_nlp.main import call_nlp 
import ast

class test_call_nlp (unittest.TestCase):
    def test_call_nlp(self):
        '''
        Test the NLP call function
        '''
        test_text = [["I love this thing."]]
        result = ast.literal_eval(call_nlp(test_text))
        sentiment_value = float(result["replies"][0])
        self.assertGreaterEqual(sentiment_value,0)

if __name__ == '__main__':
    unittest.main()
