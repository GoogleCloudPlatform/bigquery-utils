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