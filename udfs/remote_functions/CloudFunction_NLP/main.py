# Copyright 2019 Google LLC
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

import json
from google.cloud import language_v1
import pandas as pd

def nlp_handler(request):
    try:
        request_json = request.get_json()
        mode = request_json['userDefinedContext']['mode']
        calls = request_json['calls']
        if mode == "call_nlp":
            return call_nlp(calls)
        elif mode == "something_else":
            return predict_classification(calls)
        return json.dumps({"Error in Request": request_json}), 400
    except Exception as inst:
        return json.dumps( { "errorMessage": 'something unexpected in input' } ), 400


def call_nlp(calls):
    try:
        return_value = []
        client = language_v1.LanguageServiceClient()
        for call in calls:
            text = call[0]
            document = language_v1.Document(
                content=text, type_=language_v1.Document.Type.PLAIN_TEXT
            )
            sentiment = client.analyze_sentiment(
                request={"document": document}
            ).document_sentiment
            return_value.append(str(sentiment.score))
        return_json = json.dumps({"replies": return_value})
        return return_json
    except Exception as inst:
        return json.dumps( { "errorMessage": 'something unexpected in call_nlp function' } ), 400

