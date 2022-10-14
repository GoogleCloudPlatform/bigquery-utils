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
from google.cloud import translate_v2 as translate
import six

def translation_handler(request):
    try:
        request_json = request.get_json()
        mode = request_json['userDefinedContext']['mode']
        calls = request_json['calls']
        if mode == "translate_text":
            return translate_text(calls)
        return json.dumps({"Error in Request": request_json}), 400
    except Exception as inst:
        return json.dumps( { "errorMessage": 'something unexpected in input' } ), 400



def translate_text(calls):
    try:
        return_value = []
        translate_client = translate.Client()
        target="es"
        for call in calls:
            text = call[0]
            if isinstance(text, six.binary_type):
                text = text.decode("utf-8")
            result = translate_client.translate(text, target_language=target)
            return_value.append(str(result["translatedText"]))
        return_json = json.dumps({"replies": return_value})
        return return_json
    except Exception as inst:
        return json.dumps( { "errorMessage": 'something unexpected in translate_text function' } ), 400
