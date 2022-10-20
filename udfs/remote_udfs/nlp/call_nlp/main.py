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

import json
from google.cloud import language_v1
from google.api_core.retry import Retry

# https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations
client = language_v1.LanguageServiceClient()

def analyze_sentiment(request):
    '''
    This function serves the request.
    The additional parameter "mode" is part of a key value pair sent as part of the call.
    For more information on the UserDefinedContext see:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#input_format
    '''
    request_json = request.get_json()
    language = request_json['userDefinedContext']['language']
    # The input into the function from BigQuery is retrieved with the calls field. 
    calls = request_json['calls']
    try:
        return_value = []
        # Create the client to connecto the Language Service.
        for call in calls:
            # Retreive the text to be analyzed
            text = call[0]
            # Prepare the Document object to be sent to the service.
            document = language_v1.Document(
                content=text,
                type_=language_v1.Document.Type.PLAIN_TEXT,
                language=language
            )
            # Use the analyze_sentiment function to call the API.
            # This returns a sentiment analysis.
            # Append the sentiment to be returned.
            sentiment = client.analyze_sentiment(
                request={"document": document},
                # Retry default values: https://github.com/googleapis/python-api-core/blob/main/google/api_core/retry.py#L72
                retry=Retry()
            ).document_sentiment
            return_value.append(str(sentiment.score))

        # Format the JSON to be readable by BigQuery
        return_json = json.dumps({"replies": return_value})
        return return_json
    except Exception as inst:
        return json.dumps({"errorMessage": inst}), 400
