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
import six
import pandas as pd
from google.cloud import pubsub_v1


def pubsub_message(request):
    try:
        return_value = []
        request_json = request.get_json()
        calls = request_json['calls']
        project_id = "<change-me>"
        topic_id = "<change-me>"
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        for call in calls:
            text = call[0]
            data_str = "Message Inserted From BigQuery: " + text
            data = data_str.encode("utf-8")
            future = publisher.publish(topic_path, data)
            #print(future.result())
            #print(f"Published messages to {topic_path}.")
            return_value.append(str(future.result()))
        return_json = json.dumps({"replies": return_value})
        return return_json
    except Exception as inst:
        return json.dumps( { "errorMessage": 'something unexpected in input' } ), 400

