# Copyright 2020 Google, LLC.
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

# [START cloudrun_helloworld_service]
# [START run_helloworld_service]
from __future__ import print_function
import os
from flask import Flask, request, jsonify
import argparse
import google.cloud.dlp
import base64
import json


app = Flask(__name__)


@app.route("/", methods=['POST'])
def custom_masking_handler():
    request_json = request.get_json()
    mode = request_json['userDefinedContext']['mode']
    calls = request_json['calls']
    if mode == "mask":
        return dlp_custom_mask(calls)
    #elif mode == "decrypt":
    #    return dlp_decrypt(calls)
    return json.dumps({"Error in Request": request_json}), 400

def dlp_custom_mask(calls):
    return_value = []

    dlp = google.cloud.dlp_v2.DlpServiceClient()
    project="<change-me>"

    # Convert the project id into a full resource id.
    parent = f"projects/{project}"

    # Masking Character
    masking_character="#"
    
    # Number of characters to change
    number_to_mask=0
    
    #info_types=["PHONE_NUMBER"]
    
    custom_info_types = [
        {
            "info_type": {"name": "REPLACE_ALL"}, ### Definying my custom data type
            "regex": {"pattern": "([A-Z]$)"},   ### If you use . instead of ([A-Z]$) for example, every character is matched
            "likelihood": google.cloud.dlp_v2.Likelihood.POSSIBLE,
        }
    ]

    # Construct the configuration dictionary with the custom regex info type.
    inspect_config = {
        "custom_info_types": custom_info_types,
        "include_quote": True,
    }
    
    # Construct inspect configuration dictionary
    #inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}

    # Construct deidentify configuration dictionary
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "character_mask_config": {
                            "masking_character": masking_character,
                            "number_to_mask": number_to_mask,
                        }
                    }
                }
            ]
        }
    }
    
    
    
    for call in calls:
        text = call[0]

        ##############
        # Convert string to item
        item = {"value": text}
    
        # Call the API
        response = dlp.deidentify_content(
            request={
                "parent": parent,
                "deidentify_config": deidentify_config,
                "inspect_config": inspect_config,
                "item": item,
            }
        )

        # Print results
        print(response.item.value)

        
        ##############
        
        return_value.append(str(response.item.value))
    return_json = json.dumps({"replies": return_value})
    return return_json


    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
# [END run_helloworld_service]
# [END cloudrun_helloworld_service]
