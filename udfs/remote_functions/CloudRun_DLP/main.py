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
def dlp_handler():
    request_json = request.get_json()
    mode = request_json['userDefinedContext']['mode']
    calls = request_json['calls']
    if mode == "encrypt":
        return dlp_encrypt(calls)
    elif mode == "decrypt":
        return dlp_decrypt(calls)
    return json.dumps({"Error in Request": request_json}), 400

def dlp_encrypt(calls):
    return_value = []

    dlp = google.cloud.dlp_v2.DlpServiceClient()
    project="<change-me>"
    keyring_name="<change-me>"
    short_key_name="<change-me>"
    #https://cloud.google.com/dlp/docs/infotypes-reference
    info_types=["PHONE_NUMBER","EMAIL_ADDRESS","IP_ADDRESS"]
    surrogate_type="DLP_SURROGATE"
    key_name="projects/" + project + "/locations/global/keyRings/" + keyring_name + "/cryptoKeys/" + short_key_name
    wrapped_key= "<change-me>"
    parent = f"projects/{project}"
    wrapped_key = base64.b64decode(wrapped_key)

    # Construct Deterministic encryption configuration dictionary
    crypto_replace_deterministic_config = {
        "crypto_key": {
            "kms_wrapped": {"wrapped_key": wrapped_key, "crypto_key_name": key_name}
        },
    }

    # Add surrogate type
    if surrogate_type:
        crypto_replace_deterministic_config["surrogate_info_type"] = {
            "name": surrogate_type
        }

    # Construct inspect configuration dictionary
    inspect_config = {"info_types": [{"name": info_type} for info_type in info_types]}

    # Construct deidentify configuration dictionary
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "crypto_deterministic_config": crypto_replace_deterministic_config
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



def dlp_decrypt(calls):
    return_value = []

    dlp = google.cloud.dlp_v2.DlpServiceClient()
    project="<change-me>"
    keyring_name="<change-me>"
    short_key_name="<change-me>"
    #https://cloud.google.com/dlp/docs/infotypes-reference
    info_types=["PHONE_NUMBER","EMAIL_ADDRESS","IP_ADDRESS"]
    surrogate_type="DLP_SURROGATE"
    key_name="projects/" + project + "/locations/global/keyRings/" + keyring_name + "/cryptoKeys/" + short_key_name
    wrapped_key= "<change-me>"
    parent = f"projects/{project}"
    wrapped_key = base64.b64decode(wrapped_key)

    # Construct reidentify Configuration
    reidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "crypto_deterministic_config": {
                            "crypto_key": {
                                "kms_wrapped": {
                                    "wrapped_key": wrapped_key,
                                    "crypto_key_name": key_name,
                                }
                            },
                            "surrogate_info_type": {"name": surrogate_type},
                        }
                    }
                }
            ]
        }
    }

    inspect_config = {
        "custom_info_types": [
            {"info_type": {"name": surrogate_type}, "surrogate_type": {}}
        ]
    }

    for call in calls:
        text = call[0]

        ##############
        # Convert string to item
        item = {"value": text}
    
        # Call the API
        response = dlp.reidentify_content(
            request={
                "parent": parent,
                "reidentify_config": reidentify_config,
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
