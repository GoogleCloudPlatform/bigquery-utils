from __future__ import print_function
import base64
import json
import os
from google.cloud import aiplatform
import time
from datetime import datetime


def ai_handler(request):
    request_json = request.get_json()
    mode = request_json['userDefinedContext']['mode']
    calls = request_json['calls']
    if mode == "penguin_weight":
        return penguin_weight(calls)
    #elif mode == "future":
    #    return dlp_decrypt(calls)
    return json.dumps({"Error in Request": request_json}), 400


def penguin_weight(calls):
    return_value = []
         
    for call in calls:
        species=call[0]
        island=call[1]
        sex=call[2]
        culmen_length_mm=call[3]
        culmen_depth_mm=call[4]
        flipper_length_mm=call[5]

        print("Species: " + species)
        print("Island: " + island)
        print("Sex: " + sex)
        print("Culmen Length MM: " + str(culmen_length_mm))        
        print("Culmen Depth MM: " + str(culmen_depth_mm))        
        print("Flipper Length MM: " + str(flipper_length_mm))        

        ##############
        # Insert call here
        result = calculate_weight(species,island,sex,culmen_length_mm,culmen_depth_mm,flipper_length_mm)
        print("CALCULATE WEIGHT RESULT= " + str(result))
        for prediction in result.predictions:
            weight = prediction[0]
            print("Prediction: " + str(weight))
            return_value.append(str(weight))
        ##############
        
    return_json = json.dumps({"replies": return_value})
    return return_json


#  [START aiplatform_sdk_endpoint_predict_sample]
def calculate_weight(species,island,sex,culmen_length_mm,culmen_depth_mm,flipper_length_mm):

    project="<change-me>"
    location="<change-me>"
    endpoint="<change-me>"
    
    mylist =[
    {"species": species,
     "island": island,
     "sex": sex,
     "culmen_length_mm": culmen_length_mm,
     "culmen_depth_mm": culmen_depth_mm,
     "flipper_length_mm": flipper_length_mm
    }
      ]

    print(str(mylist))
    aiplatform.init(project=project, location=location)

    endpoint = aiplatform.Endpoint(endpoint)

    prediction = endpoint.predict(instances=mylist)
    #print(prediction)
    return prediction

