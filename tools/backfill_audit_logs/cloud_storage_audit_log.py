from argparse import ArgumentParser
import functools
import json

class CloudStorageAuditLog:
    """
    Given the result of a gcloud logging read, build an Object that represents what we need in order to load into 
    """
    def __init__(self, json_dict: dict):
        self.logName = get_nested_dict_values(['logName'], json_dict)

        self.resource = {
            "type": get_nested_dict_values(['resource', 'type'], json_dict),
            "labels": {
                "bucket_name": get_nested_dict_values(['resource', 'labels', 'bucket_name'], json_dict),
                "location": get_nested_dict_values(['resource', 'labels', 'location'], json_dict),
                "project_id": get_nested_dict_values(['resource', 'labels', 'project_id'], json_dict)
            }
        }

        self.protopayload_auditlog = {
            "serviceName": get_nested_dict_values(['protoPayload', 'serviceName'], json_dict),
            "methodName": get_nested_dict_values(['protoPayload', 'methodName'], json_dict),
            "resourceName": get_nested_dict_values(['protoPayload', 'resourceName'], json_dict),
            "authenticationInfo": {
                "principalEmail": get_nested_dict_values(['protoPayload', 'authenticationInfo', 'principalEmail'], json_dict),
                "serviceAccountDelegationInfo": get_nested_dict_values(['protoPayload', 'serviceAccountDelegationInfo'], json_dict),
                
            },
            "authorizationInfo": get_nested_dict_values(['protoPayload', 'authorizationInfo'], json_dict),
            "requestMetadata":{
                "callerIp": get_nested_dict_values(['protoPayload', 'requestMetadata', 'callerIp'], json_dict),
                "callerSuppliedUserAgent": get_nested_dict_values(['protoPayload', 'requestMetadata', 'callerSuppliedUserAgent'], json_dict),
            }
        }

        self.timestamp = get_nested_dict_values(['timestamp'], json_dict)
        self.receiveTimestamp = get_nested_dict_values(['receiveTimestamp'], json_dict)
        self.severity = get_nested_dict_values(['severity'], json_dict)
        self.insertId = get_nested_dict_values(['insertId'], json_dict)

        self.row = {
            "logName": self.logName,
            "resource": self.resource,
            "protopayload_auditlog": self.protopayload_auditlog,
            "timestamp": self.timestamp,
            "receiveTimestamp": self.receiveTimestamp,
            "severity": self.severity,
            "insertId": self.insertId
        }

def get_nested_dict_values(keys: list, dictionary: dict) -> str:
    return functools.reduce(lambda val, key: val.get(key) if val else None, keys, dictionary)

def get_cmd_line_args():
    parser = ArgumentParser(
        description='This tool backfills the audit data '
                    'that was created before the creation'
                    'of the Logs Router')
    parser.add_argument('--logging_raw_data',
                        help='The raw data from the results of a command like'
                             'gcloud logging read')
    parser.add_argument('--output_file_location',
                        help='where you want this utility to write the files to.')
    return parser.parse_args()


def main():
    args = get_cmd_line_args()
    with open(args.logging_raw_data, 'r') as json_file:
        json_data = json.loads(json_file)

    rows = ''
    number_of_rows = 0
    for item in json_data:
        row = json.dumps(CloudStorageAuditLog(item).row)
        if number_of_rows == 0:
            rows += row
        else:
            rows += '\n'+ row
        number_of_rows += 1

    with open(args.output_file_location, 'w') as output_file:
        output_file.write(rows)


if __name__ == '__main__':
    main()
