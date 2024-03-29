#!/bin/bash

# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Prompt user for DATASET value if not set 
if [ -z "$DATASET" ]; then
  read -p "Enter the BigQuery dataset name: " DATASET
fi

#write all tables in a dataset to a reference TXT file
bq ls --max_results=10000 ${DATASET} | awk '{ print $1 }' | sed '1,2d' > table_list.txt

#loop through each table and export policy tags (if any) to a CSV
echo "Writing to CSV..."
while IFS= read -r TABLE; do
    TAG_COUNT="`bq show --schema ${DATASET}.${TABLE} | grep "policyTags" | wc -l`"

    if [ "${TAG_COUNT}" -ge 1 ]
    then
        COLUMN_AND_TAG=`bq show --format=prettyjson ${DATASET}.${TABLE} | jq '.schema.fields[] | select(.policyTags | length>=1)'`
        COLUMN=`echo $COLUMN_AND_TAG | jq '.name'`
        TAG_ID=`echo $COLUMN_AND_TAG | jq '.policyTags.names[]'`
        echo ${TABLE},${COLUMN},${TAG_ID} | tr -d '"'
    fi
done < table_list.txt >> policy_tags.csv
echo "Done."
