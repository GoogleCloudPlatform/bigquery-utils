#!/bin/bash

DATASET=""

#write all tables in a dataset to a reference TXT file
bq ls --max_results=10000 ${DATASET} | awk '{ print $1 }' | sed '1,2d' > table_list.txt

#loop through each table and export policy tags (if any) to a CSV
echo "Writing to CSV..."
while IFS= read -r TABLE; do
    TAG_COUNT="`bq show --schema ${DATASET}.${TABLE} | grep "policyTags" | wc -l`"

    if [ "${TAG_COUNT}" -ge 1 ]
    then
        COLUMN=`bq show --format=prettyjson ${DATASET}.${TABLE} | jq '.schema.fields[] | select(.policyTags | length>=1)' | jq '.name'`
        TAG_ID=`bq show --format=prettyjson ${DATASET}.${TABLE} | jq '.schema.fields[] | select(.policyTags | length>=1)' | jq '.policyTags.names[]'`
        echo ${TABLE},${COLUMN},${TAG_ID} | tr -d '"'
    fi
done < table_list.txt >> policy_tags.csv
echo "Done."