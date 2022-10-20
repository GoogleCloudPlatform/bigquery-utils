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

PROJECT=$1
DATASET=$2
LOCATION=$3
CONNECTION_NAME=$4
ENDPOINT=$5

NLP_QUERY=" CREATE OR REPLACE  FUNCTION \`$PROJECT.$DATASET.analyze_sentiment_plain_text\` (x STRING) RETURNS STRING REMOTE WITH CONNECTION \`$CONNECTION_NAME\` OPTIONS (endpoint = '$ENDPOINT', user_defined_context = [(\"documentType\",\"PLAIN_TEXT\")])"

# Requires the bq CLI installed.

echo $NLP_QUERY
bq --location=$LOCATION query \
--use_legacy_sql=false \
$NLP_QUERY
