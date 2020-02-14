# BigQuery json2DDL
Parse BigQuery schema in JSON format and convert it to a DDL statement.

Usage: `bq_json2ddl.py <schema.json> <outputfile>`
  
Example: 
```
$ bq show --format=prettyjson bigquery-public-data:bitcoin_blockchain.transactions > transactions.json
$ cat transactions.json

{
  "creationTime": "1517334072059", 
  "description": "****NOTE: This dataset has been migrated to `bigquery-public-data.crypto_bitcoin`. Updates to the data are being sent to the new version of this dataset, whose schema is better aligned with our other cryptocurrency offerings. \n\nThis version of the data is no longer being updated and will be removed within the next 15 days.", 
  "etag": "cXmsZhVvPxjsjamAu99VKg==", 
  "id": "bigquery-public-data:bitcoin_blockchain.transactions", 
  "kind": "bigquery#table", 
  "lastModifiedTime": "1565630752730", 
  "location": "US", 
  "numBytes": "630440706916", 
  "numLongTermBytes": "630440706916", 
  "numRows": "340311544", 
  "schema": {
    "fields": [
      {
        "mode": "NULLABLE", 
        "name": "timestamp", 
        "type": "INTEGER"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "transaction_id", 
        "type": "STRING"
      }, 
      {
        "fields": [
          {
            "mode": "NULLABLE", 
            "name": "input_script_bytes", 
            "type": "BYTES"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "input_script_string", 
            "type": "STRING"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "input_script_string_error", 
            "type": "STRING"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "input_sequence_number", 
            "type": "INTEGER"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "input_pubkey_base58", 
            "type": "STRING"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "input_pubkey_base58_error", 
            "type": "STRING"
          }
        ], 
        "mode": "REPEATED", 
        "name": "inputs", 
        "type": "RECORD"
      }, 
      {
        "fields": [
          {
            "mode": "NULLABLE", 
            "name": "output_satoshis", 
            "type": "INTEGER"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "output_script_bytes", 
            "type": "BYTES"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "output_script_string", 
            "type": "STRING"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "output_script_string_error", 
            "type": "STRING"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "output_pubkey_base58", 
            "type": "STRING"
          }, 
          {
            "mode": "NULLABLE", 
            "name": "output_pubkey_base58_error", 
            "type": "STRING"
          }
        ], 
        "mode": "REPEATED", 
        "name": "outputs", 
        "type": "RECORD"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "block_id", 
        "type": "STRING"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "previous_block", 
        "type": "STRING"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "merkle_root", 
        "type": "STRING"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "nonce", 
        "type": "INTEGER"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "version", 
        "type": "INTEGER"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "work_terahash", 
        "type": "INTEGER"
      }, 
      {
        "mode": "NULLABLE", 
        "name": "work_error", 
        "type": "STRING"
      }
    ]
  }, 
  "selfLink": "https://bigquery.googleapis.com/bigquery/v2/projects/bigquery-public-data/datasets/bitcoin_blockchain/tables/transactions", 
  "tableReference": {
    "datasetId": "bitcoin_blockchain", 
    "projectId": "bigquery-public-data", 
    "tableId": "transactions"
  }, 
  "type": "TABLE"
}
$ python bq_json2ddl.py ./transactions.json ./transactions.ddl
$ cat transactions.ddl 

  CREATE TABLE `bigquery-public-data`.bitcoin_blockchain.transactions
  (
    timestamp INT64, 
    transaction_id STRING, 
    inputs ARRAY<STRUCT<  
      input_script_bytes BYTES, 
      input_script_string STRING, 
      input_script_string_error STRING, 
      input_sequence_number INT64, 
      input_pubkey_base58 STRING, 
      input_pubkey_base58_error STRING 
    >>, 
    outputs ARRAY<STRUCT<  
      output_satoshis INT64, 
      output_script_bytes BYTES, 
      output_script_string STRING, 
      output_script_string_error STRING, 
      output_pubkey_base58 STRING, 
      output_pubkey_base58_error STRING 
    >>, 
    block_id STRING, 
    previous_block STRING, 
    merkle_root STRING, 
    nonce INT64, 
    version INT64, 
    work_terahash INT64, 
    work_error STRING
  ) 
  OPTIONS(
    description = """****NOTE: This dataset has been migrated to `bigquery-public-data.crypto_bitcoin`. Updates to the data are being sent to the new version of this dataset, whose schema is better aligned with our other cryptocurrency offerings. 

This version of the data is no longer being updated and will be removed within the next 15 days."""
  )
```
