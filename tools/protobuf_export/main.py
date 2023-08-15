"""Queries Google BigQuery."""

from google.cloud import bigquery
from path.to.proto import awesome_pb2


def main():
  project_id = "your-project-id"
  client = bigquery.Client(project=project_id)
  query_job = client.query(query="""
               SELECT
			concat(word , ":",corpus) as RowKey,
			<dataset-id>.toMyProtoMessage(
			    STRUCT(
			      word, 
			      CAST(word_count AS BIGNUMERIC)
			    )
			  ) AS ProtoResult 
		FROM
				  `bigquery-public-data.samples.shakespeare`
		ORDER BY word_count DESC
		LIMIT 20
    """)
  rows = query_job.result()
  for row in rows:
    message = awesome_pb2.TestMessage()
    message.ParseFromString(row.get("ProtoResult"))
    print(
        "rowKey: {}, message: {}".format(row.get("RowKey"), message)
    )
