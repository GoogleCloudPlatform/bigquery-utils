// package Main queries Google BigQuery.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/Protobuf/proto"

	pb "path/to/proto/file_proto"
)

const (
	projectID = "your-project-id"
)

// Row contains returned row data from bigquery.
type Row struct {
	RowKey string `bigquery:"RowKey"`
	Proto  []byte `bigquery:"ProtoResult"`
}

func main() {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	rows, err := query(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	if err := printResults(os.Stdout, rows); err != nil {
		log.Fatal(err)
	}
}

// query returns a row iterator suitable for reading query results.
func query(ctx context.Context, client *bigquery.Client) (*bigquery.RowIterator, error) {

	query := client.Query(
		`SELECT 
  concat(word, ":", corpus) as RowKey, 
  <dataset-id>.toMyProtoMessage(
    STRUCT(
      word, 
      CAST(word_count AS BIGNUMERIC)
    )
  ) AS ProtoResult 
FROM 
  ` + "` bigquery - public - data.samples.shakespeare `" + ` 
LIMIT 
  100;
`)
	return query.Read(ctx)
}

// printResults prints results from a query.
func printResults(w io.Writer, iter *bigquery.RowIterator) error {
	for {
		var row Row
		err := iter.Next(&row)
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error iterating through results: %w", err)
		}
		message := &pb.TestMessage{}
		if err = proto.Unmarshal(row.Proto, message); err != nil {
			return err
		}
		fmt.Fprintf(w, "rowKey: %s, message: %v\n", row.RowKey, message)
	}
}
