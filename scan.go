package donutdb

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/donuterr"
)

func (db *DonutDB) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return db.ScanWithContext(context.Background(), input)
}
func (db *DonutDB) ScanWithContext(ctx context.Context, input *dynamodb.ScanInput, opts ...request.Option) (*dynamodb.ScanOutput, error) {
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	return nil, donuterr.ToBeImplementedErr
}

func (db *DonutDB) ScanPages(input *dynamodb.ScanInput, f func(*dynamodb.ScanOutput, bool) bool) error {
	return db.ScanPagesWithContext(context.Background(), input, f)
}
func (db *DonutDB) ScanPagesWithContext(ctx context.Context, input *dynamodb.ScanInput, f func(*dynamodb.ScanOutput, bool) bool, opts ...request.Option) error {
	return donuterr.ToBeImplementedErr
}
