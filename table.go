package donutdb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/donuterr"
)

func (db *DonutDB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	return db.CreateTableWithContext(context.Background(), input)
}

func (db *DonutDB) CreateTableWithContext(ctx context.Context, input *dynamodb.CreateTableInput, opts ...request.Option) (*dynamodb.CreateTableOutput, error) {
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	if input.BillingMode == nil || *input.BillingMode == "PROVISIONED" {
		if input.ProvisionedThroughput == nil {
			return nil, donuterr.ValidationErr("No provisioned throughput specified for the table")
		}
	} else if *input.BillingMode != "PAY_PER_REQUEST" {
		return nil, donuterr.ValidationErr("Unknown BillingMode")
	}

	tableName := *input.TableName

	tables, err := db.donutSQL.ListTables()
	if err != nil {
		return nil, err
	}

	for _, existing := range tables {
		if existing == tableName {
			return nil, donuterr.ResourceInUseErr("Cannot create preexisting table")
		}
	}

	var (
		hashKey      string
		hashKeyType  string
		rangeKey     string
		rangeKeyType string
	)

	attributes := make(map[string]string)

	for _, attr := range input.AttributeDefinitions {
		if err := attr.Validate(); err != nil {
			return nil, err
		}

		name := *attr.AttributeName
		typ := *attr.AttributeType

		if _, exists := attributes[name]; exists {
			return nil, fmt.Errorf("duplicate attribute %q", name)
		}

		if typ != "S" && typ != "N" && typ != "B" {
			return nil, fmt.Errorf("invalid attributeType %q for %q. Must be S|N|B", typ, name)
		}
		attributes[name] = typ
	}

	if len(input.KeySchema) < 1 || len(input.KeySchema) > 2 {
		return nil, fmt.Errorf("invalid KeySchema, must be exactly 1 or two attributes")
	}

	for _, keypart := range input.KeySchema {
		if err := keypart.Validate(); err != nil {
			return nil, err
		}

		name := *keypart.AttributeName
		typ := *keypart.KeyType

		attr, found := attributes[name]
		if !found {
			return nil, fmt.Errorf("missing AttributeDefinition for Key %q", name)
		}

		switch typ {
		case "HASH":
			if hashKey != "" {
				return nil, fmt.Errorf("hash key defined more than once")
			}
			hashKey = name
			hashKeyType = attr
		case "RANGE":
			if rangeKey != "" {
				return nil, fmt.Errorf("range key defined more than once")
			}
			rangeKey = name
			rangeKeyType = attr
		default:
			return nil, fmt.Errorf("unknown KeyTable: %s", typ)
		}
	}

	if hashKey == "" {
		return nil, fmt.Errorf("no HashKey defined for table")
	}

	out, err := db.donutSQL.CreateTable(tableName, hashKey, hashKeyType, rangeKey, rangeKeyType)
	if err != nil {
		return nil, err
	}

	creationTS := time.Unix(out.CreationEpoch, 0)

	result := dynamodb.CreateTableOutput{
		TableDescription: &dynamodb.TableDescription{
			CreationDateTime:     &creationTS,
			AttributeDefinitions: input.AttributeDefinitions,
			KeySchema:            input.KeySchema,
			TableName:            &tableName,
			ItemCount:            aws.Int64(0),
			TableArn:             aws.String("arn:aws:dynamodb:donutdb:000000000000:table/" + tableName),
			TableSizeBytes:       aws.Int64(0),
			TableStatus:          aws.String("ACTIVE"),
		},
	}

	return &result, nil
}

func (db *DonutDB) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	return db.ListTablesWithContext(context.Background(), input)
}
func (db *DonutDB) ListTablesWithContext(ctx context.Context, input *dynamodb.ListTablesInput, opts ...request.Option) (*dynamodb.ListTablesOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	var limit int
	if input.Limit != nil {
		limit = int(*input.Limit)
	}

	if limit < 1 || limit > 100 {
		// dynamodb api says max size for returned TableNames is 100
		limit = 100
	}

	var startTableName string
	if input.ExclusiveStartTableName != nil {
		startTableName = *input.ExclusiveStartTableName
	}

	tables, err := db.donutSQL.ListTables()
	if err != nil {
		return nil, err
	}

	startIdx := 0
	if startTableName != "" {
		for startIdx = 0; startIdx < len(tables); startIdx++ {
			if tables[startIdx] == startTableName {
				startIdx++
				break
			}
		}
	}

	output := dynamodb.ListTablesOutput{
		TableNames: make([]*string, 0),
	}
	if startIdx >= len(tables) {
		// we've seeked to the end, no more tables to return
		return &output, nil
	}

	var lastTableIdx int
	for i := 0; i+startIdx < len(tables) && i < limit; i++ {
		idx := i + startIdx

		tblName := tables[idx]

		output.TableNames = append(output.TableNames, &tblName)
		lastTableIdx = idx
	}

	if lastTableIdx < len(tables)-1 {
		output.LastEvaluatedTableName = aws.String(tables[lastTableIdx])
	}

	return &output, nil
}

func (db *DonutDB) ListTablesPages(input *dynamodb.ListTablesInput, f func(*dynamodb.ListTablesOutput, bool) bool) error {
	return db.ListTablesPagesWithContext(context.Background(), input, f)
}
func (db *DonutDB) ListTablesPagesWithContext(ctx context.Context, input *dynamodb.ListTablesInput, cb func(*dynamodb.ListTablesOutput, bool) bool, opts ...request.Option) error {

	origInput := input
	input = &dynamodb.ListTablesInput{}
	if origInput.ExclusiveStartTableName != nil {
		input.ExclusiveStartTableName = aws.String(*origInput.ExclusiveStartTableName)
	}
	if origInput.Limit != nil {
		input.Limit = aws.Int64(*origInput.Limit)
	}

	for i := 0; ; i++ {
		out, err := db.ListTablesWithContext(ctx, input)
		if err != nil {
			return err
		}

		var last bool
		if out.LastEvaluatedTableName == nil || *out.LastEvaluatedTableName == "" {
			last = true
		}

		stop := cb(out, last)

		if last || stop {
			break
		}

		input.ExclusiveStartTableName = out.LastEvaluatedTableName
	}

	return nil
}
