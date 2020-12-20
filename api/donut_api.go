package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type attributeType int

const (
	stringAttribute attributeType = 1 + iota
	numberAttribute
	binaryAttribute
	boolAttribute
	binarySetAttribute
	listAttribute
	mapAttribute
	numberSetAttribute
	nullAttribute
	stringSetAttribute
)

type attribute struct {
	attributeType attributeType
}

type table struct {
	hashKey      string
	rangeKey     string
	attributes   map[string]attribute
	creationDate time.Time

	mu    sync.Mutex
	items map[string]map[string][]item
}

type itemKey struct {
	hashKey  string
	rangeKey string
}

type item struct {
	rangeKey string
	val      []byte
}

type DBState struct {
	mu     sync.Mutex
	tables map[string]*table
}

func New() *DBState {
	db := &DBState{
		tables: make(map[string]*table),
	}
	return db
}

func (db *DBState) Dispatch(method string, body []byte) (interface{}, error) {
	switch method {
	case "CreateTable":
		var input dynamodb.CreateTableInput
		err := json.Unmarshal(body, &input)
		if err != nil {
			return nil, err
		}
		return db.CreateTable(input)
	case "ListTables":
		var input dynamodb.ListTablesInput
		err := json.Unmarshal(body, &input)
		if err != nil {
			return nil, err
		}
		return db.ListTables(input)
	}

	return nil, errors.New("Unknown method")
}

func (d *DBState) CreateTable(input dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	// "CreateTable":{
	//   "name":"CreateTable",
	//   "http":{
	//     "method":"POST",
	//     "requestUri":"/"
	//   },
	//   "input":{"shape":"CreateTableInput"},
	//   "output":{"shape":"CreateTableOutput"},
	//   "errors":[
	//     {"shape":"ResourceInUseException"},
	//     {"shape":"LimitExceededException"},
	//     {"shape":"InternalServerError"}
	//   ],
	//   "endpointdiscovery":{
	//   }
	// },

	d.mu.Lock()
	defer d.mu.Unlock()

	if input.TableName == nil || *input.TableName == "" {
		return nil, fmt.Errorf("Invalid request: TableName is required")
	}

	tableName := *input.TableName
	if _, exists := d.tables[tableName]; exists {
		return nil, fmt.Errorf("Table already exists")
	}

	tbl := table{
		attributes: make(map[string]attribute),
		items:      make(map[string]map[string][]item),
	}

	for _, attr := range input.AttributeDefinitions {
		if err := attr.Validate(); err != nil {
			return nil, err
		}

		name := *attr.AttributeName
		typ := *attr.AttributeType

		if _, exists := tbl.attributes[name]; exists {
			return nil, fmt.Errorf("duplicate attribute %q", name)
		}

		var outAttr attribute

		switch typ {
		case "S":
			outAttr.attributeType = stringAttribute
		case "N":
			outAttr.attributeType = numberAttribute
		case "B":
			outAttr.attributeType = binaryAttribute
		default:
			return nil, fmt.Errorf("invalid attributeType %q for %q. Must be S|N|B", typ, name)
		}

		tbl.attributes[name] = outAttr
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

		if _, found := tbl.attributes[name]; !found {
			return nil, fmt.Errorf("missing AttributeDefinition for Key %q", name)
		}

		switch typ {
		case "HASH":
			if tbl.hashKey != "" {
				return nil, fmt.Errorf("hash key defined more than once")
			}
			tbl.hashKey = name
		case "RANGE":
			if tbl.rangeKey != "" {
				return nil, fmt.Errorf("range key defined more than once")
			}
			tbl.rangeKey = name
		default:
			return nil, fmt.Errorf("unknown KeyTable: %s", typ)
		}
	}

	if tbl.hashKey == "" {
		return nil, fmt.Errorf("no HashKey defined for table")
	}

	// TODO(PMS)
	// Handle LocalSecondaryIndexes
	// Handle GlobalSecondaryIndexes

	tbl.creationDate = time.Now()
	d.tables[tableName] = &tbl

	result := dynamodb.CreateTableOutput{
		TableDescription: &dynamodb.TableDescription{
			CreationDateTime:     &tbl.creationDate,
			AttributeDefinitions: input.AttributeDefinitions,
			KeySchema:            input.KeySchema,
			TableName:            &tableName,
			ItemCount:            aws.Int64(0),
		},
	}

	return &result, nil
}

func (d *DBState) ListTables(input dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	// XXXX Handle pagination

	var output dynamodb.ListTablesOutput

	d.mu.Lock()
	defer d.mu.Unlock()
	for name := range d.tables {
		nn := name
		output.TableNames = append(output.TableNames, &nn)
	}

	return &output, nil
}

// func (d *DBState) PutItem(input dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
// 	if err := input.Validate(); err != nil {
// 		return nil, err
// 	}

// 	d.mu.Lock()
// 	tbl := d.tables[*input.TableName]
// 	d.mu.Unlock()

// 	if tbl == nil {
// 		return nil, fmt.Errorf("no such table: %s", *input.TableName)
// 	}

// 	hashKey := input.Item[tbl.hashKey]
// 	if hashKey == nil {
// 		return nil, fmt.Errorf("missing primary key attribute")
// 	}

// 	var rangeKey *dynamodb.AttributeValue
// 	if tbl.rangeKey != "" {
// 		rangeKey = input.Item[tbl.rangeKey]
// 		if rangeKey == nil {
// 			return nil, fmt.Errorf("missing primary key attribute")
// 		}
// 	}

// }

// PutItem
// GetItem
// Query
// Scan
// UpdateItem
// DeleteItem
// DeleteTable

// BatchExecuteStatement
// BatchGetItem
// BatchWriteItem
// CreateBackup
// CreateGlobalTable
// DeleteBackup
// DescribeBackup
// DescribeContinuousBackups
// DescribeContributorInsights
// DescribeEndpoints
// DescribeExport
// DescribeGlobalTable
// DescribeGlobalTableSettings
// DescribeKinesisStreamingDestination
// DescribeLimits
// DescribeTable
// DescribeTableReplicaAutoScaling
// DescribeTimeToLive
// DisableKinesisStreamingDestination
// EnableKinesisStreamingDestination
// ExecuteStatement
// ExecuteTransaction
// ExportTableToPointInTime
// ListBackups
// ListContributorInsights
// ListExports
// ListGlobalTables
// ListTables
// ListTagsOfResource
// RestoreTableFromBackup
// RestoreTableToPointInTime
// TagResource
// TransactGetItems
// TransactWriteItems
// UntagResource
// UpdateContinuousBackups
// UpdateContributorInsights
// UpdateGlobalTable
// UpdateGlobalTableSettings
// UpdateTable
// UpdateTableReplicaAutoScaling
// UpdateTimeToLive
