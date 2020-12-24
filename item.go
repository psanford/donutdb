package donutdb

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/donuterr"
	"github.com/psanford/donutdb/internal/donutsql"
	"github.com/twmb/murmur3"
)

func (db *DonutDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return db.PutItemWithContext(context.Background(), input)
}
func (db *DonutDB) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	if input.ConditionExpression != nil {
		return nil, donuterr.FieldNotImplementedErr("ConditionExpression")
	}

	if input.ConditionalOperator != nil {
		return nil, donuterr.FieldNotImplementedErr("ConditionalOperator")
	}

	if input.Expected != nil {
		return nil, donuterr.FieldNotImplementedErr("Expected")
	}

	if input.ExpressionAttributeNames != nil || input.ExpressionAttributeValues != nil {
		return nil, donuterr.FieldNotImplementedErr("ExpressionAttributes")
	}

	tbl, err := db.donutSQL.TableMetadata(*input.TableName)
	if err == sql.ErrNoRows {
		return nil, donuterr.ValidationErr("no such table")
	} else if err != nil {
		return nil, err
	}

	hashKeyAttr := input.Item[tbl.HashKey]
	if hashKeyAttr == nil {
		return nil, donuterr.ValidationErr("missing hash key")
	}
	var rangeKeyAttr *dynamodb.AttributeValue

	if tbl.RangeKey != "" {
		rangeKeyAttr = input.Item[tbl.RangeKey]
		if rangeKeyAttr == nil {
			return nil, donuterr.ValidationErr("missing range key")
		}
	}

	var oldItem donutsql.Item

	if tbl.RangeKey != "" {
		oldItem, err = db.donutSQL.Insert(tbl, hashKeyAttr, rangeKeyAttr, input.Item)
		if err != nil {
			return nil, err
		}
	} else {
		oldItem, err = db.donutSQL.Insert(tbl, hashKeyAttr, nil, input.Item)
		if err != nil {
			return nil, err
		}
	}

	var output dynamodb.PutItemOutput
	if input.ReturnValues != nil && *input.ReturnValues == "ALL_OLD" {
		output.Attributes = oldItem
	}

	return &output, nil
}

func (db *DonutDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return db.GetItemWithContext(context.Background(), input)
}
func (db *DonutDB) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	if input.AttributesToGet != nil {
		return nil, donuterr.FieldNotImplementedErr("AttributesToGet")
	}
	if input.ConsistentRead != nil {
		return nil, donuterr.FieldNotImplementedErr("ConsistentRead")
	}
	if input.ExpressionAttributeNames != nil {
		return nil, donuterr.FieldNotImplementedErr("ExpressionAttributeNames")
	}
	if input.ProjectionExpression != nil {
		return nil, donuterr.FieldNotImplementedErr("ProjectionExpression")
	}
	if input.ReturnConsumedCapacity != nil {
		return nil, donuterr.FieldNotImplementedErr("ReturnConsumedCapacity")
	}

	tbl, err := db.donutSQL.TableMetadata(*input.TableName)
	if err == sql.ErrNoRows {
		return nil, donuterr.ValidationErr("no such table")
	} else if err != nil {
		return nil, err
	}

	hashKeyAttr := input.Key[tbl.HashKey]
	if hashKeyAttr == nil {
		return nil, donuterr.ValidationErr("missing hash key")
	}

	var item donutsql.Item

	if tbl.RangeKey != "" {
		rangeKeyAttr := input.Key[tbl.RangeKey]
		item, err = db.donutSQL.Get(tbl, hashKeyAttr, rangeKeyAttr)
	} else {
		item, err = db.donutSQL.Get(tbl, hashKeyAttr, nil)
	}

	out := dynamodb.GetItemOutput{
		Item: item,
	}

	return &out, nil
}

func (db *DonutDB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return db.DeleteItemWithContext(context.Background(), input)
}
func (db *DonutDB) DeleteItemWithContext(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	if input.ConditionExpression != nil {
		return nil, donuterr.FieldNotImplementedErr("ConditionExpression")
	}
	if input.ConditionalOperator != nil {
		return nil, donuterr.FieldNotImplementedErr("ConditionalOperator")
	}
	if input.Expected != nil {
		return nil, donuterr.FieldNotImplementedErr("Expected")
	}
	if input.ExpressionAttributeNames != nil || input.ExpressionAttributeValues != nil {
		return nil, donuterr.FieldNotImplementedErr("ExpressionAttributes")
	}
	if input.ReturnConsumedCapacity != nil {
		return nil, donuterr.FieldNotImplementedErr("ReturnConsumedCapacity")
	}
	if input.ReturnItemCollectionMetrics != nil {
		return nil, donuterr.FieldNotImplementedErr("ReturnItemCollectionMetrics")
	}

	tbl, err := db.donutSQL.TableMetadata(*input.TableName)
	if err == sql.ErrNoRows {
		return nil, donuterr.ValidationErr("no such table")
	} else if err != nil {
		return nil, err
	}

	hashKeyAttr := input.Key[tbl.HashKey]
	if hashKeyAttr == nil {
		return nil, donuterr.ValidationErr("missing hash key")
	}

	rangeKeyAttr := input.Key[tbl.RangeKey]
	item, err := db.donutSQL.Delete(tbl, hashKeyAttr, rangeKeyAttr)
	if err != nil {
		return nil, err
	}

	var output dynamodb.DeleteItemOutput
	if input.ReturnValues != nil && *input.ReturnValues == "ALL_OLD" {
		output.Attributes = item
	}

	return &output, nil
}

func hashKeyBytes(keyAttr *dynamodb.AttributeValue, typ string) (string, interface{}, error) {
	var hashBytes []byte
	var key interface{}

	switch typ {
	case "TEXT":
		if keyAttr.S == nil {
			return "", nil, errors.New("invalid type for string key")
		}
		hashBytes = []byte(*keyAttr.S)
		key = *keyAttr.S
	case "REAL":
		if keyAttr.N == nil {
			return "", nil, errors.New("invalid type for numeric key")
		}
		f, err := strconv.ParseFloat(*keyAttr.N, 64)
		if err != nil {
			return "", nil, errors.New("non-numeric value for numeric key")
		}

		hashBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(hashBytes, math.Float64bits(f))

		key = f
	case "BLOB":
		if keyAttr.B == nil {
			return "", nil, errors.New("invalid type for binary key")
		}
		hashBytes = keyAttr.B
		key = keyAttr.B
	default:
		return "", nil, errors.New("unexpected hash key type in database")
	}

	hasher := murmur3.New64()
	hasher.Write(hashBytes)
	return hex.EncodeToString(hasher.Sum(nil)), key, nil
}

func rangeKeyI(keyAttr *dynamodb.AttributeValue, typ string) (interface{}, error) {
	switch typ {
	case "TEXT":
		if keyAttr.S == nil {
			return nil, errors.New("invalid type for string key")
		}
		return *keyAttr.S, nil
	case "REAL":
		if keyAttr.N == nil {
			return nil, errors.New("invalid type for numeric key")
		}
		f, err := strconv.ParseFloat(*keyAttr.N, 64)
		if err != nil {
			return nil, errors.New("non-numeric value for numeric key")
		}

		return f, nil
	case "BLOB":
		if keyAttr.B == nil {
			return nil, errors.New("invalid type for binary key")
		}
		return keyAttr.B, nil
	}
	return nil, errors.New("unexpected hash key type in database")
}
