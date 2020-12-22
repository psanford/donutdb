package donutdb

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
		return nil, errors.New("ConditionExpresion not yet implemented in DonutDB")
	}

	if input.ConditionalOperator != nil {
		return nil, errors.New("ConditionalOperator not yet implemented in DonutDB")
	}

	if input.Expected != nil {
		return nil, errors.New("Expected not yet implemented in DonutDB")
	}

	if input.ExpressionAttributeNames != nil || input.ExpressionAttributeValues != nil {
		return nil, errors.New("ExpressionAttributes not yet implemented in DonutDB")

	}

	tbl, err := db.getTableMetadata(*input.TableName)
	if err == sql.ErrNoRows {
		return nil, validationErr("no such table")
	} else if err != nil {
		return nil, err
	}

	hashKeyAttr := input.Item[tbl.HashKey]
	if hashKeyAttr == nil {
		return nil, validationErr("missing hash key")
	}
	var rangeKeyAttr *dynamodb.AttributeValue

	if tbl.RangeKey != "" {
		rangeKeyAttr = input.Item[tbl.RangeKey]
		if rangeKeyAttr == nil {
			return nil, validationErr("missing range key")
		}
	}

	hashedKey, keyVal, err := hashKeyBytes(hashKeyAttr, tbl.HashKeyType)
	if err != nil {
		return nil, fmt.Errorf("hash key err: %w", err)
	}

	args := []interface{}{
		hashedKey,
		keyVal,
	}
	var rangeKey interface{}

	if tbl.RangeKey != "" {
		rangeKey, err = rangeKeyI(rangeKeyAttr, tbl.RangeKeyType)
		if err != nil {
			return nil, fmt.Errorf("range key err: %w", err)
		}
		args = append(args, rangeKey)
	}

	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}

	var oldItem map[string]*dynamodb.AttributeValue
	if input.ReturnValues != nil && *input.ReturnValues == "ALL_OLD" {
		args := []interface{}{
			hashedKey,
			keyVal,
		}
		if tbl.RangeKey != "" {
			args = append(args, rangeKey)
		}
		query := fmt.Sprintf("SELECT donutdb_data from %s where donutdb_hash_key=? and '%s'=? and '%s'=?",
			tbl.Name, tbl.HashKey, tbl.RangeKey)
		row := tx.QueryRow(query, args...)
		var oldItemJSON []byte
		err = row.Scan(&oldItemJSON)
		if err == sql.ErrNoRows {
		} else if err != nil {
			tx.Rollback()
			return nil, err
		} else {
			err := json.Unmarshal(oldItemJSON, &oldItem)
			if err != nil {
				tx.Rollback()
				return nil, fmt.Errorf("corrupt old item in db: %w", err)
			}
		}
	}

	marshalledItem, err := json.Marshal(input.Item)
	if err != nil {
		return nil, fmt.Errorf("marshal item err: %s", err)
	}
	args = append(args, marshalledItem)

	qs := strings.Repeat("?,", len(args)-1) + "?"
	stmt := fmt.Sprintf("INSERT OR REPLACE INTO %s VALUES (%s)", tbl.Name, qs)

	tx.Exec(stmt, args...)

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit data err: %w", err)
	}

	output := dynamodb.PutItemOutput{
		Attributes: oldItem,
	}

	return &output, nil
}

func (db *DonutDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return db.GetItemWithContext(context.Background(), input)
}
func (db *DonutDB) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {

	return nil, ToBeImplementedErr
}

func hashKeyBytes(keyAttr *dynamodb.AttributeValue, typ string) ([]byte, interface{}, error) {
	var hashBytes []byte
	var key interface{}

	switch typ {
	case "TEXT":
		if keyAttr.S == nil {
			return nil, nil, errors.New("invalid type for string key")
		}
		hashBytes = []byte(*keyAttr.S)
		key = *keyAttr.S
	case "REAL":
		if keyAttr.N == nil {
			return nil, nil, errors.New("invalid type for numeric key")
		}
		f, err := strconv.ParseFloat(*keyAttr.N, 64)
		if err != nil {
			return nil, nil, errors.New("non-numeric value for numeric key")
		}

		hashBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(hashBytes, math.Float64bits(f))

		key = f
	case "BLOB":
		if keyAttr.B == nil {
			return nil, nil, errors.New("invalid type for binary key")
		}
		hashBytes = keyAttr.B
		key = keyAttr.B
	default:
		return nil, nil, errors.New("unexpected hash key type in database")
	}

	hasher := murmur3.New64()
	hasher.Write(hashBytes)
	return hasher.Sum(nil), key, nil
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
