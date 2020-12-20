package donutdb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
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

func (db *DonutDB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	return db.CreateTableWithContext(context.Background(), input)
}

func (db *DonutDB) CreateTableWithContext(ctx context.Context, input *dynamodb.CreateTableInput, opts ...request.Option) (*dynamodb.CreateTableOutput, error) {
	err := input.Validate()
	if err != nil {
		return nil, err
	}

	tableName := *input.TableName

	tables, err := db.listTables()
	if err != nil {
		return nil, err
	}

	for _, existing := range tables {
		if existing == tableName {
			return nil, errors.New("table already exists")
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

		var sqlType string

		switch typ {
		case "S":
			sqlType = "TEXT"
		case "N":
			sqlType = "REAL"
		case "B":
			sqlType = "BLOB"
		default:
			return nil, fmt.Errorf("invalid attributeType %q for %q. Must be S|N|B", typ, name)
		}

		attributes[name] = sqlType
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

	if rangeKey == "" {
		stmtTxt := fmt.Sprintf(`CREATE TABLE '%s' (
    donutdb_hash_key TEXT PRIMARY KEY,
    '%s' '%s',
    data BLOB
  )`, tableName, hashKey, hashKeyType)
		// use prepare to avoid executing more than one statement (a.la sql injection)
		stmt, err := db.db.Prepare(stmtTxt)
		if err != nil {
			return nil, fmt.Errorf("create table err: %q %w", stmtTxt, err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, fmt.Errorf("create table err: %q %w", stmtTxt, err)
		}

	} else {
		stmtTxt := fmt.Sprintf(`CREATE TABLE '%s' (
    donut_db_hash_key TEXT,
    '%s' %s,
    '%s' %s,
    data BLOB,
    PRIMARY KEY (donut_db_hash_key, '%s')
  )`, tableName, hashKey, hashKeyType, rangeKey, rangeKeyType, hashKey)
		// use prepare to avoid executing more than one statement (a.la sql injection)
		stmt, err := db.db.Prepare(stmtTxt)
		if err != nil {
			return nil, fmt.Errorf("create table err: %q %w", stmtTxt, err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, fmt.Errorf("create table err: %w", err)
		}

		stmt, err = db.db.Prepare(fmt.Sprintf("CREATE INDEX range_key_idx on '%s' ('%s')", tableName, rangeKey))
		if err != nil {
			return nil, fmt.Errorf("create table err: %w", err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, fmt.Errorf("create table err: %w", err)
		}
	}

	_, err = db.db.Exec(`INSERT INTO __donutdb_table_metadata
(name, creation_epoch, hash_key, hash_key_type, range_key, range_key_type, hash_function)
VALUES (?,?,?,?,?,?,?)`,
		tableName, time.Now().Unix(), hashKey, hashKeyType, rangeKey, rangeKeyType, defaultHashFunction)
	if err != nil {
		return nil, fmt.Errorf("update metadata err: %w", err)
	}

	return nil, nil
}

const defaultHashFunction = "murmur3"

func (db *DonutDB) listTables() ([]string, error) {
	rows, err := db.db.Query("SELECT name FROM __donutdb_table_metadata")
	if err != nil {
		return nil, err
	}

	var tables []string

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return tables, nil
}
