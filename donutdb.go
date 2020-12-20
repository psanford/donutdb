package donutdb

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DonutDB struct {
	db *sql.DB
}

func (db *DonutDB) Dispatch(method string, body []byte) (interface{}, error) {
	switch method {
	case "CreateTable":
		var input dynamodb.CreateTableInput
		err := json.Unmarshal(body, &input)
		if err != nil {
			return nil, err
		}
		return db.CreateTable(&input)
	case "ListTables":
		var input dynamodb.ListTablesInput
		err := json.Unmarshal(body, &input)
		if err != nil {
			return nil, err
		}
		return db.ListTables(&input)
	}

	return nil, errors.New("Unknown method")
}

func New(db *sql.DB) (*DonutDB, error) {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS __donutdb_global_state (
name TEXT NOT NULL PRIMARY KEY,
value
)`)
	if err != nil {
		return nil, err
	}

	row := db.QueryRow("SELECT value from __donutdb_global_state where name = ?", "schema_version")
	var version int
	err = row.Scan(&version)
	if err != nil {
		_, err = db.Exec("INSERT INTO __donutdb_global_state (name, value) values (?, ?)", "schema_version", 1)
		if err != nil {
			return nil, err
		}
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS __donutdb_table_metadata (
name TEXT NOT NULL PRIMARY KEY,
creation_epoch INTEGER,
hash_key TEXT,
hash_key_type TEXT,
range_key TEXT,
range_key_type TEXT,
hash_function TEXT
)`)

	if err != nil {
		return nil, err
	}

	return &DonutDB{
		db: db,
	}, nil
}
