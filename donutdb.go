package donutdb

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/donutsql"
)

type DonutDB struct {
	donutSQL *donutsql.DB
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
	donutSQL := donutsql.New(db)
	if err := donutSQL.Init(); err != nil {
		return nil, err
	}

	return &DonutDB{
		donutSQL: donutSQL,
	}, nil
}
