package donutdb

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestCreateTable(t *testing.T) {
	db := mkDB()

	var (
		hashKey  = "contradicted-McDowell"
		rangeKey = "trite-dulcimer"
	)

	input := dynamodb.CreateTableInput{
		TableName: aws.String("stereotyped-Berra"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: &hashKey,
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: &rangeKey,
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: &hashKey,
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: &rangeKey,
				KeyType:       aws.String("RANGE"),
			},
		},
	}

	_, err := db.CreateTable(&input)
	if err != nil {
		t.Fatal(err)
	}

	input2 := dynamodb.CreateTableInput{
		TableName: aws.String("Adderley-souring"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: &hashKey,
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: &hashKey,
				KeyType:       aws.String("HASH"),
			},
		},
	}

	_, err = db.CreateTable(&input2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.CreateTable(&input2)
	if err == nil {
		t.Fatal("Expected creation of duplicate table to fail")
	}

	inputInvalid := dynamodb.CreateTableInput{
		TableName: aws.String("supersedes-budge"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: &hashKey,
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: &rangeKey,
				AttributeType: aws.String("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: &hashKey,
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: &rangeKey,
				KeyType:       aws.String("HASH"),
			},
		},
	}

	_, err = db.CreateTable(&inputInvalid)
	if err == nil {
		t.Fatal("Expected multiple HASH keys to error")
	}
}

func TestListTables(t *testing.T) {
	db := mkDB()
	for i := 0; i < 499; i++ {

		hashKey := "Nelly-adolescences"
		input := dynamodb.CreateTableInput{
			TableName: aws.String(fmt.Sprintf("thrift-moonshine-%d", i)),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: &hashKey,
					AttributeType: aws.String("S"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: &hashKey,
					KeyType:       aws.String("HASH"),
				},
			},
		}

		_, err := db.CreateTable(&input)
		if err != nil {
			t.Fatal(err)
		}
	}

	input := dynamodb.ListTablesInput{
		Limit: aws.Int64(5),
	}
	out, err := db.ListTables(&input)
	if err != nil {
		t.Fatal(err)
	}

	if len(out.TableNames) != 5 {
		t.Fatalf("Expected 5 tables but got %d", len(out.TableNames))
	}

	input = dynamodb.ListTablesInput{}

	gotTables := make(map[string]bool)
	err = db.ListTablesPages(&input, func(out *dynamodb.ListTablesOutput, lastPage bool) bool {
		for _, name := range out.TableNames {
			gotTables[*name] = true
		}
		return false
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(gotTables) != 499 {
		t.Fatalf("expected 499 tables, got %d", len(gotTables))
	}
	for i := 0; i < 499; i++ {
		name := fmt.Sprintf("thrift-moonshine-%d", i)
		if !gotTables[name] {
			t.Fatalf("Missing table %s", name)
		}
	}

	var gotTablesList []string
	err = db.ListTablesPages(&input, func(out *dynamodb.ListTablesOutput, lastPage bool) bool {
		for _, name := range out.TableNames {
			gotTablesList = append(gotTablesList, *name)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(gotTablesList) != 100 {
		t.Fatalf("Expected to stop table name iteration at 100 but got %d", len(gotTablesList))
	}

}

func mkDB() *DonutDB {
	sqldb, err := sql.Open("sqlite3", ":memory:?cache=shared")
	if err != nil {
		panic(err)
	}
	db, err := New(sqldb)
	if err != nil {
		panic(err)
	}

	return db
}
