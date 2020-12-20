package donutdb

import (
	"database/sql"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestCreateTable(t *testing.T) {
	sqldb, err := sql.Open("sqlite3", ":memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(sqldb)
	if err != nil {
		t.Fatal(err)
	}

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

	_, err = db.CreateTable(&input)
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
}
