package donutdb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestPutItem(t *testing.T) {
	dbt := mkDB()

	key1 := "proton-Tolyatti"

	_, err := dbt.db.PutItem(&dynamodb.PutItemInput{
		TableName: &dbt.hashTable,
		Item: map[string]*dynamodb.AttributeValue{
			"hash_key": &dynamodb.AttributeValue{
				S: &key1,
			},
			"val1": &dynamodb.AttributeValue{
				S: aws.String("Cory-TensorFlow"),
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

}

type testDB struct {
	db             *DonutDB
	hashTable      string
	hashRangeTable string
}

func mkDB() testDB {
	db := mkEmptyDB()

	var (
		hashTblName      = "hash_tbl"
		hashRangeTblName = "hash_range_tbl"
		hashKey          = "hash_key"
		rangeKey         = "range_key"
	)

	input := dynamodb.CreateTableInput{
		TableName: aws.String(hashRangeTblName),
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
		BillingMode: aws.String("PAY_PER_REQUEST"),
	}

	_, err := db.CreateTable(&input)
	if err != nil {
		panic(err)
	}

	input = dynamodb.CreateTableInput{
		TableName: aws.String(hashTblName),
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
		BillingMode: aws.String("PAY_PER_REQUEST"),
	}

	_, err = db.CreateTable(&input)
	if err != nil {
		panic(err)
	}

	return testDB{
		db:             db,
		hashTable:      hashTblName,
		hashRangeTable: hashRangeTblName,
	}
}
