package donutdb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/go-cmp/cmp"
)

func TestPutItemHashKey(t *testing.T) {
	dbt := mkDB()

	key1 := "proton-Tolyatti"

	item1 := map[string]*dynamodb.AttributeValue{
		"hash_key": {
			S: &key1,
		},
		"val1": {
			S: aws.String("Cory-TensorFlow"),
		},
	}

	_, err := dbt.db.PutItem(&dynamodb.PutItemInput{
		TableName:    &dbt.hashTable,
		Item:         item1,
		ReturnValues: aws.String("ALL_OLD"),
	})

	if err != nil {
		t.Fatal(err)
	}

	out, err := dbt.db.GetItem(&dynamodb.GetItemInput{
		TableName: &dbt.hashTable,
		Key: map[string]*dynamodb.AttributeValue{
			"hash_key": {
				S: &key1,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(item1, out.Item); diff != "" {
		t.Fatalf("item1 mismatch: %s", diff)
	}
}

func TestPutItemHashRangeKey(t *testing.T) {
	dbt := mkDB()

	hk := "analgesics-patrimony"
	rk := 301.516
	rkStr := fmt.Sprintf("%f", rk)

	item1 := map[string]*dynamodb.AttributeValue{
		"hash_key": {
			S: &hk,
		},
		"range_key": {
			N: &rkStr,
		},
		"val1": {
			S: aws.String("attaining-Guernsey"),
		},
	}

	_, err := dbt.db.PutItem(&dynamodb.PutItemInput{
		TableName:    &dbt.hashRangeTable,
		Item:         item1,
		ReturnValues: aws.String("ALL_OLD"),
	})

	if err != nil {
		t.Fatal(err)
	}

	out, err := dbt.db.GetItem(&dynamodb.GetItemInput{
		TableName: &dbt.hashRangeTable,
		Key: map[string]*dynamodb.AttributeValue{
			"hash_key": {
				S: &hk,
			},
			"range_key": {
				N: &rkStr,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(item1, out.Item); diff != "" {
		t.Fatalf("item1 mismatch: %s", diff)
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
