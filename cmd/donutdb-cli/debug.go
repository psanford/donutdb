package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/spf13/cobra"
)

func debugCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "debug",
		Short: "Debug commands",
	}

	cmd.AddCommand(getKVCommand())

	return &cmd
}

func getKVCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "get_kv <table> <hash_key> <range_key>",
		Short: "Get kv from dynamodb",
		Run:   getKVAction,
	}

	return &cmd
}

func getKVAction(cmd *cobra.Command, args []string) {
	if len(args) < 3 {
		log.Fatalf("Usage: get_kv <dynamodb_table> <hash_key> <range_key>")
	}

	table := args[0]
	hashKey := args[1]
	rangeKey := args[2]

	sess := session.New(&aws.Config{
		Region: &region,
	})
	dynamoClient := dynamodb.New(sess)

	item, err := dynamoClient.GetItem(&dynamodb.GetItemInput{
		TableName: &table,
		Key: map[string]*dynamodb.AttributeValue{
			"hash_key": {
				S: &hashKey,
			},
			"range_key": {
				N: &rangeKey,
			},
		},
	})

	if err != nil {
		log.Fatalf("Get item err: %s", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	result := dynamoAttributeValueMapToEmptyInterfaceMap(item.Item)
	err = enc.Encode(result)
	if err != nil {
		log.Fatal(err)
	}
}

func dynamoAttributeValueMapToEmptyInterfaceMap(in map[string]*dynamodb.AttributeValue) map[string]interface{} {
	out := make(map[string]interface{})
	for k, v := range in {
		out[k] = dynamoAttributeValueToEmptyInterface(v)
	}

	return out
}

func dynamoAttributeValueToEmptyInterface(v *dynamodb.AttributeValue) interface{} {
	if v.B != nil {
		return v.B
	} else if v.BOOL != nil {
		return *v.BOOL
	} else if v.BS != nil {
		return v.BS
	} else if v.L != nil {
		out := make([]interface{}, len(v.L))
		for i, v := range v.L {
			out[i] = dynamoAttributeValueToEmptyInterface(v)
		}
		return out
	} else if v.M != nil {
		return dynamoAttributeValueMapToEmptyInterfaceMap(v.M)
	} else if v.N != nil {
		f, err := strconv.ParseFloat(*v.N, 64)
		if err != nil {
			return *v.N
		}
		return f
	} else if v.NS != nil {
		out := make([]interface{}, len(v.NS))
		for i, v := range v.NS {
			f, err := strconv.ParseFloat(*v, 64)
			if err != nil {
				out[i] = *v
			}
			out[i] = f
		}
	} else if v.NULL != nil {
		return nil
	} else if v.S != nil {
		return *v.S
	} else if v.SS != nil {
		out := make([]interface{}, len(v.L))
		for i, v := range v.L {
			out[i] = dynamoAttributeValueToEmptyInterface(v)
		}
		return out

	}

	return nil
}
