package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type dynamoServerInfo struct {
	Region    string
	Addr      string
	TableName string
	Cleanup   func()
	db        *dynamodb.DynamoDB
}

func setupDynamoServer() (*dynamoServerInfo, error) {
	info := dynamoServerInfo{
		TableName: os.Getenv("DONUTDB_DYNAMODB_TEST_TABLE_NAME"),
		Addr:      os.Getenv("DONUTDB_DYNAMODB_TEST_ADDR"),
		Region:    os.Getenv("DONUTDB_DYNAMODB_TEST_REGION"),
	}

	// if set, test will try to start local dynamo db jar
	dynamoLocalDir := os.Getenv("DONUTDB_DYNAMODB_LOCAL_DIR")

	var cleanups []func()
	info.Cleanup = func() {
		for _, cleanupFunc := range cleanups {
			cleanupFunc()
		}
	}

	if dynamoLocalDir != "" {
		log.Printf("Starting local dyamodb server")

		cmd := exec.Command("java", "-Djava.library.path="+filepath.Join(dynamoLocalDir, "DynamoDBLocal_lib"), "-jar", filepath.Join(dynamoLocalDir, "DynamoDBLocal.jar"), "-inMemory")
		err := cmd.Start()
		if err != nil {
			return nil, err
		}
		cleanups = append(cleanups, func() {
			cmd.Process.Kill()
		})

		if info.Addr == "" {
			info.Addr = "http://localhost:8000"
		}
		if info.Region == "" {
			info.Region = "us-east-2"
		}

		os.Setenv("AWS_ACCESS_KEY_ID", "fakeMyKeyId")
		os.Setenv("AWS_SECRET_ACCESS_KEY", "fakeSecretAccessKey")

		deadline := time.Now().Add(5 * time.Second)
		var connectOK bool
		for time.Now().Before(deadline) {
			resp, err := http.Get(info.Addr)
			if err != nil {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			resp.Body.Close()
			connectOK = true
			break
		}

		if !connectOK {
			return nil, fmt.Errorf("Failed to conncet to test dynamodb server within deadline")
		}
	}

	if info.Region == "" {
		return nil, fmt.Errorf("Missing required environment variables to connect to dynamodb (either local or remote)")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:     &info.Region,
			Endpoint:   &info.Addr,
			MaxRetries: aws.Int(0),
			// LogLevel: aws.LogLevel(aws.LogDebug),
			// Logger:   aws.NewDefaultLogger(),
		},
	}))
	info.db = dynamodb.New(sess)

	if info.TableName == "" {
		info.TableName = fmt.Sprintf("donutdb-test-%d", time.Now().UnixNano())

		_, err := info.db.CreateTable(&dynamodb.CreateTableInput{
			TableName: &info.TableName,
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("hash_key"),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String("range_key"),
					AttributeType: aws.String("N"),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("hash_key"),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String("range_key"),
					KeyType:       aws.String("RANGE"),
				},
			},
			BillingMode: aws.String("PAY_PER_REQUEST"),
		})

		if err != nil {
			return nil, err
		}
	}

	return &info, nil
}
