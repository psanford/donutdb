package main

// import C is necessary for us to export DonutDBRegister in the c-archive .a file

import "C"

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb"
	"github.com/psanford/sqlite3vfs"
)

var (
	region = "us-east-1"
)

//export DonutDBRegister
func DonutDBRegister() {
	tableName := os.Getenv("DONUTDB_TABLE")
	if tableName == "" {
		panic("DONUTDB_TABLE environment variable not set")
	}

	if os.Getenv("AWS_DEFAULT_REGION") != "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	sess := session.New(&aws.Config{
		Region: &region,
	})
	dynamoClient := dynamodb.New(sess)

	vfs := donutdb.New(dynamoClient, tableName)

	fmt.Printf("sqlite3vfs register donutdb\n")
	err := sqlite3vfs.RegisterVFS("donutdb", vfs)
	if err != nil {
		panic(fmt.Sprintf("Register VFS err: %s", err))
	}
	fmt.Printf("sqlite3vfs register donutdb done\n")
}

func main() {
}
