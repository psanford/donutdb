package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/google/go-cmp/cmp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/donutdb"
	"github.com/psanford/donutdb/donuthttp"
)

var dynamoAddr = flag.String("dynamo-addr", "http://127.0.0.1:8000", "Address of upstream dynamodb server (could be local dynamodb)")
var accessKey = flag.String("access-key-id", "DUMMYIDEXAMPLE", "AWS access key id")
var secretAccessKey = flag.String("secret-access-key", "DUMMYIDEXAMPLEKEY", "AWS secret access key")
var region = flag.String("region", "us-west-2", "AWS Region to use")
var nsFlag = flag.String("test-namespace", "", "Set test namespace (blank means use a new namespace (which is probably what you want)")

var ns string

func main() {
	flag.Parse()

	if *nsFlag != "" {
		ns = *nsFlag
	} else {
		ns = strconv.Itoa(int(time.Now().UnixNano()))
	}

	creds := credentials.NewStaticCredentials(*accessKey, *secretAccessKey, "")
	sessUpstream := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials: creds,
			Region:      region,
			Endpoint:    dynamoAddr,
			MaxRetries:  aws.Int(0),
		},
	}))

	upstream := dynamodb.New(sessUpstream)

	sqldb, err := sql.Open("sqlite3", ":memory:?cache=shared")
	if err != nil {
		panic(err)
	}
	ourDB, err := donutdb.New(sqldb)
	if err != nil {
		panic(err)
	}

	ourServer := donuthttp.NewServer(ourDB)
	// ourServer.Logger = logger.StdoutLogger
	ourServer.AccessKey = *accessKey
	ourServer.SecretAccessKey = *secretAccessKey

	ourServer.Start()

	credsOurs := credentials.NewStaticCredentials(*accessKey, *secretAccessKey, "")
	sessOurs := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials: credsOurs,
			Region:      region,
			Endpoint:    &ourServer.URL,
			MaxRetries:  aws.Int(0),

			// LogLevel: aws.LogLevel(aws.LogDebug),
			// Logger:   aws.NewDefaultLogger(),
		},
	}))
	us := dynamodb.New(sessOurs)

	checks := []checkFunc{
		createTableNoThroughput,
		createTablePayAsYouGo,
	}
	for _, cf := range checks {
		err = check(upstream, us, cf)
		if err != nil {
			log.Println(err)
		}
	}

}

func check(them, us dynamodbiface.DynamoDBAPI, f checkFunc) error {
	funcName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	fmt.Fprintf(os.Stderr, "# %s\n", funcName)

	them0, themErr0 := f(them)
	our0, ourErr0 := f(us)

	if errMismatch := diffErr(themErr0, ourErr0); errMismatch != nil {
		return errMismatch
	}

	opt := cmp.FilterPath(func(p cmp.Path) bool {
		last := p.Last().String()
		if last == ".TableArn" || last == ".ProvisionedThroughput" || last == ".CreationDateTime" || last == ".BillingModeSummary" {
			return true
		}
		return false
	}, cmp.Ignore())

	if diff := cmp.Diff(them0, our0, opt); diff != "" {
		return fmt.Errorf("diff: %s\n", diff)
	}

	return nil
}

type checkFunc func(db dynamodbiface.DynamoDBAPI) (interface{}, error)

func createTableNoThroughput(db dynamodbiface.DynamoDBAPI) (interface{}, error) {
	name := fmt.Sprintf("%s-perjuries-proclamation", ns)
	hashKey := "incentive-ranted"
	input := dynamodb.CreateTableInput{
		TableName: aws.String(name),
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

	return db.CreateTable(&input)
}

func createTablePayAsYouGo(db dynamodbiface.DynamoDBAPI) (interface{}, error) {
	name := fmt.Sprintf("%s-pay-as-you-go", ns)
	hashKey := "incentive-ranted"
	input := dynamodb.CreateTableInput{
		TableName: aws.String(name),
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

	return db.CreateTable(&input)
}

func diffErr(them, us error) error {
	if them == nil && us == nil {
		return nil
	}
	if (them == nil && us != nil) || (them != nil && us == nil) {
		return fmt.Errorf("err mismatch them=%q us=%q", them, us)
	}

	themReqErr, ok := them.(awserr.RequestFailure)
	if !ok {
		return fmt.Errorf("err mismatch them=%q us=%q", them, us)
	}
	usReqErr, ok := us.(awserr.RequestFailure)
	if !ok {
		return fmt.Errorf("err mismatch them=%q us=%q", them, us)
	}

	if themReqErr.Code() != usReqErr.Code() || themReqErr.Message() != usReqErr.Message() {
		return fmt.Errorf("err mismatch them_code=%s us_code=%s\nthem_msg=%q\nus_msg=%q",
			themReqErr.Code(), usReqErr.Code(), themReqErr.Message(), usReqErr.Message())
	}

	return nil
}
