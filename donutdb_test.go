package donutdb

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	_ "github.com/mattn/go-sqlite3"
)

// prove we implement the dynamodbiface
var _ dynamodbiface.DynamoDBAPI = (*DonutDB)(nil)

func TestDonutDB(t *testing.T) {
	db := DonutDB{}
	_ = db
}
