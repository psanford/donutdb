package donutdb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/psanford/donutdb/internal/donutsql"
)

type DonutDB struct {
	donutSQL *donutsql.DB
}

func (db *DonutDB) Dispatch(method string, body []byte) (interface{}, error) {

	if _, found := methodWhitelist[method]; !found {
		return nil, errors.New("Unsupported method")
	}

	methodV := reflect.ValueOf(db).MethodByName(method)
	if methodV.IsZero() {
		return nil, errors.New("Method signature doesn't exit?!")
	}

	typ := methodV.Type()
	arg0T := typ.In(0).Elem()

	arg0 := reflect.New(arg0T)

	err := json.Unmarshal(body, arg0.Interface())
	if err != nil {
		return nil, err
	}

	out := methodV.Call([]reflect.Value{arg0})
	ret0 := out[0].Interface()
	ret1 := out[1].Interface()
	if ret1 == nil {
		return ret0, nil
	}
	return ret0, ret1.(error)
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

var methodWhitelist = map[string]struct{}{
	"BatchExecuteStatement":               struct{}{},
	"BatchGetItem":                        struct{}{},
	"BatchWriteItem":                      struct{}{},
	"CreateBackup":                        struct{}{},
	"CreateGlobalTable":                   struct{}{},
	"CreateTable":                         struct{}{},
	"DeleteBackup":                        struct{}{},
	"DeleteItem":                          struct{}{},
	"DeleteTable":                         struct{}{},
	"DescribeBackup":                      struct{}{},
	"DescribeContinuousBackups":           struct{}{},
	"DescribeContributorInsights":         struct{}{},
	"DescribeEndpoints":                   struct{}{},
	"DescribeExport":                      struct{}{},
	"DescribeGlobalTable":                 struct{}{},
	"DescribeGlobalTableSettings":         struct{}{},
	"DescribeKinesisStreamingDestination": struct{}{},
	"DescribeLimits":                      struct{}{},
	"DescribeTable":                       struct{}{},
	"DescribeTableReplicaAutoScaling":     struct{}{},
	"DescribeTimeToLive":                  struct{}{},
	"DisableKinesisStreamingDestination":  struct{}{},
	"EnableKinesisStreamingDestination":   struct{}{},
	"ExecuteStatement":                    struct{}{},
	"ExecuteTransaction":                  struct{}{},
	"ExportTableToPointInTime":            struct{}{},
	"GetItem":                             struct{}{},
	"ListBackups":                         struct{}{},
	"ListContributorInsights":             struct{}{},
	"ListExports":                         struct{}{},
	"ListGlobalTables":                    struct{}{},
	"ListTables":                          struct{}{},
	"ListTagsOfResource":                  struct{}{},
	"PutItem":                             struct{}{},
	"Query":                               struct{}{},
	"RestoreTableFromBackup":              struct{}{},
	"RestoreTableToPointInTime":           struct{}{},
	"Scan":                                struct{}{},
	"TagResource":                         struct{}{},
	"TransactGetItems":                    struct{}{},
	"TransactWriteItems":                  struct{}{},
	"UntagResource":                       struct{}{},
	"UpdateContinuousBackups":             struct{}{},
	"UpdateContributorInsights":           struct{}{},
	"UpdateGlobalTable":                   struct{}{},
	"UpdateGlobalTableSettings":           struct{}{},
	"UpdateItem":                          struct{}{},
	"UpdateTable":                         struct{}{},
	"UpdateTableReplicaAutoScaling":       struct{}{},
	"UpdateTimeToLive":                    struct{}{},
}
