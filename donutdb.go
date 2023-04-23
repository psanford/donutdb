package donutdb

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/dynamo"
	"github.com/psanford/donutdb/internal/schemav1"
	"github.com/psanford/donutdb/internal/schemav2"
	"github.com/psanford/donutdb/sectorcache"
	"github.com/psanford/sqlite3vfs"
)

func New(dynamoClient *dynamodb.DynamoDB, table string, opts ...Option) sqlite3vfs.VFS {
	options := options{
		sectorSize: dynamo.DefaultSectorSize,
	}
	for _, opt := range opts {
		err := opt.setOption(&options)
		if err != nil {
			panic(err)
		}
	}

	ownerIDBytes := make([]byte, 8)
	if _, err := rand.Read(ownerIDBytes); err != nil {
		panic(err)
	}
	v := vfs{
		db:                   dynamoClient,
		table:                table,
		ownerID:              hex.EncodeToString(ownerIDBytes),
		sectorSize:           options.sectorSize,
		sectorCache:          options.sectorCache,
		defaultSchemaVersion: 2,
	}

	if options.changeLogWriter != nil {
		v.changeLogWriter = json.NewEncoder(options.changeLogWriter)
	}

	if options.defaultSchemaVersion != 0 {
		v.defaultSchemaVersion = options.defaultSchemaVersion
	}

	return &v
}

type vfs struct {
	db                   *dynamodb.DynamoDB
	table                string
	ownerID              string
	defaultSchemaVersion int
	sectorCache          sectorcache.CacheV2

	sectorSize int64

	changeLogWriter *json.Encoder
}

func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (retFile sqlite3vfs.File, retFlag sqlite3vfs.OpenFlag, retErr error) {
	if v.changeLogWriter != nil {
		r := changeLogRecord{
			TS:       time.Now(),
			Action:   "OpenStart",
			ArgName:  name,
			ArgFlags: int(flags),
		}
		v.changeLogWriter.Encode(r)
		defer func() {
			r := changeLogRecord{
				TS:       time.Now(),
				Action:   "OpenComplete",
				ArgName:  name,
				ArgFlags: int(flags),
				RetError: retErr,
			}
			v.changeLogWriter.Encode(r)
		}()
	}

	meta := dynamo.FileMetaV1V2{
		MetaVersion: v.defaultSchemaVersion,
		OrigName:    name,
		SectorSize:  v.sectorSize,
		CompressAlg: "zstd",
	}

	// try in loop incase we a racing with another client.
	// give up if we fail 100 times in a row
	for i := 0; i < 100; i++ {
		existing, err := v.db.GetItem(&dynamodb.GetItemInput{
			TableName:            &v.table,
			ConsistentRead:       aws.Bool(true),
			ProjectionExpression: aws.String("#fname"),
			ExpressionAttributeNames: map[string]*string{
				"#fname": aws.String(name),
			},
			Key: map[string]*dynamodb.AttributeValue{
				dynamo.HKey: {
					S: aws.String(dynamo.FileMetaKey),
				},
				dynamo.RKey: {
					N: aws.String("0"),
				},
			},
		})

		if err != nil {
			return nil, 0, err
		}

		if len(existing.Item) == 0 {
			fileIDBytes := make([]byte, 20)
			rand.Read(fileIDBytes)
			if _, err := rand.Read(fileIDBytes); err != nil {
				panic(err)
			}

			meta.RandID = base64.URLEncoding.EncodeToString(fileIDBytes)
			if v.defaultSchemaVersion < 2 {
				meta.DataRowKey = dynamo.FileDataPrefix + meta.RandID + "-" + name
			}
			meta.LockRowKey = dynamo.FileLockPrefix + meta.RandID + "-" + name

			metaBytes, err := json.Marshal(meta)
			if err != nil {
				return nil, 0, err
			}

			_, err = v.db.UpdateItem(&dynamodb.UpdateItemInput{
				TableName:           &v.table,
				UpdateExpression:    aws.String("SET #fname=:meta"),
				ConditionExpression: aws.String("attribute_not_exists(#fname)"),
				Key: map[string]*dynamodb.AttributeValue{
					dynamo.HKey: {
						S: aws.String(dynamo.FileMetaKey),
					},
					dynamo.RKey: {
						N: aws.String("0"),
					},
				},
				ExpressionAttributeNames: map[string]*string{
					"#fname": aws.String(name),
				},
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":meta": {
						S: aws.String(string(metaBytes)),
					},
				},
			})

			if err != nil {
				if _, match := err.(*dynamodb.ConditionalCheckFailedException); match {
					// we raced with another client, retry
					continue
				}
				return nil, 0, err
			}

			f, err := v.FileFromMeta(&meta)
			if err != nil {
				return nil, 0, err
			}
			return f, flags, nil
		} else {
			err = json.Unmarshal([]byte(*existing.Item[name].S), &meta)
			if err != nil {
				return nil, 0, fmt.Errorf("decode file metadata err: %w", err)
			}

			f, err := v.FileFromMeta(&meta)
			if err != nil {
				return nil, 0, err
			}

			return f, flags, nil
		}
	}

	return nil, flags, errors.New("failed to get/create file metadata too many times due to races")
}

func (v *vfs) FileFromMeta(meta *dynamo.FileMetaV1V2) (sqlite3vfs.File, error) {
	if meta.MetaVersion == 0 || meta.MetaVersion == 1 {
		return schemav1.FileFromMeta(meta, v.table, v.ownerID, v.db, v.changeLogWriter)
	} else if meta.MetaVersion == 2 {
		return schemav2.FileFromMeta(meta, v.table, v.ownerID, v.db, v.changeLogWriter, v.sectorCache)
	}

	return nil, errors.New("Invalid schema version")

}

func (v *vfs) Delete(name string, dirSync bool) (retErr error) {
	if v.changeLogWriter != nil {
		r := changeLogRecord{
			TS:      time.Now(),
			Action:  "DeleteStart",
			ArgName: name,
		}
		v.changeLogWriter.Encode(r)
		defer func() {
			r := changeLogRecord{
				TS:       time.Now(),
				Action:   "DeleteComplete",
				ArgName:  name,
				RetError: retErr,
			}
			v.changeLogWriter.Encode(r)
		}()
	}

	existing, err := v.db.Query(&dynamodb.QueryInput{
		TableName:            &v.table,
		Limit:                aws.Int64(1),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#fname"),
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(name),
		},
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: aws.String(dynamo.FileMetaKey),
			},
			":rk": {
				N: aws.String("0"),
			},
		},
	})

	if err != nil {
		return err
	}

	if len(existing.Items) == 0 {
		return nil
	}

	metaBytes := *existing.Items[0][name].S

	var meta dynamo.FileMetaV1V2
	err = json.Unmarshal([]byte(metaBytes), &meta)
	if err != nil {
		return fmt.Errorf("unmarshal file meta v1 err: %w", err)
	}

	_, err = v.db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:           &v.table,
		UpdateExpression:    aws.String("REMOVE #fname"),
		ConditionExpression: aws.String("#fname=:meta"),
		Key: map[string]*dynamodb.AttributeValue{
			dynamo.HKey: {
				S: aws.String(dynamo.FileMetaKey),
			},
			dynamo.RKey: {
				N: aws.String("0"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(name),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":meta": {
				S: aws.String(metaBytes),
			},
		},
	})

	if err != nil {
		return err
	}

	f, err := v.FileFromMeta(&meta)
	if err != nil {
		return err
	}

	ff := f.(interface {
		CleanupSectors(*dynamo.FileMetaV1V2) error
	})

	go ff.CleanupSectors(&meta)
	return nil
}

func (v *vfs) Access(name string, flag sqlite3vfs.AccessFlag) (retOk bool, retErr error) {
	if v.changeLogWriter != nil {
		r := changeLogRecord{
			TS:      time.Now(),
			Action:  "AccessStart",
			ArgName: name,
		}
		v.changeLogWriter.Encode(r)
		defer func() {
			r := changeLogRecord{
				TS:       time.Now(),
				Action:   "AccessComplete",
				ArgName:  name,
				RetError: retErr,
			}
			v.changeLogWriter.Encode(r)
		}()
	}

	existing, err := v.db.Query(&dynamodb.QueryInput{
		TableName:            &v.table,
		Limit:                aws.Int64(1),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#fname"),
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(name),
		},
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: aws.String(dynamo.FileMetaKey),
			},
			":rk": {
				N: aws.String("0"),
			},
		},
	})

	if err != nil {
		return false, err
	}

	exists := len(existing.Items) > 0 && len(existing.Items[0]) > 0

	if flag == sqlite3vfs.AccessExists {
		return exists, nil
	}

	return true, nil
}

func (vfs *vfs) FullPathname(name string) string {
	name = filepath.Clean(string(filepath.Separator) + name)
	return name
}
