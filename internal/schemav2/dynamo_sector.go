package schemav2

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/klauspost/compress/zstd"
	"github.com/psanford/donutdb/internal/dynamo"
)

var zstdDecoder, _ = zstd.NewReader(nil)
var uncompressFunc = func(in []byte, sectorSize int64) ([]byte, error) {
	sectorData := make([]byte, 0, sectorSize)
	return zstdDecoder.DecodeAll(in, sectorData)
}

func (f *File) getSectors(sectorIDs []string) ([]Sector, error) {
	sectors := make(map[string]Sector)

	keys := make([]map[string]*dynamodb.AttributeValue, 0, len(sectorIDs))
	for _, sectorID := range sectorIDs {
		key := "file-v2-" + f.randID + "-" + f.rawName + "-" + sectorID
		keys = append(keys, map[string]*dynamodb.AttributeValue{
			dynamo.HKey: {
				S: &key,
			},
			dynamo.RKey: {
				N: aws.String("0"),
			},
		})
	}

	for len(keys) > 0 {
		var batchKeys []map[string]*dynamodb.AttributeValue
		if len(keys) > 100 {
			batchKeys = keys[:100]
			keys = keys[100:]
		} else {
			batchKeys = keys
			keys = nil
		}

		fieldsToFetch := strings.Join([]string{"bytes", dynamo.HKey}, ",")

		args := &dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				f.table: {
					ProjectionExpression: &fieldsToFetch,
					Keys:                 batchKeys,
				},
			},
		}

		out, err := f.db.BatchGetItem(args)

		if err != nil {
			return nil, err
		}

		for _, item := range out.Responses[f.table] {
			fullID := item[dynamo.HKey].S
			parts := strings.Split(*fullID, "-")
			sectorID := parts[len(parts)-1]

			compressedSectorData := item["bytes"].B

			sectorData, err := uncompressFunc(compressedSectorData, f.sectorSize)
			if err != nil {
				panic(err)
			}

			sector := Sector{
				Data:  sectorData,
				Valid: true,
			}
			sectors[sectorID] = sector
		}
		if len(out.UnprocessedKeys) > 0 {
			keys = append(keys, out.UnprocessedKeys[f.table].Keys...)
		}
	}

	out := make([]Sector, len(sectorIDs))
	for i, sectorID := range sectorIDs {
		sector := sectors[sectorID]
		sector.ID = sectorID
		out[i] = sector
	}

	return out, nil
}
