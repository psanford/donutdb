package schemav1

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/klauspost/compress/zstd"
	"github.com/psanford/donutdb/internal/dynamo"
)

var decoder, _ = zstd.NewReader(nil)

func (f *File) getSector(sectorOffset int64) (*Sector, error) {
	rangeKeyStr := strconv.FormatInt(sectorOffset, 10)

	out, err := f.db.Query(&dynamodb.QueryInput{
		TableName:              &f.table,
		ConsistentRead:         aws.Bool(false),
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ProjectionExpression:   aws.String("bytes"),
		Limit:                  aws.Int64(1),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: &f.dataRowKey,
			},
			":rk": {
				N: &rangeKeyStr,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if len(out.Items) == 0 {
		return nil, dynamo.SectorNotFoundErr
	}

	item := out.Items[0]
	attr, ok := item["bytes"]
	if !ok {
		return nil, errors.New("no bytes attr found")
	}

	compressedSectorData := attr.B

	sectorData := make([]byte, 0, f.sectorSize)
	sectorData, err = decoder.DecodeAll(compressedSectorData, sectorData)
	if err != nil {
		panic(err)
	}

	s := Sector{
		Offset: sectorOffset,
		Data:   sectorData,
	}

	return &s, nil
}

func (f *File) getLastSector() (*Sector, error) {
	out, err := f.db.Query(&dynamodb.QueryInput{
		TableName:              &f.table,
		ConsistentRead:         aws.Bool(false),
		KeyConditionExpression: aws.String("hash_key = :hk"),
		ProjectionExpression:   aws.String("range_key, bytes"),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int64(1),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: &f.dataRowKey,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if len(out.Items) == 0 {
		return nil, dynamo.SectorNotFoundErr
	}

	item := out.Items[0]

	if item[dynamo.RKey].N == nil {
		return nil, fmt.Errorf("range_key is not a number")
	}
	sectorOffset, err := strconv.ParseInt(*item[dynamo.RKey].N, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("range_key does not parse to an int: %s %w", *item[dynamo.RKey].N, err)
	}

	compressedSectorData := item["bytes"].B

	sectorData := make([]byte, 0, f.sectorSize)
	sectorData, err = decoder.DecodeAll(compressedSectorData, sectorData)
	if err != nil {
		panic(err)
	}

	return &Sector{
		Offset: sectorOffset,
		Data:   sectorData,
	}, nil
}

func (f *File) getSectorRange(firstSector, lastSector int64) ([]Sector, error) {
	startSector := firstSector
	endSector := lastSector

	if startSector == endSector {
		sect, err := f.getSector(firstSector)
		if err != nil {
			return nil, err
		}
		return []Sector{*sect}, nil
	}

	query := "hash_key = :hk AND range_key BETWEEN :first_sector AND :last_sector"
	var sectors []Sector
	prevSectorOffset := firstSector - f.sectorSize

	for {
		startSectorStr := strconv.FormatInt(startSector, 10)
		endSectorStr := strconv.FormatInt(endSector, 10)

		out, err := f.db.Query(&dynamodb.QueryInput{
			ConsistentRead:         aws.Bool(false),
			TableName:              &f.table,
			KeyConditionExpression: &query,
			ProjectionExpression:   aws.String("range_key, bytes"),
			Limit:                  aws.Int64(1000),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":hk": {
					S: &f.dataRowKey,
				},
				":first_sector": {
					N: &startSectorStr,
				},
				":last_sector": {
					N: &endSectorStr,
				},
			},
		})

		if err != nil {
			return nil, err
		}

		for _, item := range out.Items {
			if item[dynamo.RKey].N == nil {
				return nil, fmt.Errorf("range_key is not a number")
			}
			sectorOffset, err := strconv.ParseInt(*item[dynamo.RKey].N, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("range_key does not parse to an int: %s %w", *item[dynamo.RKey].N, err)
			}

			if sectorOffset != prevSectorOffset+f.sectorSize {
				return nil, fmt.Errorf("Unexpected sector offset for range %d-%d, prev=%d got=%d expected=%d", firstSector, lastSector, prevSectorOffset, sectorOffset, prevSectorOffset+f.sectorSize)
			}

			compressedSectorData := item["bytes"].B

			sectorData := make([]byte, 0, f.sectorSize)
			sectorData, err = decoder.DecodeAll(compressedSectorData, sectorData)
			if err != nil {
				panic(err)
			}

			sectors = append(sectors, Sector{
				Offset: sectorOffset,
				Data:   sectorData,
			})
			prevSectorOffset = sectorOffset
		}

		if len(out.Items) == 0 {
			break
		}

		end := sectors[len(sectors)-1]
		if end.Offset == lastSector {
			break
		}

		startSector = end.Offset + 1
	}

	return sectors, nil
}

type Sector struct {
	Offset int64
	Data   []byte
}
