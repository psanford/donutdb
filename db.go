package donutdb

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (f *file) getSector(sectorOffset int64) (*sector, error) {
	rangeKeyStr := strconv.FormatInt(sectorOffset, 10)

	out, err := f.vfs.db.Query(&dynamodb.QueryInput{
		TableName:              &f.vfs.table,
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ProjectionExpression:   aws.String("bytes"),
		Limit:                  aws.Int64(1),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: &f.name,
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
		return nil, sectorNotFoundErr
	}

	item := out.Items[0]
	attr, ok := item["bytes"]
	if !ok {
		return nil, errors.New("no bytes attr found")
	}

	s := sector{
		offset: sectorOffset,
		data:   attr.B,
	}

	return &s, nil
}

func (f *file) writeSector(s *sector) error {
	rangeKeyStr := strconv.FormatInt(s.offset, 10)

	_, err := f.vfs.db.PutItem(&dynamodb.PutItemInput{
		TableName: &f.vfs.table,
		Item: map[string]*dynamodb.AttributeValue{
			hKey: {
				S: &f.name,
			},
			rKey: {
				N: &rangeKeyStr,
			},
			"bytes": {
				B: s.data,
			},
		},
	})

	return err
}

func (f *file) getLastSector() (*sector, error) {
	out, err := f.vfs.db.Query(&dynamodb.QueryInput{
		TableName:              &f.vfs.table,
		KeyConditionExpression: aws.String("hash_key = :hk"),
		ProjectionExpression:   aws.String("range_key, bytes"),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int64(1),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: &f.name,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if len(out.Items) == 0 {
		return nil, sectorNotFoundErr
	}

	item := out.Items[0]

	if item[rKey].N == nil {
		return nil, fmt.Errorf("range_key is not a number")
	}
	sectorOffset, err := strconv.ParseInt(*item[rKey].N, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("range_key does not parse to an int: %s %w", *item[rKey].N, err)
	}

	sectorData := item["bytes"].B

	return &sector{
		offset: sectorOffset,
		data:   sectorData,
	}, nil
}

func (f *file) getSectorRange(firstSector, lastSector int64) ([]sector, error) {
	startSector := firstSector
	endSector := lastSector

	if startSector == endSector {
		sect, err := f.getSector(firstSector)
		if err == sectorNotFoundErr {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return []sector{*sect}, nil
	}

	query := "hash_key = :hk AND range_key BETWEEN :first_sector AND :last_sector"
	var sectors []sector
	prevSectorOffset := firstSector - sectorSize

	for {
		startSectorStr := strconv.FormatInt(startSector, 10)
		endSectorStr := strconv.FormatInt(endSector, 10)

		out, err := f.vfs.db.Query(&dynamodb.QueryInput{
			TableName:              &f.vfs.table,
			KeyConditionExpression: &query,
			ProjectionExpression:   aws.String("range_key, bytes"),
			Limit:                  aws.Int64(1000),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":hk": {
					S: &f.name,
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
			if item[rKey].N == nil {
				return nil, fmt.Errorf("range_key is not a number")
			}
			sectorOffset, err := strconv.ParseInt(*item[rKey].N, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("range_key does not parse to an int: %s %w", *item[rKey].N, err)
			}

			if sectorOffset != prevSectorOffset+sectorSize {
				return nil, fmt.Errorf("Unexpected sector offset for range %d-%d, prev=%d got=%d expected=%d", firstSector, lastSector, prevSectorOffset, sectorOffset, prevSectorOffset+sectorSize)
			}

			sectorData := item["bytes"].B

			sectors = append(sectors, sector{
				offset: sectorOffset,
				data:   sectorData,
			})
			prevSectorOffset = sectorOffset
		}

		if len(out.Items) == 0 {
			break
		}

		end := sectors[len(sectors)-1]
		if end.offset == lastSector {
			break
		}

		startSector = end.offset + 1
	}

	return sectors, nil
}

var sectorNotFoundErr = errors.New("sector not found")

type sector struct {
	offset int64
	data   []byte
}
